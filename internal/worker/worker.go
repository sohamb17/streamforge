// Package worker is the stream worker: it consumes trip events from Kafka,
// windows them per partition, and commits each batch of outputs together
// with the window state and the Kafka offset that produced them, in one
// Raft proposal.
//
// Delivery semantics, in short:
//   - Kafka delivers at least once (after a crash, a rebalance, a seek).
//   - On assignment the worker resumes from the offset stored in the online
//     store, not from Kafka's committed offset. Kafka commits are only for
//     lag monitoring.
//   - The window ignores events at or below its offset; the store ignores
//     batches at or below its stored offset. Processing is deterministic, so
//     a zombie worker produces the same batch as its replacement.
//
// Together that is at-least-once delivery with idempotent application,
// i.e. effectively-once effects on the online store. It is not end-to-end
// exactly-once: the offline history and the cache are outside the Raft
// transaction (see HistorySink and Invalidator).
package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/event"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

// Store is the subset of storeclient.Client the worker needs.
type Store interface {
	ProposeBatch(ctx context.Context, b *sfv1.FeatureBatch) (bool, error)
	PartitionState(ctx context.Context, p int32) (*sfv1.PartitionState, error)
}

// HistorySink appends closed-window rows to the offline history. It must be
// idempotent on (zone, window_end): it is called before the Raft proposal,
// and again with the same rows if the worker recomputes the batch.
type HistorySink interface {
	Write(ctx context.Context, partition int32, toOffset int64, rows []window.Features) error
}

// Invalidator drops cache entries for zones whose features changed. Best
// effort: a missed invalidation is bounded by the cache TTL.
type Invalidator interface {
	Invalidate(ctx context.Context, zones []int32)
}

// Config configures a Worker.
type Config struct {
	Topic         string
	Store         Store
	History       HistorySink // optional
	Cache         Invalidator // optional
	Window        window.Config
	ProposeTimout time.Duration
	Log           *slog.Logger
}

// Worker owns the partition processors of one consumer-group member.
type Worker struct {
	cfg Config
	cl  *kgo.Client

	mu    sync.Mutex
	procs map[int32]*proc
}

// New creates a worker. Call Options() and pass them to kgo.NewClient, then
// Run with that client.
func New(cfg Config) *Worker {
	if cfg.ProposeTimout == 0 {
		cfg.ProposeTimout = 20 * time.Second
	}
	return &Worker{cfg: cfg, procs: map[int32]*proc{}}
}

// Options returns the consumer-group options that wire the worker in.
func (w *Worker) Options(group string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(w.cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(10 * time.Second),
		kgo.HeartbeatInterval(time.Second),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AdjustFetchOffsetsFn(w.adjustOffsets),
		kgo.OnPartitionsAssigned(w.onAssigned),
		kgo.OnPartitionsRevoked(w.onRevoked),
		kgo.OnPartitionsLost(w.onRevoked),
		kgo.FetchMaxBytes(16 << 20),
	}
}

// adjustOffsets runs after onAssigned and before fetching starts. It points
// each newly assigned partition right after the offset stored in the online
// store, which onAssigned already loaded. Kafka's own committed offset is
// ignored on purpose.
func (w *Worker) adjustOffsets(ctx context.Context, offs map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	for topic, parts := range offs {
		for p := range parts {
			w.mu.Lock()
			pr := w.procs[p]
			w.mu.Unlock()
			var next int64 = -1
			if pr != nil {
				next = pr.startOffset
			} else {
				st, err := w.loadState(ctx, p)
				if err != nil {
					return nil, err
				}
				next = st.ToOffset + 1
			}
			if next > 0 {
				parts[p] = kgo.NewOffset().At(next).WithEpoch(-1)
			} else {
				parts[p] = kgo.NewOffset().AtStart()
			}
		}
		offs[topic] = parts
	}
	return offs, nil
}

func (w *Worker) loadState(ctx context.Context, p int32) (*sfv1.PartitionState, error) {
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		st, err := w.cfg.Store.PartitionState(cctx, p)
		cancel()
		if err == nil {
			return st, nil
		}
		w.cfg.Log.Warn("read partition state failed; retrying", "partition", p, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// onAssigned restores each newly assigned partition from the store (a
// linearizable read of its offset and window state) before any record of it
// is fetched.
func (w *Worker) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for _, parts := range assigned {
		for _, p := range parts {
			st, err := w.loadState(ctx, p)
			if err != nil {
				return // client closing
			}
			pr := newProc(w, p, st)
			w.mu.Lock()
			if old, ok := w.procs[p]; ok {
				w.mu.Unlock()
				old.stop()
				w.mu.Lock()
			} else {
				mAssigned.Inc()
			}
			w.procs[p] = pr
			w.mu.Unlock()
			go pr.run()
			w.cfg.Log.Info("resuming partition from store", "partition", p, "offset", st.ToOffset+1,
				"events", st.EventsTotal, "late_dropped", st.LateDroppedTotal)
		}
	}
}

func (w *Worker) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	w.mu.Lock()
	var stopping []*proc
	for _, parts := range revoked {
		for _, p := range parts {
			if pr, ok := w.procs[p]; ok {
				stopping = append(stopping, pr)
				delete(w.procs, p)
				mAssigned.Dec()
			}
		}
	}
	w.mu.Unlock()
	for _, pr := range stopping {
		pr.stop()
	}
}

// Run polls Kafka until ctx is done.
func (w *Worker) Run(ctx context.Context, cl *kgo.Client) error {
	w.cl = cl
	go w.commitLoop(ctx)
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, 20000)
		fetches.EachError(func(t string, p int32, err error) {
			if ctx.Err() == nil {
				w.cfg.Log.Warn("fetch error", "partition", p, "err", err)
			}
		})
		fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
			w.mu.Lock()
			pr := w.procs[fp.Partition]
			w.mu.Unlock()
			if pr == nil || len(fp.Records) == 0 {
				return
			}
			pr.hw.Store(fp.HighWatermark)
			select {
			case pr.in <- fp.Records:
			case <-pr.stopc:
			case <-ctx.Done():
			}
		})
	}
	w.mu.Lock()
	procs := w.procs
	w.procs = map[int32]*proc{}
	w.mu.Unlock()
	for _, pr := range procs {
		pr.stop()
	}
	return ctx.Err()
}

// commitLoop commits store-acknowledged offsets to Kafka for lag dashboards.
// These commits are never read back by StreamForge.
func (w *Worker) commitLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		offs := map[int32]kgo.EpochOffset{}
		w.mu.Lock()
		for p, pr := range w.procs {
			if o := pr.acked.Load(); o >= 0 {
				offs[p] = kgo.EpochOffset{Epoch: -1, Offset: o + 1}
			}
			if hw := pr.hw.Load(); hw > 0 {
				mLag.WithLabelValues(itoa(p)).Set(float64(hw - 1 - pr.consumed.Load()))
			}
		}
		w.mu.Unlock()
		if len(offs) > 0 {
			w.cl.CommitOffsets(ctx, map[string]map[int32]kgo.EpochOffset{w.cfg.Topic: offs}, nil)
		}
	}
}

// Redeliver is a fault-injection hook for one partition: it re-sends the
// last committed batch to the store (simulating a lost acknowledgement) and
// rewinds the Kafka position by n offsets (simulating redelivery). Both
// must be ignored. It returns false if the partition is not assigned here.
func (w *Worker) Redeliver(p int32, n int64) bool {
	w.mu.Lock()
	pr := w.procs[p]
	w.mu.Unlock()
	if pr == nil {
		return false
	}
	select {
	case pr.redeliverc <- n:
		return true
	default:
		return false
	}
}

// Assigned returns the partitions this worker owns.
func (w *Worker) Assigned() []int32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]int32, 0, len(w.procs))
	for p := range w.procs {
		out = append(out, p)
	}
	return out
}

type commitResult struct {
	batch   *sfv1.FeatureBatch
	applied bool
	err     error
}

// proc processes one partition.
type proc struct {
	w    *Worker
	part int32
	log  *slog.Logger

	win         *window.Partition
	startOffset int64 // where fetching starts after assignment
	expect      int64 // next offset to accept; -1 = accept the first record seen
	gapSince    time.Time
	inflight    bool
	last        *sfv1.FeatureBatch
	in          chan []*kgo.Record
	done        chan commitResult
	redeliverc  chan int64
	stopc       chan struct{}
	stopped     chan struct{}
	once        sync.Once

	acked    atomic.Int64
	consumed atomic.Int64
	hw       atomic.Int64
}

func newProc(w *Worker, p int32, st *sfv1.PartitionState) *proc {
	pr := &proc{
		w: w, part: p, log: w.cfg.Log.With("partition", p),
		in: make(chan []*kgo.Record, 64), done: make(chan commitResult, 1), redeliverc: make(chan int64, 1),
		stopc: make(chan struct{}), stopped: make(chan struct{}),
	}
	pr.reset(st)
	return pr
}

func (pr *proc) reset(st *sfv1.PartitionState) {
	pr.win = window.Restore(store.StateFromProto(st), pr.w.cfg.Window)
	pr.expect = st.ToOffset + 1
	pr.startOffset = pr.expect
	if st.ToOffset < 0 {
		pr.expect = -1
	}
	pr.acked.Store(st.ToOffset)
	pr.consumed.Store(st.ToOffset)
	pr.gapSince = time.Time{}
}

func (pr *proc) stop() {
	pr.once.Do(func() { close(pr.stopc) })
	<-pr.stopped
}

func (pr *proc) run() {
	defer close(pr.stopped)
	pl := itoa(pr.part)
	for {
		select {
		case <-pr.stopc:
			return
		case recs := <-pr.in:
			for _, r := range recs {
				pr.consume(r, pl)
			}
		case res := <-pr.done:
			pr.inflight = false
			if res.err != nil {
				pr.log.Warn("commit failed; restoring from store", "err", res.err)
				pr.restore()
				continue
			}
			pr.last = res.batch
			pr.acked.Store(res.batch.ToOffset)
		case n := <-pr.redeliverc:
			pr.redeliver(n)
		}
		if !pr.inflight && pr.win.HasOutput() {
			b := store.BatchToProto(pr.win.Flush())
			pr.inflight = true
			go pr.commit(b)
		}
	}
}

func (pr *proc) consume(r *kgo.Record, pl string) {
	if pr.expect >= 0 && r.Offset != pr.expect {
		if r.Offset < pr.expect {
			mSkipped.WithLabelValues(pl, "duplicate").Inc()
			return
		}
		// A gap means records from before a seek are still arriving; wait
		// for the seek to take effect. If the gap persists the data was
		// deleted by retention: accept it loudly rather than stall forever.
		if pr.gapSince.IsZero() {
			pr.gapSince = time.Now()
		}
		if time.Since(pr.gapSince) < 10*time.Second {
			mSkipped.WithLabelValues(pl, "gap").Inc()
			return
		}
		pr.log.Error("offset gap persisted; accepting (data lost to retention?)", "expected", pr.expect, "got", r.Offset)
		mSkipped.WithLabelValues(pl, "gap_accepted").Inc()
	}
	pr.gapSince = time.Time{}
	pr.expect = r.Offset + 1
	t, err := event.Decode(r.Value)
	if err != nil {
		mSkipped.WithLabelValues(pl, "malformed").Inc()
		return
	}
	if !pr.win.Add(window.Event{
		Zone: t.Zone, EventTimeMs: t.EventTimeMs, FareCents: t.FareCents,
		DistanceMilli: t.DistanceMilli, Offset: r.Offset, PublishMs: t.PublishMs,
	}) {
		// Already reflected in the window state (redelivery after a rewind).
		mSkipped.WithLabelValues(pl, "duplicate").Inc()
		return
	}
	pr.consumed.Store(r.Offset)
	mEvents.WithLabelValues(pl).Inc()
	mLateDropped.WithLabelValues(pl).Set(float64(pr.win.LateDropped()))
	mWatermark.WithLabelValues(pl).Set(float64(pr.win.Watermark()))
}

// commit writes the history rows, then proposes the batch. Runs on its own
// goroutine; at most one per partition is in flight.
func (pr *proc) commit(b *sfv1.FeatureBatch) {
	ctx, cancel := context.WithTimeout(context.Background(), pr.w.cfg.ProposeTimout)
	defer cancel()
	pl := itoa(pr.part)
	if h := pr.w.cfg.History; h != nil && len(b.Features) > 0 {
		rows := make([]window.Features, len(b.Features))
		for i, f := range b.Features {
			rows[i] = store.FeaturesFromProto(f)
		}
		if err := h.Write(ctx, pr.part, b.ToOffset, rows); err != nil {
			pr.done <- commitResult{err: err}
			return
		}
	}
	t0 := time.Now()
	applied, err := pr.w.cfg.Store.ProposeBatch(ctx, b)
	if err != nil {
		mBatches.WithLabelValues(pl, "error").Inc()
		pr.done <- commitResult{err: err}
		return
	}
	mProposeLatency.Observe(time.Since(t0).Seconds())
	if applied {
		mBatches.WithLabelValues(pl, "applied").Inc()
	} else {
		mBatches.WithLabelValues(pl, "duplicate").Inc()
	}
	if b.TriggerPublishMs > 0 && len(b.Features) > 0 {
		mFreshness.Observe(float64(time.Now().UnixMilli()-b.TriggerPublishMs) / 1000)
	}
	mRows.Add(float64(len(b.Features)))
	if c := pr.w.cfg.Cache; c != nil && len(b.Features) > 0 {
		zones := make([]int32, 0, len(b.Features))
		seen := map[int32]bool{}
		for _, f := range b.Features {
			if !seen[f.Zone] {
				seen[f.Zone] = true
				zones = append(zones, f.Zone)
			}
		}
		c.Invalidate(ctx, zones)
	}
	pr.done <- commitResult{batch: b, applied: applied}
}

// restore throws away in-memory state and resumes from the store, exactly
// like a fresh process would.
func (pr *proc) restore() {
	mRestores.WithLabelValues(itoa(pr.part)).Inc()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-pr.stopc:
			cancel()
		case <-ctx.Done():
		}
	}()
	st, err := pr.w.loadState(ctx, pr.part)
	cancel()
	if err != nil {
		return // stopping
	}
	pr.reset(st)
	pr.w.seek(pr.part, st.ToOffset+1)
	pr.log.Info("restored from store", "offset", st.ToOffset)
}

func (pr *proc) redeliver(n int64) {
	if pr.last != nil {
		// Simulate a lost acknowledgement: send the same batch again.
		go func(b *sfv1.FeatureBatch) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			applied, err := pr.w.cfg.Store.ProposeBatch(ctx, b)
			if err == nil && !applied {
				mBatches.WithLabelValues(itoa(pr.part), "duplicate").Inc()
			}
			pr.log.Info("redelivered last batch", "offset", b.ToOffset, "applied_again", applied, "err", err)
		}(pr.last)
	}
	// Simulate Kafka redelivery: rewind the fetch position. Records at or
	// below the window's offset are skipped by the offset guard.
	to := pr.win.ToOffset() + 1 - n
	if to < 0 {
		to = 0
	}
	pr.expect = to
	pr.w.seek(pr.part, to)
	pr.log.Info("rewound kafka position for redelivery", "to", to)
}

func (w *Worker) seek(p int32, offset int64) {
	if w.cl == nil {
		return
	}
	w.cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{w.cfg.Topic: {p: {Epoch: -1, Offset: offset}}})
}
