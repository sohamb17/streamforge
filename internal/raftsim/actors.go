package raftsim

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/anishathalye/porcupine"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/linz"
	"github.com/sohamb17/streamforge/internal/raft"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

// Clock orders client events more finely than ticks, for histories.
type Clock struct{ T int64 }

// Now returns a strictly increasing timestamp.
func (c *Clock) Now() int64 { c.T++; return c.T }

type clientOp struct {
	in       linz.Input
	call     int64
	deadline int
	waiting  bool
	retryAt  int
}

// Client issues register operations against the cluster, follows leader
// hints, retries with the same sequence number, and records a history for
// Porcupine. One operation is outstanding at a time.
type Client struct {
	ID        int
	Name      string
	Keys      int
	KeyPrefix string
	seq       uint64
	guess     uint64
	cur       *clientOp
	History   []porcupine.Operation
}

// Idle reports whether no operation is in flight.
func (cl *Client) Idle() bool { return cl.cur == nil }

// Tick advances the client by one tick. active=false finishes the current
// operation without starting a new one.
func (cl *Client) Tick(c *Cluster, rng *rand.Rand, clk *Clock, active bool) {
	keys := cl.Keys
	if keys == 0 {
		keys = 3
	}
	if cl.cur == nil {
		if !active || rng.Intn(3) != 0 {
			return
		}
		key := fmt.Sprintf("%sk%d", cl.KeyPrefix, rng.Intn(keys))
		in := linz.Input{Op: linz.OpGet, Key: key}
		if rng.Intn(2) == 0 {
			cl.seq++
			in = linz.Input{Op: linz.OpPut, Key: key, Value: fmt.Sprintf("%s-%d", cl.Name, cl.seq)}
		}
		cl.cur = &clientOp{in: in, call: clk.Now(), deadline: c.Now + 80}
	}
	op := cl.cur
	if op.waiting || c.Now < op.retryAt {
		return
	}
	if c.Now > op.deadline {
		if op.in.Op == linz.OpPut {
			cl.record(op, linz.Output{Unknown: true}, linz.Infinity)
		}
		cl.cur = nil
		return
	}
	if cl.guess == 0 || !c.Nodes[cl.guess].Up {
		cl.guess = c.ids[rng.Intn(len(c.ids))]
	}
	target := cl.guess
	var err error
	op.waiting = true
	if op.in.Op == linz.OpPut {
		cmd := &sfv1.Command{ClientId: cl.Name, Seq: cl.seq, Op: &sfv1.Command_Put{Put: &sfv1.PutOp{Key: op.in.Key, Value: op.in.Value}}}
		data, _ := store.EncodeCommand(cmd)
		err = c.Propose(target, data, func(_ store.Result, ok bool) {
			op.waiting = false
			if ok {
				cl.record(op, linz.Output{}, clk.Now())
				cl.cur = nil
			} else {
				op.retryAt = c.Now + 1 // unknown outcome: retry with the same seq
			}
		})
	} else {
		err = c.Read(target, func(ok bool) {
			op.waiting = false
			if ok {
				v, found := c.Nodes[target].SM.Get(op.in.Key)
				cl.record(op, linz.Output{Value: v, Found: found}, clk.Now())
				cl.cur = nil
			} else {
				op.retryAt = c.Now + 1
			}
		})
	}
	if err != nil {
		op.waiting = false
		op.retryAt = c.Now + 1
		var nl *raft.NotLeaderError
		if errors.As(err, &nl) && nl.Lead != 0 {
			cl.guess = nl.Lead
		} else {
			cl.guess = 0
		}
	}
}

func (cl *Client) record(op *clientOp, out linz.Output, ret int64) {
	cl.History = append(cl.History, porcupine.Operation{ClientId: cl.ID, Input: op.in, Call: op.call, Output: out, Return: ret})
}

// Worker emulates a stream worker for one partition: it windows events
// from Log, proposes batches with their offsets, and on any unknown outcome
// or crash restores from the store (linearizable read) and resumes from the
// stored offset, as cmd/worker does.
type Worker struct {
	Part      int32
	Log       *[]window.Event // append-only partition log (offset = index)
	ClientID  string
	PerTick   int
	CrashRate int // 1-in-N chance per tick of a process crash (0 = never)

	p         *window.Partition
	next      int
	inflight  *window.Batch
	last      *window.Batch
	waiting   bool
	restoring bool
	guess     uint64
	seq       uint64

	Restores       int
	BatchesApplied int
	BatchesDup     int
	LastFreshTicks int
}

// NewWorker creates a worker with empty state.
func NewWorker(part int32, log *[]window.Event, perTick int) *Worker {
	return &Worker{Part: part, Log: log, PerTick: perTick, ClientID: fmt.Sprintf("worker-p%d", part), p: window.NewPartition(part, window.DefaultConfig())}
}

// Done reports whether everything in the log is committed or consumed.
func (w *Worker) Done() bool {
	return w.next >= len(*w.Log) && w.inflight == nil && !w.restoring && !w.waiting
}

// Crash drops all in-memory state; the next tick restores from the store.
func (w *Worker) Crash() {
	w.inflight = nil
	w.restoring = true
}

// Redeliver re-proposes the last committed batch (a lost ack) and rewinds
// consumption by n offsets. Both must be ignored by the store/window.
func (w *Worker) Redeliver(c *Cluster, n int) {
	if w.last != nil && !w.waiting && w.inflight == nil {
		b := *w.last
		w.inflight = &b
	}
	w.next -= n
	if w.next < 0 {
		w.next = 0
	}
}

// Offset returns the last offset the window has consumed.
func (w *Worker) Offset() int64 { return w.p.ToOffset() }

// Tick advances the worker.
func (w *Worker) Tick(c *Cluster, rng *rand.Rand) {
	if w.waiting {
		return
	}
	if w.guess == 0 || !c.Nodes[w.guess].Up {
		w.guess = c.ids[rng.Intn(len(c.ids))]
	}
	target := w.guess
	onErr := func(err error) {
		var nl *raft.NotLeaderError
		if errors.As(err, &nl) && nl.Lead != 0 {
			w.guess = nl.Lead
		} else {
			w.guess = 0
		}
	}
	if w.restoring {
		w.waiting = true
		if err := c.Read(target, func(ok bool) {
			w.waiting = false
			if ok {
				st := c.Nodes[target].SM.PartitionState(w.Part)
				w.p = window.Restore(st, window.DefaultConfig())
				w.next = int(st.ToOffset) + 1
				w.restoring = false
				w.Restores++
			}
		}); err != nil {
			w.waiting = false
			onErr(err)
		}
		return
	}
	if w.CrashRate > 0 && rng.Intn(w.CrashRate) == 0 {
		w.Crash()
		return
	}
	if w.inflight == nil {
		log := *w.Log
		for i := 0; i < w.PerTick && w.next < len(log); i++ {
			w.p.Add(log[w.next]) // offsets at or below the window's are ignored
			w.next++
			if w.p.HasOutput() {
				b := w.p.Flush()
				w.inflight = &b
				break
			}
		}
		if w.inflight == nil {
			return
		}
	}
	w.seq++
	cmd := &sfv1.Command{ClientId: w.ClientID, Seq: w.seq, Op: &sfv1.Command_Batch{Batch: store.BatchToProto(*w.inflight)}}
	data, _ := store.EncodeCommand(cmd)
	w.waiting = true
	sent := w.inflight
	if err := c.Propose(target, data, func(res store.Result, ok bool) {
		w.waiting = false
		switch {
		case ok:
			if res.Applied {
				w.BatchesApplied++
			} else {
				w.BatchesDup++
			}
			w.last = sent
			w.inflight = nil
		case rng.Intn(2) == 0:
			// Unknown outcome: blindly re-propose the same batch. If the
			// first attempt did commit, the store must ignore this one.
			w.guess = 0
		default:
			// Or: do not guess, rebuild from what the store says.
			w.inflight = nil
			w.restoring = true
		}
	}); err != nil {
		w.waiting = false
		onErr(err)
	}
}
