// Package raftnode runs a raft.Core as a real server: it owns the timers,
// the disk (DiskStorage) and the network (Transport), and it applies
// committed entries to the store state machine.
//
// All Core and state-machine mutation happens on one goroutine (run). Other
// goroutines talk to it through channels, and read the state machine under
// a read lock after a linearizable barrier when they need one.
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sohamb17/streamforge/internal/raft"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

// Errors returned to callers.
var (
	// ErrUnknownOutcome: the proposal was accepted by this node as leader,
	// but leadership was lost before it committed here. It may still commit
	// under the next leader. Retry with the same client sequence number.
	ErrUnknownOutcome = errors.New("raftnode: outcome unknown (leadership lost)")
	// ErrDropped: a different entry was committed at the proposal's index,
	// so the proposal definitely did not commit.
	ErrDropped = errors.New("raftnode: proposal dropped")
	ErrStopped = errors.New("raftnode: stopped")
)

// Transport sends Raft messages to peers.
type Transport interface {
	Send(msgs []raft.Message)
}

// Config configures a Node.
type Config struct {
	ID            uint64
	Peers         []uint64
	Dir           string
	Fsync         bool
	TickInterval  time.Duration
	ElectionTick  int
	HeartbeatTick int
	SnapshotEvery uint64
	Transport     Transport
	Logger        *slog.Logger
}

type propReq struct {
	data  []byte
	start time.Time
	resp  chan propResp
}

type propResp struct {
	res store.Result
	err error
}

type pendingProp struct {
	term  uint64
	start time.Time
	resp  chan propResp
}

type readWaiter struct {
	start time.Time
	ch    chan error
}

type pendingRead struct {
	index    uint64
	hasIndex bool
	waiters  []readWaiter
}

// Node is a running Raft server with the store state machine.
type Node struct {
	cfg     Config
	log     *slog.Logger
	core    *raft.Core
	storage *DiskStorage
	smMu    sync.RWMutex
	sm      *store.SM

	recvc chan raft.Message
	propc chan propReq
	readc chan readWaiter
	fnc   chan func()
	stopc chan struct{}
	donec chan struct{}

	props     map[uint64]pendingProp
	reads     map[string]*pendingRead
	readBatch []readWaiter
	readSeq   uint64
	lastHS    raft.HardState
	snapIndex uint64

	role      atomic.Int32
	lead      atomic.Uint64
	startedAt time.Time

	elections          uint64
	snapshotsTaken     atomic.Uint64
	snapshotsInstalled atomic.Uint64
}

// Start recovers state from disk and starts the node.
func Start(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ds, st, err := OpenDiskStorage(cfg.Dir, cfg.Fsync)
	if err != nil {
		return nil, err
	}
	sm := store.New(window.DefaultBucketMs)
	var snapIndex uint64
	if st.Snapshot != nil {
		if err := sm.Restore(st.Snapshot.Data); err != nil {
			return nil, err
		}
		snapIndex = st.Snapshot.Index
	}
	core := raft.NewCore(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		PreVote: true, CheckQuorum: true,
		Rand: rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID))),
	}, st)
	n := &Node{
		cfg: cfg, log: cfg.Logger.With("node", cfg.ID), core: core, storage: ds, sm: sm,
		recvc: make(chan raft.Message, 4096), propc: make(chan propReq, 4096),
		readc: make(chan readWaiter, 4096), fnc: make(chan func(), 64),
		stopc: make(chan struct{}), donec: make(chan struct{}),
		props: map[uint64]pendingProp{}, reads: map[string]*pendingRead{},
		lastHS: st.HardState, snapIndex: snapIndex, startedAt: time.Now(),
	}
	n.log.Info("recovered", "term", st.HardState.Term, "commit", st.HardState.Commit,
		"snapshot", snapIndex, "entries", len(st.Entries))
	go n.run()
	return n, nil
}

// Stop halts the node.
func (n *Node) Stop() {
	select {
	case <-n.stopc:
	default:
		close(n.stopc)
	}
	<-n.donec
	n.storage.Close()
}

// Step delivers a message from a peer. It never blocks: if the inbox is
// full the message is dropped, which Raft tolerates.
func (n *Node) Step(m raft.Message) {
	select {
	case n.recvc <- m:
	default:
		mMsgsDropped.WithLabelValues("inbox_full").Inc()
	}
}

// IsLeader reports the last known role.
func (n *Node) IsLeader() bool { return raft.Role(n.role.Load()) == raft.Leader }

// Leader returns the last known leader id (0 if unknown).
func (n *Node) Leader() uint64 { return n.lead.Load() }

// Propose replicates data and waits until it is applied on this node.
func (n *Node) Propose(ctx context.Context, data []byte) (store.Result, error) {
	if !n.IsLeader() {
		return store.Result{}, &raft.NotLeaderError{Lead: n.Leader()}
	}
	req := propReq{data: data, start: time.Now(), resp: make(chan propResp, 1)}
	select {
	case n.propc <- req:
	case <-ctx.Done():
		return store.Result{}, ctx.Err()
	case <-n.stopc:
		return store.Result{}, ErrStopped
	}
	select {
	case r := <-req.resp:
		return r.res, r.err
	case <-ctx.Done():
		// The entry may still commit; the caller's retry is deduplicated.
		return store.Result{}, ctx.Err()
	case <-n.stopc:
		return store.Result{}, ErrStopped
	}
}

// LinearizableRead returns once this node's state machine reflects every
// write committed before the call (ReadIndex). Only the leader serves it.
func (n *Node) LinearizableRead(ctx context.Context) error {
	if !n.IsLeader() {
		return &raft.NotLeaderError{Lead: n.Leader()}
	}
	w := readWaiter{start: time.Now(), ch: make(chan error, 1)}
	select {
	case n.readc <- w:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopc:
		return ErrStopped
	}
}

// View runs f with shared access to the state machine.
func (n *Node) View(f func(sm *store.SM)) {
	n.smMu.RLock()
	defer n.smMu.RUnlock()
	f(n.sm)
}

// Status is a consistent view of Raft internals.
type Status struct {
	raft.Status
	SentTo             map[uint64]uint64
	PartitionOffsets   map[int32]int64
	DuplicateBatches   uint64
	StartedAt          time.Time
	SnapshotsTaken     uint64
	SnapshotsInstalled uint64
	Fingerprint        string
}

// Status queries the run loop.
func (n *Node) Status(ctx context.Context) (Status, error) { return n.status(ctx, false) }

// StatusWithFingerprint also hashes the state machine (O(state size)).
func (n *Node) StatusWithFingerprint(ctx context.Context) (Status, error) { return n.status(ctx, true) }

func (n *Node) status(ctx context.Context, withFingerprint bool) (Status, error) {
	ch := make(chan Status, 1)
	f := func() {
		s := Status{Status: n.core.Status(), SentTo: map[uint64]uint64{}, StartedAt: n.startedAt,
			SnapshotsTaken: n.snapshotsTaken.Load(), SnapshotsInstalled: n.snapshotsInstalled.Load()}
		for k, v := range n.core.SentTo {
			s.SentTo[k] = v
		}
		s.PartitionOffsets = n.sm.PartitionOffsets()
		s.DuplicateBatches = n.sm.DuplicateBatches
		if withFingerprint {
			s.Fingerprint = n.sm.Fingerprint()
		}
		ch <- s
	}
	select {
	case n.fnc <- f:
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-n.stopc:
		return Status{}, ErrStopped
	}
	select {
	case s := <-ch:
		return s, nil
	case <-ctx.Done():
		return Status{}, ctx.Err()
	}
}

func (n *Node) run() {
	defer close(n.donec)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.core.Tick()
		case m := <-n.recvc:
			n.core.Step(m)
			for i := 0; i < 512; i++ {
				select {
				case m := <-n.recvc:
					n.core.Step(m)
					continue
				default:
				}
				break
			}
		case p := <-n.propc:
			n.propose(p)
			// Group commit: take everything already queued into one Ready.
			for i := 0; i < 1024; i++ {
				select {
				case p := <-n.propc:
					n.propose(p)
					continue
				default:
				}
				break
			}
		case r := <-n.readc:
			n.readBatch = append(n.readBatch, r)
			for i := 0; i < 1024; i++ {
				select {
				case r := <-n.readc:
					n.readBatch = append(n.readBatch, r)
					continue
				default:
				}
				break
			}
		case f := <-n.fnc:
			f()
		case <-n.stopc:
			n.failAll(ErrStopped)
			return
		}
		n.flushReads()
		if err := n.processReady(); err != nil {
			// A storage failure is fatal: continuing could acknowledge
			// writes that are not durable.
			n.log.Error("fatal storage error", "err", err)
			panic(err)
		}
	}
}

func (n *Node) propose(p propReq) {
	idx, term, err := n.core.Propose(p.data)
	if err != nil {
		mProposals.WithLabelValues("not_leader").Inc()
		p.resp <- propResp{err: err}
		return
	}
	n.props[idx] = pendingProp{term: term, start: p.start, resp: p.resp}
}

// flushReads issues one ReadIndex for every read queued since the last
// round, so a burst of reads costs one heartbeat round, not one each.
func (n *Node) flushReads() {
	if len(n.readBatch) == 0 {
		return
	}
	batch := n.readBatch
	n.readBatch = nil
	n.readSeq++
	ctx := strconv.AppendUint([]byte("r"), n.readSeq, 10)
	if err := n.core.ReadIndex(ctx); err != nil {
		for _, w := range batch {
			w.ch <- err
		}
		return
	}
	n.reads[string(ctx)] = &pendingRead{waiters: batch}
}

func (n *Node) failAll(err error) {
	for idx, p := range n.props {
		p.resp <- propResp{err: err}
		delete(n.props, idx)
	}
	for k, r := range n.reads {
		for _, w := range r.waiters {
			w.ch <- err
		}
		delete(n.reads, k)
	}
}

func (n *Node) processReady() error {
	for n.core.HasReady() {
		rd := n.core.Ready()
		if ss := rd.SoftState; ss != nil {
			wasLeader := raft.Role(n.role.Load()) == raft.Leader
			n.role.Store(int32(ss.Role))
			n.lead.Store(ss.Lead)
			if wasLeader && ss.Role != raft.Leader {
				for idx, p := range n.props {
					mProposals.WithLabelValues("unknown").Inc()
					p.resp <- propResp{err: ErrUnknownOutcome}
					delete(n.props, idx)
				}
			}
			if ss.Role == raft.Leader {
				mIsLeader.Set(1)
			} else {
				mIsLeader.Set(0)
			}
			if e := n.core.ElectionsStarted; e > n.elections {
				mElections.Add(float64(e - n.elections))
				n.elections = e
			}
			n.log.Info("role change", "role", ss.Role.String(), "leader", ss.Lead, "term", n.core.Status().Term)
		}

		// 1. Persist.
		t0 := time.Now()
		if rd.Snapshot != nil {
			hs := n.lastHS
			if rd.HardState != nil {
				hs = *rd.HardState
			}
			if err := n.storage.SaveSnapshot(rd.Snapshot, hs, nil); err != nil {
				return err
			}
			n.snapIndex = rd.Snapshot.Index
		}
		termOrVote := rd.HardState != nil && (rd.HardState.Term != n.lastHS.Term || rd.HardState.Vote != n.lastHS.Vote)
		if rd.HardState != nil || len(rd.Entries) > 0 {
			if err := n.storage.Save(rd.HardState, rd.Entries, termOrVote); err != nil {
				return err
			}
			mFsync.Observe(time.Since(t0).Seconds())
		}
		if rd.HardState != nil {
			n.lastHS = *rd.HardState
			mTerm.Set(float64(rd.HardState.Term))
			mCommit.Set(float64(rd.HardState.Commit))
		}

		// 2. Send.
		if len(rd.Messages) > 0 {
			for _, m := range rd.Messages {
				mMsgsSent.WithLabelValues(m.Type.String()).Inc()
			}
			n.cfg.Transport.Send(rd.Messages)
		}

		// 3. Apply.
		if rd.Snapshot != nil || len(rd.CommittedEntries) > 0 {
			n.smMu.Lock()
			if rd.Snapshot != nil {
				if err := n.sm.Restore(rd.Snapshot.Data); err != nil {
					n.smMu.Unlock()
					return fmt.Errorf("install snapshot: %w", err)
				}
				n.snapshotsInstalled.Add(1)
				mSnapshots.WithLabelValues("installed").Inc()
				n.log.Info("installed snapshot from leader", "index", rd.Snapshot.Index)
			}
			results := make([]store.Result, len(rd.CommittedEntries))
			for i, e := range rd.CommittedEntries {
				results[i] = n.sm.Apply(e)
			}
			n.smMu.Unlock()
			mApplyBatch.Observe(float64(len(rd.CommittedEntries)))
			for i, e := range rd.CommittedEntries {
				if p, ok := n.props[e.Index]; ok {
					delete(n.props, e.Index)
					if p.term == e.Term {
						mProposals.WithLabelValues("committed").Inc()
						mCommitLatency.Observe(time.Since(p.start).Seconds())
						p.resp <- propResp{res: results[i]}
					} else {
						mProposals.WithLabelValues("dropped").Inc()
						p.resp <- propResp{err: ErrDropped}
					}
				}
			}
			mApplied.Set(float64(n.sm.AppliedIndex()))
		}

		// 4. Reads.
		for _, rs := range rd.ReadStates {
			if r, ok := n.reads[string(rs.Context)]; ok {
				r.index, r.hasIndex = rs.Index, true
			}
		}
		for _, ctx := range rd.ReadsDropped {
			if r, ok := n.reads[string(ctx)]; ok {
				delete(n.reads, string(ctx))
				for _, w := range r.waiters {
					w.ch <- &raft.NotLeaderError{Lead: n.lead.Load()}
				}
			}
		}
		applied := n.sm.AppliedIndex()
		for k, r := range n.reads {
			if r.hasIndex && applied >= r.index {
				delete(n.reads, k)
				for _, w := range r.waiters {
					mReadIndexLatency.Observe(time.Since(w.start).Seconds())
					w.ch <- nil
				}
			}
		}

		// Local snapshot policy.
		if n.cfg.SnapshotEvery > 0 && applied-n.snapIndex >= n.cfg.SnapshotEvery {
			if err := n.snapshot(applied); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Node) snapshot(applied uint64) error {
	n.smMu.RLock()
	data, err := n.sm.Snapshot()
	n.smMu.RUnlock()
	if err != nil {
		return err
	}
	if err := n.core.Compact(applied, data); err != nil {
		return err
	}
	if err := n.storage.SaveSnapshot(n.core.Snapshot(), n.lastHS, n.core.EntriesFrom(applied+1)); err != nil {
		return err
	}
	n.snapIndex = applied
	n.snapshotsTaken.Add(1)
	mSnapshots.WithLabelValues("taken").Inc()
	return nil
}
