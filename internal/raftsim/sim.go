// Package raftsim runs a whole StreamForge Raft group inside one process on
// a simulated clock and a simulated network. Message delays, loss,
// partitions and crashes are drawn from a seeded RNG, so every run is
// reproducible from its seed.
//
// It drives the real raft.Core and the real store.SM exactly the way the
// production node driver does (persist, send, apply), with "disk" being a
// per-node struct that survives crashes. It is used by the randomized
// safety and linearizability tests and by the in-browser demo.
package raftsim

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"

	"github.com/sohamb17/streamforge/internal/raft"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

// Config for a simulated cluster.
type Config struct {
	N             int
	Seed          int64
	ElectionTick  int
	HeartbeatTick int
	MinDelay      int // ticks
	MaxDelay      int // ticks
	DropRate      float64
	SnapshotEvery uint64 // entries between local snapshots (0 = never)
}

// DefaultConfig mirrors the production timing ratios.
func DefaultConfig(seed int64) Config {
	return Config{N: 5, Seed: seed, ElectionTick: 10, HeartbeatTick: 2, MinDelay: 1, MaxDelay: 2, SnapshotEvery: 100}
}

type disk struct {
	hs      raft.HardState
	snap    *raft.Snapshot
	entries []raft.Entry
}

type proposal struct {
	term uint64
	done func(res store.Result, ok bool)
}

type pendingRead struct {
	index    uint64
	hasIndex bool
	done     func(ok bool)
}

// Node is one simulated server.
type Node struct {
	ID    uint64
	Up    bool
	Core  *raft.Core
	SM    *store.SM
	disk  disk
	props map[uint64]proposal
	reads map[string]*pendingRead
	// Last observed soft state, for the visualization.
	Role raft.Role
	Lead uint64
}

// MsgEvent describes one message for visualizations.
type MsgEvent struct {
	Msg       raft.Message
	SentAt    int
	DeliverAt int
	Dropped   bool
}

type inflight struct {
	m  raft.Message
	at int
}

// Cluster is a simulated Raft group.
type Cluster struct {
	cfg      Config
	rng      *rand.Rand
	Now      int
	Nodes    map[uint64]*Node
	ids      []uint64
	inflight []inflight
	blocked  map[[2]uint64]bool

	// OnMessage is called for every message sent (including dropped ones).
	OnMessage func(MsgEvent)

	leaders    map[uint64]uint64 // term -> leader
	appliedLog map[uint64]uint64 // index -> hash(term, data)
	Violations []string
	readSeq    uint64
}

// New creates a cluster with all nodes up.
func New(cfg Config) *Cluster {
	c := &Cluster{
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(cfg.Seed)),
		Nodes:      map[uint64]*Node{},
		blocked:    map[[2]uint64]bool{},
		leaders:    map[uint64]uint64{},
		appliedLog: map[uint64]uint64{},
	}
	for i := 1; i <= cfg.N; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	for _, id := range c.ids {
		n := &Node{ID: id}
		c.Nodes[id] = n
		c.start(n)
	}
	return c
}

// IDs returns node ids in order.
func (c *Cluster) IDs() []uint64 { return c.ids }

func (c *Cluster) start(n *Node) {
	n.Up = true
	n.props = map[uint64]proposal{}
	n.reads = map[string]*pendingRead{}
	n.SM = store.New(window.DefaultBucketMs)
	if n.disk.snap != nil {
		if err := n.SM.Restore(n.disk.snap.Data); err != nil {
			panic(err)
		}
	}
	n.Core = raft.NewCore(raft.Config{
		ID: n.ID, Peers: c.ids,
		ElectionTick: c.cfg.ElectionTick, HeartbeatTick: c.cfg.HeartbeatTick,
		PreVote: true, CheckQuorum: true,
		Rand: rand.New(rand.NewSource(c.rng.Int63())),
	}, raft.Storage{HardState: n.disk.hs, Snapshot: n.disk.snap, Entries: append([]raft.Entry(nil), n.disk.entries...)})
	n.Role, n.Lead = raft.Follower, raft.None
}

// Crash stops a node. Its disk survives; everything in memory is lost.
func (c *Cluster) Crash(id uint64) {
	n := c.Nodes[id]
	if !n.Up {
		return
	}
	n.Up = false
	for _, p := range n.props {
		p.done(store.Result{}, false)
	}
	for _, r := range n.reads {
		r.done(false)
	}
	n.props, n.reads = nil, nil
}

// Restart brings a crashed node back from its disk.
func (c *Cluster) Restart(id uint64) {
	if n := c.Nodes[id]; !n.Up {
		c.start(n)
	}
}

// Partition splits the cluster into the given groups. Nodes not listed are
// isolated from everyone.
func (c *Cluster) Partition(groups ...[]uint64) {
	c.blocked = map[[2]uint64]bool{}
	group := map[uint64]int{}
	for i, g := range groups {
		for _, id := range g {
			group[id] = i + 1
		}
	}
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a != b && (group[a] == 0 || group[a] != group[b]) {
				c.blocked[[2]uint64{a, b}] = true
			}
		}
	}
}

// Heal removes all partitions.
func (c *Cluster) Heal() { c.blocked = map[[2]uint64]bool{} }

// Blocked reports whether a->b is cut.
func (c *Cluster) Blocked(a, b uint64) bool { return c.blocked[[2]uint64{a, b}] }

// SetDropRate changes random message loss.
func (c *Cluster) SetDropRate(r float64) { c.cfg.DropRate = r }

// Leader returns the leader with the highest term among live nodes, or 0.
func (c *Cluster) Leader() uint64 {
	var best, term uint64
	for _, id := range c.ids {
		n := c.Nodes[id]
		if !n.Up {
			continue
		}
		st := n.Core.Status()
		if st.Role == raft.Leader && st.Term >= term {
			best, term = id, st.Term
		}
	}
	return best
}

// Step advances the simulation by one tick: every live node ticks, then all
// messages due by now are delivered.
func (c *Cluster) Step() {
	c.Now++
	for _, id := range c.ids {
		if n := c.Nodes[id]; n.Up {
			n.Core.Tick()
			c.process(n)
		}
	}
	// Deliver due messages. Delivery can produce new messages with a delay
	// of at least one tick, so a single pass suffices.
	var due []inflight
	rest := c.inflight[:0]
	for _, f := range c.inflight {
		if f.at <= c.Now {
			due = append(due, f)
		} else {
			rest = append(rest, f)
		}
	}
	c.inflight = rest
	sort.SliceStable(due, func(i, j int) bool { return due[i].at < due[j].at })
	for _, f := range due {
		n := c.Nodes[f.m.To]
		if !n.Up || c.blocked[[2]uint64{f.m.From, f.m.To}] {
			continue
		}
		n.Core.Step(f.m)
		c.process(n)
	}
	c.checkLeaders()
}

func (c *Cluster) send(m raft.Message) {
	ev := MsgEvent{Msg: m, SentAt: c.Now}
	if c.blocked[[2]uint64{m.From, m.To}] || c.rng.Float64() < c.cfg.DropRate {
		ev.Dropped = true
	} else {
		d := c.cfg.MinDelay
		if c.cfg.MaxDelay > c.cfg.MinDelay {
			d += c.rng.Intn(c.cfg.MaxDelay - c.cfg.MinDelay + 1)
		}
		ev.DeliverAt = c.Now + d
		c.inflight = append(c.inflight, inflight{m: m, at: ev.DeliverAt})
	}
	if c.OnMessage != nil {
		c.OnMessage(ev)
	}
}

// process emulates the node driver: persist, send, apply, serve reads.
func (c *Cluster) process(n *Node) {
	for n.Up && n.Core.HasReady() {
		rd := n.Core.Ready()
		if rd.SoftState != nil {
			if n.Role == raft.Leader && rd.SoftState.Role != raft.Leader {
				for idx, p := range n.props {
					p.done(store.Result{}, false) // outcome unknown
					delete(n.props, idx)
				}
			}
			n.Role, n.Lead = rd.SoftState.Role, rd.SoftState.Lead
		}
		// 1. persist
		if rd.Snapshot != nil {
			n.disk.snap = rd.Snapshot
			n.disk.entries = nil
		}
		if rd.HardState != nil {
			n.disk.hs = *rd.HardState
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			kept := n.disk.entries[:0]
			for _, e := range n.disk.entries {
				if e.Index < first {
					kept = append(kept, e)
				}
			}
			n.disk.entries = append(kept, rd.Entries...)
		}
		// 2. send
		for _, m := range rd.Messages {
			c.send(m)
		}
		// 3. apply
		if rd.Snapshot != nil {
			if err := n.SM.Restore(rd.Snapshot.Data); err != nil {
				panic(err)
			}
		}
		for _, e := range rd.CommittedEntries {
			res := n.SM.Apply(e)
			c.checkApplied(n.ID, e)
			if p, ok := n.props[e.Index]; ok {
				delete(n.props, e.Index)
				p.done(res, p.term == e.Term)
			}
		}
		// 4. reads
		for _, rs := range rd.ReadStates {
			if r, ok := n.reads[string(rs.Context)]; ok {
				r.index, r.hasIndex = rs.Index, true
			}
		}
		for _, ctx := range rd.ReadsDropped {
			if r, ok := n.reads[string(ctx)]; ok {
				delete(n.reads, string(ctx))
				r.done(false)
			}
		}
		for k, r := range n.reads {
			if r.hasIndex && n.SM.AppliedIndex() >= r.index {
				delete(n.reads, k)
				r.done(true)
			}
		}
		// Local snapshot policy.
		if c.cfg.SnapshotEvery > 0 {
			var snapIdx uint64
			if n.disk.snap != nil {
				snapIdx = n.disk.snap.Index
			}
			if ap := n.SM.AppliedIndex(); ap-snapIdx >= c.cfg.SnapshotEvery {
				data, err := n.SM.Snapshot()
				if err != nil {
					panic(err)
				}
				if err := n.Core.Compact(ap, data); err == nil {
					t, _ := termOf(n, ap)
					n.disk.snap = &raft.Snapshot{Index: ap, Term: t, Data: data}
					kept := n.disk.entries[:0]
					for _, e := range n.disk.entries {
						if e.Index > ap {
							kept = append(kept, e)
						}
					}
					n.disk.entries = kept
				}
			}
		}
	}
}

func termOf(n *Node, idx uint64) (uint64, bool) { return n.Core.Term(idx) }

func entryHash(e raft.Entry) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%d/", e.Term, e.Type)
	h.Write(e.Data)
	return h.Sum64()
}

func (c *Cluster) checkApplied(id uint64, e raft.Entry) {
	h := entryHash(e)
	if prev, ok := c.appliedLog[e.Index]; ok && prev != h {
		c.Violations = append(c.Violations, fmt.Sprintf("tick %d: node %d applied a different entry at index %d", c.Now, id, e.Index))
	}
	c.appliedLog[e.Index] = h
}

func (c *Cluster) checkLeaders() {
	for _, id := range c.ids {
		n := c.Nodes[id]
		if !n.Up {
			continue
		}
		st := n.Core.Status()
		if st.Role != raft.Leader {
			continue
		}
		if l, ok := c.leaders[st.Term]; ok && l != id {
			c.Violations = append(c.Violations, fmt.Sprintf("tick %d: two leaders in term %d: %d and %d", c.Now, st.Term, l, id))
		}
		c.leaders[st.Term] = id
	}
}

// Propose submits data at node id. done is called once with ok=true if the
// entry was committed and applied at that node, or ok=false if the outcome
// is unknown (leadership lost, crash, overwritten).
func (c *Cluster) Propose(id uint64, data []byte, done func(store.Result, bool)) error {
	n := c.Nodes[id]
	if !n.Up {
		return fmt.Errorf("node %d down", id)
	}
	idx, term, err := n.Core.Propose(data)
	if err != nil {
		return err
	}
	n.props[idx] = proposal{term: term, done: done}
	c.process(n)
	return nil
}

// Read performs a ReadIndex read at node id; done(true) means the node's
// state machine may now be read linearizably.
func (c *Cluster) Read(id uint64, done func(ok bool)) error {
	n := c.Nodes[id]
	if !n.Up {
		return fmt.Errorf("node %d down", id)
	}
	c.readSeq++
	ctx := []byte(fmt.Sprintf("r%d", c.readSeq))
	if err := n.Core.ReadIndex(ctx); err != nil {
		return err
	}
	n.reads[string(ctx)] = &pendingRead{done: done}
	c.process(n)
	return nil
}

// Converged reports whether all live nodes have applied the same prefix up
// to the same commit index.
func (c *Cluster) Converged() bool {
	var commit uint64
	first := true
	for _, id := range c.ids {
		n := c.Nodes[id]
		if !n.Up {
			continue
		}
		st := n.Core.Status()
		if first {
			commit, first = st.Commit, false
		} else if st.Commit != commit {
			return false
		}
		if n.SM.AppliedIndex() != st.Commit {
			return false
		}
	}
	return true
}

// SnapshotsEqual reports whether every live node's state machine serializes
// to the same features and partition state. (Byte equality is not required:
// maps encode in random order.)
func (c *Cluster) StatesEqual() bool {
	var ref []byte
	for _, id := range c.ids {
		n := c.Nodes[id]
		if !n.Up {
			continue
		}
		b := []byte(fmt.Sprint(n.SM.Features(nil), n.SM.PartitionOffsets()))
		if ref == nil {
			ref = b
		} else if !bytes.Equal(ref, b) {
			return false
		}
	}
	return true
}
