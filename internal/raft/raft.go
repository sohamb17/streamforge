package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
)

// Config configures a Core.
type Config struct {
	ID    uint64
	Peers []uint64 // all members, including ID
	// ElectionTick is the minimum election timeout in ticks; the actual
	// timeout is drawn uniformly from [ElectionTick, 2*ElectionTick).
	ElectionTick  int
	HeartbeatTick int
	// PreVote makes a node check that it could win before disrupting the
	// cluster with a higher term (thesis section 9.6).
	PreVote bool
	// CheckQuorum makes a leader step down when it has not heard from a
	// majority for an election timeout, and makes followers that heard from
	// a leader recently ignore vote requests (thesis section 6.2).
	CheckQuorum bool
	// MaxEntriesPerMsg caps entries per AppendEntries message (0 = 256).
	MaxEntriesPerMsg int
	// MaxApplyBatch caps entries per Ready.CommittedEntries (0 = 1024).
	MaxApplyBatch int
	// Rand seeds the randomized election timeout. Required for determinism
	// in simulation; defaults to a time-independent seed of the node id.
	Rand *rand.Rand
}

// Storage is what a restarting node recovered from disk.
type Storage struct {
	HardState HardState
	Snapshot  *Snapshot // may be nil
	Entries   []Entry   // entries after the snapshot, in order
}

type progress struct {
	match, next    uint64
	recentActive   bool
	probing        bool // after a rejection: send one App per heartbeat until a match
	snapPending    uint64
	snapSentAtTick int
}

type readRequest struct {
	index uint64
	ctx   []byte
	acks  map[uint64]bool
}

// Core is the Raft state machine of one node.
type Core struct {
	id    uint64
	peers []uint64
	cfg   Config
	rng   *rand.Rand

	term uint64
	vote uint64
	lead uint64
	role Role
	log  raftLog

	prs   map[uint64]*progress
	votes map[uint64]bool

	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int
	ticks                     int

	snapshot *Snapshot // latest local snapshot, with data, for lagging followers

	pendingBcast bool
	readQueue    []*readRequest // ReadIndex requests awaiting a quorum
	readBlocked  [][]byte       // ReadIndex requests waiting for the first commit of the term

	// output buffers
	msgs         []Message
	readStates   []ReadState
	readsDropped [][]byte
	pendingSnap  *Snapshot
	prevHS       HardState
	prevSS       SoftState
	ssReported   bool

	// counters for observability
	ElectionsStarted uint64
	SentTo           map[uint64]uint64
}

// NewCore creates a core, restoring persisted state if any.
func NewCore(cfg Config, st Storage) *Core {
	if cfg.ElectionTick <= cfg.HeartbeatTick || cfg.HeartbeatTick <= 0 {
		panic("raft: ElectionTick must exceed HeartbeatTick > 0")
	}
	if cfg.MaxEntriesPerMsg == 0 {
		cfg.MaxEntriesPerMsg = 256
	}
	if cfg.MaxApplyBatch == 0 {
		cfg.MaxApplyBatch = 1024
	}
	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(int64(cfg.ID) * 7919))
	}
	peers := append([]uint64(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	c := &Core{id: cfg.ID, peers: peers, cfg: cfg, rng: rng, SentTo: map[uint64]uint64{}}
	if st.Snapshot != nil {
		c.log.restore(st.Snapshot)
		c.snapshot = st.Snapshot
	}
	c.log.append(st.Entries...)
	c.log.stable = c.log.lastIndex()
	c.term, c.vote = st.HardState.Term, st.HardState.Vote
	if st.HardState.Commit > c.log.committed {
		c.log.commitTo(st.HardState.Commit)
	}
	c.prevHS = c.hardState()
	c.becomeFollower(c.term, None)
	return c
}

func (c *Core) hardState() HardState {
	return HardState{Term: c.term, Vote: c.vote, Commit: c.log.committed}
}

func (c *Core) quorum() int { return len(c.peers)/2 + 1 }

// ---------------------------------------------------------------- roles

func (c *Core) reset(term uint64) {
	if c.term != term {
		c.term = term
		c.vote = None
	}
	c.lead = None
	c.electionElapsed = 0
	c.heartbeatElapsed = 0
	c.randomizedElectionTimeout = c.cfg.ElectionTick + c.rng.Intn(c.cfg.ElectionTick)
	c.votes = map[uint64]bool{}
	c.pendingBcast = false
	c.dropReads()
}

func (c *Core) dropReads() {
	for _, r := range c.readQueue {
		c.readsDropped = append(c.readsDropped, r.ctx)
	}
	c.readsDropped = append(c.readsDropped, c.readBlocked...)
	c.readQueue, c.readBlocked = nil, nil
}

func (c *Core) becomeFollower(term, lead uint64) {
	c.reset(term)
	c.role = Follower
	c.lead = lead
	c.prs = nil
}

func (c *Core) becomePreCandidate() {
	// PreVote does not change the term or the vote; it only asks.
	c.role = PreCandidate
	c.lead = None
	c.votes = map[uint64]bool{}
	c.electionElapsed = 0
	c.randomizedElectionTimeout = c.cfg.ElectionTick + c.rng.Intn(c.cfg.ElectionTick)
	c.dropReads()
}

func (c *Core) becomeCandidate() {
	c.reset(c.term + 1)
	c.role = Candidate
	c.vote = c.id
	c.ElectionsStarted++
}

func (c *Core) becomeLeader() {
	c.reset(c.term)
	c.role = Leader
	c.lead = c.id
	c.prs = map[uint64]*progress{}
	last := c.log.lastIndex()
	for _, p := range c.peers {
		c.prs[p] = &progress{next: last + 1, probing: true, recentActive: true}
	}
	// Commit a no-op of the new term: entries from earlier terms are only
	// committed indirectly, once an entry of the current term commits.
	c.appendEntry(Entry{Type: EntryNoop})
	c.bcastAppend()
}

func (c *Core) campaign(pre bool) {
	if c.quorum() == 1 {
		c.becomeCandidate()
		c.becomeLeader()
		return
	}
	var t MsgType
	var term uint64
	if pre {
		c.becomePreCandidate()
		t, term = MsgPreVote, c.term+1
	} else {
		c.becomeCandidate()
		t, term = MsgVote, c.term
	}
	c.votes[c.id] = true
	for _, p := range c.peers {
		if p == c.id {
			continue
		}
		c.send(Message{Type: t, To: p, Term: term, Index: c.log.lastIndex(), LogTerm: c.log.lastTerm()})
	}
}

// ---------------------------------------------------------------- time

// Tick advances logical time by one tick.
func (c *Core) Tick() {
	c.ticks++
	if c.role == Leader {
		c.tickLeader()
		return
	}
	c.electionElapsed++
	if c.electionElapsed >= c.randomizedElectionTimeout {
		c.electionElapsed = 0
		c.campaign(c.cfg.PreVote)
	}
}

func (c *Core) tickLeader() {
	c.heartbeatElapsed++
	c.electionElapsed++
	if c.electionElapsed >= c.cfg.ElectionTick {
		c.electionElapsed = 0
		if c.cfg.CheckQuorum {
			active := 0
			for id, pr := range c.prs {
				if id == c.id || pr.recentActive {
					active++
				}
				pr.recentActive = false
			}
			if active < c.quorum() {
				// Cannot reach a majority: stop accepting writes and reads
				// so clients go find the real leader.
				c.becomeFollower(c.term, None)
				return
			}
		}
	}
	if c.heartbeatElapsed >= c.cfg.HeartbeatTick {
		c.heartbeatElapsed = 0
		// Re-attach the newest pending read: if the heartbeat round that
		// carried it was lost, this one confirms it (and all older reads).
		var ctx []byte
		if n := len(c.readQueue); n > 0 {
			ctx = c.readQueue[n-1].ctx
		}
		c.bcastHeartbeat(ctx)
	}
}

// ---------------------------------------------------------------- input

// Step processes a message received from a peer.
func (c *Core) Step(m Message) {
	switch {
	case m.Term > c.term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			inLease := c.cfg.CheckQuorum && c.lead != None && c.electionElapsed < c.cfg.ElectionTick
			if inLease {
				// We heard from a live leader recently. A server that wants
				// a higher term is probably partitioned or slow; ignore it.
				return
			}
		}
		switch {
		case m.Type == MsgPreVote:
			// Never change term because of a PreVote.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A granted pre-vote carries the term we would campaign in.
		default:
			lead := None
			if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
				lead = m.From
			}
			c.becomeFollower(m.Term, lead)
		}
	case m.Term < c.term:
		if (c.cfg.CheckQuorum || c.cfg.PreVote) && (m.Type == MsgHeartbeat || m.Type == MsgApp) {
			// A stale leader. Tell it our term so it steps down.
			c.send(Message{To: m.From, Type: MsgAppResp})
		} else if m.Type == MsgPreVote {
			c.send(Message{To: m.From, Type: MsgPreVoteResp, Term: c.term, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgVote, MsgPreVote:
		canVote := c.vote == m.From ||
			(c.vote == None && c.lead == None) ||
			(m.Type == MsgPreVote && m.Term > c.term)
		if canVote && c.log.isUpToDate(m.Index, m.LogTerm) {
			c.send(Message{To: m.From, Type: voteResp(m.Type), Term: m.Term})
			if m.Type == MsgVote {
				c.electionElapsed = 0
				c.vote = m.From
			}
		} else {
			c.send(Message{To: m.From, Type: voteResp(m.Type), Term: c.term, Reject: true})
		}
		return
	}

	switch c.role {
	case Leader:
		c.stepLeader(m)
	case Candidate, PreCandidate:
		c.stepCandidate(m)
	default:
		c.stepFollower(m)
	}
}

func voteResp(t MsgType) MsgType {
	if t == MsgVote {
		return MsgVoteResp
	}
	return MsgPreVoteResp
}

func (c *Core) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		// Someone already won this term.
		c.becomeFollower(m.Term, m.From)
		c.stepFollower(m)
	case MsgVoteResp, MsgPreVoteResp:
		if (c.role == Candidate) != (m.Type == MsgVoteResp) {
			return // stale response from the other phase
		}
		c.votes[m.From] = !m.Reject
		granted, rejected := 0, 0
		for _, v := range c.votes {
			if v {
				granted++
			} else {
				rejected++
			}
		}
		switch {
		case granted >= c.quorum():
			if c.role == PreCandidate {
				c.campaign(false)
			} else {
				c.becomeLeader()
			}
		case rejected >= c.quorum():
			c.becomeFollower(c.term, None)
		}
	}
}

func (c *Core) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		c.electionElapsed = 0
		c.lead = m.From
		c.handleAppend(m)
	case MsgHeartbeat:
		c.electionElapsed = 0
		c.lead = m.From
		if m.Commit > c.log.committed {
			c.log.commitTo(min(m.Commit, c.log.lastIndex()))
		}
		c.send(Message{To: m.From, Type: MsgHeartbeatResp, Context: m.Context})
	case MsgSnap:
		c.electionElapsed = 0
		c.lead = m.From
		c.handleSnapshot(m)
	}
}

func (c *Core) handleAppend(m Message) {
	if m.Index < c.log.committed {
		// Everything up to our commit index is known to match.
		c.send(Message{To: m.From, Type: MsgAppResp, Index: c.log.committed})
		return
	}
	if c.log.matchTerm(m.Index, m.LogTerm) {
		lastNew := c.log.maybeAppend(m.Index, m.Entries)
		c.log.commitTo(min(m.Commit, lastNew))
		c.send(Message{To: m.From, Type: MsgAppResp, Index: lastNew})
		return
	}
	// Reject, hinting where the leader should retry: skip the whole
	// conflicting term instead of backing up one entry per round trip.
	hint := min(m.Index, c.log.lastIndex())
	if t, ok := c.log.term(hint); ok && hint == m.Index {
		for hint > c.log.committed {
			pt, ok := c.log.term(hint - 1)
			if !ok || pt != t {
				break
			}
			hint--
		}
		hint--
	}
	c.send(Message{To: m.From, Type: MsgAppResp, Index: m.Index, Reject: true, RejectHint: hint})
}

func (c *Core) handleSnapshot(m Message) {
	s := m.Snapshot
	if s.Index <= c.log.committed {
		c.send(Message{To: m.From, Type: MsgAppResp, Index: c.log.committed})
		return
	}
	if c.log.matchTerm(s.Index, s.Term) {
		// We already have the entries; just learn the commit.
		c.log.commitTo(s.Index)
		c.send(Message{To: m.From, Type: MsgAppResp, Index: s.Index})
		return
	}
	c.log.restore(s)
	c.snapshot = s
	c.pendingSnap = s
	c.send(Message{To: m.From, Type: MsgAppResp, Index: s.Index})
}

func (c *Core) stepLeader(m Message) {
	pr := c.prs[m.From]
	if pr == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.recentActive = true
		if m.Reject {
			// Ignore stale rejections: in probe mode only the single
			// outstanding message counts; when pipelining, any rejection
			// above the match index is real.
			if m.Index <= pr.match || (pr.probing && m.Index != pr.next-1) {
				return
			}
			pr.next = max(pr.match+1, min(m.Index, m.RejectHint+1))
			pr.probing = true
			c.sendAppend(m.From)
			return
		}
		if m.Index > pr.match {
			pr.match = m.Index
			pr.probing = false
			pr.snapPending = 0
		}
		if m.Index+1 > pr.next {
			pr.next = m.Index + 1
		}
		if c.maybeCommit() {
			c.bcastAppend()
		} else if pr.next <= c.log.lastIndex() {
			c.sendAppend(m.From)
		}
	case MsgHeartbeatResp:
		pr.recentActive = true
		if pr.match < c.log.lastIndex() {
			c.sendAppend(m.From)
		}
		if len(m.Context) > 0 {
			c.ackRead(m.From, m.Context)
		}
	}
}

func (c *Core) maybeCommit() bool {
	ms := make([]uint64, 0, len(c.peers))
	for id, pr := range c.prs {
		if id == c.id {
			ms = append(ms, c.log.lastIndex())
		} else {
			ms = append(ms, pr.match)
		}
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] > ms[j] })
	n := ms[c.quorum()-1]
	if n <= c.log.committed {
		return false
	}
	// Only entries of the current term are committed by counting replicas
	// (Figure 8 of the Raft paper shows why).
	if t, _ := c.log.term(n); t != c.term {
		return false
	}
	c.log.commitTo(n)
	c.releaseBlockedReads()
	return true
}

// ---------------------------------------------------------------- output

func (c *Core) send(m Message) {
	m.From = c.id
	if m.Term == 0 {
		m.Term = c.term
	}
	c.SentTo[m.To]++
	c.msgs = append(c.msgs, m)
}

func (c *Core) appendEntry(e Entry) uint64 {
	e.Term = c.term
	e.Index = c.log.lastIndex() + 1
	c.log.append(e)
	if c.quorum() == 1 {
		c.maybeCommit()
	}
	return e.Index
}

func (c *Core) sendAppend(to uint64) {
	pr := c.prs[to]
	if pr.next <= c.log.snapIndex {
		// The follower needs entries we already compacted: ship the snapshot,
		// but not more often than once per election timeout.
		if pr.snapPending != 0 && c.ticks-pr.snapSentAtTick < c.cfg.ElectionTick {
			return
		}
		pr.snapPending = c.snapshot.Index
		pr.snapSentAtTick = c.ticks
		c.send(Message{To: to, Type: MsgSnap, Snapshot: c.snapshot})
		pr.next = c.snapshot.Index + 1
		pr.probing = true
		return
	}
	prev := pr.next - 1
	prevTerm, _ := c.log.term(prev)
	max := c.cfg.MaxEntriesPerMsg
	if pr.probing {
		max = min(max, 64)
	}
	es := c.log.slice(pr.next, c.log.lastIndex()+1, max)
	c.send(Message{To: to, Type: MsgApp, Index: prev, LogTerm: prevTerm, Entries: es, Commit: c.log.committed})
	if len(es) > 0 && !pr.probing {
		// Optimistic pipelining: assume delivery; a rejection rewinds next.
		pr.next = es[len(es)-1].Index + 1
	}
}

func (c *Core) bcastAppend() {
	c.pendingBcast = false
	for _, p := range c.peers {
		if p != c.id {
			c.sendAppend(p)
		}
	}
}

func (c *Core) bcastHeartbeat(ctx []byte) {
	for _, p := range c.peers {
		if p == c.id {
			continue
		}
		// Never tell a follower to commit past what it is known to have.
		commit := min(c.prs[p].match, c.log.committed)
		c.send(Message{To: p, Type: MsgHeartbeat, Commit: commit, Context: ctx})
	}
}

// ---------------------------------------------------------------- API

// Propose appends data to the log if this node is the leader and returns
// the entry's index and term. The entry is committed once Ready returns it
// in CommittedEntries; if a different entry shows up at that index, or
// leadership is lost, the proposal failed and the caller should retry.
func (c *Core) Propose(data []byte) (index, term uint64, err error) {
	if c.role != Leader {
		return 0, 0, &NotLeaderError{Lead: c.lead}
	}
	idx := c.appendEntry(Entry{Type: EntryNormal, Data: data})
	c.pendingBcast = true
	return idx, c.term, nil
}

// ReadIndex starts a linearizable read tagged ctx (section 6.4 of the Raft
// thesis): the leader records its commit index and confirms it is still
// leader with a heartbeat round. Ready then returns a ReadState; the read is
// served once that index is applied.
func (c *Core) ReadIndex(ctx []byte) error {
	if c.role != Leader {
		return &NotLeaderError{Lead: c.lead}
	}
	if t, _ := c.log.term(c.log.committed); t != c.term {
		// A new leader does not know the true commit index until its no-op
		// commits. Hold the request until then.
		c.readBlocked = append(c.readBlocked, ctx)
		return nil
	}
	c.startRead(ctx)
	return nil
}

func (c *Core) startRead(ctx []byte) {
	if c.quorum() == 1 {
		c.readStates = append(c.readStates, ReadState{Index: c.log.committed, Context: ctx})
		return
	}
	c.readQueue = append(c.readQueue, &readRequest{index: c.log.committed, ctx: ctx, acks: map[uint64]bool{c.id: true}})
	c.bcastHeartbeat(ctx)
}

func (c *Core) releaseBlockedReads() {
	if len(c.readBlocked) == 0 {
		return
	}
	b := c.readBlocked
	c.readBlocked = nil
	for _, ctx := range b {
		c.startRead(ctx)
	}
}

func (c *Core) ackRead(from uint64, ctx []byte) {
	for i, r := range c.readQueue {
		if !bytes.Equal(r.ctx, ctx) {
			continue
		}
		r.acks[from] = true
		if len(r.acks) >= c.quorum() {
			// A majority confirmed our leadership after this request (and
			// every earlier one) was registered: release them in order.
			for _, rr := range c.readQueue[:i+1] {
				c.readStates = append(c.readStates, ReadState{Index: rr.index, Context: rr.ctx})
			}
			c.readQueue = c.readQueue[i+1:]
		}
		return
	}
}

// Compact records that the state machine has persisted a snapshot covering
// the log up to index (which must already be applied) and drops the
// covered entries from memory.
func (c *Core) Compact(index uint64, data []byte) error {
	if index > c.log.applied {
		return fmt.Errorf("raft: compact %d beyond applied %d", index, c.log.applied)
	}
	t, ok := c.log.term(index)
	if !ok {
		return fmt.Errorf("raft: compact: unknown index %d", index)
	}
	c.snapshot = &Snapshot{Index: index, Term: t, Data: data}
	c.log.compact(index)
	return nil
}

// HasReady reports whether Ready would return work.
func (c *Core) HasReady() bool {
	if c.pendingBcast {
		return true
	}
	return len(c.msgs) > 0 || c.log.stable < c.log.lastIndex() || c.log.applied < c.log.committed ||
		c.pendingSnap != nil || len(c.readStates) > 0 || len(c.readsDropped) > 0 ||
		c.hardState() != c.prevHS || !c.ssReported || c.softState() != c.prevSS
}

func (c *Core) softState() SoftState { return SoftState{Role: c.role, Lead: c.lead} }

// Ready returns pending work and marks it as handed out. The driver must
// finish processing one Ready before calling Ready again.
func (c *Core) Ready() Ready {
	if c.pendingBcast && c.role == Leader {
		c.bcastAppend()
	}
	var rd Ready
	if ss := c.softState(); !c.ssReported || ss != c.prevSS {
		rd.SoftState = &ss
		c.prevSS = ss
		c.ssReported = true
	}
	if hs := c.hardState(); hs != c.prevHS {
		rd.HardState = &hs
		c.prevHS = hs
	}
	if c.pendingSnap != nil {
		rd.Snapshot = c.pendingSnap
		c.pendingSnap = nil
	}
	rd.Entries = c.log.unstable()
	c.log.stable = c.log.lastIndex()
	rd.CommittedEntries = c.log.nextCommitted(c.cfg.MaxApplyBatch)
	if n := len(rd.CommittedEntries); n > 0 {
		c.log.applied = rd.CommittedEntries[n-1].Index
	}
	rd.Messages = c.msgs
	c.msgs = nil
	rd.ReadStates = c.readStates
	c.readStates = nil
	rd.ReadsDropped = c.readsDropped
	c.readsDropped = nil
	return rd
}

// Status is a read-only view for dashboards and tests.
type Status struct {
	ID, Term, Vote, Lead       uint64
	Role                       Role
	Commit, Applied, LastIndex uint64
	SnapIndex                  uint64
	Progress                   map[uint64]PeerProgress
	ElectionsStarted           uint64
}

// PeerProgress is the leader's view of one follower.
type PeerProgress struct {
	Match, Next  uint64
	RecentActive bool
}

// Status returns a snapshot of internal state.
func (c *Core) Status() Status {
	s := Status{
		ID: c.id, Term: c.term, Vote: c.vote, Lead: c.lead, Role: c.role,
		Commit: c.log.committed, Applied: c.log.applied, LastIndex: c.log.lastIndex(),
		SnapIndex: c.log.snapIndex, ElectionsStarted: c.ElectionsStarted,
	}
	if c.role == Leader {
		s.Progress = map[uint64]PeerProgress{}
		for id, pr := range c.prs {
			m := pr.match
			if id == c.id {
				m = c.log.lastIndex()
			}
			s.Progress[id] = PeerProgress{Match: m, Next: pr.next, RecentActive: pr.recentActive || id == c.id}
		}
	}
	return s
}

// Term returns the entry term at index, for drivers resolving proposals.
func (c *Core) Term(index uint64) (uint64, bool) { return c.log.term(index) }

// EntriesFrom returns a copy of the log from index lo (which must be above
// the snapshot) to the end. Drivers use it to rewrite storage after a
// snapshot.
func (c *Core) EntriesFrom(lo uint64) []Entry {
	if lo <= c.log.snapIndex {
		lo = c.log.snapIndex + 1
	}
	return c.log.slice(lo, c.log.lastIndex()+1, 0)
}

// Snapshot returns the latest local snapshot, or nil.
func (c *Core) Snapshot() *Snapshot { return c.snapshot }
