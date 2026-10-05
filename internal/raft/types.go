// Package raft is StreamForge's Raft consensus core.
//
// The core is a pure state machine: it never touches the network, the disk
// or the clock. Time advances only through Tick, input arrives only through
// Step, Propose and ReadIndex, and all output (messages to send, entries to
// persist, entries to apply) is collected by Ready. The node driver in
// internal/raftnode performs the I/O. The same core runs in the 5-node
// Docker cluster, in deterministic simulation tests and in the browser demo.
//
// Implemented: leader election with randomized timeouts, PreVote, CheckQuorum
// with leader leases for vote rejection, log replication with fast conflict
// backtracking, commit only of current-term entries, snapshots with
// InstallSnapshot, and ReadIndex for linearizable reads.
// Not implemented: membership changes (the cluster is fixed at 5 nodes) and
// leadership transfer.
package raft

import "fmt"

// None is the zero node id, meaning "no node".
const None uint64 = 0

// EntryType distinguishes client data from internal entries.
type EntryType uint32

const (
	EntryNormal EntryType = 0
	// EntryNoop is appended by every new leader so that it can commit an
	// entry of its own term (see section 5.4.2 and 8 of the Raft paper).
	EntryNoop EntryType = 1
)

// Entry is one log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// MsgType enumerates Raft messages.
type MsgType uint32

const (
	MsgVote MsgType = iota + 1
	MsgVoteResp
	MsgPreVote
	MsgPreVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
)

var msgNames = map[MsgType]string{
	MsgVote: "Vote", MsgVoteResp: "VoteResp", MsgPreVote: "PreVote", MsgPreVoteResp: "PreVoteResp",
	MsgApp: "App", MsgAppResp: "AppResp", MsgHeartbeat: "Heartbeat", MsgHeartbeatResp: "HeartbeatResp",
	MsgSnap: "Snap",
}

func (t MsgType) String() string {
	if s, ok := msgNames[t]; ok {
		return s
	}
	return fmt.Sprintf("Msg(%d)", uint32(t))
}

// Snapshot is a state machine image covering the log up to Index.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Message is one Raft RPC or RPC response.
type Message struct {
	Type       MsgType
	From       uint64
	To         uint64
	Term       uint64
	LogTerm    uint64 // App: term of entry at Index. Vote: term of candidate's last entry.
	Index      uint64 // App: index preceding Entries. Vote: candidate's last index. AppResp: match index or rejected index.
	Entries    []Entry
	Commit     uint64
	Reject     bool
	RejectHint uint64
	Snapshot   *Snapshot
	Context    []byte // ReadIndex request id carried on heartbeats
}

// HardState is the state that must be persisted before messages are sent.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// Role of a node.
type Role int

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// SoftState is volatile state reported for observability.
type SoftState struct {
	Role Role
	Lead uint64
}

// ReadState confirms a ReadIndex request: once the state machine has applied
// Index, a read tagged with Context is linearizable.
type ReadState struct {
	Index   uint64
	Context []byte
}

// Ready is the batch of work the driver must perform, in this order:
//  1. persist Snapshot (if any), then HardState and Entries (truncating any
//     stored entries at or after Entries[0].Index first);
//  2. send Messages;
//  3. apply Snapshot (if any) and then CommittedEntries to the state machine;
//  4. serve ReadStates once their index is applied.
type Ready struct {
	SoftState        *SoftState
	HardState        *HardState
	Snapshot         *Snapshot
	Entries          []Entry
	CommittedEntries []Entry
	Messages         []Message
	ReadStates       []ReadState
	// ReadsDropped lists ReadIndex contexts that will never be confirmed
	// because this node lost leadership; the driver should fail them.
	ReadsDropped [][]byte
}

// IsEmpty reports whether the Ready carries no work.
func (r *Ready) IsEmpty() bool {
	return r.SoftState == nil && r.HardState == nil && r.Snapshot == nil && len(r.Entries) == 0 &&
		len(r.CommittedEntries) == 0 && len(r.Messages) == 0 && len(r.ReadStates) == 0 && len(r.ReadsDropped) == 0
}

// Errors returned to callers.
type NotLeaderError struct{ Lead uint64 }

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("raft: not leader (leader hint %d)", e.Lead)
}

// ErrNotReady is returned by ReadIndex while a new leader has not yet
// committed an entry of its term. Callers retry shortly.
var ErrNotReady = fmt.Errorf("raft: leader has not committed an entry in its term yet")
