package raftnode

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/raft"
)

// GRPCTransport sends Raft messages over gRPC. Each peer has a bounded
// queue and one sender goroutine that batches whatever is queued into a
// single RPC. When a peer is slow or down its queue fills and messages are
// dropped instead of blocking the Raft loop.
//
// It also implements transport-level partitions (Block) for the live demo;
// the scripted fault tests use iptables instead.
type GRPCTransport struct {
	self  uint64
	log   *slog.Logger
	peers map[uint64]*peer

	mu      sync.RWMutex
	blocked map[uint64]bool
	recv    func(raft.Message)
}

type peer struct {
	id   uint64
	addr string
	q    chan raft.Message
	conn *grpc.ClientConn
	cli  sfv1.RaftTransportServiceClient
}

// NewGRPCTransport creates a transport. addrs maps peer id to host:port.
func NewGRPCTransport(self uint64, addrs map[uint64]string, log *slog.Logger) (*GRPCTransport, error) {
	t := &GRPCTransport{self: self, log: log, peers: map[uint64]*peer{}, blocked: map[uint64]bool{}}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64<<20)))
		if err != nil {
			return nil, err
		}
		p := &peer{id: id, addr: addr, q: make(chan raft.Message, 8192), conn: conn, cli: sfv1.NewRaftTransportServiceClient(conn)}
		t.peers[id] = p
		go t.sendLoop(p)
	}
	return t, nil
}

// SetReceiver sets the function called for every inbound message.
func (t *GRPCTransport) SetReceiver(f func(raft.Message)) {
	t.mu.Lock()
	t.recv = f
	t.mu.Unlock()
}

// Block replaces the set of peers this node cannot talk to (both ways).
func (t *GRPCTransport) Block(ids []uint64) {
	b := map[uint64]bool{}
	for _, id := range ids {
		b[id] = true
	}
	t.mu.Lock()
	t.blocked = b
	t.mu.Unlock()
}

// Blocked returns the currently blocked peers.
func (t *GRPCTransport) Blocked() []uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []uint64
	for id := range t.blocked {
		out = append(out, id)
	}
	return out
}

func (t *GRPCTransport) isBlocked(id uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.blocked[id]
}

// Send enqueues messages; it never blocks.
func (t *GRPCTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		p, ok := t.peers[m.To]
		if !ok {
			continue
		}
		if t.isBlocked(m.To) {
			mMsgsDropped.WithLabelValues("partition").Inc()
			continue
		}
		select {
		case p.q <- m:
		default:
			mMsgsDropped.WithLabelValues("queue_full").Inc()
		}
	}
}

func (t *GRPCTransport) sendLoop(p *peer) {
	var failing bool
	for m := range p.q {
		batch := []*sfv1.RaftMessage{ToProto(m)}
		size := len(m.Entries)
	drain:
		for len(batch) < 512 && size < 4096 {
			select {
			case m := <-p.q:
				batch = append(batch, ToProto(m))
				size += len(m.Entries)
			default:
				break drain
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := p.cli.Send(ctx, &sfv1.SendRequest{Messages: batch})
		cancel()
		if err != nil {
			mMsgsDropped.WithLabelValues("send_error").Add(float64(len(batch)))
			if !failing {
				t.log.Warn("raft send failed", "peer", p.id, "err", err)
				failing = true
			}
			// Back off briefly so a dead peer does not spin the CPU; Raft
			// retries on its own schedule.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if failing {
			t.log.Info("raft send recovered", "peer", p.id)
			failing = false
		}
	}
}

// Server is the gRPC handler for inbound Raft messages.
type Server struct {
	sfv1.UnimplementedRaftTransportServiceServer
	T *GRPCTransport
}

// Send implements RaftTransportServiceServer.
func (s *Server) Send(_ context.Context, req *sfv1.SendRequest) (*sfv1.SendResponse, error) {
	s.T.mu.RLock()
	recv := s.T.recv
	s.T.mu.RUnlock()
	for _, pm := range req.Messages {
		m := FromProto(pm)
		if s.T.isBlocked(m.From) {
			mMsgsDropped.WithLabelValues("partition").Inc()
			continue
		}
		if recv != nil {
			recv(m)
		}
	}
	return &sfv1.SendResponse{}, nil
}

// ToProto converts a Raft message for the wire.
func ToProto(m raft.Message) *sfv1.RaftMessage {
	pm := &sfv1.RaftMessage{
		Type: uint32(m.Type), From: m.From, To: m.To, Term: m.Term, LogTerm: m.LogTerm,
		Index: m.Index, Commit: m.Commit, Reject: m.Reject, RejectHint: m.RejectHint, Context: m.Context,
	}
	if len(m.Entries) > 0 {
		pm.Entries = make([]*sfv1.RaftEntry, len(m.Entries))
		for i, e := range m.Entries {
			pm.Entries[i] = &sfv1.RaftEntry{Term: e.Term, Index: e.Index, Type: uint32(e.Type), Data: e.Data}
		}
	}
	if m.Snapshot != nil {
		pm.Snapshot = &sfv1.RaftSnapshot{Index: m.Snapshot.Index, Term: m.Snapshot.Term, Data: m.Snapshot.Data}
	}
	return pm
}

// FromProto converts a wire message.
func FromProto(pm *sfv1.RaftMessage) raft.Message {
	m := raft.Message{
		Type: raft.MsgType(pm.Type), From: pm.From, To: pm.To, Term: pm.Term, LogTerm: pm.LogTerm,
		Index: pm.Index, Commit: pm.Commit, Reject: pm.Reject, RejectHint: pm.RejectHint, Context: pm.Context,
	}
	if len(pm.Entries) > 0 {
		m.Entries = make([]raft.Entry, len(pm.Entries))
		for i, e := range pm.Entries {
			m.Entries[i] = raft.Entry{Term: e.Term, Index: e.Index, Type: raft.EntryType(e.Type), Data: e.Data}
		}
	}
	if pm.Snapshot != nil {
		m.Snapshot = &raft.Snapshot{Index: pm.Snapshot.Index, Term: pm.Snapshot.Term, Data: pm.Snapshot.Data}
	}
	return m
}
