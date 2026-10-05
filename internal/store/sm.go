// Package store is the online store's replicated state machine: everything
// that is applied, in log order, on every one of the five Raft nodes.
//
// It holds three things:
//   - the latest feature row per zone (what serving reads),
//   - per Kafka partition, the committed offset and window operator state
//     (what a restarting stream worker resumes from),
//   - a small register map plus per-client sequence numbers, used by the
//     linearizability probe and to make client retries exactly-once.
//
// Apply is deterministic and does no I/O, so every replica that applies the
// same log ends in the same state.
package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/raft"
	"github.com/sohamb17/streamforge/internal/window"
)

// Result is the outcome of applying one entry.
type Result struct {
	Index uint64
	// Applied is false for a duplicate: a client retry whose seq was already
	// applied, or a feature batch whose offset is not newer than the stored
	// offset. Duplicates change nothing.
	Applied bool
	Err     error
	// Count is the number of keys a purge deleted.
	Count uint64
}

type clientRecord struct {
	Seq   uint64
	Index uint64
}

// SM is the state machine.
type SM struct {
	bucketMs int64

	applied    uint64
	kv         map[string]string
	clients    map[string]clientRecord
	partitions map[int32]*window.Replica
	features   map[int32]window.Features
	// featureIndex records the log index at which each zone last changed.
	featureIndex map[int32]uint64

	DuplicateBatches uint64
	DuplicateOps     uint64

	// OnBatch, if set, is called after a feature batch is applied (not for
	// duplicates). Used for metrics and cache invalidation; must not block.
	OnBatch func(index uint64, b *sfv1.FeatureBatch)
}

// New returns an empty state machine.
func New(bucketMs int64) *SM {
	return &SM{
		bucketMs:     bucketMs,
		kv:           map[string]string{},
		clients:      map[string]clientRecord{},
		partitions:   map[int32]*window.Replica{},
		features:     map[int32]window.Features{},
		featureIndex: map[int32]uint64{},
	}
}

// AppliedIndex returns the index of the last applied entry.
func (s *SM) AppliedIndex() uint64 { return s.applied }

// EncodeCommand serializes a command for proposal.
func EncodeCommand(c *sfv1.Command) ([]byte, error) { return proto.Marshal(c) }

// DecodeCommand parses a proposed command.
func DecodeCommand(b []byte) (*sfv1.Command, error) {
	c := &sfv1.Command{}
	return c, proto.Unmarshal(b, c)
}

// Apply applies one committed entry.
func (s *SM) Apply(e raft.Entry) Result {
	if e.Index <= s.applied {
		panic(fmt.Sprintf("store: apply %d after %d", e.Index, s.applied))
	}
	s.applied = e.Index
	if e.Type != raft.EntryNormal || len(e.Data) == 0 {
		return Result{Index: e.Index, Applied: true}
	}
	cmd, err := DecodeCommand(e.Data)
	if err != nil {
		// A corrupt entry is applied as a no-op on every replica alike.
		return Result{Index: e.Index, Err: fmt.Errorf("store: decode entry %d: %w", e.Index, err)}
	}
	switch op := cmd.Op.(type) {
	case *sfv1.Command_Put:
		if cmd.ClientId != "" {
			if rec, ok := s.clients[cmd.ClientId]; ok && cmd.Seq <= rec.Seq {
				s.DuplicateOps++
				return Result{Index: rec.Index, Applied: false}
			}
			s.clients[cmd.ClientId] = clientRecord{Seq: cmd.Seq, Index: e.Index}
		}
		s.kv[op.Put.Key] = op.Put.Value
		return Result{Index: e.Index, Applied: true}
	case *sfv1.Command_Batch:
		return s.applyBatch(e.Index, op.Batch)
	case *sfv1.Command_Purge:
		p := op.Purge.Prefix
		if p == "" {
			return Result{Index: e.Index, Err: fmt.Errorf("store: empty purge prefix")}
		}
		var n uint64
		for k := range s.kv {
			if strings.HasPrefix(k, p) {
				delete(s.kv, k)
				n++
			}
		}
		for c := range s.clients {
			if strings.HasPrefix(c, p) {
				delete(s.clients, c)
			}
		}
		return Result{Index: e.Index, Applied: true, Count: n}
	default:
		return Result{Index: e.Index, Err: fmt.Errorf("store: unknown command in entry %d", e.Index)}
	}
}

func (s *SM) applyBatch(index uint64, pb *sfv1.FeatureBatch) Result {
	r, ok := s.partitions[pb.Partition]
	if !ok {
		r = window.NewReplica(pb.Partition, s.bucketMs)
		s.partitions[pb.Partition] = r
	}
	// Offsets are stored in the same atomic step as the outputs, so this
	// single comparison makes redelivered or re-proposed batches harmless.
	if !r.Apply(BatchFromProto(pb)) {
		s.DuplicateBatches++
		return Result{Index: index, Applied: false}
	}
	for _, f := range pb.Features {
		cur, ok := s.features[f.Zone]
		if !ok || f.WindowEndMs >= cur.WindowEndMs {
			s.features[f.Zone] = FeaturesFromProto(f)
			s.featureIndex[f.Zone] = index
		}
	}
	if s.OnBatch != nil {
		s.OnBatch(index, pb)
	}
	return Result{Index: index, Applied: true}
}

// Get reads a register.
func (s *SM) Get(key string) (string, bool) {
	v, ok := s.kv[key]
	return v, ok
}

// Features returns the latest row per requested zone (all zones if empty).
func (s *SM) Features(zones []int32) []window.Features {
	var out []window.Features
	if len(zones) == 0 {
		for _, f := range s.features {
			out = append(out, f)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Zone < out[j].Zone })
		return out
	}
	for _, z := range zones {
		if f, ok := s.features[z]; ok {
			out = append(out, f)
		}
	}
	return out
}

// PartitionState returns the committed state of one partition.
func (s *SM) PartitionState(p int32) window.State {
	if r, ok := s.partitions[p]; ok {
		return r.State()
	}
	return window.NewReplica(p, s.bucketMs).State()
}

// PartitionOffsets returns the stored offset per partition.
func (s *SM) PartitionOffsets() map[int32]int64 {
	out := map[int32]int64{}
	for p, r := range s.partitions {
		out[p] = r.ToOffset()
	}
	return out
}

// snapshotV1 is the on-disk snapshot format.
type snapshotV1 struct {
	Applied          uint64
	KV               map[string]string
	Clients          map[string]clientRecord
	Partitions       []window.State
	Features         []window.Features
	FeatureIndex     map[int32]uint64
	DuplicateBatches uint64
	DuplicateOps     uint64
}

// Snapshot serializes the full state.
func (s *SM) Snapshot() ([]byte, error) {
	snap := snapshotV1{
		Applied: s.applied, KV: s.kv, Clients: s.clients, FeatureIndex: s.featureIndex,
		Features: s.Features(nil), DuplicateBatches: s.DuplicateBatches, DuplicateOps: s.DuplicateOps,
	}
	ps := make([]int32, 0, len(s.partitions))
	for p := range s.partitions {
		ps = append(ps, p)
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	for _, p := range ps {
		snap.Partitions = append(snap.Partitions, s.partitions[p].State())
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&snap); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore replaces the state with a snapshot.
func (s *SM) Restore(data []byte) error {
	var snap snapshotV1
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return fmt.Errorf("store: restore snapshot: %w", err)
	}
	n := New(s.bucketMs)
	n.OnBatch = s.OnBatch
	n.applied = snap.Applied
	if snap.KV != nil {
		n.kv = snap.KV
	}
	if snap.Clients != nil {
		n.clients = snap.Clients
	}
	if snap.FeatureIndex != nil {
		n.featureIndex = snap.FeatureIndex
	}
	for _, st := range snap.Partitions {
		n.partitions[st.Partition] = window.ReplicaFromState(st, s.bucketMs)
	}
	for _, f := range snap.Features {
		n.features[f.Zone] = f
	}
	n.DuplicateBatches, n.DuplicateOps = snap.DuplicateBatches, snap.DuplicateOps
	*s = *n
	return nil
}

// Fingerprint hashes the complete state in a canonical order. Replicas that
// applied the same log prefix return the same value.
func (s *SM) Fingerprint() string {
	h := sha256.New()
	fmt.Fprintf(h, "applied=%d\n", s.applied)
	keys := make([]string, 0, len(s.kv))
	for k := range s.kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "kv %q=%q\n", k, s.kv[k])
	}
	cids := make([]string, 0, len(s.clients))
	for k := range s.clients {
		cids = append(cids, k)
	}
	sort.Strings(cids)
	for _, k := range cids {
		fmt.Fprintf(h, "client %q=%+v\n", k, s.clients[k])
	}
	for _, f := range s.Features(nil) {
		fmt.Fprintf(h, "f %+v\n", f)
	}
	ps := make([]int32, 0, len(s.partitions))
	for p := range s.partitions {
		ps = append(ps, p)
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	for _, p := range ps {
		fmt.Fprintf(h, "p %+v\n", s.partitions[p].State())
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
