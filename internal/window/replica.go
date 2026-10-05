package window

import "math"

// Replica is the replicated copy of one partition's window State, as held by
// the online store's state machine. It applies Batches produced by Flush and
// evicts with the same rule as Partition, so Replica.State() always equals
// the worker's Partition.Snapshot() at the batch's offset.
type Replica struct {
	BucketMs int64
	st       State
	buckets  map[bucketKey]Agg
}

// NewReplica returns an empty replica for a partition.
func NewReplica(partition int32, bucketMs int64) *Replica {
	return &Replica{
		BucketMs: bucketMs,
		st: State{
			Partition:    partition,
			ToOffset:     -1,
			MaxEventTime: math.MinInt64,
			ClosedUntil:  math.MinInt64,
		},
		buckets: make(map[bucketKey]Agg),
	}
}

// ReplicaFromState rebuilds a replica from a full State (snapshot restore).
func ReplicaFromState(s State, bucketMs int64) *Replica {
	r := &Replica{BucketMs: bucketMs, st: s, buckets: make(map[bucketKey]Agg, len(s.Buckets))}
	r.st.Buckets = nil
	for _, b := range s.Buckets {
		r.buckets[bucketKey{b.Zone, b.StartMs}] = b.Agg
	}
	return r
}

// ToOffset returns the last applied offset (-1 if none).
func (r *Replica) ToOffset() int64 { return r.st.ToOffset }

// Apply applies a batch if it is newer than the stored offset. It returns
// false for a duplicate or stale batch, which is then ignored entirely: this
// is the idempotency check that turns at-least-once delivery into
// effectively-once effects.
func (r *Replica) Apply(b Batch) bool {
	if b.ToOffset <= r.st.ToOffset {
		return false
	}
	for _, bk := range b.Buckets {
		r.buckets[bucketKey{bk.Zone, bk.StartMs}] = bk.Agg
	}
	r.st.ToOffset = b.ToOffset
	r.st.MaxEventTime = b.MaxEventTime
	r.st.ClosedUntil = b.ClosedUntil
	r.st.LateDropped = b.LateDropped
	r.st.Events = b.Events
	limit := EvictLimit(b.ClosedUntil, r.BucketMs)
	for k := range r.buckets {
		if k.start < limit {
			delete(r.buckets, k)
		}
	}
	return true
}

// State returns a copy of the full state, buckets sorted.
func (r *Replica) State() State {
	s := r.st
	s.Buckets = make([]Bucket, 0, len(r.buckets))
	for k, a := range r.buckets {
		s.Buckets = append(s.Buckets, Bucket{Zone: k.zone, StartMs: k.start, Agg: a})
	}
	SortBuckets(s.Buckets)
	return s
}
