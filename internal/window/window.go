// Package window implements StreamForge's event-time windowing for one Kafka
// partition: per-zone one-minute buckets, a watermark with a fixed lateness
// allowance, and feature rows emitted when the watermark closes a window.
//
// The package is deliberately free of I/O, goroutines and wall-clock reads.
// Given the same events in the same order it produces byte-identical output,
// which is what makes replay, crash recovery and the batch oracle comparable.
package window

import (
	"fmt"
	"math"
	"sort"
)

// Defaults for the zone_demand_v1 feature view.
const (
	DefaultBucketMs   int64 = 60_000 // 1-minute buckets
	DefaultLatenessMs int64 = 30_000 // watermark = max event time - 30 s
	HistoryBuckets    int64 = 60     // longest feature looks back 60 buckets
)

// Event is one trip start as read from Kafka.
type Event struct {
	Zone          int32
	EventTimeMs   int64
	FareCents     int64
	DistanceMilli int64 // miles * 1000
	Offset        int64 // Kafka offset within the partition
	PublishMs     int64 // wall-clock publish time (metrics only, never used for logic)
}

// Agg is the exact aggregate of one bucket.
type Agg struct {
	Count         int64
	FareCents     int64
	DistanceMilli int64
}

// Bucket is an Agg addressed by zone and bucket start.
type Bucket struct {
	Zone    int32
	StartMs int64
	Agg
}

// Features is the output row for one zone at one window end. It holds only
// exact integer sums; see Derive for the floating point features.
type Features struct {
	Zone             int32
	WindowEndMs      int64
	Trips5m          int64
	Trips30m         int64
	Trips60m         int64
	Count15m         int64
	FareCents15m     int64
	DistanceMilli15m int64
}

// Config controls bucket width and the lateness allowance.
type Config struct {
	BucketMs   int64
	LatenessMs int64
}

// DefaultConfig returns the configuration used by zone_demand_v1.
func DefaultConfig() Config {
	return Config{BucketMs: DefaultBucketMs, LatenessMs: DefaultLatenessMs}
}

// State is the persistent part of a Partition. It is what the stream worker
// commits through Raft together with its outputs, and what it restores from
// after a crash.
type State struct {
	Partition    int32
	ToOffset     int64 // last offset reflected; -1 if none
	MaxEventTime int64 // math.MinInt64 if no event yet
	ClosedUntil  int64 // windows ending <= ClosedUntil are emitted; math.MinInt64 if uninitialized
	LateDropped  int64
	Events       int64
	Buckets      []Bucket // sorted by (zone, start)
}

// Batch is everything produced since the previous Flush.
type Batch struct {
	Partition        int32
	ToOffset         int64
	MaxEventTime     int64
	ClosedUntil      int64
	LateDropped      int64
	Events           int64
	Buckets          []Bucket   // buckets changed since the last flush (absolute values)
	Features         []Features // outputs of windows closed since the last flush
	TriggerPublishMs int64      // publish time of the event that closed the newest window
}

type bucketKey struct {
	zone  int32
	start int64
}

type zoneState struct {
	buckets map[int64]*Agg
	active  bool // emitted a non-zero row at the most recent closed window
}

// Partition holds the window state of one Kafka partition.
type Partition struct {
	cfg   Config
	st    State // Buckets field unused here; live buckets are in zones
	zones map[int32]*zoneState
	// maxBucketStart is the newest bucket start seen; used to skip idle gaps.
	maxBucketStart int64

	dirty            map[bucketKey]struct{}
	pending          []Features
	triggerPublishMs int64
}

// NewPartition returns an empty partition.
func NewPartition(id int32, cfg Config) *Partition {
	return Restore(State{
		Partition:    id,
		ToOffset:     -1,
		MaxEventTime: math.MinInt64,
		ClosedUntil:  math.MinInt64,
	}, cfg)
}

// Restore rebuilds a Partition from committed state.
func Restore(s State, cfg Config) *Partition {
	if cfg.BucketMs <= 0 || cfg.LatenessMs < 0 {
		panic(fmt.Sprintf("window: invalid config %+v", cfg))
	}
	p := &Partition{
		cfg:            cfg,
		st:             s,
		zones:          make(map[int32]*zoneState),
		maxBucketStart: math.MinInt64,
		dirty:          make(map[bucketKey]struct{}),
	}
	p.st.Buckets = nil
	for _, b := range s.Buckets {
		z := p.zone(b.Zone)
		a := b.Agg
		z.buckets[b.StartMs] = &a
		if b.StartMs > p.maxBucketStart {
			p.maxBucketStart = b.StartMs
		}
	}
	// A zone is active if it had any trips in the 60 buckets before the last
	// closed window. That is derivable from the retained buckets because
	// eviction keeps everything from ClosedUntil - 60 buckets onward.
	if s.ClosedUntil != math.MinInt64 {
		for _, z := range p.zones {
			f := p.sum(z, s.ClosedUntil)
			z.active = f.Trips60m > 0
		}
	}
	return p
}

func (p *Partition) zone(id int32) *zoneState {
	z, ok := p.zones[id]
	if !ok {
		z = &zoneState{buckets: make(map[int64]*Agg)}
		p.zones[id] = z
	}
	return z
}

func floorDiv(t, w int64) int64 {
	q := t / w
	if t%w != 0 && t < 0 {
		q--
	}
	return q * w
}

// Watermark returns the current watermark (max event time minus lateness).
func (p *Partition) Watermark() int64 {
	if p.st.MaxEventTime == math.MinInt64 {
		return math.MinInt64
	}
	return p.st.MaxEventTime - p.cfg.LatenessMs
}

// ToOffset returns the last offset reflected in the state.
func (p *Partition) ToOffset() int64 { return p.st.ToOffset }

// LateDropped returns the number of events dropped for being too late.
func (p *Partition) LateDropped() int64 { return p.st.LateDropped }

// Events returns the number of events consumed.
func (p *Partition) Events() int64 { return p.st.Events }

// ClosedUntil returns the end of the newest emitted window.
func (p *Partition) ClosedUntil() int64 { return p.st.ClosedUntil }

// Add consumes one event. Events must be fed in Kafka offset order. An event
// whose offset is not greater than ToOffset was already reflected and is
// ignored; the return value reports whether it was consumed.
func (p *Partition) Add(e Event) bool {
	if e.Offset <= p.st.ToOffset {
		return false
	}
	p.st.ToOffset = e.Offset
	p.st.Events++
	b := p.cfg.BucketMs
	start := floorDiv(e.EventTimeMs, b)
	if p.st.ClosedUntil == math.MinInt64 {
		// First event: nothing before its bucket will ever be emitted.
		p.st.ClosedUntil = start
	}
	if e.EventTimeMs < p.st.ClosedUntil {
		// Its bucket's window already closed and was served. Count it, never
		// merge it silently.
		p.st.LateDropped++
		return true
	}
	z := p.zone(e.Zone)
	a, ok := z.buckets[start]
	if !ok {
		a = &Agg{}
		z.buckets[start] = a
	}
	a.Count++
	a.FareCents += e.FareCents
	a.DistanceMilli += e.DistanceMilli
	p.dirty[bucketKey{e.Zone, start}] = struct{}{}
	if start > p.maxBucketStart {
		p.maxBucketStart = start
	}
	if e.EventTimeMs > p.st.MaxEventTime {
		p.st.MaxEventTime = e.EventTimeMs
	}
	p.advance(e.PublishMs)
	return true
}

// advance closes every window whose end is at or before the watermark.
func (p *Partition) advance(publishMs int64) {
	b := p.cfg.BucketMs
	wm := p.Watermark()
	for p.st.ClosedUntil+b <= wm {
		end := p.st.ClosedUntil + b
		if p.idle(end) {
			// No zone has data that any window up to the watermark could
			// see, and no zone owes a final zero row. Jump straight ahead.
			p.st.ClosedUntil = floorDiv(wm, b)
			p.evict()
			continue
		}
		p.close(end)
		p.st.ClosedUntil = end
		p.evict()
		p.triggerPublishMs = publishMs
	}
}

func (p *Partition) idle(end int64) bool {
	for _, z := range p.zones {
		if z.active {
			return false
		}
	}
	return p.maxBucketStart < end-HistoryBuckets*p.cfg.BucketMs
}

// sum computes the feature sums for zone z at window end `end`.
func (p *Partition) sum(z *zoneState, end int64) Features {
	b := p.cfg.BucketMs
	f := Features{WindowEndMs: end}
	for start, a := range z.buckets {
		if start >= end {
			continue
		}
		age := (end - start) / b // 1 = newest closed bucket
		if age > HistoryBuckets {
			continue
		}
		f.Trips60m += a.Count
		if age <= 30 {
			f.Trips30m += a.Count
		}
		if age <= 15 {
			f.Count15m += a.Count
			f.FareCents15m += a.FareCents
			f.DistanceMilli15m += a.DistanceMilli
		}
		if age <= 5 {
			f.Trips5m += a.Count
		}
	}
	return f
}

func (p *Partition) close(end int64) {
	ids := make([]int32, 0, len(p.zones))
	for id := range p.zones {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		z := p.zones[id]
		f := p.sum(z, end)
		f.Zone = id
		if f.Trips60m > 0 || z.active {
			// A zone that just went quiet gets exactly one all-zero row so the
			// online store does not keep serving its last busy values.
			p.pending = append(p.pending, f)
		}
		z.active = f.Trips60m > 0
	}
}

// evict drops buckets no future window can see. The state machine applies
// the same rule to its copy of the state, so evictions are never shipped.
func (p *Partition) evict() {
	limit := p.st.ClosedUntil - HistoryBuckets*p.cfg.BucketMs
	for id, z := range p.zones {
		for start := range z.buckets {
			if start < limit {
				delete(z.buckets, start)
				delete(p.dirty, bucketKey{id, start})
			}
		}
		if len(z.buckets) == 0 && !z.active {
			delete(p.zones, id)
		}
	}
}

// EvictLimit returns the bucket start below which buckets are dropped once
// windows up to closedUntil have been emitted.
func EvictLimit(closedUntil, bucketMs int64) int64 {
	if closedUntil == math.MinInt64 {
		return math.MinInt64
	}
	return closedUntil - HistoryBuckets*bucketMs
}

// HasOutput reports whether Flush would return any feature rows.
func (p *Partition) HasOutput() bool { return len(p.pending) > 0 }

// Flush returns everything produced since the previous flush and resets the
// change tracking. The caller commits the batch atomically.
func (p *Partition) Flush() Batch {
	keys := make([]bucketKey, 0, len(p.dirty))
	for k := range p.dirty {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].zone != keys[j].zone {
			return keys[i].zone < keys[j].zone
		}
		return keys[i].start < keys[j].start
	})
	bs := make([]Bucket, 0, len(keys))
	for _, k := range keys {
		if z, ok := p.zones[k.zone]; ok {
			if a, ok := z.buckets[k.start]; ok {
				bs = append(bs, Bucket{Zone: k.zone, StartMs: k.start, Agg: *a})
			}
		}
	}
	out := Batch{
		Partition:        p.st.Partition,
		ToOffset:         p.st.ToOffset,
		MaxEventTime:     p.st.MaxEventTime,
		ClosedUntil:      p.st.ClosedUntil,
		LateDropped:      p.st.LateDropped,
		Events:           p.st.Events,
		Buckets:          bs,
		Features:         p.pending,
		TriggerPublishMs: p.triggerPublishMs,
	}
	p.dirty = make(map[bucketKey]struct{})
	p.pending = nil
	return out
}

// Snapshot returns the full state, buckets sorted by (zone, start).
func (p *Partition) Snapshot() State {
	s := p.st
	s.Buckets = nil
	for id, z := range p.zones {
		for start, a := range z.buckets {
			s.Buckets = append(s.Buckets, Bucket{Zone: id, StartMs: start, Agg: *a})
		}
	}
	SortBuckets(s.Buckets)
	return s
}

// SortBuckets sorts by (zone, start).
func SortBuckets(bs []Bucket) {
	sort.Slice(bs, func(i, j int) bool {
		if bs[i].Zone != bs[j].Zone {
			return bs[i].Zone < bs[j].Zone
		}
		return bs[i].StartMs < bs[j].StartMs
	})
}

// Derive turns exact sums into the named feature values served to models.
func Derive(f Features) map[string]float64 {
	v := map[string]float64{
		"trips_5m":           float64(f.Trips5m),
		"trips_30m":          float64(f.Trips30m),
		"trips_60m":          float64(f.Trips60m),
		"fare_mean_15m":      0,
		"distance_mean_15m":  0,
		"demand_spike_ratio": 0,
	}
	if f.Count15m > 0 {
		v["fare_mean_15m"] = float64(f.FareCents15m) / 100 / float64(f.Count15m)
		v["distance_mean_15m"] = float64(f.DistanceMilli15m) / 1000 / float64(f.Count15m)
	}
	if f.Trips60m > 0 {
		// Last 5 minutes against the trailing hour's average 5-minute count.
		v["demand_spike_ratio"] = float64(f.Trips5m) / (float64(f.Trips60m) / 12)
	}
	return v
}

// FeatureNames lists the derived features in display order.
var FeatureNames = []string{"trips_5m", "trips_30m", "trips_60m", "fare_mean_15m", "distance_mean_15m", "demand_spike_ratio"}
