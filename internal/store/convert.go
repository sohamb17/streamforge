package store

import (
	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/window"
)

// BatchToProto converts a window batch into the replicated command payload.
func BatchToProto(b window.Batch) *sfv1.FeatureBatch {
	out := &sfv1.FeatureBatch{
		Partition:        b.Partition,
		ToOffset:         b.ToOffset,
		MaxEventTimeMs:   b.MaxEventTime,
		ClosedUntilMs:    b.ClosedUntil,
		LateDroppedTotal: b.LateDropped,
		EventsTotal:      b.Events,
		TriggerPublishMs: b.TriggerPublishMs,
		Buckets:          make([]*sfv1.Bucket, len(b.Buckets)),
		Features:         make([]*sfv1.ZoneFeatures, len(b.Features)),
	}
	for i, bk := range b.Buckets {
		out.Buckets[i] = BucketToProto(bk)
	}
	for i, f := range b.Features {
		out.Features[i] = FeaturesToProto(f)
	}
	return out
}

// BatchFromProto is the inverse of BatchToProto.
func BatchFromProto(p *sfv1.FeatureBatch) window.Batch {
	b := window.Batch{
		Partition:        p.Partition,
		ToOffset:         p.ToOffset,
		MaxEventTime:     p.MaxEventTimeMs,
		ClosedUntil:      p.ClosedUntilMs,
		LateDropped:      p.LateDroppedTotal,
		Events:           p.EventsTotal,
		TriggerPublishMs: p.TriggerPublishMs,
		Buckets:          make([]window.Bucket, len(p.Buckets)),
		Features:         make([]window.Features, len(p.Features)),
	}
	for i, bk := range p.Buckets {
		b.Buckets[i] = BucketFromProto(bk)
	}
	for i, f := range p.Features {
		b.Features[i] = FeaturesFromProto(f)
	}
	return b
}

func BucketToProto(b window.Bucket) *sfv1.Bucket {
	return &sfv1.Bucket{Zone: b.Zone, StartMs: b.StartMs, Count: b.Count, FareCents: b.FareCents, DistanceMilli: b.DistanceMilli}
}

func BucketFromProto(b *sfv1.Bucket) window.Bucket {
	return window.Bucket{Zone: b.Zone, StartMs: b.StartMs, Agg: window.Agg{Count: b.Count, FareCents: b.FareCents, DistanceMilli: b.DistanceMilli}}
}

func FeaturesToProto(f window.Features) *sfv1.ZoneFeatures {
	return &sfv1.ZoneFeatures{
		Zone: f.Zone, WindowEndMs: f.WindowEndMs,
		Trips_5M: f.Trips5m, Trips_30M: f.Trips30m, Trips_60M: f.Trips60m,
		Count_15M: f.Count15m, FareCents_15M: f.FareCents15m, DistanceMilli_15M: f.DistanceMilli15m,
	}
}

func FeaturesFromProto(f *sfv1.ZoneFeatures) window.Features {
	return window.Features{
		Zone: f.Zone, WindowEndMs: f.WindowEndMs,
		Trips5m: f.Trips_5M, Trips30m: f.Trips_30M, Trips60m: f.Trips_60M,
		Count15m: f.Count_15M, FareCents15m: f.FareCents_15M, DistanceMilli15m: f.DistanceMilli_15M,
	}
}

// StateToProto converts a partition state.
func StateToProto(s window.State) *sfv1.PartitionState {
	out := &sfv1.PartitionState{
		Partition: s.Partition, ToOffset: s.ToOffset, MaxEventTimeMs: s.MaxEventTime,
		ClosedUntilMs: s.ClosedUntil, LateDroppedTotal: s.LateDropped, EventsTotal: s.Events,
		Buckets: make([]*sfv1.Bucket, len(s.Buckets)),
	}
	for i, b := range s.Buckets {
		out.Buckets[i] = BucketToProto(b)
	}
	return out
}

// StateFromProto converts a partition state.
func StateFromProto(p *sfv1.PartitionState) window.State {
	s := window.State{
		Partition: p.Partition, ToOffset: p.ToOffset, MaxEventTime: p.MaxEventTimeMs,
		ClosedUntil: p.ClosedUntilMs, LateDropped: p.LateDroppedTotal, Events: p.EventsTotal,
		Buckets: make([]window.Bucket, len(p.Buckets)),
	}
	for i, b := range p.Buckets {
		s.Buckets[i] = BucketFromProto(b)
	}
	return s
}
