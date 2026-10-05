package window

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// genEvents produces a mostly ordered stream with skewed zones, a share of
// slightly late events, a share of very late events and occasional idle gaps.
func genEvents(rng *rand.Rand, n int) []Event {
	evs := make([]Event, 0, n)
	t := int64(1_700_000_000_000)
	for i := 0; i < n; i++ {
		t += int64(rng.Intn(4000))
		if rng.Intn(500) == 0 {
			t += int64(rng.Intn(3 * 3600 * 1000)) // idle gap, up to 3 h
		}
		et := t
		switch r := rng.Intn(100); {
		case r < 10:
			et -= int64(rng.Intn(30_000)) // within lateness
		case r < 13:
			et -= int64(30_000 + rng.Intn(300_000)) // beyond lateness
		}
		zone := int32(1 + int(math.Floor(math.Pow(rng.Float64(), 3)*20)))
		evs = append(evs, Event{
			Zone:          zone,
			EventTimeMs:   et,
			FareCents:     int64(500 + rng.Intn(5000)),
			DistanceMilli: int64(rng.Intn(20000)),
			Offset:        int64(i),
		})
	}
	return evs
}

// naive recomputes the same outputs by brute force: it replays the watermark
// rule and, at every window close, scans all accepted events.
func naive(evs []Event, cfg Config) ([]Features, int64) {
	b := cfg.BucketMs
	var accepted []Event
	var out []Features
	closed := int64(math.MinInt64)
	maxT := int64(math.MinInt64)
	active := map[int32]bool{}
	var late int64
	for _, e := range evs {
		if closed == math.MinInt64 {
			closed = floorDiv(e.EventTimeMs, b)
		}
		if e.EventTimeMs < closed {
			late++
			continue
		}
		accepted = append(accepted, e)
		if e.EventTimeMs > maxT {
			maxT = e.EventTimeMs
		}
		for closed+b <= maxT-cfg.LatenessMs {
			end := closed + b
			sums := map[int32]*Features{}
			for _, a := range accepted {
				if a.EventTimeMs >= end || a.EventTimeMs < end-HistoryBuckets*b {
					continue
				}
				f := sums[a.Zone]
				if f == nil {
					f = &Features{Zone: a.Zone, WindowEndMs: end}
					sums[a.Zone] = f
				}
				age := (end - floorDiv(a.EventTimeMs, b)) / b
				f.Trips60m++
				if age <= 30 {
					f.Trips30m++
				}
				if age <= 15 {
					f.Count15m++
					f.FareCents15m += a.FareCents
					f.DistanceMilli15m += a.DistanceMilli
				}
				if age <= 5 {
					f.Trips5m++
				}
			}
			zs := map[int32]bool{}
			for z := range sums {
				zs[z] = true
			}
			for z, a := range active {
				if a {
					zs[z] = true
				}
			}
			ids := make([]int32, 0, len(zs))
			for z := range zs {
				ids = append(ids, z)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			for _, z := range ids {
				f := sums[z]
				if f == nil {
					f = &Features{Zone: z, WindowEndMs: end}
				}
				out = append(out, *f)
				active[z] = f.Trips60m > 0
			}
			closed = end
		}
	}
	return out, late
}

func runAll(p *Partition, evs []Event) []Features {
	var out []Features
	for _, e := range evs {
		p.Add(e)
		if p.HasOutput() {
			out = append(out, p.Flush().Features...)
		}
	}
	return out
}

func TestMatchesNaive(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		evs := genEvents(rng, 3000)
		cfg := DefaultConfig()
		want, wantLate := naive(evs, cfg)
		p := NewPartition(0, cfg)
		got := runAll(p, evs)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: %d rows vs naive %d rows", seed, len(got), len(want))
		}
		if p.LateDropped() != wantLate {
			t.Fatalf("seed %d: late %d vs %d", seed, p.LateDropped(), wantLate)
		}
		if wantLate == 0 || len(want) == 0 {
			t.Fatalf("seed %d: generator did not exercise late data or windows", seed)
		}
	}
}

// TestRestoreIsTransparent crashes the worker at random points: it restores
// from the replicated state (built only from flushed batches) and continues
// from the stored offset. The output must equal an uninterrupted run.
func TestRestoreIsTransparent(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		evs := genEvents(rng, 3000)
		cfg := DefaultConfig()
		want := runAll(NewPartition(0, cfg), evs)

		rep := NewReplica(0, cfg.BucketMs)
		p := NewPartition(0, cfg)
		var got []Features
		for i := 0; i < len(evs); {
			e := evs[i]
			p.Add(e)
			i++
			if p.HasOutput() {
				b := p.Flush()
				got = append(got, b.Features...)
				if !rep.Apply(b) {
					t.Fatalf("seed %d: replica rejected fresh batch", seed)
				}
				if rep.Apply(b) {
					t.Fatalf("seed %d: replica accepted duplicate batch", seed)
				}
				if !reflect.DeepEqual(rep.State(), p.Snapshot()) {
					t.Fatalf("seed %d: replica state diverged at offset %d", seed, b.ToOffset)
				}
			}
			if rng.Intn(150) == 0 {
				// Crash: in-flight unflushed work is lost. Resume after the
				// committed offset with state rebuilt from the replica.
				p = Restore(rep.State(), cfg)
				i = int(rep.ToOffset()) + 1
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: crash/restore run differs (%d vs %d rows)", seed, len(got), len(want))
		}
	}
}

func TestDuplicateEventsIgnored(t *testing.T) {
	cfg := DefaultConfig()
	evs := genEvents(rand.New(rand.NewSource(7)), 500)
	p := NewPartition(0, cfg)
	a := runAll(p, evs)
	// Redelivering already-consumed offsets changes nothing.
	b := runAll(p, evs[:200])
	if len(b) != 0 || p.Events() != int64(len(evs)) {
		t.Fatalf("duplicates were consumed")
	}
	if len(a) == 0 {
		t.Fatal("no output")
	}
}

func TestDeriveSpike(t *testing.T) {
	v := Derive(Features{Trips5m: 10, Trips60m: 60, Count15m: 4, FareCents15m: 4000, DistanceMilli15m: 8000})
	if v["demand_spike_ratio"] != 2 || v["fare_mean_15m"] != 10 || v["distance_mean_15m"] != 2 {
		t.Fatalf("derive: %v", v)
	}
}
