// Command verify checks StreamForge's correctness claims against the log.
//
//	verify oracle   recompute every partition from Kafka offset 0 to the
//	                store's committed offset; diff window state and the
//	                latest feature row per zone against the online store.
//	verify backfill replay the log through the same window code and write
//	                the rows to the "__backfill" feature view in Postgres.
//	verify skew     sample online features, then check the point-in-time
//	                join over the offline history returns the same rows.
//	verify dump     print a fingerprint of the store's features and state.
//	verify parity   compare the streaming history with the backfill, row by
//	                row, for every window both have closed.
//
// oracle and parity expect a quiescent pipeline (the replay finished and the
// workers caught up); -wait-idle waits for that.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/sohamb17/streamforge/deploy/sql"
	"github.com/sohamb17/streamforge/internal/history"
	"github.com/sohamb17/streamforge/internal/oracle"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/storeclient"
	"github.com/sohamb17/streamforge/internal/window"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: verify oracle|backfill|parity [flags]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "Kafka bootstrap servers")
	topic := fs.String("topic", "trips", "topic")
	stores := fs.String("store", "1=localhost:7001", "store nodes")
	pg := fs.String("postgres", "", "PostgreSQL DSN")
	waitIdle := fs.Duration("wait-idle", 0, "wait until store offsets are unchanged for this long (0 = do not wait)")
	timeout := fs.Duration("timeout", 30*time.Minute, "overall timeout")
	skewFor := fs.Duration("sample-for", 30*time.Second, "skew: how long to sample the online store")
	fs.Parse(os.Args[2:])

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	addrs, err := storeclient.ParseAddrs(*stores)
	must(err)
	sc, err := storeclient.New(addrs, "verify")
	must(err)
	bl := strings.Split(*brokers, ",")
	parts, err := oracle.Partitions(ctx, bl, *topic)
	must(err)
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	if *waitIdle > 0 {
		waitForIdle(ctx, sc, parts, *waitIdle)
	}
	cfg := window.DefaultConfig()

	switch cmd {
	case "oracle":
		report := map[string]any{"partitions": len(parts)}
		var stateMismatch, featureMismatch, zonesChecked, events int64
		want := map[int32]window.Features{}
		for _, p := range parts {
			ps, err := sc.PartitionState(ctx, p)
			must(err)
			got := store.StateFromProto(ps)
			st, err := oracle.Replay(ctx, bl, *topic, p, got.ToOffset, cfg, func(f window.Features) { want[f.Zone] = f })
			must(err)
			events += st.Events
			if !reflect.DeepEqual(normalize(st), normalize(got)) {
				stateMismatch++
				fmt.Fprintf(os.Stderr, "partition %d: window state differs (oracle offset %d events %d late %d; store offset %d events %d late %d)\n",
					p, st.ToOffset, st.Events, st.LateDropped, got.ToOffset, got.Events, got.LateDropped)
			}
		}
		rows, _, err := sc.ZoneFeatures(ctx, nil)
		must(err)
		got := map[int32]window.Features{}
		for _, r := range rows {
			got[r.Zone] = store.FeaturesFromProto(r)
		}
		for z, f := range want {
			zonesChecked++
			if g, ok := got[z]; !ok || g != f {
				featureMismatch++
				fmt.Fprintf(os.Stderr, "zone %d: store %+v oracle %+v\n", z, got[z], f)
			}
		}
		for z := range got {
			if _, ok := want[z]; !ok {
				featureMismatch++
				fmt.Fprintf(os.Stderr, "zone %d: in store but not produced by oracle\n", z)
			}
		}
		report["events_replayed"] = events
		report["zones_checked"] = zonesChecked
		report["feature_mismatches"] = featureMismatch
		report["partition_state_mismatches"] = stateMismatch
		emit(report)
		if featureMismatch+stateMismatch > 0 {
			os.Exit(1)
		}
	case "backfill":
		if *pg == "" {
			must(fmt.Errorf("-postgres required"))
		}
		h, err := history.Open(ctx, *pg, sql.FeatureHistory)
		must(err)
		h.View = history.BackfillView
		_, err = h.Pool.Exec(ctx, `DELETE FROM feature_history WHERE feature_view = $1`, history.BackfillView)
		must(err)
		var rows int64
		for _, p := range parts {
			ps, err := sc.PartitionState(ctx, p)
			must(err)
			var buf []window.Features
			flush := func() {
				if len(buf) > 0 {
					must(h.Write(ctx, p, ps.ToOffset, buf))
					rows += int64(len(buf))
					buf = buf[:0]
				}
			}
			_, err = oracle.Replay(ctx, bl, *topic, p, ps.ToOffset, cfg, func(f window.Features) {
				buf = append(buf, f)
				if len(buf) >= 5000 {
					flush()
				}
			})
			must(err)
			flush()
		}
		emit(map[string]any{"backfill_rows": rows})
	case "parity":
		if *pg == "" {
			must(fmt.Errorf("-postgres required"))
		}
		h, err := history.Open(ctx, *pg, sql.FeatureHistory)
		must(err)
		// Compare only windows every partition has closed in the store.
		var cut int64 = 1<<62 - 1
		for _, p := range parts {
			ps, err := sc.PartitionState(ctx, p)
			must(err)
			if ps.ToOffset >= 0 && ps.ClosedUntilMs < cut {
				cut = ps.ClosedUntilMs
			}
		}
		r, err := h.Parity(ctx, time.UnixMilli(cut))
		must(err)
		conflicts, err := h.ConflictingRewrites(ctx)
		must(err)
		emit(map[string]any{
			"compared_through":     time.UnixMilli(cut).UTC().Format(time.RFC3339),
			"rows_matched":         r.Matched,
			"rows_mismatched":      r.Mismatched,
			"missing_in_stream":    r.MissingInStream,
			"missing_in_backfill":  r.MissingInBackfill,
			"conflicting_rewrites": conflicts,
		})
		if r.Mismatched+r.MissingInStream+r.MissingInBackfill+conflicts > 0 || r.Matched == 0 {
			os.Exit(1)
		}
	case "skew":
		// Training/serving skew check: sample what the online store serves
		// (linearizable reads) over time, then ask the offline history, via
		// the point-in-time join, what a training row at that window end
		// would contain. Every sample must match exactly.
		if *pg == "" {
			must(fmt.Errorf("-postgres required"))
		}
		h, err := history.Open(ctx, *pg, sql.FeatureHistory)
		must(err)
		var samples []window.Features
		deadline := time.Now().Add(*skewFor)
		for time.Now().Before(deadline) {
			rows, _, err := sc.ZoneFeatures(ctx, nil)
			must(err)
			for i := 0; i < 5 && len(rows) > 0; i++ {
				samples = append(samples, store.FeaturesFromProto(rows[(len(samples)*7+i*31)%len(rows)]))
			}
			time.Sleep(500 * time.Millisecond)
		}
		_, err = h.Pool.Exec(ctx, `DELETE FROM training_labels`)
		must(err)
		for _, f := range samples {
			_, err := h.Pool.Exec(ctx, `INSERT INTO training_labels (zone, label_time) VALUES ($1, to_timestamp($2/1000.0)) ON CONFLICT DO NOTHING`, f.Zone, f.WindowEndMs)
			must(err)
		}
		rows, err := h.Pool.Query(ctx, history.PointInTimeSQL, history.View)
		must(err)
		joined := map[[2]int64]window.Features{}
		for rows.Next() {
			var zone int32
			var lt, we time.Time
			var label *float64
			var f window.Features
			var weNull *time.Time
			must(rows.Scan(&zone, &lt, &label, &weNull, &f.Trips5m, &f.Trips30m, &f.Trips60m, &f.Count15m, &f.FareCents15m, &f.DistanceMilli15m))
			if weNull != nil {
				we = *weNull
				f.Zone, f.WindowEndMs = zone, we.UnixMilli()
			}
			joined[[2]int64{int64(zone), lt.UnixMilli()}] = f
		}
		must(rows.Err())
		var match, mismatch int
		for _, f := range samples {
			if joined[[2]int64{int64(f.Zone), f.WindowEndMs}] == f {
				match++
			} else {
				mismatch++
				fmt.Fprintf(os.Stderr, "skew: zone %d at %d: online %+v offline %+v\n", f.Zone, f.WindowEndMs, f, joined[[2]int64{int64(f.Zone), f.WindowEndMs}])
			}
		}
		emit(map[string]any{"online_samples": len(samples), "point_in_time_matches": match, "mismatches": mismatch})
		if mismatch > 0 || match == 0 {
			os.Exit(1)
		}
	case "dump":
		// A fingerprint of the online store's feature rows and partition
		// states, for comparing two independent runs.
		h := sha256.New()
		rows, _, err := sc.ZoneFeatures(ctx, nil)
		must(err)
		for _, r := range rows {
			fmt.Fprintf(h, "%+v\n", store.FeaturesFromProto(r))
		}
		var evs, late int64
		for _, p := range parts {
			ps, err := sc.PartitionState(ctx, p)
			must(err)
			st := normalize(store.StateFromProto(ps))
			fmt.Fprintf(h, "%+v\n", st)
			evs += st.Events
			late += st.LateDropped
		}
		emit(map[string]any{"zones": len(rows), "events": evs, "late_dropped": late, "sha256": hex.EncodeToString(h.Sum(nil))})
	default:
		fmt.Fprintln(os.Stderr, "unknown command", cmd)
		os.Exit(2)
	}
}

func normalize(s window.State) window.State {
	if len(s.Buckets) == 0 {
		s.Buckets = nil
	}
	return s
}

func waitForIdle(ctx context.Context, sc *storeclient.Client, parts []int32, d time.Duration) {
	prev := ""
	since := time.Now()
	for ctx.Err() == nil {
		var b strings.Builder
		for _, p := range parts {
			if ps, err := sc.PartitionState(ctx, p); err == nil {
				fmt.Fprintf(&b, "%d:%d,", p, ps.ToOffset)
			}
		}
		if cur := b.String(); cur != prev {
			prev, since = cur, time.Now()
		} else if time.Since(since) >= d {
			return
		}
		time.Sleep(time.Second)
	}
}

func emit(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		os.Exit(1)
	}
}
