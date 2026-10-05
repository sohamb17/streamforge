// Command loadgen is an open-loop (constant-rate) load generator for
// FeatureService.GetFeatures.
//
// Request i is scheduled at start + i/rate and sent at that time whether or
// not earlier requests have finished. Latency is measured from the
// scheduled time, not from when the request was actually sent, so a server
// stall shows up as latency for every request that should have been sent
// during the stall. That avoids coordinated omission, which a closed-loop
// client (send, wait, send) suffers from: it stops sending exactly when the
// server is slow, and so never records the slow period.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
)

type sample struct {
	sec        int // second since start (scheduled time)
	latUs      int64
	code       string
	servedFrom sfv1.ServedFrom
	measured   bool
}

func main() {
	target := flag.String("target", "localhost:50051", "FeatureService address")
	rate := flag.Float64("rate", 1000, "requests per second (open loop)")
	duration := flag.Duration("duration", 60*time.Second, "measured duration")
	warmup := flag.Duration("warmup", 10*time.Second, "warm-up before measuring (not recorded)")
	batch := flag.Int("batch", 10, "entities per request")
	mode := flag.String("mode", "bounded", "bounded | linearizable")
	deadline := flag.Duration("deadline", 100*time.Millisecond, "per-request deadline")
	conns := flag.Int("conns", 4, "gRPC connections")
	zipf := flag.Float64("zipf", 1.2, "zone popularity skew (Zipf s; <=1 uniform)")
	out := flag.String("out", "", "write the JSON report here as well")
	label := flag.String("label", "", "free-form label stored in the report")
	flag.Parse()

	clients := make([]sfv1.FeatureServiceClient, *conns)
	for i := range clients {
		cc, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		clients[i] = sfv1.NewFeatureServiceClient(cc)
	}
	readMode := sfv1.ReadMode_READ_MODE_BOUNDED_STALENESS
	if *mode == "linearizable" {
		readMode = sfv1.ReadMode_READ_MODE_LINEARIZABLE
	}

	// Precompute requests so generating them costs nothing on the hot path.
	rng := rand.New(rand.NewSource(1))
	var z *rand.Zipf
	if *zipf > 1 {
		z = rand.NewZipf(rng, *zipf, 1, 262)
	}
	pool := make([][]string, 4096)
	for i := range pool {
		ids := make([]string, *batch)
		for j := range ids {
			v := rng.Intn(263)
			if z != nil {
				v = int(z.Uint64())
			}
			ids[j] = strconv.Itoa(v + 1)
		}
		pool[i] = ids
	}

	total := int64(math.Round(*rate * (warmup.Seconds() + duration.Seconds())))
	interval := time.Duration(float64(time.Second) / *rate)
	results := make(chan sample, 65536)
	var inflight, maxInflight atomic.Int64
	var wg sync.WaitGroup

	hAll := hdrhistogram.New(1, 60_000_000, 3)
	hBy := map[sfv1.ServedFrom]*hdrhistogram.Histogram{}
	codes := map[string]int64{}
	errBySec := map[int]int64{}
	var measured int64
	collectDone := make(chan struct{})
	go func() {
		for s := range results {
			if !s.measured {
				continue
			}
			measured++
			codes[s.code]++
			if s.code != "OK" {
				errBySec[s.sec]++
				// Failed requests still count in the overall latency (a
				// deadline miss is a slow answer), but not per source.
				hAll.RecordValue(s.latUs)
				continue
			}
			hAll.RecordValue(s.latUs)
			h, ok := hBy[s.servedFrom]
			if !ok {
				h = hdrhistogram.New(1, 60_000_000, 3)
				hBy[s.servedFrom] = h
			}
			h.RecordValue(s.latUs)
		}
		close(collectDone)
	}()

	fmt.Fprintf(os.Stderr, "loadgen: %.0f req/s x %d entities, %s, deadline %v, warm-up %v, measure %v\n",
		*rate, *batch, *mode, *deadline, *warmup, *duration)
	start := time.Now()
	measureFrom := start.Add(*warmup)
	var lateSends int64
	for i := int64(0); i < total; i++ {
		intended := start.Add(time.Duration(i) * interval)
		// Sleep, never spin: a busy-wait here burned a whole core and
		// starved the system under test on a 2-vCPU host. Timer slack only
		// delays the send; latency is still measured from `intended`, so
		// any slack is counted against the server, not hidden.
		if d := time.Until(intended); d > 0 {
			time.Sleep(d)
		} else if -d > 10*time.Millisecond {
			lateSends++ // the generator itself fell behind
		}
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		wg.Add(1)
		go func(i int64, intended time.Time) {
			defer wg.Done()
			defer inflight.Add(-1)
			ctx, cancel := context.WithDeadline(context.Background(), intended.Add(*deadline))
			resp, err := clients[i%int64(len(clients))].GetFeatures(ctx, &sfv1.GetFeaturesRequest{
				FeatureView: "zone_demand_v1", EntityIds: pool[i%int64(len(pool))], Mode: readMode})
			cancel()
			s := sample{sec: int(intended.Sub(start).Seconds()), latUs: time.Since(intended).Microseconds(), code: status.Code(err).String(), measured: !intended.Before(measureFrom)}
			if err == nil {
				s.servedFrom = resp.ServedFrom
			}
			results <- s
		}(i, intended)
	}
	sendSpan := time.Since(start)
	wg.Wait()
	close(results)
	<-collectDone

	q := func(h *hdrhistogram.Histogram) map[string]float64 {
		return map[string]float64{
			"count":  float64(h.TotalCount()),
			"p50_ms": float64(h.ValueAtQuantile(50)) / 1000, "p90_ms": float64(h.ValueAtQuantile(90)) / 1000,
			"p99_ms": float64(h.ValueAtQuantile(99)) / 1000, "p999_ms": float64(h.ValueAtQuantile(99.9)) / 1000,
			"max_ms": float64(h.Max()) / 1000, "mean_ms": h.Mean() / 1000,
		}
	}
	by := map[string]any{}
	for k, h := range hBy {
		by[k.String()] = q(h)
	}
	host, _ := os.Hostname()
	report := map[string]any{
		"label": *label, "host": host, "cpus": runtime.NumCPU(),
		"target": *target, "mode": *mode, "rate_rps": *rate, "batch": *batch, "deadline_ms": deadline.Milliseconds(),
		"zipf": *zipf, "conns": *conns, "warmup_s": warmup.Seconds(), "duration_s": duration.Seconds(),
		"open_loop":         true,
		"latency_from":      "scheduled send time (coordinated-omission corrected)",
		"requests":          measured,
		"codes":             codes,
		"errors_by_second":  errBySec,
		"overall":           q(hAll),
		"by_served_from":    by,
		"max_inflight":      maxInflight.Load(),
		"late_sends_10ms":   lateSends,
		"achieved_send_rps": float64(total) / sendSpan.Seconds(),
		"finished_at":       time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		os.WriteFile(*out, b, 0o644)
	}
}
