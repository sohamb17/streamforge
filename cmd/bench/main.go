// Command bench measures a running StreamForge stack over a fixed window.
//
//	bench measure -duration 10m -rate 8000 -label tlc-8k -out run.json
//
// It reads Kafka log-end offsets at the start and end of the window (events
// published), Prometheus for everything the services export (events
// consumed, consumer lag, freshness, Raft commit latency), and the Docker
// API for CPU seconds per container. A run counts as sustained only if the
// consumer lag did not grow: the least-squares slope of total lag over the
// window must stay under 1% of the offered rate.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sohamb17/streamforge/internal/dockerapi"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "report" {
		rf := flag.NewFlagSet("report", flag.ExitOnError)
		dir := rf.String("dir", "bench/results/gate4", "results directory")
		rf.Parse(os.Args[2:])
		must(writeReport(*dir))
		return
	}
	if len(os.Args) < 2 || os.Args[1] != "measure" {
		fmt.Fprintln(os.Stderr, "usage: bench measure|report [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("measure", flag.ExitOnError)
	prom := fs.String("prom", "http://prometheus:9090", "Prometheus URL")
	brokers := fs.String("brokers", "kafka:9092", "Kafka")
	topic := fs.String("topic", "trips", "topic")
	project := fs.String("project", "streamforge", "compose project (for CPU accounting)")
	dur := fs.Duration("duration", 10*time.Minute, "measurement window")
	rate := fs.Float64("rate", 0, "offered event rate (for the report and the stability check)")
	label := fs.String("label", "", "label")
	source := fs.String("source", "tlc", "data source used by the replayer")
	out := fs.String("out", "", "write JSON here too")
	fs.Parse(os.Args[2:])

	ctx := context.Background()
	kcl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	must(err)
	adm := kadm.NewClient(kcl)
	dk := dockerapi.New("/var/run/docker.sock")

	endOffsets := func() int64 {
		o, err := adm.ListEndOffsets(ctx, *topic)
		must(err)
		var s int64
		o.Each(func(lo kadm.ListedOffset) { s += lo.Offset })
		return s
	}
	cpu := func() map[string]float64 {
		out := map[string]float64{}
		cs, err := dk.List(ctx, *project)
		if err != nil {
			return out
		}
		for _, c := range cs {
			if c.State != "running" {
				continue
			}
			if st, err := dk.Stats(ctx, c.ID); err == nil {
				out[strings.TrimPrefix(c.Names[0], "/")] = float64(st.CPU.Usage.Total) / 1e9
			}
		}
		return out
	}

	fmt.Fprintf(os.Stderr, "bench: measuring %v (offered %.0f ev/s)\n", *dur, *rate)
	t0 := time.Now()
	pub0 := endOffsets()
	cpu0 := cpu()
	var lagT, lagV []float64
	for time.Since(t0) < *dur {
		if v, ok := instant(*prom, `sum(streamforge_worker_consumer_lag)`, time.Now()); ok {
			lagT = append(lagT, time.Since(t0).Seconds())
			lagV = append(lagV, v)
		}
		time.Sleep(5 * time.Second)
	}
	t1 := time.Now()
	pub1 := endOffsets()
	cpu1 := cpu()
	// Let Prometheus scrape the final state before querying the window.
	time.Sleep(6 * time.Second)
	win := strconv.Itoa(int(t1.Sub(t0).Seconds())) + "s"
	at := t1
	q := func(expr string) float64 {
		v, _ := instant(*prom, strings.ReplaceAll(expr, "$W", win), at)
		return v
	}
	hq := func(metric string, qq float64) float64 {
		return q(fmt.Sprintf(`histogram_quantile(%g, sum(increase(%s_bucket[$W])) by (le))`, qq, metric))
	}

	secs := t1.Sub(t0).Seconds()
	consumed := q(`sum(increase(streamforge_worker_events_total[$W]))`)
	slope, intercept := linreg(lagT, lagV)
	maxLag := 0.0
	for _, v := range lagV {
		maxLag = math.Max(maxLag, v)
	}
	cpuSec := map[string]float64{}
	var cpuTotal float64
	for k, v := range cpu1 {
		if v0, ok := cpu0[k]; ok {
			cpuSec[k] = round((v - v0) / secs) // average cores used
			cpuTotal += (v - v0) / secs
		}
	}
	report := map[string]any{
		"label": *label, "source": *source, "offered_rate_eps": *rate,
		"window_s": round(secs), "started": t0.UTC().Format(time.RFC3339),
		"host_cpus":                runtime.NumCPU(),
		"published_events":         pub1 - pub0,
		"published_rate_eps":       round(float64(pub1-pub0) / secs),
		"consumed_rate_eps":        round(consumed / secs),
		"lag_first":                first(lagV),
		"lag_last":                 last(lagV),
		"lag_max":                  maxLag,
		"lag_slope_eps":            round(slope),
		"lag_intercept":            round(intercept),
		"sustained":                *rate > 0 && slope <= 0.01**rate,
		"freshness_p50_ms":         round(hq("streamforge_freshness_seconds", 0.5) * 1000),
		"freshness_p99_ms":         round(hq("streamforge_freshness_seconds", 0.99) * 1000),
		"raft_commit_p50_ms":       round(hq("streamforge_raft_commit_latency_seconds", 0.5) * 1000),
		"raft_commit_p99_ms":       round(hq("streamforge_raft_commit_latency_seconds", 0.99) * 1000),
		"raft_persist_p99_ms":      round(hq("streamforge_raft_persist_seconds", 0.99) * 1000),
		"propose_rtt_p99_ms":       round(hq("streamforge_worker_propose_latency_seconds", 0.99) * 1000),
		"raft_proposals_committed": q(`sum(increase(streamforge_raft_proposals_total{outcome="committed"}[$W]))`),
		"batches_applied":          q(`sum(increase(streamforge_worker_batches_total{outcome="applied"}[$W]))`),
		"feature_rows_committed":   q(`sum(increase(streamforge_worker_feature_rows_total[$W]))`),
		"late_dropped_total":       q(`sum(streamforge_worker_late_dropped)`),
		"cpu_cores_by_container":   cpuSec,
		"cpu_cores_total":          round(cpuTotal),
		"quantile_note":            "Prometheus histogram_quantile over the window: linear interpolation inside exponential buckets (0.25 ms x 2^k), so values are bucket-resolution estimates.",
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		must(os.WriteFile(*out, b, 0o644))
	}
}

func instant(prom, expr string, at time.Time) (float64, bool) {
	u := prom + "/api/v1/query?query=" + url.QueryEscape(expr) + "&time=" + strconv.FormatFloat(float64(at.UnixMilli())/1000, 'f', 3, 64)
	resp, err := http.Get(u)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	var r struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.NewDecoder(resp.Body).Decode(&r) != nil || len(r.Data.Result) == 0 {
		return 0, false
	}
	s, _ := r.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) {
		return 0, false
	}
	return v, true
}

func linreg(x, y []float64) (slope, intercept float64) {
	n := float64(len(x))
	if n < 2 {
		return 0, 0
	}
	var sx, sy, sxx, sxy float64
	for i := range x {
		sx += x[i]
		sy += y[i]
		sxx += x[i] * x[i]
		sxy += x[i] * y[i]
	}
	d := n*sxx - sx*sx
	if d == 0 {
		return 0, sy / n
	}
	slope = (n*sxy - sx*sy) / d
	return slope, (sy - slope*sx) / n
}

func first(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	return v[0]
}

func last(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1]
}

func round(v float64) float64 { return math.Round(v*100) / 100 }

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}
