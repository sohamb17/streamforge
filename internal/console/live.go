package console

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sohamb17/streamforge/internal/dockerapi"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/storeclient"
)

// Config for the live collector.
type Config struct {
	StoreAddrs  map[uint64]string
	AdminAddrs  map[uint64]string // node id -> host:port of the storenode admin HTTP
	WorkerAdmin []string          // host:port of worker admin HTTP endpoints
	Brokers     []string
	Topic       string
	Prometheus  string
	Project     string // compose project for container state and chaos
	Chaos       bool
	Log         *slog.Logger
}

// Live collects the snapshot from the running system.
type Live struct {
	cfg    Config
	sc     *storeclient.Client
	docker *dockerapi.Client

	mu       sync.Mutex
	snap     Snapshot
	prevSent map[uint64]map[uint64]uint64
	prevAt   time.Time
	logs     []LogEntry
	blocked  map[uint64][]uint64
	roles    map[uint64]string

	chaos  *chaosState
	checks *checks
}

// NewLive builds the collector.
func NewLive(cfg Config) (*Live, error) {
	sc, err := storeclient.New(cfg.StoreAddrs, "console")
	if err != nil {
		return nil, err
	}
	sc.PerAttemptTimeout = 400 * time.Millisecond
	l := &Live{
		cfg: cfg, sc: sc, docker: dockerapi.New("/var/run/docker.sock"),
		prevSent: map[uint64]map[uint64]uint64{}, blocked: map[uint64][]uint64{}, roles: map[uint64]string{},
	}
	l.snap.Mode = "live"
	l.snap.Chaos.Enabled = cfg.Chaos
	l.chaos = newChaos(l)
	l.checks = newChecks(l)
	return l, nil
}

// Run starts all collector loops.
func (l *Live) Run(ctx context.Context) {
	go l.every(ctx, 250*time.Millisecond, l.pollNodes)
	go l.every(ctx, time.Second, l.pollContainers)
	go l.every(ctx, 2*time.Second, l.pollPrometheus)
	go l.every(ctx, time.Second, l.pollZones)
	go l.checks.runLinz(ctx)
	go l.checks.runOracle(ctx)
	l.addLog("info", "console started")
}

func (l *Live) every(ctx context.Context, d time.Duration, f func(context.Context)) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		f(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Snapshot returns a copy of the current state.
func (l *Live) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.snap
	s.Now = time.Now().UnixMilli()
	s.Nodes = append([]Node(nil), s.Nodes...)
	s.Links = append([]Link(nil), s.Links...)
	s.Zones = append([]Zone(nil), s.Zones...)
	s.Workers = append([]Worker(nil), s.Workers...)
	s.Log = append([]LogEntry(nil), l.logs...)
	s.Checks = l.checks.view()
	s.Chaos = l.chaos.view()
	return s
}

func (l *Live) addLog(kind, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	l.mu.Lock()
	l.logs = append(l.logs, LogEntry{T: time.Now().UnixMilli(), Kind: kind, Msg: msg})
	if len(l.logs) > 60 {
		l.logs = l.logs[len(l.logs)-60:]
	}
	l.mu.Unlock()
	if l.cfg.Log != nil {
		l.cfg.Log.Info(msg, "kind", kind)
	}
}

func (l *Live) pollNodes(ctx context.Context) {
	now := time.Now()
	type res struct {
		id   uint64
		n    Node
		sent map[uint64]uint64
	}
	ids := l.sc.IDs()
	out := make([]res, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id uint64) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			st, err := l.sc.Status(cctx, id)
			r := res{id: id, n: Node{ID: id, State: "unreachable", Role: "unknown"}}
			if err == nil {
				r.n = Node{ID: id, State: "up", Role: st.Role, Term: st.Term, Lead: st.LeaderId, Commit: st.CommitIndex,
					Applied: st.AppliedIndex, LastIndex: st.LastLogIndex, SnapIndex: st.SnapshotIndex, SnapshotsInstalled: st.SnapshotsInstalled}
				r.sent = st.SentTo
			}
			out[i] = r
		}(i, id)
	}
	wg.Wait()

	l.mu.Lock()
	dt := now.Sub(l.prevAt).Seconds()
	containerState := map[uint64]string{}
	for _, n := range l.snap.Nodes {
		if n.State == "down" || n.State == "paused" {
			containerState[n.ID] = n.State
		}
	}
	var nodes []Node
	var links []Link
	var leader, term uint64
	for _, r := range out {
		n := r.n
		if n.State != "up" {
			if cs, ok := containerState[n.ID]; ok {
				n.State = cs
			}
		}
		n.Blocked = l.blocked[n.ID]
		nodes = append(nodes, n)
		if n.State == "up" && n.Role == "leader" && n.Term >= term {
			leader, term = n.ID, n.Term
		}
		if n.Term > term && n.State == "up" {
			term = n.Term
		}
		if r.sent != nil {
			prev := l.prevSent[r.id]
			for to, c := range r.sent {
				rate := 0.0
				if prev != nil && dt > 0 && c >= prev[to] {
					rate = float64(c-prev[to]) / dt
				}
				links = append(links, Link{From: r.id, To: to, Rate: math.Round(rate*10) / 10, Blocked: contains(l.blocked[r.id], to)})
			}
			l.prevSent[r.id] = r.sent
		}
		// Role-change log.
		key := n.Role
		if n.State != "up" {
			key = n.State
		}
		if old, ok := l.roles[n.ID]; ok && old != key {
			if key == "leader" {
				l.logs = append(l.logs, LogEntry{T: now.UnixMilli(), Kind: "raft", Msg: fmt.Sprintf("node %d became leader (term %d)", n.ID, n.Term)})
			} else if key == "down" || key == "paused" || key == "unreachable" {
				l.logs = append(l.logs, LogEntry{T: now.UnixMilli(), Kind: "raft", Msg: fmt.Sprintf("node %d is %s", n.ID, key)})
			} else if old == "down" || old == "paused" || old == "unreachable" {
				l.logs = append(l.logs, LogEntry{T: now.UnixMilli(), Kind: "raft", Msg: fmt.Sprintf("node %d is back as %s", n.ID, key)})
			}
		}
		l.roles[n.ID] = key
	}
	sort.Slice(links, func(i, j int) bool {
		if links[i].From != links[j].From {
			return links[i].From < links[j].From
		}
		return links[i].To < links[j].To
	})
	l.prevAt = now
	l.snap.Nodes, l.snap.Links, l.snap.Leader, l.snap.Term = nodes, links, leader, term
	l.mu.Unlock()
	l.chaos.observeLeader(leader, now)
}

func contains(xs []uint64, x uint64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func (l *Live) pollContainers(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cs, err := l.docker.List(cctx, l.cfg.Project)
	if err != nil {
		return
	}
	var workers []Worker
	nodeState := map[uint64]string{}
	for _, c := range cs {
		svc := c.Service()
		state := "up"
		switch c.State {
		case "running":
		case "paused":
			state = "paused"
		default:
			state = "down"
		}
		if strings.HasPrefix(svc, "storenode") {
			if id, err := strconv.ParseUint(strings.TrimPrefix(svc, "storenode"), 10, 64); err == nil {
				nodeState[id] = state
			}
		}
		if strings.HasPrefix(svc, "worker") {
			workers = append(workers, Worker{Name: svc, State: state})
		}
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].Name < workers[j].Name })
	l.mu.Lock()
	for i := range l.snap.Nodes {
		if s, ok := nodeState[l.snap.Nodes[i].ID]; ok && s != "up" {
			l.snap.Nodes[i].State = s
		}
	}
	l.snap.Workers = workers
	l.mu.Unlock()
}

func (l *Live) prom(ctx context.Context, expr string) float64 {
	u := l.cfg.Prometheus + "/api/v1/query?query=" + url.QueryEscape(expr)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
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
		return 0
	}
	s, _ := r.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func (l *Live) pollPrometheus(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	q := func(e string) float64 { return l.prom(cctx, e) }
	in := Ingest{
		EventsPerSec:      q(`sum(rate(streamforge_worker_events_total[30s]))`),
		EventsTotal:       q(`sum(streamforge_worker_events_total)`),
		Lag:               q(`sum(streamforge_worker_consumer_lag)`),
		LateDropped:       q(`sum(streamforge_worker_late_dropped)`),
		DuplicatesIgnored: q(`sum(streamforge_worker_batches_total{outcome="duplicate"})`),
		RecordsSkipped:    q(`sum(streamforge_worker_records_skipped_total)`),
		FreshnessP50Ms:    1000 * q(`histogram_quantile(0.5, sum(rate(streamforge_freshness_seconds_bucket[2m])) by (le))`),
		FreshnessP99Ms:    1000 * q(`histogram_quantile(0.99, sum(rate(streamforge_freshness_seconds_bucket[2m])) by (le))`),
		CommitP99Ms:       1000 * q(`histogram_quantile(0.99, sum(rate(streamforge_raft_commit_latency_seconds_bucket[1m])) by (le))`),
		BatchesPerSec:     q(`sum(rate(streamforge_worker_batches_total[30s]))`),
	}
	sv := Serving{
		RPS:      q(`sum(rate(streamforge_serving_requests_total[30s]))`),
		P50Ms:    1000 * q(`histogram_quantile(0.5, sum(rate(streamforge_serving_latency_seconds_bucket[1m])) by (le))`),
		P99Ms:    1000 * q(`histogram_quantile(0.99, sum(rate(streamforge_serving_latency_seconds_bucket[1m])) by (le))`),
		HitRatio: q(`sum(rate(streamforge_serving_cache_lookups_total{result="hit"}[1m])) / sum(rate(streamforge_serving_cache_lookups_total[1m]))`),
	}
	speed := q(`max(streamforge_replayer_speedup)`)
	l.mu.Lock()
	l.snap.Ingest, l.snap.Serving, l.snap.Speedup = in, sv, speed
	l.mu.Unlock()
}

func (l *Live) pollZones(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	rows, _, err := l.sc.ZoneFeatures(cctx, nil)
	if err != nil {
		return
	}
	zones := make([]Zone, 0, len(rows))
	var maxT int64
	for _, r := range rows {
		f := store.FeaturesFromProto(r)
		zones = append(zones, ZoneFrom(f))
		if f.WindowEndMs > maxT {
			maxT = f.WindowEndMs
		}
	}
	l.mu.Lock()
	l.snap.Zones = zones
	if maxT > 0 {
		l.snap.EventTimeMs = maxT
	}
	l.mu.Unlock()
}

// Chaos runs a fault-injection action on behalf of a visitor.
func (l *Live) Chaos(ctx context.Context, action, ip string) error {
	return l.chaos.Do(ctx, action, ip)
}
