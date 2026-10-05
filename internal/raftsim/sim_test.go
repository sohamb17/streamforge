package raftsim

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/linz"
	"github.com/sohamb17/streamforge/internal/raft"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

func genEvents(rng *rand.Rand, n int) []window.Event {
	evs := make([]window.Event, n)
	t := int64(1_700_000_000_000)
	for i := range evs {
		t += int64(rng.Intn(9000))
		et := t
		if rng.Intn(10) == 0 {
			et -= int64(rng.Intn(60_000))
		}
		evs[i] = window.Event{Zone: int32(1 + rng.Intn(12)), EventTimeMs: et, FareCents: int64(rng.Intn(6000)), DistanceMilli: int64(rng.Intn(9000)), Offset: int64(i)}
	}
	return evs
}

func latestPerZone(evs []window.Event) []window.Features {
	p := window.NewPartition(0, window.DefaultConfig())
	last := map[int32]window.Features{}
	for _, e := range evs {
		p.Add(e)
		for _, f := range p.Flush().Features {
			last[f.Zone] = f
		}
	}
	sm := store.New(window.DefaultBucketMs)
	_ = sm
	out := make([]window.Features, 0, len(last))
	for z := int32(0); z < 100; z++ {
		if f, ok := last[z]; ok {
			out = append(out, f)
		}
	}
	return out
}

func seeds(t *testing.T) int {
	if s := os.Getenv("RAFTSIM_SEEDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if testing.Short() {
		return 10
	}
	return 60
}

// TestRandomizedFaults is the main safety test. Each seed runs a 5-node
// cluster through random crashes, restarts, partitions and message loss
// while register clients and a stream worker run against it, then checks:
// election safety, state machine safety, convergence after healing,
// linearizability of the client history (Porcupine), and that the online
// features equal a sequential batch recomputation (the oracle).
func TestRandomizedFaults(t *testing.T) {
	n := seeds(t)
	for seed := int64(1); seed <= int64(n); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runScenario(t, seed, normalChurn) })
	}
}

// TestRandomizedHeavyChurn crashes and restarts nodes several times more
// often, on a 3-node cluster, to reach rare interleavings such as the
// "Figure 8" scenario from the Raft paper (a leader committing an entry from
// an older term by counting replicas, which a later leader then overwrites).
func TestRandomizedHeavyChurn(t *testing.T) {
	n := seeds(t)
	for seed := int64(1); seed <= int64(n); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runScenario(t, seed, heavyChurn) })
	}
}

type churn struct {
	nodes                      int
	crash, restart, part, heal int // per-mille thresholds (cumulative)
	maxDown                    int
}

var (
	normalChurn = churn{nodes: 5, crash: 8, restart: 25, part: 32, heal: 45, maxDown: 2}
	heavyChurn  = churn{nodes: 3, crash: 40, restart: 110, part: 125, heal: 150, maxDown: 2}
)

func runScenario(t *testing.T, seed int64, ch churn) {
	rng := rand.New(rand.NewSource(seed * 1009))
	cfg := DefaultConfig(seed)
	cfg.N = ch.nodes
	cfg.DropRate = rng.Float64() * 0.05
	cfg.MaxDelay = 1 + rng.Intn(3)
	cfg.SnapshotEvery = uint64(30 + rng.Intn(100))
	c := New(cfg)
	clk := &Clock{}
	var clients []*Client
	for i := 0; i < 4; i++ {
		clients = append(clients, &Client{ID: i, Name: fmt.Sprintf("c%d", i)})
	}
	evs := genEvents(rng, 2500)
	w := NewWorker(0, &evs, 15)
	w.CrashRate = 200

	const faultTicks = 2500
	for i := 0; i < faultTicks; i++ {
		switch r := rng.Intn(1000); {
		case r < ch.crash:
			down := 0
			for _, n := range c.Nodes {
				if !n.Up {
					down++
				}
			}
			if down < ch.maxDown || rng.Intn(4) == 0 {
				c.Crash(c.ids[rng.Intn(len(c.ids))])
			}
		case r < ch.restart:
			c.Restart(c.ids[rng.Intn(len(c.ids))])
		case r < ch.part:
			perm := rng.Perm(len(c.ids))
			k := 1 + rng.Intn(len(c.ids)-1)
			var a, b []uint64
			for j, p := range perm {
				if j < k {
					a = append(a, c.ids[p])
				} else {
					b = append(b, c.ids[p])
				}
			}
			c.Partition(a, b)
		case r < ch.heal:
			c.Heal()
		}
		for _, cl := range clients {
			cl.Tick(c, rng, clk, true)
		}
		w.Tick(c, rng)
		c.Step()
	}
	// Heal everything and let the system finish.
	c.Heal()
	c.SetDropRate(0)
	for _, id := range c.ids {
		c.Restart(id)
	}
	for i := 0; i < 4000 && !(w.Done() && allIdle(clients) && c.Converged() && i > 200); i++ {
		for _, cl := range clients {
			cl.Tick(c, rng, clk, false)
		}
		w.Tick(c, rng)
		c.Step()
	}

	if len(c.Violations) > 0 {
		t.Fatalf("safety violations: %v", c.Violations)
	}
	if !c.Converged() || !c.StatesEqual() {
		t.Fatalf("cluster did not converge after healing")
	}
	if !w.Done() {
		t.Fatalf("worker did not finish: offset=%d/%d restoring=%v", w.Offset(), len(evs), w.restoring)
	}
	want := latestPerZone(evs)
	for _, id := range c.ids {
		got := c.Nodes[id].SM.Features(nil)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d features differ from oracle (%d vs %d rows)", id, len(got), len(want))
		}
	}
	var hist []porcupine.Operation
	completed := 0
	for _, cl := range clients {
		hist = append(hist, cl.History...)
		for _, op := range cl.History {
			if op.Return != linz.Infinity {
				completed++
			}
		}
	}
	if completed < 20 {
		t.Fatalf("too little progress: %d completed ops", completed)
	}
	res, info := porcupine.CheckOperationsVerbose(linz.Model, hist, 30*time.Second)
	if res != porcupine.Ok {
		f, _ := os.CreateTemp("", "linz-*.html")
		_ = porcupine.Visualize(linz.Model, info, f)
		t.Fatalf("history not linearizable (%v); visualization: %s", res, f.Name())
	}
	t.Logf("seed %d: %d ops (%d completed), %d worker restores, %d duplicate batches ignored, %d elections, final term %d",
		seed, len(hist), completed, w.Restores, c.Nodes[1].SM.DuplicateBatches, totalElections(c), c.Nodes[c.Leader()].Core.Status().Term)
}

func allIdle(cs []*Client) bool {
	for _, c := range cs {
		if !c.Idle() {
			return false
		}
	}
	return true
}

func totalElections(c *Cluster) uint64 {
	var n uint64
	for _, node := range c.Nodes {
		n += node.Core.ElectionsStarted
	}
	return n
}

// TestLeaderCrashElectsNewLeader is a basic liveness check.
func TestLeaderCrashElectsNewLeader(t *testing.T) {
	c := New(DefaultConfig(42))
	for i := 0; i < 100; i++ {
		c.Step()
	}
	l := c.Leader()
	if l == 0 {
		t.Fatal("no leader elected")
	}
	c.Crash(l)
	for i := 0; i < 100; i++ {
		c.Step()
	}
	if nl := c.Leader(); nl == 0 || nl == l {
		t.Fatalf("no new leader after crash (old %d new %d)", l, nl)
	}
}

// TestMinorityCannotCommit partitions the leader with one follower: the
// pair must not commit anything, the majority side must elect and commit.
func TestMinorityCannotCommit(t *testing.T) {
	c := New(DefaultConfig(7))
	for i := 0; i < 100; i++ {
		c.Step()
	}
	l := c.Leader()
	var other uint64
	var majority []uint64
	for _, id := range c.ids {
		if id == l {
			continue
		}
		if other == 0 {
			other = id
		} else {
			majority = append(majority, id)
		}
	}
	c.Partition([]uint64{l, other}, majority)
	committedOnMinority := false
	_ = c.Propose(l, []byte{}, func(_ store.Result, ok bool) { committedOnMinority = ok })
	for i := 0; i < 200; i++ {
		c.Step()
	}
	if committedOnMinority {
		t.Fatal("minority side committed a write")
	}
	if c.Nodes[l].Core.Status().Role == raft.Leader {
		t.Fatal("old leader did not step down (CheckQuorum)")
	}
	nl := c.Leader()
	if nl == 0 || nl == l || nl == other {
		t.Fatalf("majority side has no leader: %d", nl)
	}
	ok := false
	_ = c.Propose(nl, []byte{}, func(_ store.Result, r bool) { ok = r })
	for i := 0; i < 50; i++ {
		c.Step()
	}
	if !ok {
		t.Fatal("majority side could not commit")
	}
	c.Heal()
	for i := 0; i < 200; i++ {
		c.Step()
	}
	if !c.Converged() || len(c.Violations) > 0 {
		t.Fatalf("no convergence after heal: %v", c.Violations)
	}
}

// TestDeposedLeaderCannotServeStaleReads isolates the leader. Once the
// majority has elected a new leader and committed a newer value, a ReadIndex
// read on the old leader must never succeed: it cannot collect heartbeat
// acknowledgements from a majority. Reading the old leader's local state
// without that check would return the stale value.
func TestDeposedLeaderCannotServeStaleReads(t *testing.T) {
	overlaps := 0
	for seed := int64(1); seed <= 200; seed++ {
		c := New(DefaultConfig(seed))
		for i := 0; i < 60; i++ {
			c.Step()
		}
		old := c.Leader()
		if old == 0 {
			continue
		}
		put := func(id uint64, v string) bool {
			data, _ := store.EncodeCommand(&sfv1.Command{Op: &sfv1.Command_Put{Put: &sfv1.PutOp{Key: "x", Value: v}}})
			ok := false
			if c.Propose(id, data, func(_ store.Result, r bool) { ok = r }) != nil {
				return false
			}
			for i := 0; i < 20 && !ok; i++ {
				c.Step()
			}
			return ok
		}
		if !put(old, "old") {
			continue
		}
		var rest []uint64
		for _, id := range c.ids {
			if id != old {
				rest = append(rest, id)
			}
		}
		c.Partition([]uint64{old}, rest)
		var nl uint64
		for i := 0; i < 60 && nl == 0; i++ {
			c.Step()
			if l := c.Leader(); l != 0 && l != old {
				nl = l
			}
		}
		if nl == 0 || !put(nl, "new") {
			continue
		}
		if c.Nodes[old].Core.Status().Role != raft.Leader {
			continue // old leader already stepped down; no overlap to test
		}
		overlaps++
		readOK := false
		if err := c.Read(old, func(ok bool) { readOK = ok }); err != nil {
			continue
		}
		for i := 0; i < 60; i++ {
			c.Step()
			if readOK {
				v, _ := c.Nodes[old].SM.Get("x")
				t.Fatalf("seed %d: deposed leader %d served a read (value %q) after %d committed \"new\"", seed, old, v, nl)
			}
		}
	}
	if overlaps == 0 {
		t.Fatal("no seed produced two simultaneous leaders; test is vacuous")
	}
	t.Logf("%d seeds had an old and a new leader at the same time; none served a stale read", overlaps)
}
