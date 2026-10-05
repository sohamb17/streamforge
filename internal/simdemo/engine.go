// Package simdemo is the in-browser version of StreamForge: the real Raft
// core, store state machine, windowing code and stream workers, running a
// 5-node cluster on a simulated clock and network inside one process
// (compiled to WebAssembly for the GitHub Pages demo). It replays an
// embedded slice of real NYC TLC trips and produces the same
// console.Snapshot as the live dashboard backend.
//
// What is simulated: the network (delays, loss, partitions), process
// crashes and pauses, and time. What is real: every line of consensus,
// state machine, windowing and recovery logic.
package simdemo

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/sohamb17/streamforge/internal/linz"
	"github.com/sohamb17/streamforge/internal/raftsim"
	console "github.com/sohamb17/streamforge/internal/snapshot"
	"github.com/sohamb17/streamforge/internal/window"
)

//go:embed trips.bin.gz
var tripsGz []byte

// Simulation constants.
const (
	TickMs     = 40 // simulated milliseconds per Raft tick
	Partitions = 3
	linzEvery  = 12_000 // simulated ms per linearizability window
)

type trip struct {
	t                    int64
	zone                 int32
	fareCents, distMilli int64
}

func loadTrips() ([]trip, error) {
	zr, err := gzip.NewReader(bytes.NewReader(tripsGz))
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(raw)
	var base int64
	if err := binary.Read(r, binary.LittleEndian, &base); err != nil {
		return nil, err
	}
	var out []trip
	t := base
	for r.Len() > 0 {
		dt, err1 := binary.ReadUvarint(r)
		z, err2 := binary.ReadUvarint(r)
		f, err3 := binary.ReadUvarint(r)
		d, err4 := binary.ReadUvarint(r)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			return nil, fmt.Errorf("simdemo: corrupt trips data")
		}
		t += int64(dt) * 1000
		out = append(out, trip{t: t, zone: int32(z), fareCents: int64(f), distMilli: int64(d) * 10})
	}
	return out, nil
}

type shadow struct {
	w *window.Partition
}

type pending struct {
	release int64
	e       window.Event
	part    int
}

// Engine is a running simulation.
type Engine struct {
	c       *raftsim.Cluster
	rng     *rand.Rand
	clk     *raftsim.Clock
	clients []*raftsim.Client
	workers []*raftsim.Worker
	logs    [Partitions][]window.Event
	shadows [Partitions]*shadow
	rows    map[int32][]window.Features

	trips     []trip
	tripIdx   int
	loopN     int64
	loopShift int64
	late      []pending

	SimMs     int64   // simulated wall time
	EventTime int64   // current replay event time
	Speedup   float64 // event-time ms per simulated ms
	tickAcc   float64

	sentCount  map[[2]uint64]float64
	linkRate   map[[2]uint64]float64
	rateMark   int64
	evCount    float64
	evRate     float64
	logs2      []console.LogEntry
	roles      map[uint64]string
	linz       []console.LinzWindow
	linzStart  int64
	linzClose  bool
	linzSeq    int
	chaosSeen  bool
	oracle     console.Oracle
	oracleMark int64

	active        *console.ChaosAction
	healFn        func()
	cooldownUntil int64
	failWait      int64
	failOld       uint64
	lastFailover  int64
	dropUntil     int64
}

// New starts a simulation.
func New(seed int64) (*Engine, error) {
	trips, err := loadTrips()
	if err != nil {
		return nil, err
	}
	cfg := raftsim.DefaultConfig(seed)
	cfg.MaxDelay = 3
	cfg.SnapshotEvery = 300
	e := &Engine{
		c: raftsim.New(cfg), rng: rand.New(rand.NewSource(seed)), clk: &raftsim.Clock{},
		trips: trips, Speedup: 60, rows: map[int32][]window.Features{},
		sentCount: map[[2]uint64]float64{}, linkRate: map[[2]uint64]float64{}, roles: map[uint64]string{},
	}
	e.loopShift = trips[len(trips)-1].t - trips[0].t + 60_000
	e.EventTime = trips[0].t
	e.c.OnMessage = func(ev raftsim.MsgEvent) {
		if !ev.Dropped {
			e.sentCount[[2]uint64{ev.Msg.From, ev.Msg.To}]++
		}
	}
	for p := 0; p < Partitions; p++ {
		w := raftsim.NewWorker(int32(p), &e.logs[p], 40)
		e.workers = append(e.workers, w)
		e.shadows[p] = &shadow{w: window.NewPartition(int32(p), window.DefaultConfig())}
	}
	for i := 0; i < 2; i++ {
		e.clients = append(e.clients, &raftsim.Client{ID: i, Name: fmt.Sprintf("probe%d", i), Keys: 3, KeyPrefix: "w0/"})
	}
	e.log("info", "simulated 5-node cluster started; replaying %d real TLC trips at %.0fx", len(trips), e.Speedup)
	return e, nil
}

func (e *Engine) log(kind, format string, a ...any) {
	e.logs2 = append(e.logs2, console.LogEntry{T: e.SimMs, Kind: kind, Msg: fmt.Sprintf(format, a...)})
	if len(e.logs2) > 60 {
		e.logs2 = e.logs2[len(e.logs2)-60:]
	}
}

// Advance runs the simulation for simMs simulated milliseconds.
func (e *Engine) Advance(simMs float64) {
	if simMs > 2000 {
		simMs = 2000 // a backgrounded tab must not stall the page on return
	}
	e.tickAcc += simMs
	for e.tickAcc >= TickMs {
		e.tickAcc -= TickMs
		e.tick()
	}
}

func (e *Engine) tick() {
	e.SimMs += TickMs
	// 1. New events arrive, keyed by zone into partitions.
	e.EventTime += int64(float64(TickMs) * e.Speedup)
	for {
		tr := e.trips[e.tripIdx]
		t := tr.t + e.loopN*e.loopShift
		if t > e.EventTime {
			break
		}
		p := int(tr.zone) % Partitions
		ev := window.Event{Zone: tr.zone, EventTimeMs: t, FareCents: tr.fareCents, DistanceMilli: tr.distMilli, PublishMs: e.SimMs}
		if e.rng.Intn(500) == 0 {
			// Published late: up to 90 s of event time after it happened.
			e.late = append(e.late, pending{release: t + int64(e.rng.Intn(90_000)), e: ev, part: p})
		} else {
			e.publish(p, ev)
		}
		e.tripIdx++
		if e.tripIdx == len(e.trips) {
			e.tripIdx = 0
			e.loopN++
		}
	}
	keep := e.late[:0]
	for _, l := range e.late {
		if l.release <= e.EventTime {
			e.publish(l.part, l.e)
		} else {
			keep = append(keep, l)
		}
	}
	e.late = keep

	// 2. Actors and the cluster.
	if e.dropUntil != 0 && e.SimMs >= e.dropUntil {
		e.c.SetDropRate(0)
		e.dropUntil = 0
	}
	for _, w := range e.workers {
		w.Tick(e.c, e.rng)
	}
	e.tickLinz()
	e.c.Step()
	e.observe()

	// 3. Heal faults on schedule.
	if e.active != nil && e.SimMs >= e.active.HealAt {
		e.heal()
	}
	if e.SimMs-e.oracleMark >= 3000 {
		e.oracleMark = e.SimMs
		e.checkOracle()
	}
	if e.SimMs-e.rateMark >= 1000 {
		dt := float64(e.SimMs-e.rateMark) / 1000
		for k, v := range e.sentCount {
			e.linkRate[k] = v / dt
			e.sentCount[k] = 0
		}
		e.evRate = e.evCount / dt
		e.evCount = 0
		e.rateMark = e.SimMs
	}
}

func (e *Engine) publish(p int, ev window.Event) {
	ev.Offset = int64(len(e.logs[p]))
	e.logs[p] = append(e.logs[p], ev)
	e.evCount++
	// The shadow oracle consumes the same log independently: no Raft, no
	// crashes, no retries.
	sh := e.shadows[p]
	sh.w.Add(ev)
	if sh.w.HasOutput() {
		for _, f := range sh.w.Flush().Features {
			rs := append(e.rows[f.Zone], f)
			if len(rs) > 240 {
				rs = rs[len(rs)-240:]
			}
			e.rows[f.Zone] = rs
		}
	}
}

func (e *Engine) tickLinz() {
	if e.active != nil {
		e.chaosSeen = true
	}
	if !e.linzClose && e.SimMs-e.linzStart >= linzEvery {
		e.linzClose = true // stop starting operations; let in-flight ones finish
	}
	idle := true
	for _, cl := range e.clients {
		cl.Tick(e.c, e.rng, e.clk, !e.linzClose)
		idle = idle && cl.Idle()
	}
	if !e.linzClose || !idle {
		return
	}
	var hist []porcupine.Operation
	for _, cl := range e.clients {
		hist = append(hist, cl.History...)
		cl.History = nil
	}
	verdict := "ok"
	switch porcupine.CheckOperationsTimeout(linz.Model, hist, 2*time.Second) {
	case porcupine.Illegal:
		verdict = "illegal"
	case porcupine.Unknown:
		verdict = "unknown"
	}
	e.linz = append(e.linz, console.LinzWindow{End: e.SimMs, Seconds: int((e.SimMs - e.linzStart) / 1000), Ops: len(hist), Verdict: verdict, DuringChaos: e.chaosSeen})
	if len(e.linz) > 40 {
		e.linz = e.linz[len(e.linz)-40:]
	}
	if e.chaosSeen && verdict == "ok" {
		e.log("check", "linearizability: %d ops recorded during a fault, history is linearizable", len(hist))
	}
	e.linzSeq++
	for _, cl := range e.clients {
		cl.KeyPrefix = fmt.Sprintf("w%d/", e.linzSeq)
	}
	e.linzStart, e.linzClose, e.chaosSeen = e.SimMs, false, e.active != nil
}

func (e *Engine) leader() uint64 { return e.c.Leader() }

func (e *Engine) observe() {
	l := e.leader()
	if e.failWait != 0 && l != 0 && l != e.failOld {
		e.lastFailover = e.SimMs - e.failWait
		e.failWait = 0
		e.log("raft", "new leader: node %d, %d ms after the fault", l, e.lastFailover)
	}
	for _, id := range e.c.IDs() {
		n := e.c.Nodes[id]
		key := n.Role.String()
		switch {
		case !n.Up:
			key = "down"
		case n.Paused:
			key = "paused"
		}
		if old, ok := e.roles[id]; ok && old != key {
			switch {
			case key == "leader":
				e.log("raft", "node %d became leader (term %d)", id, n.Core.Status().Term)
			case key == "down" || key == "paused":
				e.log("raft", "node %d is %s", id, key)
			case old == "down" || old == "paused":
				e.log("raft", "node %d is back as %s", id, key)
			}
		}
		e.roles[id] = key
	}
}

func (e *Engine) checkOracle() {
	l := e.leader()
	if l == 0 {
		return
	}
	sm := e.c.Nodes[l].SM
	var checked, bad, pending int
	for _, f := range sm.Features(nil) {
		rs := e.rows[f.Zone]
		found := false
		for i := len(rs) - 1; i >= 0; i-- {
			if rs[i].WindowEndMs == f.WindowEndMs {
				found = true
				checked++
				if rs[i] != f {
					bad++
				}
				break
			}
		}
		if !found {
			if len(rs) == 0 || rs[len(rs)-1].WindowEndMs < f.WindowEndMs {
				pending++
			} else {
				checked++
				bad++
			}
		}
	}
	o := e.oracle
	o.At, o.ZonesChecked, o.Mismatches, o.Pending = e.SimMs, checked, bad, pending
	o.TotalChecks += int64(checked)
	o.TotalBad += int64(bad)
	var evs int64
	for p := range e.logs {
		evs += int64(len(e.logs[p]))
	}
	o.Events = evs
	e.oracle = o
}

// Chaos runs a fault. It returns "" on success or an explanation.
func (e *Engine) Chaos(action string) string {
	if action == "heal" {
		e.heal()
		return ""
	}
	if e.active != nil || e.SimMs < e.cooldownUntil {
		return "another fault is running or cooling down"
	}
	l := e.leader()
	var follower uint64
	var fs []uint64
	for _, id := range e.c.IDs() {
		if id != l && e.c.Nodes[id].Up && !e.c.Nodes[id].Paused {
			fs = append(fs, id)
		}
	}
	if len(fs) > 0 {
		follower = fs[e.rng.Intn(len(fs))]
	}
	dur := int64(10_000)
	target := ""
	switch action {
	case "kill-leader", "pause-leader", "partition":
		if l == 0 {
			return "no leader right now"
		}
	}
	switch action {
	case "kill-leader":
		e.c.Crash(l)
		target = fmt.Sprintf("node %d", l)
		e.failWait, e.failOld = e.SimMs, l
		e.healFn = func() { e.c.Restart(l) }
	case "pause-leader":
		e.c.Pause(l)
		target = fmt.Sprintf("node %d", l)
		e.failWait, e.failOld = e.SimMs, l
		e.healFn = func() { e.c.Unpause(l) }
	case "kill-follower":
		if follower == 0 {
			return "no follower available"
		}
		e.c.Crash(follower)
		target = fmt.Sprintf("node %d", follower)
		e.healFn = func() { e.c.Restart(follower) }
	case "partition":
		if len(fs) < 4 {
			return "partition needs all five nodes up"
		}
		sort.Slice(fs, func(i, j int) bool { return fs[i] < fs[j] })
		minority := []uint64{l, fs[0]}
		majority := fs[1:]
		e.c.Partition(minority, majority)
		target = fmt.Sprintf("{%d,%d} | {%d,%d,%d}", minority[0], minority[1], majority[0], majority[1], majority[2])
		e.failWait, e.failOld = e.SimMs, l
		e.healFn = e.c.Heal
	case "kill-worker":
		for _, w := range e.workers {
			w.Crash()
		}
		target = "all stream workers"
		e.healFn = func() {}
		dur = 3000
	case "redeliver":
		for _, w := range e.workers {
			w.Redeliver(e.c, 300)
		}
		target = "every partition (re-send last batch, rewind 300 offsets)"
		e.healFn = func() {}
		dur = 3000
	case "lossy-network":
		e.c.SetDropRate(0.3)
		e.dropUntil = e.SimMs + dur
		target = "30% of Raft messages dropped"
		e.healFn = func() { e.c.SetDropRate(0) }
	default:
		return "unknown action " + action
	}
	e.active = &console.ChaosAction{Name: action, Target: target, Started: e.SimMs, HealAt: e.SimMs + dur}
	e.chaosSeen = true
	e.log("chaos", "%s: %s", action, target)
	return ""
}

func (e *Engine) heal() {
	if e.active == nil {
		return
	}
	if e.healFn != nil {
		e.healFn()
	}
	e.log("chaos", "healed: %s", e.active.Name)
	e.active, e.healFn = nil, nil
	e.cooldownUntil = e.SimMs + 2000
}

// Snapshot renders the simulation in the dashboard's format.
func (e *Engine) Snapshot() console.Snapshot {
	s := console.Snapshot{Mode: "sim", Now: e.SimMs, EventTimeMs: e.EventTime, Speedup: e.Speedup}
	l := e.leader()
	s.Leader = l
	for _, id := range e.c.IDs() {
		n := e.c.Nodes[id]
		st := n.Core.Status()
		state := "up"
		switch {
		case !n.Up:
			state = "down"
		case n.Paused:
			state = "paused"
		}
		var blocked []uint64
		for _, o := range e.c.IDs() {
			if o != id && e.c.Blocked(id, o) {
				blocked = append(blocked, o)
			}
		}
		node := console.Node{ID: id, State: state, Role: st.Role.String(), Term: st.Term, Lead: st.Lead,
			Commit: st.Commit, Applied: st.Applied, LastIndex: st.LastIndex, SnapIndex: st.SnapIndex, Blocked: blocked}
		if !n.Up {
			node.Role = "down"
		}
		s.Nodes = append(s.Nodes, node)
		if st.Term > s.Term && n.Up {
			s.Term = st.Term
		}
		for _, o := range e.c.IDs() {
			if o != id {
				s.Links = append(s.Links, console.Link{From: id, To: o, Rate: e.linkRate[[2]uint64{id, o}], Blocked: e.c.Blocked(id, o)})
			}
		}
	}
	for _, m := range e.c.InFlight() {
		span := float64(m.DeliverAt - m.SentAt)
		p := 1.0
		if span > 0 {
			p = (float64(e.c.Now-m.SentAt) + e.tickAcc/TickMs) / span
		}
		if p > 1 {
			p = 1
		}
		s.Msgs = append(s.Msgs, console.MsgView{From: m.Msg.From, To: m.Msg.To, Type: m.Msg.Type.String(), P: p})
	}
	var lag, consumed float64
	var late int64
	for i, w := range e.workers {
		lag += float64(int64(len(e.logs[i])) - 1 - w.Offset())
		consumed += float64(w.Offset() + 1)
	}
	if l != 0 {
		sm := e.c.Nodes[l].SM
		for p := 0; p < Partitions; p++ {
			late += sm.PartitionState(int32(p)).LateDropped
		}
		s.Ingest.DuplicatesIgnored = float64(sm.DuplicateBatches)
		for _, f := range sm.Features(nil) {
			s.Zones = append(s.Zones, console.ZoneFrom(f))
		}
	}
	s.Ingest.EventsPerSec = e.evRate
	s.Ingest.EventsTotal = consumed
	s.Ingest.Lag = lag
	s.Ingest.LateDropped = float64(late)
	s.Checks = console.Checks{Linz: append([]console.LinzWindow(nil), e.linz...), Oracle: e.oracle}
	s.Chaos = console.Chaos{Enabled: true, CooldownUntil: e.cooldownUntil, LastFailoverMs: e.lastFailover}
	if e.active != nil {
		a := *e.active
		s.Chaos.Active = &a
	}
	for i, w := range e.workers {
		_ = w
		s.Workers = append(s.Workers, console.Worker{Name: fmt.Sprintf("worker-p%d", i), State: "up"})
	}
	s.Log = append([]console.LogEntry(nil), e.logs2...)
	return s
}
