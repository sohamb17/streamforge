// Package snapshot defines the JSON the dashboard renders: one snapshot of
// the whole system (Raft nodes, message flow, ingest, features per zone,
// correctness checks, chaos state). The live console and the in-browser
// simulator both produce it, so one UI serves both.
package snapshot

import (
	"math"

	"github.com/sohamb17/streamforge/internal/window"
)

// Snapshot is everything the dashboard renders.
type Snapshot struct {
	Mode        string     `json:"mode"` // "live" | "sim"
	Now         int64      `json:"now"`
	EventTimeMs int64      `json:"eventTimeMs"`
	Speedup     float64    `json:"speedup"`
	Term        uint64     `json:"term"`
	Leader      uint64     `json:"leader"`
	Nodes       []Node     `json:"nodes"`
	Links       []Link     `json:"links"`
	Ingest      Ingest     `json:"ingest"`
	Serving     Serving    `json:"serving"`
	Zones       []Zone     `json:"zones,omitempty"`
	Checks      Checks     `json:"checks"`
	Chaos       Chaos      `json:"chaos"`
	Workers     []Worker   `json:"workers"`
	Log         []LogEntry `json:"log"`
	// Msgs lists Raft messages on the wire (simulation only; the live
	// cluster reports per-link rates instead).
	Msgs []MsgView `json:"msgs,omitempty"`
}

// MsgView is one in-flight message for animation.
type MsgView struct {
	From uint64  `json:"from"`
	To   uint64  `json:"to"`
	Type string  `json:"type"`
	P    float64 `json:"p"` // progress 0..1
}

// Node is one Raft member.
type Node struct {
	ID                 uint64   `json:"id"`
	State              string   `json:"state"` // up | down | paused | unreachable
	Role               string   `json:"role"`
	Term               uint64   `json:"term"`
	Lead               uint64   `json:"lead"`
	Commit             uint64   `json:"commit"`
	Applied            uint64   `json:"applied"`
	LastIndex          uint64   `json:"lastIndex"`
	SnapIndex          uint64   `json:"snapIndex"`
	SnapshotsInstalled uint64   `json:"snapshotsInstalled"`
	Blocked            []uint64 `json:"blocked,omitempty"`
}

// Link is the message flow from one node to another.
type Link struct {
	From    uint64  `json:"from"`
	To      uint64  `json:"to"`
	Rate    float64 `json:"rate"` // messages per second
	Blocked bool    `json:"blocked"`
}

// Ingest summarizes the streaming side.
type Ingest struct {
	EventsPerSec      float64 `json:"eventsPerSec"`
	EventsTotal       float64 `json:"eventsTotal"`
	Lag               float64 `json:"lag"`
	LateDropped       float64 `json:"lateDropped"`
	DuplicatesIgnored float64 `json:"duplicatesIgnored"`
	RecordsSkipped    float64 `json:"recordsSkipped"`
	FreshnessP50Ms    float64 `json:"freshnessP50Ms"`
	FreshnessP99Ms    float64 `json:"freshnessP99Ms"`
	CommitP99Ms       float64 `json:"commitP99Ms"`
	BatchesPerSec     float64 `json:"batchesPerSec"`
}

// Serving summarizes FeatureService.
type Serving struct {
	RPS      float64 `json:"rps"`
	P50Ms    float64 `json:"p50Ms"`
	P99Ms    float64 `json:"p99Ms"`
	HitRatio float64 `json:"hitRatio"`
}

// Zone is the latest feature row of one zone, with derived values.
type Zone struct {
	ID    int32   `json:"id"`
	T5    int64   `json:"t5"`
	T30   int64   `json:"t30"`
	T60   int64   `json:"t60"`
	Fare  float64 `json:"fare"`
	Dist  float64 `json:"dist"`
	Spike float64 `json:"spike"`
	AsOf  int64   `json:"asOf"`
}

// Checks are the continuously running correctness checks.
type Checks struct {
	Linz   []LinzWindow `json:"linz"`
	Oracle Oracle       `json:"oracle"`
}

// LinzWindow is one checked probe window.
type LinzWindow struct {
	End         int64  `json:"end"`
	Seconds     int    `json:"seconds"`
	Ops         int    `json:"ops"`
	Verdict     string `json:"verdict"` // ok | illegal | unknown
	DuringChaos bool   `json:"duringChaos"`
}

// Oracle is the latest comparison of the store against an independent
// recomputation from the Kafka log.
type Oracle struct {
	At           int64 `json:"at"`
	ZonesChecked int   `json:"zonesChecked"`
	Mismatches   int   `json:"mismatches"`
	Pending      int   `json:"pending"`
	WarmingUp    bool  `json:"warmingUp"`
	Events       int64 `json:"events"`
	TotalChecks  int64 `json:"totalChecks"`
	TotalBad     int64 `json:"totalBad"`
}

// Chaos is the fault-injection state.
type Chaos struct {
	Enabled        bool         `json:"enabled"`
	Active         *ChaosAction `json:"active,omitempty"`
	CooldownUntil  int64        `json:"cooldownUntil"`
	LastFailoverMs int64        `json:"lastFailoverMs"`
}

// ChaosAction is a running fault.
type ChaosAction struct {
	Name    string `json:"name"`
	Target  string `json:"target"`
	Started int64  `json:"started"`
	HealAt  int64  `json:"healAt"`
}

// Worker is one stream worker process.
type Worker struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

// LogEntry is one line of the event log.
type LogEntry struct {
	T    int64  `json:"t"`
	Kind string `json:"kind"` // chaos | raft | check | info
	Msg  string `json:"msg"`
}

// ZoneFrom converts a feature row for the UI.
func ZoneFrom(f window.Features) Zone {
	v := window.Derive(f)
	r2 := func(x float64) float64 { return math.Round(x*100) / 100 }
	return Zone{ID: f.Zone, T5: f.Trips5m, T30: f.Trips30m, T60: f.Trips60m,
		Fare: r2(v["fare_mean_15m"]), Dist: r2(v["distance_mean_15m"]), Spike: r2(v["demand_spike_ratio"]), AsOf: f.WindowEndMs}
}
