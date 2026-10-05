// Package console is the backend of the live dashboard: it collects the
// state of the running system into a snapshot.Snapshot, streams it to
// browsers, runs continuous correctness checks and the chaos API.
package console

import "github.com/sohamb17/streamforge/internal/snapshot"

// Aliases keep the console code short.
type (
	Snapshot    = snapshot.Snapshot
	Node        = snapshot.Node
	Link        = snapshot.Link
	Ingest      = snapshot.Ingest
	Serving     = snapshot.Serving
	Zone        = snapshot.Zone
	Checks      = snapshot.Checks
	LinzWindow  = snapshot.LinzWindow
	Oracle      = snapshot.Oracle
	Chaos       = snapshot.Chaos
	ChaosAction = snapshot.ChaosAction
	Worker      = snapshot.Worker
	LogEntry    = snapshot.LogEntry
)

// ZoneFrom converts a feature row for the UI.
var ZoneFrom = snapshot.ZoneFrom
