package simdemo

import "testing"

// TestEngineSurvivesEveryFault runs the browser simulation for several
// simulated minutes, injecting each fault in turn, and requires every
// linearizability window to pass and the oracle to find no mismatches.
func TestEngineSurvivesEveryFault(t *testing.T) {
	e, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	e.Advance(0)
	for i := 0; i < 50; i++ {
		e.Advance(200) // 10 s warm-up
	}
	actions := []string{"kill-leader", "partition", "pause-leader", "kill-follower", "kill-worker", "redeliver", "lossy-network"}
	for _, a := range actions {
		if msg := e.Chaos(a); msg != "" {
			t.Fatalf("%s refused: %s", a, msg)
		}
		for i := 0; i < 100; i++ { // 20 s: fault, heal, cooldown
			e.Advance(200)
		}
	}
	for i := 0; i < 100; i++ {
		e.Advance(200)
	}
	s := e.Snapshot()
	if len(s.Checks.Linz) < 5 {
		t.Fatalf("only %d linearizability windows", len(s.Checks.Linz))
	}
	ops, chaos := 0, 0
	for _, w := range s.Checks.Linz {
		if w.Verdict != "ok" {
			t.Fatalf("window ending %d: %s", w.End, w.Verdict)
		}
		ops += w.Ops
		if w.DuringChaos {
			chaos++
		}
	}
	if s.Checks.Oracle.TotalBad != 0 || s.Checks.Oracle.TotalChecks == 0 {
		t.Fatalf("oracle: %+v", s.Checks.Oracle)
	}
	if s.Leader == 0 || len(s.Zones) < 50 {
		t.Fatalf("leader %d, %d zones", s.Leader, len(s.Zones))
	}
	t.Logf("%d windows (%d during faults), %d ops linearizable; oracle %d zone checks, 0 bad; %d zones; dups ignored %.0f; term %d",
		len(s.Checks.Linz), chaos, ops, s.Checks.Oracle.TotalChecks, len(s.Zones), s.Ingest.DuplicatesIgnored, s.Term)
}
