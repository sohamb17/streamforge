// Command probe records a client history against the store cluster for a
// fixed duration, then checks it for linearizability with Porcupine.
//
// Fault scripts run it in the background while they kill, pause and
// partition store nodes. Output (in -out):
//
//	history.jsonl.gz   every operation with invoke/response times
//	result.json        counts, checker verdict, longest write gap
//	linearizability.html  Porcupine's visualization of the history
//	start.txt          wall-clock start (unix ms), to align fault events
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/sohamb17/streamforge/internal/linz"
	"github.com/sohamb17/streamforge/internal/probe"
	"github.com/sohamb17/streamforge/internal/storeclient"
)

func main() {
	stores := flag.String("store", "1=localhost:7001", "store nodes")
	dur := flag.Duration("duration", 60*time.Second, "how long to run")
	clients := flag.Int("clients", 8, "concurrent clients")
	keys := flag.Int("keys", 16, "distinct registers")
	opTimeout := flag.Duration("op-timeout", 2*time.Second, "per-operation deadline")
	think := flag.Duration("think", 2*time.Millisecond, "pause between a client's operations")
	out := flag.String("out", "probe-out", "output directory")
	checkTimeout := flag.Duration("check-timeout", 120*time.Second, "Porcupine time budget")
	analyze := flag.String("analyze", "", "analyze a finished scenario directory instead of running")
	localReads := flag.Bool("local-reads", false, "negative control: read any node's local state (NOT linearizable)")
	summarize := flag.String("summarize", "", "write SUMMARY.md for a directory of finished scenarios")
	flag.Parse()
	if *summarize != "" {
		must(summarizeAll(*summarize))
		return
	}
	if *analyze != "" {
		must(analyzeScenario(*analyze))
		return
	}

	addrs, err := storeclient.ParseAddrs(*stores)
	must(err)
	must(os.MkdirAll(*out, 0o755))
	rec := &probe.Recorder{Start: time.Now()}
	must(os.WriteFile(filepath.Join(*out, "start.txt"), []byte(fmt.Sprint(rec.Start.UnixMilli())), 0o644))
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	prefix := fmt.Sprintf("probe%d", rec.Start.UnixNano())
	fmt.Fprintf(os.Stderr, "probe: %d clients, %d keys, %v\n", *clients, *keys, *dur)
	must(probe.Run(ctx, probe.Config{Addrs: addrs, Clients: *clients, Keys: *keys, KeyPrefix: prefix,
		OpTimeout: *opTimeout, Think: *think, Seed: rec.Start.UnixNano(), LocalReads: *localReads}, rec))

	f, err := os.Create(filepath.Join(*out, "history.jsonl.gz"))
	must(err)
	gz := gzip.NewWriter(f)
	w := bufio.NewWriter(gz)
	enc := json.NewEncoder(w)
	for _, op := range rec.Ops() {
		enc.Encode(op)
	}
	w.Flush()
	gz.Close()
	f.Close()

	res, info := probe.Check(rec, *checkTimeout)
	b, _ := json.MarshalIndent(res, "", "  ")
	must(os.WriteFile(filepath.Join(*out, "result.json"), b, 0o644))
	fmt.Println(string(b))
	// The visualization grows with the history; write it when it is needed
	// (a violation) or small enough to open in a browser.
	if res.Linearizable != "ok" || res.Ops <= 3000 {
		vf, err := os.Create(filepath.Join(*out, "linearizability.html"))
		must(err)
		porcupine.Visualize(linz.Model, info, vf)
		vf.Close()
	}
	if res.Linearizable == "illegal" && !*localReads {
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		os.Exit(1)
	}
}

// analyzeScenario relates fault events (lines "unixms FAULT <name>" in
// events.txt) to the recorded history: for each fault it reports the
// longest stretch with no successful write that overlaps the 20 seconds
// after the fault, i.e. the client-observed write-unavailability window.
func analyzeScenario(dir string) error {
	startB, err := os.ReadFile(filepath.Join(dir, "probe", "start.txt"))
	if err != nil {
		return err
	}
	var startMs int64
	fmt.Sscan(string(startB), &startMs)
	f, err := os.Open(filepath.Join(dir, "probe", "history.jsonl.gz"))
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	var ops []probe.Op
	dec := json.NewDecoder(gz)
	for dec.More() {
		var op probe.Op
		if err := dec.Decode(&op); err != nil {
			return err
		}
		ops = append(ops, op)
	}
	var writes []float64 // completion times, seconds since start
	for _, op := range ops {
		if op.Kind == "put" && !op.Unknown {
			writes = append(writes, float64(op.RetNs)/1e9)
		}
	}
	sort.Float64s(writes)
	ev, err := os.ReadFile(filepath.Join(dir, "events.txt"))
	if err != nil {
		return err
	}
	type faultResult struct {
		Fault          string  `json:"fault"`
		AtSeconds      float64 `json:"at_s"`
		UnavailableMs  float64 `json:"write_unavailable_ms"`
		GapStartSecond float64 `json:"gap_start_s"`
	}
	var out []faultResult
	for _, line := range strings.Split(string(ev), "\n") {
		fs := strings.Fields(line)
		if len(fs) < 3 || fs[1] != "FAULT" {
			continue
		}
		var ms int64
		fmt.Sscan(fs[0], &ms)
		at := float64(ms-startMs) / 1000
		r := faultResult{Fault: strings.Join(fs[2:], " "), AtSeconds: at}
		for i := 1; i < len(writes); i++ {
			a, b := writes[i-1], writes[i]
			if b < at || a > at+20 {
				continue
			}
			if g := (b - a) * 1000; g > r.UnavailableMs {
				r.UnavailableMs, r.GapStartSecond = g, a
			}
		}
		out = append(out, r)
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	return os.WriteFile(filepath.Join(dir, "unavailability.json"), b, 0o644)
}

// summarizeAll writes a Markdown table over every scenario directory.
func summarizeAll(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("# Fault-injection results\n\n")
	b.WriteString("Generated by `faults/summarize.sh` from the files in each scenario directory. ")
	b.WriteString("Write unavailability is the longest stretch with no successful client write that overlaps the 20 s after the fault ")
	b.WriteString("(8 closed-loop probe clients, so resolution is a few ms). Linearizability is Porcupine's verdict on the full recorded history.\n\n")
	b.WriteString("| Scenario | Fault | Ops recorded | Unknown-outcome writes | Linearizable | Write unavailability | Converged | Notes |\n|---|---|---|---|---|---|---|---|\n")
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		ev, _ := os.ReadFile(filepath.Join(dir, "events.txt"))
		evs := string(ev)
		var faults, notes []string
		for _, line := range strings.Split(evs, "\n") {
			fs := strings.Fields(line)
			if len(fs) < 2 {
				continue
			}
			msg := strings.Join(fs[1:], " ")
			switch {
			case fs[1] == "FAULT":
				faults = append(faults, strings.Join(fs[2:], " "))
			case strings.Contains(msg, "unchanged"), strings.Contains(msg, "VIOLATION"), strings.Contains(msg, "installed"),
				strings.Contains(msg, "duplicate"), strings.Contains(msg, "new leader"), strings.Contains(msg, "EXPECTED"):
				notes = append(notes, msg)
			}
		}
		row := []string{e.Name(), strings.Join(faults, "; "), "-", "-", "-", "-", "-", strings.Join(notes, "; ")}
		var res probe.Result
		if rb, err := os.ReadFile(filepath.Join(dir, "probe", "result.json")); err == nil && json.Unmarshal(rb, &res) == nil {
			row[2] = fmt.Sprint(res.Ops)
			row[3] = fmt.Sprint(res.PutsUnknown)
			row[4] = res.Linearizable
		}
		var un []struct {
			Fault string  `json:"fault"`
			Ms    float64 `json:"write_unavailable_ms"`
		}
		if ub, err := os.ReadFile(filepath.Join(dir, "unavailability.json")); err == nil && json.Unmarshal(ub, &un) == nil {
			var parts []string
			for _, u := range un {
				parts = append(parts, fmt.Sprintf("%.0f ms", u.Ms))
			}
			row[5] = strings.Join(parts, ", ")
		}
		switch {
		case strings.Contains(evs, "CONVERGED"):
			row[6] = "yes (same applied index and state fingerprint on all 5)"
		case strings.Contains(evs, "DIVERGED"):
			row[6] = "**NO**"
		}
		if lb, err := os.ReadFile(filepath.Join(dir, "loadgen.json")); err == nil {
			var l struct {
				Requests int64            `json:"requests"`
				Codes    map[string]int64 `json:"codes"`
				Overall  struct {
					P99 float64 `json:"p99_ms"`
				} `json:"overall"`
			}
			if json.Unmarshal(lb, &l) == nil {
				row[2] = fmt.Sprint(l.Requests) + " GetFeatures"
				row[4] = "n/a (serving path)"
				row[5] = fmt.Sprintf("outcomes %v, p99 %.1f ms", l.Codes, l.Overall.P99)
			}
		}
		if ob, err := os.ReadFile(filepath.Join(dir, "oracle.json")); err == nil {
			var o map[string]any
			if json.Unmarshal(ob, &o) == nil {
				row[4] = fmt.Sprintf("oracle: %v feature / %v state mismatches over %v events", o["feature_mismatches"], o["partition_state_mismatches"], o["events_replayed"])
			}
		}
		b.WriteString("| " + strings.Join(row, " | ") + " |\n")
	}
	return os.WriteFile(filepath.Join(root, "SUMMARY.md"), []byte(b.String()), 0o644)
}
