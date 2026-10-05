// Package probe drives concurrent register clients against the live store
// cluster and records every operation (invoke time, response time, input,
// output) so the history can be checked for linearizability with Porcupine.
//
// Each client is sequential and has its own client id, so the store's
// (client id, sequence) deduplication applies. A Put whose deadline expires
// is recorded with unknown outcome (it may or may not have taken effect); a
// Get that fails is dropped from the history, since reads have no effect.
package probe

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/sohamb17/streamforge/internal/linz"
	"github.com/sohamb17/streamforge/internal/storeclient"
)

// Config for a probe run.
type Config struct {
	Addrs     map[uint64]string
	Clients   int
	Keys      int
	KeyPrefix string        // distinct per run so histories are independent
	OpTimeout time.Duration // per-operation deadline
	Think     time.Duration // pause between a client's operations
	Seed      int64
	// LocalReads makes Gets read any node's local state instead of doing a
	// linearizable ReadIndex read. Only for the negative control.
	LocalReads bool
}

// Op is one recorded operation. Times are nanoseconds since Start.
type Op struct {
	Client  int    `json:"client"`
	Kind    string `json:"kind"` // "put" | "get"
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Found   bool   `json:"found,omitempty"`
	Unknown bool   `json:"unknown,omitempty"`
	CallNs  int64  `json:"call_ns"`
	RetNs   int64  `json:"ret_ns"` // -1 when unknown
}

// Recorder accumulates a history.
type Recorder struct {
	Start time.Time // wall-clock start, to align with fault timestamps
	mu    sync.Mutex
	ops   []Op
	// failed counts operations that returned an error (excluded or unknown).
	failedGets, unknownPuts int
}

// Ops returns a copy of the history.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}

func (r *Recorder) add(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// Run executes the probe until ctx is done.
func Run(ctx context.Context, cfg Config, rec *Recorder) error {
	if rec.Start.IsZero() {
		rec.Start = time.Now()
	}
	start := rec.Start
	var wg sync.WaitGroup
	errc := make(chan error, cfg.Clients)
	for i := 0; i < cfg.Clients; i++ {
		cli, err := storeclient.New(cfg.Addrs, fmt.Sprintf("%s/client-%d", cfg.KeyPrefix, i))
		if err != nil {
			return err
		}
		cli.PerAttemptTimeout = cfg.OpTimeout / 2
		wg.Add(1)
		go func(id int, cli *storeclient.Client) {
			defer wg.Done()
			defer cli.Close()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(id)))
			seq := 0
			for ctx.Err() == nil {
				key := fmt.Sprintf("%s/k%d", cfg.KeyPrefix, rng.Intn(cfg.Keys))
				octx, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
				call := time.Since(start).Nanoseconds()
				if rng.Intn(2) == 0 {
					seq++
					val := fmt.Sprintf("c%d-%d", id, seq)
					err := cli.Put(octx, key, val)
					ret := time.Since(start).Nanoseconds()
					if err != nil {
						rec.mu.Lock()
						rec.unknownPuts++
						rec.mu.Unlock()
						rec.add(Op{Client: id, Kind: "put", Key: key, Value: val, Unknown: true, CallNs: call, RetNs: -1})
					} else {
						rec.add(Op{Client: id, Kind: "put", Key: key, Value: val, CallNs: call, RetNs: ret})
					}
				} else {
					var v string
					var found bool
					var err error
					if cfg.LocalReads {
						ids := cli.IDs()
						v, found, err = cli.GetLocal(octx, ids[rng.Intn(len(ids))], key)
					} else {
						v, found, err = cli.Get(octx, key, true)
					}
					ret := time.Since(start).Nanoseconds()
					if err != nil {
						rec.mu.Lock()
						rec.failedGets++
						rec.mu.Unlock()
					} else {
						rec.add(Op{Client: id, Kind: "get", Key: key, Value: v, Found: found, CallNs: call, RetNs: ret})
					}
				}
				cancel()
				if cfg.Think > 0 {
					time.Sleep(cfg.Think)
				}
			}
		}(i, cli)
	}
	wg.Wait()
	close(errc)
	return nil
}

// ToPorcupine converts a history.
func ToPorcupine(ops []Op) []porcupine.Operation {
	out := make([]porcupine.Operation, 0, len(ops))
	for _, o := range ops {
		in := linz.Input{Op: linz.OpGet, Key: o.Key}
		outp := linz.Output{Value: o.Value, Found: o.Found}
		if o.Kind == "put" {
			in = linz.Input{Op: linz.OpPut, Key: o.Key, Value: o.Value}
			outp = linz.Output{Unknown: o.Unknown}
		}
		ret := o.RetNs
		if o.Unknown {
			ret = linz.Infinity
		}
		out = append(out, porcupine.Operation{ClientId: o.Client, Input: in, Call: o.CallNs, Output: outp, Return: ret})
	}
	return out
}

// Result summarizes a checked history.
type Result struct {
	Ops            int     `json:"ops"`
	PutsOK         int     `json:"puts_ok"`
	PutsUnknown    int     `json:"puts_unknown"`
	GetsOK         int     `json:"gets_ok"`
	GetsFailed     int     `json:"gets_failed"`
	Linearizable   string  `json:"linearizable"` // "ok" | "illegal" | "unknown" (checker timed out)
	CheckSeconds   float64 `json:"check_seconds"`
	MaxWriteGapMs  float64 `json:"max_write_gap_ms"`
	MaxWriteGapAt  float64 `json:"max_write_gap_starts_at_s"` // seconds since start
	WriteGapsOver1 int     `json:"write_gaps_over_1s"`
	// Closed-loop client latencies (each client waits for its previous op).
	PutP50Ms   float64 `json:"put_p50_ms"`
	PutP99Ms   float64 `json:"put_p99_ms"`
	GetP50Ms   float64 `json:"get_p50_ms"`
	GetP99Ms   float64 `json:"get_p99_ms"`
	PutsPerSec float64 `json:"puts_per_sec"`
	GetsPerSec float64 `json:"gets_per_sec"`
}

func pct(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	i := int(q * float64(len(xs)-1))
	return xs[i]
}

// Check runs Porcupine on the history and computes write availability.
func Check(rec *Recorder, timeout time.Duration) (Result, porcupine.LinearizationInfo) {
	ops := rec.Ops()
	var r Result
	r.Ops = len(ops)
	var okWrites []int64
	var putLat, getLat []float64
	var lastRet int64
	for _, o := range ops {
		if o.RetNs > lastRet {
			lastRet = o.RetNs
		}
		if !o.Unknown {
			if o.Kind == "put" {
				putLat = append(putLat, float64(o.RetNs-o.CallNs)/1e6)
			} else {
				getLat = append(getLat, float64(o.RetNs-o.CallNs)/1e6)
			}
		}
		switch {
		case o.Kind == "put" && o.Unknown:
			r.PutsUnknown++
		case o.Kind == "put":
			r.PutsOK++
			okWrites = append(okWrites, o.RetNs)
		default:
			r.GetsOK++
		}
	}
	rec.mu.Lock()
	r.GetsFailed = rec.failedGets
	rec.mu.Unlock()
	r.PutP50Ms, r.PutP99Ms = pct(putLat, 0.5), pct(putLat, 0.99)
	r.GetP50Ms, r.GetP99Ms = pct(getLat, 0.5), pct(getLat, 0.99)
	if lastRet > 0 {
		r.PutsPerSec = float64(r.PutsOK) / (float64(lastRet) / 1e9)
		r.GetsPerSec = float64(r.GetsOK) / (float64(lastRet) / 1e9)
	}
	sort.Slice(okWrites, func(i, j int) bool { return okWrites[i] < okWrites[j] })
	for i := 1; i < len(okWrites); i++ {
		gap := float64(okWrites[i]-okWrites[i-1]) / 1e6
		if gap > r.MaxWriteGapMs {
			r.MaxWriteGapMs = gap
			r.MaxWriteGapAt = float64(okWrites[i-1]) / 1e9
		}
		if gap > 1000 {
			r.WriteGapsOver1++
		}
	}
	t0 := time.Now()
	res, info := porcupine.CheckOperationsVerbose(linz.Model, ToPorcupine(ops), timeout)
	r.CheckSeconds = time.Since(t0).Seconds()
	switch res {
	case porcupine.Ok:
		r.Linearizable = "ok"
	case porcupine.Illegal:
		r.Linearizable = "illegal"
	default:
		r.Linearizable = "unknown"
	}
	return r, info
}

// WriteGaps returns gaps between consecutive successful writes longer than
// min, as (start seconds, length ms) pairs, for timelines.
func WriteGaps(ops []Op, min time.Duration) [][2]float64 {
	var ts []int64
	for _, o := range ops {
		if o.Kind == "put" && !o.Unknown {
			ts = append(ts, o.RetNs)
		}
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	var out [][2]float64
	for i := 1; i < len(ts); i++ {
		if d := ts[i] - ts[i-1]; d > min.Nanoseconds() {
			out = append(out, [2]float64{float64(ts[i-1]) / 1e9, float64(d) / 1e6})
		}
	}
	return out
}
