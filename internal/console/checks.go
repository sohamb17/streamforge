package console

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sohamb17/streamforge/internal/event"
	"github.com/sohamb17/streamforge/internal/probe"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

// checks runs two correctness checks continuously against the live system:
//
//   - linearizability: two probe clients read and write registers through
//     Raft in 30-second windows; each window's history is checked with
//     Porcupine, including windows during injected faults.
//   - shadow oracle: an independent consumer recomputes every feature row
//     from the Kafka log with no Raft, no crashes and no retries, and the
//     store's latest row per zone is compared with it.
type checks struct {
	l *Live

	mu     sync.Mutex
	linz   []LinzWindow
	oracle Oracle
}

func newChecks(l *Live) *checks { return &checks{l: l} }

func (c *checks) view() Checks {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Checks{Linz: append([]LinzWindow(nil), c.linz...), Oracle: c.oracle}
}

func (c *checks) runLinz(ctx context.Context) {
	const window = 30 * time.Second
	for ctx.Err() == nil {
		rec := &probe.Recorder{Start: time.Now()}
		wctx, cancel := context.WithTimeout(ctx, window)
		chaosSeen := false
		done := make(chan struct{})
		go func() {
			t := time.NewTicker(500 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					if c.l.chaos.view().Active != nil {
						c.mu.Lock()
						chaosSeen = true
						c.mu.Unlock()
					}
				}
			}
		}()
		prefix := fmt.Sprintf("console%d", rec.Start.UnixNano())
		_ = probe.Run(wctx, probe.Config{
			Addrs: c.l.cfg.StoreAddrs, Clients: 2, Keys: 4, KeyPrefix: prefix,
			OpTimeout: 2 * time.Second, Think: 25 * time.Millisecond, Seed: rec.Start.UnixNano(),
		}, rec)
		cancel()
		close(done)
		if ctx.Err() != nil {
			return
		}
		res, _ := probe.Check(rec, 20*time.Second)
		// The window is checked; drop its keys and client records so the
		// replicated state does not grow without bound.
		go func(prefix string) {
			pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
			defer pcancel()
			_, _ = c.l.sc.Purge(pctx, prefix+"/")
		}(prefix)
		c.mu.Lock()
		w := LinzWindow{End: time.Now().UnixMilli(), Seconds: int(window.Seconds()), Ops: res.Ops, Verdict: res.Linearizable, DuringChaos: chaosSeen}
		c.linz = append(c.linz, w)
		if len(c.linz) > 40 {
			c.linz = c.linz[len(c.linz)-40:]
		}
		c.mu.Unlock()
		if res.Linearizable != "ok" {
			c.l.addLog("check", "linearizability check returned %q for a %d-op window", res.Linearizable, res.Ops)
		} else if chaosSeen {
			c.l.addLog("check", "linearizability: %d ops recorded during a fault, history is linearizable", res.Ops)
		}
	}
}

const shadowRowsPerZone = 240

func (c *checks) runOracle(ctx context.Context) {
	for ctx.Err() == nil {
		if err := c.shadow(ctx); err != nil && ctx.Err() == nil {
			c.l.addLog("info", "shadow oracle restarting: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}

type shadowPart struct {
	w          *window.Partition
	firstOff   int64
	firstEvent int64
}

func (c *checks) shadow(ctx context.Context) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.l.cfg.Brokers...), kgo.ConsumeTopics(c.l.cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxBytes(32<<20))
	if err != nil {
		return err
	}
	defer cl.Close()
	parts := map[int32]*shadowPart{}
	rows := map[int32][]window.Features{}
	var events int64
	var totalChecks, totalBad int64
	next := time.Now().Add(15 * time.Second)
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) {
			t, err := event.Decode(r.Value)
			if err != nil {
				return
			}
			sp := parts[r.Partition]
			if sp == nil {
				sp = &shadowPart{w: window.NewPartition(r.Partition, window.DefaultConfig()), firstOff: r.Offset, firstEvent: t.EventTimeMs}
				parts[r.Partition] = sp
			}
			if !sp.w.Add(window.Event{Zone: t.Zone, EventTimeMs: t.EventTimeMs, FareCents: t.FareCents, DistanceMilli: t.DistanceMilli, Offset: r.Offset}) {
				return
			}
			events++
			if sp.w.HasOutput() {
				for _, f := range sp.w.Flush().Features {
					rs := append(rows[f.Zone], f)
					if len(rs) > shadowRowsPerZone {
						rs = rs[len(rs)-shadowRowsPerZone:]
					}
					rows[f.Zone] = rs
				}
			}
		})
		if time.Now().Before(next) {
			continue
		}
		next = time.Now().Add(15 * time.Second)

		warming := false
		for _, sp := range parts {
			// A shadow that did not start at offset 0 (retention) needs an
			// hour of event time before its windows equal the store's.
			if sp.firstOff > 0 && sp.w.ClosedUntil()-sp.firstEvent < 62*60*1000 {
				warming = true
			}
		}
		qctx, qcancel := context.WithTimeout(ctx, 2*time.Second)
		got, _, err := c.l.sc.ZoneFeatures(qctx, nil)
		qcancel()
		if err != nil {
			continue
		}
		var checked, bad, pending int
		for _, g := range got {
			f := store.FeaturesFromProto(g)
			rs := rows[f.Zone]
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
				if len(rs) == 0 || rs[len(rs)-1].WindowEndMs < f.WindowEndMs || warming {
					pending++ // the shadow has not reached this window yet
				} else {
					checked++
					bad++
				}
			}
		}
		if !warming {
			totalChecks += int64(checked)
			totalBad += int64(bad)
		}
		c.mu.Lock()
		c.oracle = Oracle{At: time.Now().UnixMilli(), ZonesChecked: checked, Mismatches: bad, Pending: pending,
			WarmingUp: warming, Events: events, TotalChecks: totalChecks, TotalBad: totalBad}
		c.mu.Unlock()
		if bad > 0 && !warming {
			c.l.addLog("check", "oracle: %d of %d zones differ from the recomputation", bad, checked)
		}
	}
	return ctx.Err()
}
