// Package oracle recomputes features in batch, straight from the Kafka log,
// with no Raft, no crashes and no retries. Comparing its output with the
// online store (or the offline history) is the correctness check for the
// distributed path: if crashes, redeliveries or leader changes ever caused
// double counting or lost updates, the two would differ.
package oracle

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sohamb17/streamforge/internal/event"
	"github.com/sohamb17/streamforge/internal/window"
)

// Partitions lists the topic's partitions.
func Partitions(ctx context.Context, brokers []string, topic string) ([]int32, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	td, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return nil, err
	}
	t, ok := td[topic]
	if !ok || t.Err != nil {
		return nil, fmt.Errorf("oracle: topic %s not found", topic)
	}
	return t.Partitions.Numbers(), nil
}

// Replay consumes one partition from its first offset through upTo
// (inclusive) and feeds every event to a fresh window. emit is called for
// every output row in order. It returns the final window state.
func Replay(ctx context.Context, brokers []string, topic string, p int32, upTo int64, cfg window.Config, emit func(window.Features)) (window.State, error) {
	w := window.NewPartition(p, cfg)
	if upTo < 0 {
		return w.Snapshot(), nil
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {p: kgo.NewOffset().AtStart()}}),
		kgo.FetchMaxBytes(32<<20))
	if err != nil {
		return window.State{}, err
	}
	defer cl.Close()
	for w.ToOffset() < upTo {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return window.State{}, err
		}
		var ferr error
		fs.EachError(func(_ string, _ int32, err error) { ferr = err })
		if ferr != nil {
			return window.State{}, ferr
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset > upTo {
				return
			}
			t, err := event.Decode(r.Value)
			if err != nil {
				return
			}
			w.Add(window.Event{Zone: t.Zone, EventTimeMs: t.EventTimeMs, FareCents: t.FareCents, DistanceMilli: t.DistanceMilli, Offset: r.Offset})
			if w.HasOutput() {
				for _, f := range w.Flush().Features {
					if emit != nil {
						emit(f)
					}
				}
			}
		})
	}
	return w.Snapshot(), nil
}
