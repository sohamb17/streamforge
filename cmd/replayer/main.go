// Command replayer publishes trip events to Kafka at a controlled rate.
//
// Sources: a monthly NYC TLC Parquet file (sorted by pickup time) or the
// seeded synthetic generator. Pacing: either event-time speed-up (-speedup
// 60 plays one hour of trips per minute) or a fixed event rate (-rate).
// With -loop the month repeats forever, each loop shifted by one month of
// event time so event time keeps increasing.
//
// A seeded fraction of events can be published late (-late-fraction), to
// exercise the watermark and late-drop policy.
package main

import (
	"container/heap"
	"context"
	"errors"
	"flag"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sohamb17/streamforge/internal/event"
	"github.com/sohamb17/streamforge/internal/synth"
	"github.com/sohamb17/streamforge/internal/tlc"
)

var (
	mPublished = promauto.NewCounter(prometheus.CounterOpts{Name: "streamforge_replayer_events_total", Help: "Events published."})
	mErrors    = promauto.NewCounter(prometheus.CounterOpts{Name: "streamforge_replayer_errors_total", Help: "Produce errors."})
	mLate      = promauto.NewCounter(prometheus.CounterOpts{Name: "streamforge_replayer_late_injected_total", Help: "Events deliberately published late."})
	mEventTime = promauto.NewGauge(prometheus.GaugeOpts{Name: "streamforge_replayer_event_time_ms", Help: "Event time of the last published event."})
	mSpeed     = promauto.NewGauge(prometheus.GaugeOpts{Name: "streamforge_replayer_speedup", Help: "Current event-time speed-up."})
	mLoop      = promauto.NewGauge(prometheus.GaugeOpts{Name: "streamforge_replayer_loop", Help: "Current replay loop."})
)

// delayed is a min-heap of late events keyed by the stream time at which
// they are released.
type lateItem struct {
	release int64
	t       event.Trip
}

type delayed []lateItem

func (d delayed) Len() int            { return len(d) }
func (d delayed) Less(i, j int) bool  { return d[i].release < d[j].release }
func (d delayed) Swap(i, j int)       { d[i], d[j] = d[j], d[i] }
func (d *delayed) Push(x interface{}) { *d = append(*d, x.(lateItem)) }
func (d *delayed) Pop() interface{} {
	o := *d
	x := o[len(o)-1]
	*d = o[:len(o)-1]
	return x
}

type source interface {
	next() (event.Trip, bool)
}

type sliceSource struct {
	trips     []event.Trip
	i         int
	loop      bool
	loopShift int64
	loopN     int64
	from, to  int64
}

func (s *sliceSource) next() (event.Trip, bool) {
	for {
		if s.i >= len(s.trips) {
			if !s.loop {
				return event.Trip{}, false
			}
			s.i = 0
			s.loopN++
			mLoop.Set(float64(s.loopN))
		}
		t := s.trips[s.i]
		s.i++
		if (s.from != 0 && t.EventTimeMs < s.from) || (s.to != 0 && t.EventTimeMs >= s.to) {
			continue
		}
		t.EventTimeMs += s.loopN * s.loopShift
		return t, true
	}
}

type synthSource struct{ g *synth.Generator }

func (s synthSource) next() (event.Trip, bool) { return s.g.Next(), true }

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap servers")
	topic := flag.String("topic", "trips", "topic")
	partitions := flag.Int("partitions", 6, "partitions when creating the topic")
	retention := flag.Duration("retention", 72*time.Hour, "topic retention when creating it")
	src := flag.String("source", "tlc", "tlc | synthetic")
	tlcFile := flag.String("tlc-file", "data/yellow_tripdata_2026-03.parquet", "TLC parquet file")
	tlcMonth := flag.String("tlc-month", "2026-03", "month of the TLC file (YYYY-MM)")
	speedup := flag.Float64("speedup", 60, "event-time speed-up (0 = as fast as possible)")
	rate := flag.Float64("rate", 0, "fixed publish rate in events/s (overrides -speedup pacing)")
	loop := flag.Bool("loop", false, "repeat the month forever")
	maxEvents := flag.Int64("max-events", 0, "stop after this many events (0 = unlimited)")
	lateFrac := flag.Float64("late-fraction", 0.002, "fraction of events published late")
	lateMax := flag.Duration("late-max", 90*time.Second, "maximum lateness (event time)")
	seed := flag.Int64("seed", 1, "seed for late injection and synthetic data")
	zipf := flag.Float64("zipf", 1.1, "synthetic: Zipf skew (<=1 uniform)")
	synthRate := flag.Float64("synthetic-per-minute", 6000, "synthetic: events per minute of event time")
	from := flag.String("from", "", "only replay event time >= this (RFC3339)")
	to := flag.String("to", "", "only replay event time < this (RFC3339)")
	httpAddr := flag.String("http", ":9102", "metrics/admin address")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "replayer")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var s source

	switch *src {
	case "tlc":
		month, err := time.Parse("2006-01", *tlcMonth)
		if err != nil {
			log.Error("bad -tlc-month", "err", err)
			os.Exit(2)
		}
		t0 := time.Now()
		trips, st, err := tlc.Load(*tlcFile, month)
		if err != nil {
			log.Error("load tlc", "err", err)
			os.Exit(1)
		}
		log.Info("loaded TLC file", "file", *tlcFile, "rows", st.Rows, "kept", st.Kept, "bad_time", st.BadTime,
			"bad_zone", st.BadZone, "bad_fare", st.BadFare, "bad_distance", st.BadDistance, "took", time.Since(t0).String())
		ss := &sliceSource{trips: trips, loop: *loop, loopShift: month.AddDate(0, 1, 0).UnixMilli() - month.UnixMilli()}
		if *from != "" {
			ft, err := time.Parse(time.RFC3339, *from)
			if err != nil {
				log.Error("bad -from", "err", err)
				os.Exit(2)
			}
			ss.from = ft.UnixMilli()
		}
		if *to != "" {
			tt, err := time.Parse(time.RFC3339, *to)
			if err != nil {
				log.Error("bad -to", "err", err)
				os.Exit(2)
			}
			ss.to = tt.UnixMilli()
		}
		s = ss

	case "synthetic":
		startMs := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
		s = synthSource{synth.New(synth.Config{Seed: *seed, Zipf: *zipf, EventsPerMinute: *synthRate, StartMs: startMs})}
	default:
		log.Error("unknown -source", "source", *src)
		os.Exit(2)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*brokers),
		kgo.DefaultProduceTopic(*topic),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(200_000),
	)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()
	if err := ensureTopic(ctx, cl, *topic, *partitions, *retention); err != nil {
		log.Error("ensure topic", "err", err)
		os.Exit(1)
	}

	var speedBits atomic.Uint64
	speedBits.Store(math.Float64bits(*speedup))
	mSpeed.Set(*speedup)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	// POST /admin/speed?x=120 changes the event-time speed-up live.
	mux.HandleFunc("/admin/speed", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		x, err := strconv.ParseFloat(r.URL.Query().Get("x"), 64)
		if err != nil || x < 0 || x > 100000 {
			http.Error(w, "bad x", http.StatusBadRequest)
			return
		}
		speedBits.Store(math.Float64bits(x))
		mSpeed.Set(x)
		w.Write([]byte("ok"))
	})
	go http.ListenAndServe(*httpAddr, mux)

	rng := rand.New(rand.NewSource(*seed))
	var late delayed
	var published, published0 int64
	wallStart := time.Now()
	var streamStart int64 = math.MinInt64
	curSpeed := *speedup
	produce := func(t event.Trip) {
		t.PublishMs = time.Now().UnixMilli()
		// The record timestamp is the publish (wall) time, not the event
		// time: Kafka's time-based retention would otherwise delete a
		// replayed month of 2026-03 trips minutes after writing it.
		cl.Produce(ctx, &kgo.Record{Key: event.Key(t.Zone), Value: event.Encode(t), Timestamp: time.UnixMilli(t.PublishMs)},
			func(_ *kgo.Record, err error) {
				if err != nil {
					mErrors.Inc()
					return
				}
				mPublished.Inc()
			})
		published++
		mEventTime.Set(float64(t.EventTimeMs))
	}
	log.Info("replaying", "source", *src, "speedup", *speedup, "rate", *rate, "loop", *loop, "late_fraction", *lateFrac)
	for ctx.Err() == nil {
		if *maxEvents > 0 && published >= *maxEvents {
			break
		}
		t, ok := s.next()
		if !ok {
			break
		}
		if streamStart == math.MinInt64 {
			streamStart = t.EventTimeMs
		}
		// Pacing.
		if sp := math.Float64frombits(speedBits.Load()); sp != curSpeed {
			// Re-anchor so a speed change does not cause a burst or stall.
			curSpeed = sp
			wallStart = time.Now()
			streamStart = t.EventTimeMs
			published0 = published
		}
		var due time.Time
		switch {
		case *rate > 0:
			due = wallStart.Add(time.Duration(float64(published-published0) / *rate * float64(time.Second)))
		case curSpeed > 0:
			due = wallStart.Add(time.Duration(float64(t.EventTimeMs-streamStart) / curSpeed * float64(time.Millisecond)))
		}
		if d := time.Until(due); d > time.Millisecond {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		}
		// Release late events whose time has come, then this one (or hold it).
		for late.Len() > 0 && late[0].release <= t.EventTimeMs {
			produce(heap.Pop(&late).(lateItem).t)
		}
		if *lateFrac > 0 && rng.Float64() < *lateFrac {
			mLate.Inc()
			heap.Push(&late, lateItem{t.EventTimeMs + rng.Int63n(int64(*lateMax/time.Millisecond)+1), t})
			continue
		}
		produce(t)
	}
	for late.Len() > 0 {
		produce(heap.Pop(&late).(lateItem).t)
	}
	fctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Flush(fctx); err != nil {
		log.Error("flush", "err", err)
	}
	log.Info("done", "published", published, "took", time.Since(wallStart).String())
}

func ensureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int, retention time.Duration) error {
	adm := kadm.NewClient(cl)
	ret := strconv.FormatInt(retention.Milliseconds(), 10)
	for attempt := 0; ; attempt++ {
		resp, err := adm.CreateTopic(ctx, int32(partitions), 1, map[string]*string{"retention.ms": &ret}, topic)
		if err == nil && resp.Err == nil {
			return nil
		}
		if err == nil && errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return nil
		}
		if attempt > 60 {
			if err == nil {
				err = resp.Err
			}
			return err
		}
		time.Sleep(time.Second) // broker still starting
	}
}
