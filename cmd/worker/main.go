// Command worker runs one stream worker (one consumer-group member).
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sohamb17/streamforge/deploy/sql"
	"github.com/sohamb17/streamforge/internal/cache"
	"github.com/sohamb17/streamforge/internal/history"
	"github.com/sohamb17/streamforge/internal/storeclient"
	"github.com/sohamb17/streamforge/internal/window"
	"github.com/sohamb17/streamforge/internal/worker"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap servers")
	topic := flag.String("topic", "trips", "topic")
	group := flag.String("group", "streamforge-workers", "consumer group")
	stores := flag.String("store", "1=localhost:7001", "store nodes: id=host:port,...")
	pg := flag.String("postgres", "", "PostgreSQL DSN for the offline history (empty = disabled)")
	redisAddr := flag.String("redis", "", "Redis address for cache invalidation (empty = disabled)")
	lateness := flag.Duration("lateness", 30*time.Second, "allowed lateness")
	httpAddr := flag.String("http", ":9103", "metrics/admin address")
	name := flag.String("name", "", "worker name (default hostname)")
	flag.Parse()

	if *name == "" {
		*name, _ = os.Hostname()
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "worker", "worker", *name)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addrs, err := storeclient.ParseAddrs(*stores)
	if err != nil {
		log.Error("bad -store", "err", err)
		os.Exit(2)
	}
	sc, err := storeclient.New(addrs, "worker-"+*name)
	if err != nil {
		log.Error("store client", "err", err)
		os.Exit(1)
	}
	cfg := worker.Config{
		Topic: *topic, Store: sc, Log: log,
		Window: window.Config{BucketMs: window.DefaultBucketMs, LatenessMs: lateness.Milliseconds()},
	}
	if *pg != "" {
		h, err := history.Open(ctx, *pg, sql.FeatureHistory)
		if err != nil {
			log.Error("postgres", "err", err)
			os.Exit(1)
		}
		cfg.History = h
	}
	if *redisAddr != "" {
		cfg.Cache = &cache.Cache{R: redis.NewClient(&redis.Options{
			Addr: *redisAddr, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond,
			WriteTimeout: 100 * time.Millisecond, MaxRetries: -1, DialerRetries: 1, ContextTimeoutEnabled: true,
		})}
	}
	w := worker.New(cfg)
	opts := append([]kgo.Opt{kgo.SeedBrokers(*brokers), kgo.ClientID(*name)}, w.Options(*group)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	// POST /admin/redeliver?partition=0&n=500 re-sends the last committed
	// batch and rewinds Kafka by n offsets. Both must be ignored downstream.
	mux.HandleFunc("/admin/redeliver", func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(rw, "POST only", http.StatusMethodNotAllowed)
			return
		}
		n, _ := strconv.ParseInt(r.URL.Query().Get("n"), 10, 64)
		if n <= 0 || n > 1_000_000 {
			n = 500
		}
		var done []int32
		ps := w.Assigned()
		if q := r.URL.Query().Get("partition"); q != "" {
			p, _ := strconv.Atoi(q)
			ps = []int32{int32(p)}
		}
		for _, p := range ps {
			if w.Redeliver(p, n) {
				done = append(done, p)
			}
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"partitions":` + jsonInts(done) + `}`))
	})
	go http.ListenAndServe(*httpAddr, mux)

	log.Info("worker up", "topic", *topic, "group", *group)
	if err := w.Run(ctx, cl); err != nil && ctx.Err() == nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
}

func jsonInts(xs []int32) string {
	s := "["
	for i, x := range xs {
		if i > 0 {
			s += ","
		}
		s += strconv.Itoa(int(x))
	}
	return s + "]"
}
