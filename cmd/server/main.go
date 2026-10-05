// Command server runs FeatureService (gRPC) plus a small JSON endpoint for
// curl and the dashboard:
//
//	GET /v1/features?zones=161,237&mode=linearizable
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/cache"
	"github.com/sohamb17/streamforge/internal/serving"
	"github.com/sohamb17/streamforge/internal/storeclient"
)

func main() {
	listen := flag.String("listen", ":50051", "gRPC address")
	httpAddr := flag.String("http", ":8081", "metrics and JSON address")
	stores := flag.String("store", "1=localhost:7001", "store nodes")
	redisAddr := flag.String("redis", "", "Redis address (empty disables the cache)")
	ttl := flag.Duration("cache-ttl", 2*time.Second, "cache TTL = staleness bound for cache hits")
	defDeadline := flag.Duration("default-deadline", 200*time.Millisecond, "deadline applied when the caller sets none")
	maxDeadline := flag.Duration("max-deadline", 2*time.Second, "cap on caller deadlines")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "server")
	addrs, err := storeclient.ParseAddrs(*stores)
	if err != nil {
		log.Error("bad -store", "err", err)
		os.Exit(2)
	}
	sc, err := storeclient.New(addrs, "server")
	if err != nil {
		log.Error("store client", "err", err)
		os.Exit(1)
	}
	// A read attempt against a dead node must fail fast so the retry can
	// reach the new leader inside the caller's deadline.
	sc.PerAttemptTimeout = 150 * time.Millisecond
	srv := &serving.Server{Store: sc, DefaultDeadline: *defDeadline, MaxDeadline: *maxDeadline}
	if *redisAddr != "" {
		srv.Cache = &cache.Cache{TTL: *ttl, R: redis.NewClient(&redis.Options{
			Addr: *redisAddr, DialTimeout: 100 * time.Millisecond,
			ReadTimeout: 50 * time.Millisecond, WriteTimeout: 50 * time.Millisecond,
			PoolSize: 64, MaxRetries: -1,
			// Fail fast: one dial attempt, and honor the request deadline
			// (go-redis ignores context deadlines unless told otherwise).
			DialerRetries: 1, ContextTimeoutEnabled: true,
		})}
	}

	gs := grpc.NewServer()
	sfv1.RegisterFeatureServiceServer(gs, srv)
	reflection.Register(gs) // lets grpcurl and ghz discover the API
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	go gs.Serve(lis)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/v1/features", func(w http.ResponseWriter, r *http.Request) {
		req := &sfv1.GetFeaturesRequest{FeatureView: serving.FeatureView, EntityIds: strings.Split(r.URL.Query().Get("zones"), ",")}
		if r.URL.Query().Get("mode") == "linearizable" {
			req.Mode = sfv1.ReadMode_READ_MODE_LINEARIZABLE
		}
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		resp, err := srv.GetFeatures(ctx, req)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": status.Convert(err).Message(), "code": status.Code(err).String()})
			return
		}
		b, _ := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(resp)
		w.Write(b)
	})
	go http.ListenAndServe(*httpAddr, mux)
	log.Info("server up", "grpc", *listen, "http", *httpAddr, "cache", *redisAddr != "", "ttl", ttl.String())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	gs.GracefulStop()
}
