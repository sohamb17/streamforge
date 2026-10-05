// Command console serves the live dashboard: the web UI, a Server-Sent
// Events stream of the system snapshot, and the chaos API.
//
//	GET  /api/stream          text/event-stream of console.Snapshot (2/s)
//	GET  /api/snapshot        one snapshot as JSON
//	POST /api/chaos/{action}  inject a self-healing fault (see chaos.go)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sohamb17/streamforge/internal/console"
	"github.com/sohamb17/streamforge/internal/storeclient"
)

func main() {
	listen := flag.String("listen", ":8080", "HTTP address")
	webDir := flag.String("web", "/srv/web", "built dashboard directory")
	stores := flag.String("store", "1=localhost:7001", "store nodes (gRPC)")
	adminPort := flag.String("admin-port", "9100", "storenode admin HTTP port (same host names as -store)")
	workers := flag.String("workers", "worker1:9103,worker2:9103", "worker admin endpoints")
	brokers := flag.String("brokers", "localhost:9092", "Kafka")
	topic := flag.String("topic", "trips", "topic")
	prom := flag.String("prometheus", "http://localhost:9090", "Prometheus URL")
	project := flag.String("compose-project", "streamforge", "compose project name")
	chaos := flag.String("chaos", "on", "on | off: allow visitors to inject faults")
	server := flag.String("server", "", "FeatureService address; with -model-qps the console calls it like a model would")
	modelQPS := flag.Float64("model-qps", 20, "simulated model requests per second to FeatureService (0 = off)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "console")
	addrs, err := storeclient.ParseAddrs(*stores)
	if err != nil {
		log.Error("bad -store", "err", err)
		os.Exit(2)
	}
	admin := map[uint64]string{}
	for id, a := range addrs {
		host, _, _ := net.SplitHostPort(a)
		admin[id] = net.JoinHostPort(host, *adminPort)
	}
	live, err := console.NewLive(console.Config{
		StoreAddrs: addrs, AdminAddrs: admin, WorkerAdmin: strings.Split(*workers, ","),
		Brokers: strings.Split(*brokers, ","), Topic: *topic, Prometheus: *prom, Project: *project,
		Chaos: *chaos == "on", Log: log,
	})
	if err != nil {
		log.Error("console", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	live.Run(ctx)
	if *server != "" && *modelQPS > 0 {
		go console.ModelTraffic(ctx, *server, *modelQPS, log)
	}

	cors := func(w http.ResponseWriter) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(live.Snapshot())
	})
	mux.HandleFunc("/api/stream", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			b, _ := json.Marshal(live.Snapshot())
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			fl.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-t.C:
			}
		}
	})
	mux.HandleFunc("/api/chaos/", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		if r.Method == http.MethodOptions {
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		action := strings.TrimPrefix(r.URL.Path, "/api/chaos/")
		ip := r.Header.Get("X-Forwarded-For")
		if ip == "" {
			ip, _, _ = net.SplitHostPort(r.RemoteAddr)
		}
		ip = strings.TrimSpace(strings.Split(ip, ",")[0])
		cctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		if err := live.Chaos(cctx, action, ip); err != nil {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"ok": action})
	})
	fsrv := http.FileServer(http.Dir(*webDir))
	mux.Handle("/", fsrv)
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go srv.ListenAndServe()
	log.Info("console up", "listen", *listen, "chaos", *chaos)
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
}
