// Command storenode runs one member of the 5-node Raft online store.
//
//	storenode -id 1 -peers 1=storenode1:7000,2=storenode2:7000,... -data /data
//
// One gRPC port serves both Raft traffic (RaftTransportService) and the
// store API (StoreService). An HTTP port serves /metrics, /healthz and the
// admin endpoints used by fault injection.
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
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/raftnode"
	"github.com/sohamb17/streamforge/internal/storeclient"
	"github.com/sohamb17/streamforge/internal/storeserver"
)

func main() {
	id := flag.Uint64("id", 1, "node id")
	peers := flag.String("peers", "1=localhost:7001", "all members: id=host:port,...")
	listen := flag.String("listen", ":7000", "gRPC listen address")
	httpAddr := flag.String("http", ":9100", "metrics/admin listen address")
	dir := flag.String("data", "./data/raft", "data directory")
	fsync := flag.Bool("fsync", true, "fsync the WAL before acknowledging (disable only for experiments)")
	tick := flag.Duration("tick", 20*time.Millisecond, "Raft tick")
	electionTicks := flag.Int("election-ticks", 15, "election timeout in ticks (randomized up to 2x)")
	heartbeatTicks := flag.Int("heartbeat-ticks", 3, "heartbeat interval in ticks")
	snapEvery := flag.Uint64("snapshot-every", 2000, "entries between snapshots")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("svc", "storenode")
	addrs, err := storeclient.ParseAddrs(*peers)
	if err != nil {
		log.Error("bad -peers", "err", err)
		os.Exit(2)
	}
	ids := make([]uint64, 0, len(addrs))
	for p := range addrs {
		ids = append(ids, p)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	tr, err := raftnode.NewGRPCTransport(*id, addrs, log)
	if err != nil {
		log.Error("transport", "err", err)
		os.Exit(1)
	}
	node, err := raftnode.Start(raftnode.Config{
		ID: *id, Peers: ids, Dir: *dir, Fsync: *fsync,
		TickInterval: *tick, ElectionTick: *electionTicks, HeartbeatTick: *heartbeatTicks,
		SnapshotEvery: *snapEvery, Transport: tr, Logger: log,
	})
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	tr.SetReceiver(node.Step)

	gs := grpc.NewServer(grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))
	sfv1.RegisterRaftTransportServiceServer(gs, &raftnode.Server{T: tr})
	sfv1.RegisterStoreServiceServer(gs, &storeserver.Server{Node: node})
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	go gs.Serve(lis)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	// POST /admin/block?peers=4,5 drops all Raft traffic to and from those
	// peers (a transport-level partition). POST /admin/block clears it.
	mux.HandleFunc("/admin/block", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var blocked []uint64
		for _, s := range strings.Split(r.URL.Query().Get("peers"), ",") {
			if s == "" {
				continue
			}
			v, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				http.Error(w, "bad peer", http.StatusBadRequest)
				return
			}
			blocked = append(blocked, v)
		}
		tr.Block(blocked)
		log.Warn("transport partition set", "blocked", blocked)
		json.NewEncoder(w).Encode(map[string]any{"blocked": blocked})
	})
	mux.HandleFunc("/admin/status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		st, err := node.StatusWithFingerprint(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": st, "role": st.Role.String(), "blocked": tr.Blocked()})
	})
	go http.ListenAndServe(*httpAddr, mux)
	log.Info("storenode up", "id", *id, "peers", *peers, "fsync", *fsync)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	gs.Stop()
	node.Stop()
}
