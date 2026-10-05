package raftnode_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/raft"
	"github.com/sohamb17/streamforge/internal/raftnode"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/storeclient"
	"github.com/sohamb17/streamforge/internal/storeserver"
)

type testNode struct {
	id   uint64
	dir  string
	node *raftnode.Node
	gs   *grpc.Server
	lis  net.Listener
}

type testCluster struct {
	t     *testing.T
	addrs map[uint64]string
	ids   []uint64
	nodes map[uint64]*testNode
	base  string
}

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newCluster(t *testing.T, n int) *testCluster {
	c := &testCluster{t: t, addrs: map[uint64]string{}, nodes: map[uint64]*testNode{}, base: t.TempDir()}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, uint64(i))
		c.addrs[uint64(i)] = freePort(t)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) start(id uint64) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	tr, err := raftnode.NewGRPCTransport(id, c.addrs, log)
	if err != nil {
		c.t.Fatal(err)
	}
	dir := filepath.Join(c.base, fmt.Sprint(id))
	node, err := raftnode.Start(raftnode.Config{
		ID: id, Peers: c.ids, Dir: dir, Fsync: true, TickInterval: 10 * time.Millisecond,
		ElectionTick: 15, HeartbeatTick: 3, SnapshotEvery: 50, Transport: tr, Logger: log,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	tr.SetReceiver(node.Step)
	gs := grpc.NewServer()
	sfv1.RegisterRaftTransportServiceServer(gs, &raftnode.Server{T: tr})
	sfv1.RegisterStoreServiceServer(gs, &storeserver.Server{Node: node})
	var lis net.Listener
	for i := 0; i < 50; i++ {
		if lis, err = net.Listen("tcp", c.addrs[id]); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	go gs.Serve(lis)
	c.nodes[id] = &testNode{id: id, dir: dir, node: node, gs: gs, lis: lis}
}

func (c *testCluster) stop(id uint64) {
	tn, ok := c.nodes[id]
	if !ok {
		return
	}
	tn.gs.Stop()
	tn.node.Stop()
	delete(c.nodes, id)
}

func (c *testCluster) leader() uint64 {
	for id, tn := range c.nodes {
		if tn.node.IsLeader() {
			return id
		}
	}
	return 0
}

func (c *testCluster) waitLeader(timeout time.Duration) uint64 {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != 0 {
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0
}

func TestClusterPutGetFailoverAndCatchUp(t *testing.T) {
	c := newCluster(t, 5)
	cli, err := storeclient.New(c.addrs, "test-client")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	old := c.waitLeader(5 * time.Second)

	for i := 0; i < 40; i++ {
		if err := cli.Put(ctx, "k", fmt.Sprint(i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Take a follower down, write enough to force a snapshot past it.
	var lagger uint64
	for _, id := range c.ids {
		if id != old {
			lagger = id
			break
		}
	}
	c.stop(lagger)
	c.stop(old) // and crash the leader
	t0 := time.Now()
	for i := 40; i < 160; i++ {
		if err := cli.Put(ctx, "k", fmt.Sprint(i)); err != nil {
			t.Fatalf("put %d after leader crash: %v", i, err)
		}
		if i == 40 {
			t.Logf("first write after leader crash took %v", time.Since(t0))
		}
	}
	nl := c.leader()
	if nl == 0 || nl == old {
		t.Fatalf("expected a new leader, got %d", nl)
	}
	v, ok, err := cli.Get(ctx, "k", true)
	if err != nil || !ok || v != "159" {
		t.Fatalf("linearizable get = %q %v %v", v, ok, err)
	}
	// Restart both from disk; the lagger must catch up by snapshot.
	c.start(old)
	c.start(lagger)
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := c.nodes[lagger].node.Status(ctx)
		if err == nil && st.Applied >= st.Commit && st.Commit > 160 {
			if st.SnapshotsInstalled == 0 {
				t.Fatalf("lagger caught up without a snapshot install (applied %d)", st.Applied)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lagger did not catch up: %+v %v", st.Status, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range c.ids {
		var v string
		c.nodes[id].node.View(func(sm *store.SM) { v, _ = sm.Get("k") })
		if v != "159" {
			t.Fatalf("node %d has k=%q after catch-up", id, v)
		}
	}
}

func TestRecoverFromTornWAL(t *testing.T) {
	dir := t.TempDir()
	ds, _, err := raftnode.OpenDiskStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	var es []raft.Entry
	for i := uint64(1); i <= 10; i++ {
		es = append(es, raft.Entry{Term: 1, Index: i, Data: []byte(fmt.Sprintf("e%d", i))})
	}
	hs := raft.HardState{Term: 1, Vote: 1, Commit: 10}
	if err := ds.Save(&hs, es, true); err != nil {
		t.Fatal(err)
	}
	// Conflict: entries 8.. replaced by term 2.
	if err := ds.Save(nil, []raft.Entry{{Term: 2, Index: 8, Data: []byte("x")}}, false); err != nil {
		t.Fatal(err)
	}
	ds.Close()
	// Simulate a torn write: append garbage half-record.
	f, _ := os.OpenFile(filepath.Join(dir, "wal"), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{200, 0, 0, 0, 1, 2, 3})
	f.Close()
	_, st, err := raftnode.OpenDiskStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Entries) != 8 || st.Entries[7].Term != 2 || st.HardState.Term != 1 {
		t.Fatalf("recovered %d entries, last %+v, hs %+v", len(st.Entries), st.Entries[len(st.Entries)-1], st.HardState)
	}
	if st.HardState.Commit != 8 {
		t.Fatalf("commit should be clamped to last index, got %d", st.HardState.Commit)
	}
}
