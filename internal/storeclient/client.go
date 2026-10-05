// Package storeclient is the client library for the 5-node online store.
// It finds the leader (following "sf-leader" hints), retries on leader
// changes, and makes retries safe: Puts carry (client id, sequence number)
// and feature batches carry Kafka offsets, so the state machine applies
// each at most once no matter how often it is re-sent.
package storeclient

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
)

const leaderTrailer = "sf-leader"

// Client talks to the store cluster.
type Client struct {
	closers []*grpc.ClientConn
	ids     []uint64
	conns   map[uint64]sfv1.StoreServiceClient
	leader  atomic.Uint64
	next    atomic.Uint64

	clientID string
	putMu    sync.Mutex // one outstanding Put per client id (see Put)
	seq      uint64

	// PerAttemptTimeout bounds one RPC attempt so a dead node does not eat
	// the whole caller deadline.
	PerAttemptTimeout time.Duration
}

// New dials every node. addrs maps node id to host:port.
func New(addrs map[uint64]string, clientID string) (*Client, error) {
	c := &Client{conns: map[uint64]sfv1.StoreServiceClient{}, clientID: clientID, PerAttemptTimeout: 1500 * time.Millisecond}
	for id, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
		if err != nil {
			return nil, err
		}
		c.conns[id] = sfv1.NewStoreServiceClient(conn)
		c.closers = append(c.closers, conn)
		c.ids = append(c.ids, id)
	}
	for i := range c.ids {
		for j := i + 1; j < len(c.ids); j++ {
			if c.ids[j] < c.ids[i] {
				c.ids[i], c.ids[j] = c.ids[j], c.ids[i]
			}
		}
	}
	return c, nil
}

// ParseAddrs parses "1=host:port,2=host:port".
func ParseAddrs(s string) (map[uint64]string, error) {
	out := map[uint64]string{}
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			part := s[start:i]
			start = i + 1
			if part == "" {
				continue
			}
			eq := -1
			for j := range part {
				if part[j] == '=' {
					eq = j
					break
				}
			}
			if eq < 0 {
				return nil, fmt.Errorf("bad peer %q", part)
			}
			id, err := strconv.ParseUint(part[:eq], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("bad peer id %q", part)
			}
			out[id] = part[eq+1:]
		}
	}
	return out, nil
}

// IDs returns node ids.
func (c *Client) IDs() []uint64 { return c.ids }

// Leader returns the last known leader id.
func (c *Client) Leader() uint64 { return c.leader.Load() }

func (c *Client) target() uint64 {
	if l := c.leader.Load(); l != 0 {
		return l
	}
	return c.ids[int(c.next.Add(1))%len(c.ids)]
}

// ErrUnknown is returned when the deadline expired while the outcome of a
// write was unknown: it may or may not have been applied.
var ErrUnknown = errors.New("storeclient: write outcome unknown at deadline")

// call runs fn against the leader until it succeeds, fails permanently or
// ctx expires. write marks operations whose outcome can be unknown.
func (c *Client) call(ctx context.Context, write bool, fn func(ctx context.Context, cli sfv1.StoreServiceClient, opts ...grpc.CallOption) error) error {
	unknown := false
	backoff := 5 * time.Millisecond
	for {
		id := c.target()
		var md metadata.MD
		actx, cancel := context.WithTimeout(ctx, c.PerAttemptTimeout)
		err := fn(actx, c.conns[id], grpc.Trailer(&md))
		cancel()
		if err == nil {
			c.leader.Store(id)
			return nil
		}
		code := status.Code(err)
		switch code {
		case codes.FailedPrecondition:
			// Not the leader. Follow the hint if there is one.
			c.leader.Store(0)
			if v := md.Get(leaderTrailer); len(v) > 0 {
				if h, _ := strconv.ParseUint(v[0], 10, 64); h != 0 && h != id {
					c.leader.Store(h)
					continue
				}
			}
		case codes.InvalidArgument, codes.NotFound, codes.Aborted:
			if code == codes.Aborted && write {
				// Definitely not applied (overwritten); safe to retry.
				break
			}
			return err
		case codes.Unknown, codes.DeadlineExceeded, codes.Canceled:
			if write {
				unknown = true
			}
			c.leader.Store(0)
		default: // Unavailable and transport errors
			c.leader.Store(0)
		}
		select {
		case <-ctx.Done():
			if unknown {
				return ErrUnknown
			}
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 100*time.Millisecond {
			backoff *= 2
		}
	}
}

// Put writes a register linearizably. A Client allows one Put at a time:
// the state machine remembers only the latest sequence number per client
// id, so concurrent Puts from one id could be mistaken for duplicates.
func (c *Client) Put(ctx context.Context, key, value string) error {
	c.putMu.Lock()
	defer c.putMu.Unlock()
	c.seq++
	req := &sfv1.PutRequest{ClientId: c.clientID, Seq: c.seq, Key: key, Value: value}
	return c.call(ctx, true, func(ctx context.Context, cli sfv1.StoreServiceClient, o ...grpc.CallOption) error {
		_, err := cli.Put(ctx, req, o...)
		return err
	})
}

// Get reads a register. Linearizable reads go through the leader.
func (c *Client) Get(ctx context.Context, key string, linearizable bool) (string, bool, error) {
	var resp *sfv1.GetResponse
	err := c.call(ctx, false, func(ctx context.Context, cli sfv1.StoreServiceClient, o ...grpc.CallOption) error {
		var err error
		resp, err = cli.Get(ctx, &sfv1.GetRequest{Key: key, Linearizable: linearizable}, o...)
		return err
	})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// ProposeBatch commits a feature batch. applied is false if the store had
// already applied this offset (a retry or replay), which is also success.
func (c *Client) ProposeBatch(ctx context.Context, b *sfv1.FeatureBatch) (applied bool, err error) {
	req := &sfv1.ProposeBatchRequest{ClientId: c.clientID, Batch: b}
	err = c.call(ctx, true, func(ctx context.Context, cli sfv1.StoreServiceClient, o ...grpc.CallOption) error {
		resp, err := cli.ProposeBatch(ctx, req, o...)
		if err == nil {
			applied = resp.Applied
		}
		return err
	})
	return applied, err
}

// PartitionState reads a partition's committed state linearizably.
func (c *Client) PartitionState(ctx context.Context, p int32) (*sfv1.PartitionState, error) {
	var st *sfv1.PartitionState
	err := c.call(ctx, false, func(ctx context.Context, cli sfv1.StoreServiceClient, o ...grpc.CallOption) error {
		resp, err := cli.GetPartitionState(ctx, &sfv1.GetPartitionStateRequest{Partition: p}, o...)
		if err == nil {
			st = resp.State
		}
		return err
	})
	return st, err
}

// ZoneFeatures reads feature rows linearizably from the leader.
func (c *Client) ZoneFeatures(ctx context.Context, zones []int32) ([]*sfv1.ZoneFeatures, uint64, error) {
	var resp *sfv1.GetZoneFeaturesResponse
	err := c.call(ctx, false, func(ctx context.Context, cli sfv1.StoreServiceClient, o ...grpc.CallOption) error {
		var err error
		resp, err = cli.GetZoneFeatures(ctx, &sfv1.GetZoneFeaturesRequest{Zones: zones, Linearizable: true}, o...)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return resp.Features, resp.AppliedIndex, nil
}

// LocalZoneFeatures reads feature rows from one specific node's local state
// (no consistency guarantee; used by the dashboard and replication tests).
func (c *Client) LocalZoneFeatures(ctx context.Context, node uint64, zones []int32) (*sfv1.GetZoneFeaturesResponse, error) {
	cli, ok := c.conns[node]
	if !ok {
		return nil, fmt.Errorf("unknown node %d", node)
	}
	return cli.GetZoneFeatures(ctx, &sfv1.GetZoneFeaturesRequest{Zones: zones})
}

// Status queries one node.
func (c *Client) Status(ctx context.Context, node uint64) (*sfv1.NodeStatus, error) {
	cli, ok := c.conns[node]
	if !ok {
		return nil, fmt.Errorf("unknown node %d", node)
	}
	resp, err := cli.Status(ctx, &sfv1.StatusRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Status, nil
}

// GetLocal reads a register from one node's local state machine with no
// consistency guarantee. It exists for the negative-control experiment that
// shows the linearizability checker does catch stale reads.
func (c *Client) GetLocal(ctx context.Context, node uint64, key string) (string, bool, error) {
	cli, ok := c.conns[node]
	if !ok {
		return "", false, fmt.Errorf("unknown node %d", node)
	}
	resp, err := cli.Get(ctx, &sfv1.GetRequest{Key: key})
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// Close releases the connections.
func (c *Client) Close() {
	for _, cc := range c.closers {
		cc.Close()
	}
}

// Purge deletes every register with the given key prefix (and dedup
// records of client ids with that prefix). Retrying is safe: it is
// idempotent.
func (c *Client) Purge(ctx context.Context, prefix string) (uint64, error) {
	var n uint64
	err := c.call(ctx, true, func(ctx context.Context, cli sfv1.StoreServiceClient, o ...grpc.CallOption) error {
		resp, err := cli.Purge(ctx, &sfv1.PurgeRequest{Prefix: prefix}, o...)
		if err == nil {
			n = resp.Deleted
		}
		return err
	})
	return n, err
}
