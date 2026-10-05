// Package storeserver exposes a raftnode.Node over the StoreService gRPC API.
package storeserver

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/raft"
	"github.com/sohamb17/streamforge/internal/raftnode"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

// LeaderTrailer is the gRPC trailer naming the leader's id on a
// FailedPrecondition (not leader) error.
const LeaderTrailer = "sf-leader"

// Server implements StoreService.
type Server struct {
	sfv1.UnimplementedStoreServiceServer
	Node *raftnode.Node
}

func toStatus(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		_ = grpc.SetTrailer(ctx, metadata.Pairs(LeaderTrailer, strconv.FormatUint(nl.Lead, 10)))
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, raft.ErrNotReady):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, raftnode.ErrUnknownOutcome):
		return status.Error(codes.Unknown, err.Error())
	case errors.Is(err, raftnode.ErrDropped):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *Server) propose(ctx context.Context, cmd *sfv1.Command) (store.Result, error) {
	data, err := store.EncodeCommand(cmd)
	if err != nil {
		return store.Result{}, status.Error(codes.InvalidArgument, err.Error())
	}
	res, err := s.Node.Propose(ctx, data)
	if err != nil {
		return res, toStatus(ctx, err)
	}
	if res.Err != nil {
		return res, status.Error(codes.InvalidArgument, res.Err.Error())
	}
	return res, nil
}

// Put implements StoreService.
func (s *Server) Put(ctx context.Context, req *sfv1.PutRequest) (*sfv1.PutResponse, error) {
	res, err := s.propose(ctx, &sfv1.Command{ClientId: req.ClientId, Seq: req.Seq,
		Op: &sfv1.Command_Put{Put: &sfv1.PutOp{Key: req.Key, Value: req.Value}}})
	if err != nil {
		return nil, err
	}
	return &sfv1.PutResponse{RaftIndex: res.Index}, nil
}

// Purge implements StoreService.
func (s *Server) Purge(ctx context.Context, req *sfv1.PurgeRequest) (*sfv1.PurgeResponse, error) {
	res, err := s.propose(ctx, &sfv1.Command{Op: &sfv1.Command_Purge{Purge: &sfv1.PurgeOp{Prefix: req.Prefix}}})
	if err != nil {
		return nil, err
	}
	return &sfv1.PurgeResponse{Deleted: res.Count}, nil
}

// Get implements StoreService.
func (s *Server) Get(ctx context.Context, req *sfv1.GetRequest) (*sfv1.GetResponse, error) {
	if req.Linearizable {
		if err := s.Node.LinearizableRead(ctx); err != nil {
			return nil, toStatus(ctx, err)
		}
	}
	resp := &sfv1.GetResponse{}
	s.Node.View(func(sm *store.SM) {
		resp.Value, resp.Found = sm.Get(req.Key)
		resp.AppliedIndex = sm.AppliedIndex()
	})
	return resp, nil
}

// ProposeBatch implements StoreService.
func (s *Server) ProposeBatch(ctx context.Context, req *sfv1.ProposeBatchRequest) (*sfv1.ProposeBatchResponse, error) {
	res, err := s.propose(ctx, &sfv1.Command{ClientId: req.ClientId, Seq: req.Seq,
		Op: &sfv1.Command_Batch{Batch: req.Batch}})
	if err != nil {
		return nil, err
	}
	return &sfv1.ProposeBatchResponse{Applied: res.Applied, RaftIndex: res.Index}, nil
}

// GetPartitionState implements StoreService. Always linearizable: a worker
// must never resume from a stale offset.
func (s *Server) GetPartitionState(ctx context.Context, req *sfv1.GetPartitionStateRequest) (*sfv1.GetPartitionStateResponse, error) {
	if err := s.Node.LinearizableRead(ctx); err != nil {
		return nil, toStatus(ctx, err)
	}
	var st window.State
	s.Node.View(func(sm *store.SM) { st = sm.PartitionState(req.Partition) })
	return &sfv1.GetPartitionStateResponse{State: store.StateToProto(st)}, nil
}

// GetZoneFeatures implements StoreService.
func (s *Server) GetZoneFeatures(ctx context.Context, req *sfv1.GetZoneFeaturesRequest) (*sfv1.GetZoneFeaturesResponse, error) {
	if req.Linearizable {
		if err := s.Node.LinearizableRead(ctx); err != nil {
			return nil, toStatus(ctx, err)
		}
	}
	resp := &sfv1.GetZoneFeaturesResponse{}
	s.Node.View(func(sm *store.SM) {
		for _, f := range sm.Features(req.Zones) {
			resp.Features = append(resp.Features, store.FeaturesToProto(f))
		}
		resp.AppliedIndex = sm.AppliedIndex()
	})
	return resp, nil
}

// Status implements StoreService.
func (s *Server) Status(ctx context.Context, req *sfv1.StatusRequest) (*sfv1.StatusResponse, error) {
	status := s.Node.Status
	if req.Fingerprint {
		status = s.Node.StatusWithFingerprint
	}
	st, err := status(ctx)
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	ns := &sfv1.NodeStatus{
		StateFingerprint: st.Fingerprint,
		Id:               st.ID, Role: st.Role.String(), Term: st.Term, LeaderId: st.Lead,
		CommitIndex: st.Commit, AppliedIndex: st.Applied, LastLogIndex: st.LastIndex,
		SnapshotIndex: st.SnapIndex, SentTo: st.SentTo, PartitionOffsets: st.PartitionOffsets,
		DuplicateBatchesIgnored: st.DuplicateBatches, ElectionsStarted: st.ElectionsStarted,
		StartedAtMs: st.StartedAt.UnixMilli(), SnapshotsTaken: st.SnapshotsTaken,
		SnapshotsInstalled: st.SnapshotsInstalled,
	}
	for id, p := range st.Progress {
		ns.Peers = append(ns.Peers, &sfv1.PeerProgress{Id: id, MatchIndex: p.Match, NextIndex: p.Next, RecentlyActive: p.RecentActive})
	}
	return &sfv1.StatusResponse{Status: ns}, nil
}
