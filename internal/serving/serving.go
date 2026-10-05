// Package serving implements FeatureService: the read path a model calls.
//
// Read modes (one per request, stated in the response):
//   - BOUNDED_STALENESS: Redis first; misses are read from the Raft leader
//     with ReadIndex and written back to Redis with a TTL. A cache hit can be
//     at most TTL older than the committed state, and the response says so
//     in staleness_bound_ms. This is never called linearizable.
//   - LINEARIZABLE: always a ReadIndex read on the leader; no cache.
//
// Every call needs a deadline. The server caps it, propagates it to Redis
// and to the store, and answers DEADLINE_EXCEEDED instead of queueing.
package serving

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sfv1 "github.com/sohamb17/streamforge/gen/streamforge/v1"
	"github.com/sohamb17/streamforge/internal/cache"
	"github.com/sohamb17/streamforge/internal/store"
	"github.com/sohamb17/streamforge/internal/window"
)

// FeatureView is the only view served today.
const FeatureView = "zone_demand_v1"

// MaxEntities bounds one request.
const MaxEntities = 300

// Store is the read API of the online store.
type Store interface {
	ZoneFeatures(ctx context.Context, zones []int32) ([]*sfv1.ZoneFeatures, uint64, error)
}

var (
	latencyBuckets = prometheus.ExponentialBuckets(0.0001, 2, 17) // 0.1 ms .. 6.5 s
	mLatency       = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "streamforge_serving_latency_seconds", Help: "Server-side GetFeatures latency.", Buckets: latencyBuckets},
		[]string{"mode", "served_from"})
	mRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_serving_requests_total", Help: "GetFeatures requests by gRPC code."}, []string{"code"})
	mCache = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_serving_cache_lookups_total", Help: "Per-entity cache lookups by result (hit, miss, error, bypassed)."}, []string{"result"})
	mCoalesced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamforge_serving_coalesced_reads_total", Help: "Cache misses served by another request's in-flight store read."})
	mBreaker = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamforge_serving_cache_breaker_trips_total", Help: "Times the cache was bypassed after a Redis error."})
)

// Server implements FeatureServiceServer.
type Server struct {
	sfv1.UnimplementedFeatureServiceServer
	Store Store
	Cache *cache.Cache // nil disables caching
	// DefaultDeadline applies when the caller sent none; MaxDeadline caps
	// what callers ask for.
	DefaultDeadline time.Duration
	MaxDeadline     time.Duration

	// Circuit breaker: after a Redis error the cache is bypassed for
	// BreakerCooldown, so a dead Redis costs one timeout, not one per
	// request.
	BreakerCooldown time.Duration
	cacheOffUntil   atomic.Int64 // unix nanos

	// flight coalesces concurrent store reads for the same set of missed
	// zones: when a hot key expires, one request refills it and the others
	// wait for that result instead of stampeding the Raft leader.
	flight singleflight.Group
}

func (s *Server) cacheUsable() bool {
	return s.Cache != nil && time.Now().UnixNano() >= s.cacheOffUntil.Load()
}

func (s *Server) tripBreaker() {
	cd := s.BreakerCooldown
	if cd == 0 {
		cd = time.Second
	}
	s.cacheOffUntil.Store(time.Now().Add(cd).UnixNano())
	mBreaker.Inc()
}

// GetFeatures implements FeatureServiceServer.
func (s *Server) GetFeatures(ctx context.Context, req *sfv1.GetFeaturesRequest) (resp *sfv1.GetFeaturesResponse, err error) {
	start := time.Now()
	mode := req.Mode
	if mode == sfv1.ReadMode_READ_MODE_UNSPECIFIED {
		mode = sfv1.ReadMode_READ_MODE_BOUNDED_STALENESS
	}
	defer func() {
		mRequests.WithLabelValues(status.Code(err).String()).Inc()
		if err == nil {
			mLatency.WithLabelValues(modeLabel(mode), resp.ServedFrom.String()).Observe(time.Since(start).Seconds())
		}
	}()

	dl, ok := ctx.Deadline()
	switch {
	case !ok:
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.DefaultDeadline)
		defer cancel()
	case time.Until(dl) > s.MaxDeadline:
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.MaxDeadline)
		defer cancel()
	}

	if req.FeatureView != "" && req.FeatureView != FeatureView {
		return nil, status.Errorf(codes.NotFound, "unknown feature view %q", req.FeatureView)
	}
	if len(req.EntityIds) == 0 || len(req.EntityIds) > MaxEntities {
		return nil, status.Errorf(codes.InvalidArgument, "need 1..%d entity ids", MaxEntities)
	}
	zones := make([]int32, 0, len(req.EntityIds))
	seen := map[int32]bool{}
	for _, id := range req.EntityIds {
		z, err := strconv.Atoi(id)
		if err != nil || z < 1 || z > 263 {
			return nil, status.Errorf(codes.InvalidArgument, "entity id %q is not a TLC zone (1..263)", id)
		}
		if !seen[int32(z)] {
			seen[int32(z)] = true
			zones = append(zones, int32(z))
		}
	}

	rows := make(map[int32]*sfv1.ZoneFeatures, len(zones))
	misses := zones
	fromCache := 0
	useCache := mode == sfv1.ReadMode_READ_MODE_BOUNDED_STALENESS && s.cacheUsable()
	if mode == sfv1.ReadMode_READ_MODE_BOUNDED_STALENESS && s.Cache != nil && !useCache {
		mCache.WithLabelValues("bypassed").Add(float64(len(zones)))
	}
	if useCache {
		hit, cerr := s.Cache.Get(ctx, zones)
		if cerr != nil {
			// Redis is down or slow: serve from the store. Only the latency
			// changes; correctness does not depend on the cache.
			mCache.WithLabelValues("error").Add(float64(len(zones)))
			s.tripBreaker()
		} else {
			misses = misses[:0:0]
			for _, z := range zones {
				if f, ok := hit[z]; ok {
					rows[z] = f
					fromCache++
				} else {
					misses = append(misses, z)
				}
			}
			mCache.WithLabelValues("hit").Add(float64(fromCache))
			mCache.WithLabelValues("miss").Add(float64(len(misses)))
		}
	}
	if len(misses) > 0 {
		var got []*sfv1.ZoneFeatures
		var err error
		if useCache {
			got, err = s.coalescedRead(ctx, misses)
		} else {
			got, _, err = s.Store.ZoneFeatures(ctx, misses)
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, status.Error(codes.DeadlineExceeded, "store read did not finish before the deadline")
			}
			if st, ok := status.FromError(err); ok {
				return nil, status.Error(codes.Unavailable, st.Message())
			}
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		for _, f := range got {
			rows[f.Zone] = f
		}
		if useCache && len(got) > 0 {
			// Write back off the request path; a failure only costs future
			// hits (and trips the breaker).
			go func() {
				wctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				if err := s.Cache.Set(wctx, got); err != nil {
					s.tripBreaker()
				}
			}()
		}
	}

	resp = &sfv1.GetFeaturesResponse{ServedFrom: sfv1.ServedFrom_SERVED_FROM_LEADER}
	if fromCache > 0 {
		resp.ServedFrom = sfv1.ServedFrom_SERVED_FROM_CACHE
		if fromCache < len(zones) {
			resp.ServedFrom = sfv1.ServedFrom_SERVED_FROM_CACHE_AND_LEADER
		}
		// The bound is the worst case over the entities returned.
		resp.StalenessBoundMs = s.Cache.TTL.Milliseconds()
	}
	for _, id := range req.EntityIds {
		z, _ := strconv.Atoi(id)
		fv := &sfv1.FeatureVector{EntityId: id}
		if f, ok := rows[int32(z)]; ok {
			wf := store.FeaturesFromProto(f)
			fv.Found = true
			fv.WindowEndMs = wf.WindowEndMs
			fv.AsOfEventTimeMs = wf.WindowEndMs
			fv.Values = window.Derive(wf)
		}
		resp.Features = append(resp.Features, fv)
	}
	return resp, nil
}

// coalescedRead shares one linearizable store read among concurrent cache
// misses for the same zones. Every waiter still gets a value that was
// committed when the shared read started, which is within the bounded
// staleness these (cache-mode) requests already accept.
func (s *Server) coalescedRead(ctx context.Context, zones []int32) ([]*sfv1.ZoneFeatures, error) {
	key := make([]byte, 0, len(zones)*4)
	for _, z := range zones {
		key = strconv.AppendInt(key, int64(z), 10)
		key = append(key, ',')
	}
	ch := s.flight.DoChan(string(key), func() (any, error) {
		// Detached from any one caller's cancellation, bounded by the cap.
		rctx, cancel := context.WithTimeout(context.Background(), s.MaxDeadline)
		defer cancel()
		got, _, err := s.Store.ZoneFeatures(rctx, zones)
		return got, err
	})
	select {
	case r := <-ch:
		if r.Shared {
			mCoalesced.Inc()
		}
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.([]*sfv1.ZoneFeatures), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func modeLabel(m sfv1.ReadMode) string {
	if m == sfv1.ReadMode_READ_MODE_LINEARIZABLE {
		return "linearizable"
	}
	return "bounded_staleness"
}
