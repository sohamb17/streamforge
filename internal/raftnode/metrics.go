package raftnode

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Latency buckets in seconds: 0.25 ms .. ~8 s. Histograms (not summaries)
// so quantiles can be aggregated across nodes in PromQL.
var latencyBuckets = prometheus.ExponentialBuckets(0.00025, 2, 16)

var (
	mTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamforge_raft_term", Help: "Current Raft term."})
	mIsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamforge_raft_is_leader", Help: "1 if this node is the leader."})
	mCommit = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamforge_raft_commit_index", Help: "Commit index."})
	mApplied = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamforge_raft_applied_index", Help: "Applied index."})
	mElections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamforge_raft_elections_started_total", Help: "Elections this node started (real votes, not pre-votes)."})
	mProposals = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_raft_proposals_total", Help: "Proposals by outcome."}, []string{"outcome"})
	mCommitLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "streamforge_raft_commit_latency_seconds", Help: "Leader-side time from Propose to apply.", Buckets: latencyBuckets})
	mReadIndexLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "streamforge_raft_readindex_latency_seconds", Help: "Time from ReadIndex request to readable.", Buckets: latencyBuckets})
	mFsync = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "streamforge_raft_persist_seconds", Help: "Time to persist one Ready (write + fsync).", Buckets: latencyBuckets})
	mSnapshots = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_raft_snapshots_total", Help: "Snapshots taken locally or installed from the leader."}, []string{"kind"})
	mMsgsSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_raft_messages_sent_total", Help: "Raft messages handed to the transport."}, []string{"type"})
	mMsgsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_raft_messages_dropped_total", Help: "Raft messages dropped by the transport."}, []string{"reason"})
	mApplyBatch = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "streamforge_raft_apply_batch_entries", Help: "Entries applied per Ready.", Buckets: prometheus.ExponentialBuckets(1, 2, 12)})
)
