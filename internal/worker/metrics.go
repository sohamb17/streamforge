package worker

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func itoa(p int32) string { return strconv.Itoa(int(p)) }

var latencyBuckets = prometheus.ExponentialBuckets(0.00025, 2, 18)

var (
	mEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_worker_events_total", Help: "Events consumed into window state."}, []string{"partition"})
	mSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_worker_records_skipped_total", Help: "Kafka records not applied, by reason."}, []string{"partition", "reason"})
	mLateDropped = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamforge_worker_late_dropped", Help: "Events dropped as later than the lateness allowance (cumulative, from window state)."}, []string{"partition"})
	mWatermark = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamforge_worker_watermark_ms", Help: "Current watermark (event time)."}, []string{"partition"})
	mLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "streamforge_worker_consumer_lag", Help: "Kafka high watermark minus last consumed offset."}, []string{"partition"})
	mBatches = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_worker_batches_total", Help: "Batches proposed, by outcome (applied, duplicate, error)."}, []string{"partition", "outcome"})
	mRows = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streamforge_worker_feature_rows_total", Help: "Feature rows committed."})
	mRestores = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streamforge_worker_restores_total", Help: "Times a partition was rebuilt from the store after an error."}, []string{"partition"})
	mAssigned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "streamforge_worker_partitions_assigned", Help: "Partitions owned by this worker."})
	mProposeLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "streamforge_worker_propose_latency_seconds", Help: "ProposeBatch round trip.", Buckets: latencyBuckets})
	mFreshness = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "streamforge_freshness_seconds",
		Help:    "Wall time from publishing the event that closed a window to that window's features being committed (linearizably readable).",
		Buckets: latencyBuckets})
)
