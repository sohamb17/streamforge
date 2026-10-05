// Package history writes and reads the offline feature history in
// PostgreSQL and implements the point-in-time join used for training data.
package history

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sohamb17/streamforge/internal/window"
)

// View is the only feature view today.
const View = "zone_demand_v1"

// BackfillView is where the backfill job writes, for parity comparison.
const BackfillView = View + "__backfill"

// Store writes history rows for one feature view.
type Store struct {
	Pool *pgxpool.Pool
	View string
}

// Open connects and makes sure the schema exists.
func Open(ctx context.Context, dsn string, schema string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	for i := 0; ; i++ {
		_, err = pool.Exec(ctx, schema)
		if err == nil || i > 60 || ctx.Err() != nil {
			break
		}
		time.Sleep(time.Second) // database still starting
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool, View: View}, nil
}

const insertSQL = `
INSERT INTO feature_history AS h (feature_view, zone, window_end, trips_5m, trips_30m, trips_60m,
    count_15m, fare_cents_15m, distance_milli_15m, source_partition, source_offset)
SELECT $1, z, to_timestamp(w / 1000.0), t5, t30, t60, c15, f15, d15, $2, $3
FROM unnest($4::int[], $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[], $9::bigint[], $10::bigint[], $11::bigint[])
     AS u(z, w, t5, t30, t60, c15, f15, d15)
ON CONFLICT (feature_view, zone, window_end) DO UPDATE
    SET conflicting_rewrites = h.conflicting_rewrites + 1
    WHERE (h.trips_5m, h.trips_30m, h.trips_60m, h.count_15m, h.fare_cents_15m, h.distance_milli_15m)
          IS DISTINCT FROM
          (EXCLUDED.trips_5m, EXCLUDED.trips_30m, EXCLUDED.trips_60m, EXCLUDED.count_15m, EXCLUDED.fare_cents_15m, EXCLUDED.distance_milli_15m)`

// Write upserts rows idempotently. Implements worker.HistorySink.
func (s *Store) Write(ctx context.Context, partition int32, toOffset int64, rows []window.Features) error {
	n := len(rows)
	z := make([]int32, n)
	w := make([]int64, n)
	t5, t30, t60, c15, f15, d15 := make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
	for i, r := range rows {
		z[i], w[i] = r.Zone, r.WindowEndMs
		t5[i], t30[i], t60[i] = r.Trips5m, r.Trips30m, r.Trips60m
		c15[i], f15[i], d15[i] = r.Count15m, r.FareCents15m, r.DistanceMilli15m
	}
	_, err := s.Pool.Exec(ctx, insertSQL, s.View, partition, toOffset, z, w, t5, t30, t60, c15, f15, d15)
	return err
}

// PointInTimeSQL returns, for every training label, the latest feature row
// whose window ended at or before the label time: exactly what a model
// serving at label_time could have read, and nothing from the future.
const PointInTimeSQL = `
SELECT l.zone, l.label_time, l.label,
       f.window_end, f.trips_5m, f.trips_30m, f.trips_60m, f.count_15m, f.fare_cents_15m, f.distance_milli_15m
FROM training_labels l
LEFT JOIN LATERAL (
    SELECT h.window_end, h.trips_5m, h.trips_30m, h.trips_60m, h.count_15m, h.fare_cents_15m, h.distance_milli_15m
    FROM feature_history h
    WHERE h.feature_view = $1 AND h.zone = l.zone AND h.window_end <= l.label_time
    ORDER BY h.window_end DESC
    LIMIT 1
) f ON true
ORDER BY l.zone, l.label_time`

// Rows reads all history rows of the view, ordered, for parity checks.
func (s *Store) Rows(ctx context.Context, from, to time.Time) ([]window.Features, error) {
	rows, err := s.Pool.Query(ctx, `
SELECT zone, (extract(epoch from window_end) * 1000)::bigint, trips_5m, trips_30m, trips_60m, count_15m, fare_cents_15m, distance_milli_15m
FROM feature_history WHERE feature_view = $1 AND window_end >= $2 AND window_end < $3
ORDER BY window_end, zone`, s.View, from, to)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (window.Features, error) {
		var f window.Features
		err := r.Scan(&f.Zone, &f.WindowEndMs, &f.Trips5m, &f.Trips30m, &f.Trips60m, &f.Count15m, &f.FareCents15m, &f.DistanceMilli15m)
		return f, err
	})
}

// ConflictingRewrites returns the total count of rewrites that carried
// different values (expected: 0).
func (s *Store) ConflictingRewrites(ctx context.Context) (int64, error) {
	var n int64
	err := s.Pool.QueryRow(ctx, `SELECT coalesce(sum(conflicting_rewrites),0) FROM feature_history`).Scan(&n)
	return n, err
}

// ParityResult compares the streaming history with the backfill.
type ParityResult struct {
	Matched, Mismatched, MissingInStream, MissingInBackfill int64
}

// Parity compares the two views for windows ending at or before cut.
func (s *Store) Parity(ctx context.Context, cut time.Time) (ParityResult, error) {
	var r ParityResult
	err := s.Pool.QueryRow(ctx, `
WITH s AS (SELECT * FROM feature_history WHERE feature_view = $1 AND window_end <= $3),
     b AS (SELECT * FROM feature_history WHERE feature_view = $2 AND window_end <= $3)
SELECT
  count(*) FILTER (WHERE s.zone IS NOT NULL AND b.zone IS NOT NULL AND
      (s.trips_5m, s.trips_30m, s.trips_60m, s.count_15m, s.fare_cents_15m, s.distance_milli_15m) =
      (b.trips_5m, b.trips_30m, b.trips_60m, b.count_15m, b.fare_cents_15m, b.distance_milli_15m)),
  count(*) FILTER (WHERE s.zone IS NOT NULL AND b.zone IS NOT NULL AND
      (s.trips_5m, s.trips_30m, s.trips_60m, s.count_15m, s.fare_cents_15m, s.distance_milli_15m) <>
      (b.trips_5m, b.trips_30m, b.trips_60m, b.count_15m, b.fare_cents_15m, b.distance_milli_15m)),
  count(*) FILTER (WHERE s.zone IS NULL),
  count(*) FILTER (WHERE b.zone IS NULL)
FROM s FULL OUTER JOIN b ON s.zone = b.zone AND s.window_end = b.window_end`,
		View, BackfillView, cut).Scan(&r.Matched, &r.Mismatched, &r.MissingInStream, &r.MissingInBackfill)
	return r, err
}
