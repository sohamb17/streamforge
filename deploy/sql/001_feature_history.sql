-- Offline feature history: one row per (feature view, zone, window end).
-- Rows are appended by stream workers before each Raft proposal and by the
-- backfill job. Both run the same window code, so a row written twice must
-- carry identical values; a write that disagrees is counted, never applied.
CREATE TABLE IF NOT EXISTS feature_history (
    feature_view        text        NOT NULL,
    zone                integer     NOT NULL,
    window_end          timestamptz NOT NULL,
    trips_5m            bigint      NOT NULL,
    trips_30m           bigint      NOT NULL,
    trips_60m           bigint      NOT NULL,
    count_15m           bigint      NOT NULL,
    fare_cents_15m      bigint      NOT NULL,
    distance_milli_15m  bigint      NOT NULL,
    source_partition    integer     NOT NULL,
    source_offset       bigint      NOT NULL,
    written_at          timestamptz NOT NULL DEFAULT now(),
    conflicting_rewrites integer    NOT NULL DEFAULT 0,
    PRIMARY KEY (feature_view, zone, window_end)
);

-- Labels for training examples: (zone, label_time) pairs, e.g. "was there a
-- demand spike in the 15 minutes after label_time".
CREATE TABLE IF NOT EXISTS training_labels (
    zone        integer     NOT NULL,
    label_time  timestamptz NOT NULL,
    label       double precision,
    PRIMARY KEY (zone, label_time)
);
