"""Train a small demand-spike classifier from StreamForge's offline history.

Label: will a zone's next 15 minutes be busier than 1.5x the 15-minute
average of its trailing hour? It is computed from the history itself (the
row 15 minutes later), written into training_labels, and the features come
from the point-in-time join: the latest feature row with window_end <=
label_time, i.e. only what a model serving at that moment could have seen.

    pip install -r ml/requirements.txt
    python ml/train.py --dsn postgresql://streamforge:streamforge@localhost:5432/streamforge
"""
import argparse, json, time

import joblib
import numpy as np
import pandas as pd
import psycopg
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from features import NAMES, from_history_row, to_vector

LABEL_SQL = """
DELETE FROM training_labels;
INSERT INTO training_labels (zone, label_time, label)
SELECT now_.zone, now_.window_end,
       CASE WHEN later.count_15m > 1.5 * (now_.trips_60m / 4.0) THEN 1 ELSE 0 END
FROM feature_history now_
JOIN feature_history later
  ON later.feature_view = now_.feature_view AND later.zone = now_.zone
 AND later.window_end = now_.window_end + interval '15 minutes'
WHERE now_.feature_view = 'zone_demand_v1' AND now_.trips_60m >= 8
  AND extract(minute from now_.window_end)::int % 5 = 0;
"""

PIT_SQL = """
SELECT l.zone, l.label_time, l.label,
       f.window_end, f.trips_5m, f.trips_30m, f.trips_60m, f.count_15m, f.fare_cents_15m, f.distance_milli_15m
FROM training_labels l
LEFT JOIN LATERAL (
    SELECT h.window_end, h.trips_5m, h.trips_30m, h.trips_60m, h.count_15m, h.fare_cents_15m, h.distance_milli_15m
    FROM feature_history h
    WHERE h.feature_view = 'zone_demand_v1' AND h.zone = l.zone AND h.window_end <= l.label_time
    ORDER BY h.window_end DESC LIMIT 1
) f ON true
ORDER BY l.label_time, l.zone
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://streamforge:streamforge@localhost:5432/streamforge")
    ap.add_argument("--out", default="ml/model.joblib")
    ap.add_argument("--report", default="ml/report.json")
    a = ap.parse_args()

    with psycopg.connect(a.dsn) as conn:
        conn.execute(LABEL_SQL)
        cur = conn.execute(PIT_SQL)
        df = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    df = df.dropna(subset=["window_end"])
    # The join must never look into the future.
    leaks = int((df["window_end"] > df["label_time"]).sum())
    assert leaks == 0, f"{leaks} rows use features from after their label time"

    X = np.array([to_vector(from_history_row(r), int(r["window_end"].timestamp() * 1000)) for r in df.to_dict("records")])
    y = df["label"].astype(int).to_numpy()
    # Split by time, never randomly: train on the past, test on the future.
    cut = df["label_time"].quantile(0.7)
    tr = (df["label_time"] <= cut).to_numpy()
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))
    t0 = time.time()
    model.fit(X[tr], y[tr])
    p = model.predict_proba(X[~tr])[:, 1]
    baseline = X[~tr][:, NAMES.index("demand_spike_ratio")]
    report = {
        "examples": int(len(y)), "train": int(tr.sum()), "test": int((~tr).sum()),
        "positive_rate_test": round(float(y[~tr].mean()), 4),
        "train_from": str(df["label_time"].min()), "split_at": str(cut), "test_to": str(df["label_time"].max()),
        "roc_auc": round(float(roc_auc_score(y[~tr], p)), 4),
        "average_precision": round(float(average_precision_score(y[~tr], p)), 4),
        "baseline_spike_ratio_roc_auc": round(float(roc_auc_score(y[~tr], baseline)), 4),
        "future_leaks_in_join": leaks,
        "fit_seconds": round(time.time() - t0, 2),
        "features": NAMES,
    }
    joblib.dump(model, a.out)
    json.dump(report, open(a.report, "w"), indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
