"""Score zones online with the trained model, reading features from the
running FeatureService (its JSON endpoint), through the same to_vector()
used in training.

    python ml/score_online.py --server http://localhost:8081
"""
import argparse, time

import joblib
import requests

from features import to_vector

BUSY = [161, 237, 236, 162, 132, 230, 186, 142, 170, 163, 234, 68, 79, 48, 140, 141, 239, 107, 263, 246]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="http://localhost:8081")
    ap.add_argument("--model", default="ml/model.joblib")
    ap.add_argument("--every", type=float, default=5)
    a = ap.parse_args()
    model = joblib.load(a.model)
    while True:
        r = requests.get(f"{a.server}/v1/features", params={"zones": ",".join(map(str, BUSY))}, timeout=2)
        r.raise_for_status()
        body = r.json()
        rows = [f for f in body["features"] if f.get("found")]
        X = [to_vector(f["values"], int(f["windowEndMs"])) for f in rows]
        probs = model.predict_proba(X)[:, 1] if X else []
        ranked = sorted(zip(rows, probs), key=lambda x: -x[1])[:5]
        print(f"served_from={body['servedFrom']} staleness_bound_ms={body['stalenessBoundMs']}")
        for f, p in ranked:
            print(f"  zone {f['entityId']:>3}  P(spike in next 15 min) = {p:.2f}   trips_5m={f['values']['trips_5m']:.0f}")
        time.sleep(a.every)


if __name__ == "__main__":
    main()
