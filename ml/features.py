"""One definition of the model's input, shared by training and serving.

Training reads exact integer sums from the offline history (point-in-time
join); serving reads the named values FeatureService returns. Both go
through to_vector(), so the two paths cannot drift apart silently.
"""
import math

NAMES = ["trips_5m", "trips_30m", "trips_60m", "fare_mean_15m", "distance_mean_15m", "demand_spike_ratio", "hour_sin", "hour_cos"]


def from_history_row(r: dict) -> dict:
    """Same derivation as window.Derive in Go."""
    c15 = r["count_15m"]
    t60 = r["trips_60m"]
    return {
        "trips_5m": float(r["trips_5m"]),
        "trips_30m": float(r["trips_30m"]),
        "trips_60m": float(t60),
        "fare_mean_15m": (r["fare_cents_15m"] / 100 / c15) if c15 else 0.0,
        "distance_mean_15m": (r["distance_milli_15m"] / 1000 / c15) if c15 else 0.0,
        "demand_spike_ratio": (r["trips_5m"] / (t60 / 12)) if t60 else 0.0,
    }


def to_vector(values: dict, window_end_ms: int) -> list:
    hour = (window_end_ms // 3_600_000) % 24  # event time is NYC local time stored as UTC
    v = [float(values[n]) for n in NAMES[:6]]
    v.append(math.sin(2 * math.pi * hour / 24))
    v.append(math.cos(2 * math.pi * hour / 24))
    return v
