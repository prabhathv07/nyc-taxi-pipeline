"""
Download NYC TLC Yellow Taxi parquet files for the pipeline.

Tries to pull the official monthly parquet files from the TLC site.
If the download fails, falls back to generating synthetic data with the
same TLC schema and similar defects (negative fares, zero trips, bad timestamps)
so the cleaning logic still gets a real workout.

Usage:
    python scripts/get_data.py              # try real download, else synthetic
    python scripts/get_data.py --synthetic  # force synthetic
"""
import sys
import urllib.request
import numpy as np
import pandas as pd
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1] / "ingestion"))
from config import RAW_DIR, SEED_ZONE, PAYMENT_TYPES

MONTHS = ["2024-01", "2024-02"]
BASE = "https://d37ci6vzurychx.cloudfront.net"


def try_download() -> bool:
    ok = True
    try:
        for m in MONTHS:
            url = f"{BASE}/trip-data/yellow_tripdata_{m}.parquet"
            dest = RAW_DIR / f"yellow_tripdata_{m}.parquet"
            if not dest.exists():
                print("downloading", url)
                urllib.request.urlretrieve(url, dest)
        zone = SEED_ZONE
        if not zone.exists():
            urllib.request.urlretrieve(f"{BASE}/misc/taxi_zone_lookup.csv", zone)
        return True
    except Exception as e:
        print("download failed:", e)
        return False


def synth_month(month: str, n: int, rng) -> pd.DataFrame:
    """Generate one month of synthetic TLC-schema yellow-taxi rows including dirty data."""
    start = pd.Timestamp(month + "-01")
    pickup = start + pd.to_timedelta(rng.uniform(0, 28 * 24 * 3600, n), unit="s")
    dur_min = rng.gamma(2.0, 6.0, n)
    dropoff = pickup + pd.to_timedelta(dur_min * 60, unit="s")
    dist = np.round(np.maximum(0, rng.gamma(2.0, 1.6, n)), 2)
    fare = np.round(3.0 + dist * 2.8 + dur_min * 0.35, 2)
    tip = np.round(np.where(rng.random(n) < 0.65, fare * rng.uniform(0, 0.30, n), 0), 2)
    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2], n),
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff,
        "passenger_count": rng.choice([1, 1, 1, 2, 3, 4, 5, 6], n).astype("float64"),
        "trip_distance": dist,
        "RatecodeID": rng.choice([1, 1, 1, 2, 3, 4, 5], n).astype("float64"),
        "store_and_fwd_flag": rng.choice(["N", "Y"], n, p=[.98, .02]),
        "PULocationID": rng.integers(1, 264, n),
        "DOLocationID": rng.integers(1, 264, n),
        "payment_type": rng.choice([1, 1, 1, 2, 2, 3, 4], n),
        "fare_amount": fare,
        "extra": np.round(rng.choice([0, 0.5, 1.0, 2.5], n), 2),
        "mta_tax": 0.5,
        "tip_amount": tip,
        "tolls_amount": np.round(np.where(rng.random(n) < 0.08, rng.uniform(2, 12, n), 0), 2),
        "improvement_surcharge": 0.3,
        "congestion_surcharge": rng.choice([0.0, 2.5], n, p=[.2, .8]),
        "Airport_fee": rng.choice([0.0, 1.75], n, p=[.9, .1]),
    })
    df["total_amount"] = np.round(
        df.fare_amount + df.extra + df.mta_tax + df.tip_amount + df.tolls_amount
        + df.improvement_surcharge + df.congestion_surcharge + df.Airport_fee, 2)

    # inject dirty rows (~7-8% total) to match real TLC defect rates
    k = len(df)
    neg = rng.choice(k, int(k * 0.02), replace=False)
    df.loc[neg, ["fare_amount", "total_amount"]] *= -1
    zero = rng.choice(k, int(k * 0.03), replace=False)
    df.loc[zero, "trip_distance"] = 0.0
    badt = rng.choice(k, int(k * 0.015), replace=False)
    df.loc[badt, "tpep_dropoff_datetime"] = df.loc[badt, "tpep_pickup_datetime"] - pd.Timedelta(minutes=5)
    nullt = rng.choice(k, int(k * 0.005), replace=False)
    df.loc[nullt, "tpep_dropoff_datetime"] = pd.NaT
    outp = rng.choice(k, int(k * 0.01), replace=False)
    df.loc[outp, "passenger_count"] = 0
    return df


def make_synthetic():
    rng = np.random.default_rng(7)
    rows_per_month = 1_250_000
    for m in MONTHS:
        dest = RAW_DIR / f"yellow_tripdata_{m}.parquet"
        df = synth_month(m, rows_per_month, rng)
        df.to_parquet(dest, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        print(f"wrote {dest.name}: {len(df):,} rows")
    if not SEED_ZONE.exists():
        boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
        zdf = pd.DataFrame({
            "LocationID": range(1, 264),
            "Borough": [boroughs[i % len(boroughs)] for i in range(263)],
            "Zone": [f"Zone {i}" for i in range(1, 264)],
            "service_zone": ["Yellow Zone"] * 263,
        })
        zdf.to_csv(SEED_ZONE, index=False)
        print("wrote synthetic taxi_zone_lookup.csv (263 zones)")


if __name__ == "__main__":
    force_synth = "--synthetic" in sys.argv
    if force_synth or not try_download():
        print("=> generating synthetic TLC-schema data")
        make_synthetic()
    print("data ready in", RAW_DIR)
