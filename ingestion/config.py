"""Paths and shared config for the NYC Taxi pipeline."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
SILVER_DIR = Path(os.environ.get("NYC_SILVER", ROOT / "data" / "silver"))
WAREHOUSE = Path(os.environ.get("NYC_WAREHOUSE", ROOT / "warehouse" / "nyc_taxi.duckdb"))
SEED_ZONE = ROOT / "dbt_project" / "seeds" / "taxi_zone_lookup.csv"

# TLC payment type codes from the data dictionary
PAYMENT_TYPES = {1: "Credit card", 2: "Cash", 3: "No charge",
                 4: "Dispute", 5: "Unknown", 6: "Voided trip"}

for d in (RAW_DIR, SILVER_DIR, WAREHOUSE.parent):
    d.mkdir(parents=True, exist_ok=True)
