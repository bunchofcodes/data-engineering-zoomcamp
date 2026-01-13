#!/usr/bin/env python
# coding: utf-8

import os
import requests
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# ========================
# CONFIG
# ========================
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
LOCAL_FILE = "green_tripdata_2025-11.parquet"

PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_HOST = "db"          # <-- service name (PALING AMAN)
PG_PORT = 5432          # <-- INTERNAL port
PG_DB = "ny_taxi"

TABLE_NAME = "green_taxi_data_2025_11"

BATCH_SIZE = 100_000

# ========================
# SCHEMA
# ========================
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

# ========================
# HELPERS
# ========================
def download_file(url, local_file):
    if os.path.exists(local_file):
        print("File already exists, skip download.")
        return

    print("Downloading parquet file...")
    r = requests.get(url, stream=True)
    r.raise_for_status()

    with open(local_file, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    print("Download completed.")

def apply_schema(df):
    # parse datetime
    for col in parse_dates:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # apply dtype
    for col, t in dtype.items():
        if col not in df.columns:
            continue

        if t in ["Int64", "float64"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(t)
        else:
            df[col] = df[col].astype(t)

    return df

# ========================
# MAIN INGEST
# ========================
def main():
    download_file(URL, LOCAL_FILE)

    engine = create_engine(
        f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    pf = pq.ParquetFile(LOCAL_FILE)

    first_batch = True

    for batch in tqdm(pf.iter_batches(batch_size=BATCH_SIZE), total=pf.num_row_groups):
        df = batch.to_pandas()

        df = apply_schema(df)

        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace" if first_batch else "append",
            index=False
        )

        first_batch = False

    print("Ingestion completed successfully.")

if __name__ == "__main__":
    main()
