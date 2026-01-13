#!/usr/bin/env python

import pandas as pd
from sqlalchemy import create_engine

def main():
    # 1. Source CSV
    url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
        "taxi_zone_lookup.csv"
    )

    print("Downloading zone lookup data...")
    zone = pd.read_csv(url)

    # 2. Postgres connection (HOST → docker)
    engine = create_engine(
        "postgresql://postgres:postgres@localhost:5433/ny_taxi"
    )

    # 3. Show schema (optional, for debugging)
    print("PostgreSQL schema:")
    print(pd.io.sql.get_schema(zone, name="zone", con=engine))

    # 4. Create table
    print("Creating table `zone`...")
    zone.to_sql(
        name="zone",
        con=engine,
        if_exists="replace",
        index=False
    )

    print("Zone table successfully created.")

if __name__ == "__main__":
    main()
