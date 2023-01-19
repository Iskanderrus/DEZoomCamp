#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

csv_name = 'csv_output_data.csv'

os.system(f"wget {URL} -O {csv_name}")
df = pd.read_csv(csv_name, low_memory=False, compression='gzip')

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

