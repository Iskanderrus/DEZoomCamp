import os
import argparse
from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine


@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, url):
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine = create_engine(postgres_url, pool_pre_ping=True)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Ingest Flow")
def main():
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '5432'
    db = 'ny_taxi'
    table_name = 'yellow_taxi_trips'
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'

    ingest_data(user=user, password=password, host=host, port=port, db=db, table_name=table_name, url=csv_url)


if __name__ == '__main__':
    main()
