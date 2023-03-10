import os
from datetime import timedelta

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1)
)
def extract_data(url):
    """
    function to grab data from .gz or .csv file
    :param url: url of the file with data
    :return: pandas dataframe
    """
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(
        csv_name,
        iterator=True,
        chunksize=100000
    )

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    """
    function to transform data (in this particular case deleting all rides with 0 passengers)
    :param df: pandas dataframe
    :return: transformed pandas dataframe
    """
    print(f'pre: missing passenger count {df["passenger_count"].isin([0]).sum()}')

    df = df[df['passenger_count'] != 0]

    print(f'pre: missing passenger count {df["passenger_count"].isin([0]).sum()}')

    return df


@task(
    log_prints=True,
    retries=3
)
def ingest_data(table_name, df):
    """
    function to ingest transformed dataframe into the database
    :param table_name: table name
    :param df: dataframe to ingest into the database
    """
    connection_block = SqlAlchemyConnector.load('sqlconnection')
    with connection_block.get_connection(begin=False) as engine:
        df.head(0).to_sql(
            name=table_name,
            con=engine,
            if_exists='replace'
        )

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append'
        )


@flow(name="Ingest Flow")
def main():
    table_name = 'yellow_taxi_trips'
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    raw_data = extract_data(url=csv_url)
    data = transform_data(raw_data)
    ingest_data(
        table_name=table_name,
        df=data
    )


if __name__ == '__main__':
    main()
