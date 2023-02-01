from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path: 
    '''Download trip data from GCS'''
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('dez-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=f'/home/alex/DEZoomCamp/WEEK_2/02_gcp/')
    return Path(f'/home/alex/DEZoomCamp/WEEK_2/02_gcp/{gcs_path}')

@task()
def transform(path: Path) -> pd.DataFrame: 
    '''Data cleaning example'''
    df = pd.read_parquet(path)
    print(f'pre: missing passenger count values: {df.passenger_count.isna().sum()}')
    df.passenger_count.fillna(0, inplace=True)
    print(f'post: missing passenger count values: {df.passenger_count.isna().sum()}')
    return df

@flow()
def etl_gcs_to_bq(): 
    """
    Main ETL flow to load data into Big Query
    """
    color = 'yellow'
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)

if __name__ == '__main__': 
    etl_gcs_to_bq()