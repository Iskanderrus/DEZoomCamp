from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint 
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(
    retries=3, 
    cache_key_fn=task_input_hash, 
    cache_expiration=timedelta(days=0.1)
    )
def fetch(dataset_url): 
    """
    Read taxi data from web to Pandas DataFrame
    """

    # if randint(0, 1) > 0: 
    #     raise Exception

    df = pd.read_csv(dataset_url, low_memory=True)

    return df 


@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """
    Fix some dtype issues
    """ 
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'shape of the dataframe:\n\t\t\t\t\t\t\t\t\t\trows:{df.shape[0]}\n\t\t\t\t\t\t\t\t\t\tcolumns:{df.shape[1]}')
    return df 


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path: 
    """Write DataFrame out as parquet file"""
    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip', engine='fastparquet', append=True)
    return path

@task
def write_gcs(path: Path) -> None: 
    """Uploading local parquet file to Google Cloud Storage"""
    gcs_block = GcsBucket.load('dez-gcs')
    gcs_block.upload_from_path(
        from_path=f'{path}', 
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str): 
    """
    The main ETL function
    """

    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    cleaned_df = clean(df)
    path = write_local(cleaned_df, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list = [1, 2], 
    year: int = 2021,
    color: str = 'yellow'
): 
    for month in months: 
        etl_web_to_gcs(month=month, year=year, color=color)

if __name__ == '__main__': 
    color = 'yellow'
    months = [2, 3, 4]
    year = 2021
    etl_parent_flow(months=months, color=color, year=year)