from pathlib import Path
import pandas as pd
from prefect import flow, task
from datetime import datetime
import os
from prefect_gcp.cloud_storage import GcsBucket





@task(retries=3)
def fetch(file_name): 
    """Read data into pandas dataframe"""
    df = pd.read_csv(file_name, low_memory=True)
    print('Data loaded into pandas')
    return df

@task()
def clean_films(df): 

    return df

@task()
def clean_financials(df): 

    return df 

@task()
def write_local(df):
    if 'production_company' in df.columns: 
        path = Path(f"data/{datetime.now().strftime('%Y%m%d')}_imdb_financials.parquet")
    else: 
         path = Path(f"data/{datetime.now().strftime('%Y%m%d')}_imdb_films.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path): 
    gcp_cloud_storage_bucket_block = GcsBucket.load("imdb-gsc-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f'{path}',
        to_path=path
    )
    return

@flow
def etl_disc_to_gcs(): 
    """"Main ETL function"""
    files = ['/home/alex/VisualStudioCode/DEZoomCamp/DEZoomCamp/Project/imdb_financial/imdb_financial/spiders/data/films_financials.csv', 
             '/home/alex/VisualStudioCode/DEZoomCamp/DEZoomCamp/Project/imdb/imdb/spiders/data/films.csv']
    
    # fetching data
    df_financials = fetch(files[0])
    df_films = fetch(files[1])

    # cleaning data
    df_financials = clean_financials(df_financials)
    df_films = clean_films(df_films)

    # writing local files
    financials_path = write_local(df_financials)
    films_path = write_local(df_films)

    # write to GCS
    write_gcs(financials_path)
    write_gcs(films_path)



if __name__ == '__main__': 
    etl_disc_to_gcs()
