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
    df = pd.read_csv(df, low_memory=True)
    df['popularity'] = df.loc[:, 'popularity'].apply(lambda x: x.strip('.')).apply(lambda x: x.replace(',', ''))
    df[['title', 'episode', 'age', 'duration', 'genre', 'votes', 'description']] = df.loc[:,
                                                                                   ['title', 'episode', 'age',
                                                                                    'duration',
                                                                                    'genre', 'votes',
                                                                                    'description']].apply(
        lambda x: x.str.replace('\n', '')).apply(lambda x: x.str.strip())

    df['year'] = df['year'].str.extract(r'(\d{4}?)', expand=False)
    df['episode_year'] = df['episode_year'].str.extract(r'(\d{4}?)', expand=False)
    df['duration'] = df.loc[:, 'duration'].astype(str).apply(lambda x: x.split(' ')[0]).apply(
        lambda x: x.replace(',', '')).astype(float)
    df['votes'] = df.loc[:, 'votes'].astype(str).apply(lambda x: x.replace(',', ''))
    df['votes'] = pd.to_numeric(df['votes'], downcast='float', errors='coerce')
    df['votes'].fillna(0, inplace=True)
    df['votes'] = pd.to_numeric(df['votes'], downcast='integer')
    df['popularity'] = pd.to_numeric(df['popularity'], downcast='integer')
    df = df[df['year'].notna()]
    df['year'] = pd.to_numeric(df['year'], downcast='integer')
    df['episode_year'] = pd.to_numeric(df['episode_year'], downcast='float')
    # df['rating'].fillna('Not Rated', inplace=True)
    # df['episode'].fillna('No Episodes', inplace=True)
    # df['age'].fillna('Not Rated', inplace=True)
    # df['duration'] = df['duration'].fillna(0)
    # df['duration'] = pd.to_numeric(df['duration'], downcast='integer')
    # df['description'].fillna('No description', inplace=True)
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
    all_files = os.listdir("./imdb/imdb/spiders/data")
    films_files = list(filter(lambda f: f.endswith('.csv'), all_files))

    # fetching data
    df_financials = fetch('./imdb_financial/imdb_financial/spiders/data/films_financials.csv')
    df_films = []
    for file in films_files:
        df_films.append(fetch(file))

    # cleaning data
    cleaned_financials = clean_financials(df_financials)
    cleaned_df_list = []
    for df in df_films:
        cleaned_df_list.append(clean_films(df))

    # writing local files
    financials_path = write_local(cleaned_financials)
    films_paths = []
    for df in cleaned_df_list:
        films_paths.append(write_local(df))

    # write to GCS
    write_gcs(financials_path)
    for film_path in films_paths:
        write_gcs(film_path)


if __name__ == '__main__':
    etl_disc_to_gcs()
