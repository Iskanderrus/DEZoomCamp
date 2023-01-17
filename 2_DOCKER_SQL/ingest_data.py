import os
import argparse
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'yellow_tripdata_2021-01.parquet'
    # download the file:
    os.system(f"wget {url} -O {parquet_name}")
    # create an engine to connect to the database:
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet(parquet_name,
                         engine='pyarrow')
    print(pd.io.sql.get_schema(df,
                               name=table_name,
                               con=engine))

    # pulling the df.columns to database:
    df.head(0).to_sql(name=table_name,
                      con=engine,
                      if_exists='replace')

    # pulling the entire df to database:
    df.to_sql(name='yellow_taxi_data',
              con=engine,
              if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquette data to PostgreSQL')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()
    main(args)
