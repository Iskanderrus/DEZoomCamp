from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
# from imdb.imdb.spiders.imdb_spider import ImdbSpiderSpider
# from scrapy.crawler import CrawlerProcess
from datetime import datetime
import os


# @task
# def crawl_data(): 
#     date = datetime.now().strftime('%Y%m%d')
#     process = CrawlerProcess(settings={
#     'FEED_URI': f'./data/{date}_films.csv', 
#     'FEED_FORMAT': 'csv'
#     })

#     process.crawl(ImdbSpiderSpider)
#     process.start()
#     full_path = os.path.abspath(f'./data/{date}_films.csv')
#     return full_path


@task(retries=3)
def fetch(file_name): 
    """Read data into pandas dataframe"""
    df = pd.read_csv(file_name, low_memory=True)
    print('Data loaded into pandas')
    return df

@flow
def etl_disc_to_gcs(): 
    """"Main ETL function"""

    data_set = "./imdb/imdb/data/films.csv"
    df = fetch(data_set)



if __name__ == '__main__': 
    etl_disc_to_gcs()
