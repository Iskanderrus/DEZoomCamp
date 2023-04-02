from pathlib import Path
import pandas as pd
from prefect import flow, tasc
from prefect_gcp.cloud_storage import GcsBucket
from .imdb.imdb.spiders.imdb_spider import ImdbSpiderSpider

def etl_dick_to_gcs(): 
    """"Main ETL function"""
    
    data_set = "./imdb/imdb/data/films.scv"
