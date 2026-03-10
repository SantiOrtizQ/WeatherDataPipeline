from fileinput import filename

from airflow.sdk import task, chain, dag
from datetime import datetime
import requests
import logging
from io import StringIO
# postgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

#s3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#variables from config
from include.config import CONN_ID, RAW_DATA_NAME, BUCKET_NAME


#create logger
logger=logging.getLogger(__name__)
''' --- DEFINE ALL TASKS --- '''

@task
def fetch_weather_data():
    response=requests.get("http://api.openweathermap.org/geo/1.0/direct?q=London&appid=dc4d36eaf2e989621f13e835fbc190a4")
    if response.status_code==200:
        data=response.json()
        logger.info("Successfully retrieved data")
        return data
    else:
        raise TimeoutError("Data could not be retrieved from API")

@task
def store_raw_s3(data):
    # check if data is empty
    if not data:
        raise ValueError("")
    
    # define hook
    hook=S3Hook(aws_conn_id=CONN_ID)

    data=str(data)[1:-1]
    # define buffer
    buffer=StringIO()

    #load file into S3
    hook.load_string(
        string_data=data,
        key=RAW_DATA_NAME,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logger.info("Successfully loaded data into S3 Bucket.")

@task
def transform_weather_data():
    pass

@task
def load_to_postgres():
    pass




''' --- DEFINE DAG --- '''

@dag(
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    description="This DAG ingest data from a weather API and process it\
        through AWS databases to finally load it into postgres."
)
def weather_data_pipeline():
    
    fetch=fetch_weather_data()
    store=store_raw_s3(fetch)
    transform=transform_weather_data()
    load=load_to_postgres()

    chain(fetch, store, transform, load)

weather_data_pipeline()