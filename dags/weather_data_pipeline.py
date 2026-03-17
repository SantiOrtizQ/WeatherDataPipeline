from airflow.sdk import task, chain, dag
from datetime import datetime, timezone

import logging
import pandas as pd
import json
from io import StringIO

#load tasks
from include.longtasks import fetch_data, transform_data

# postgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

#s3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#variables from config
from include.config import CONN_ID, RAW_DATA_NAME, BUCKET_NAME, PROCESSED_FILE_NAME, POSTGRES_CONN_ID


# create logger
logger=logging.getLogger(__name__)






''' --- DEFINE ALL TASKS --- '''

'''----------------------------------------------------------------------------'''

# GET DATA FROM API
@task
def fetch_and_store():

    #get weather data
    weather_info=fetch_data(logger)

    # build file_name
    now=datetime.now(timezone.utc)
    file_name=f"{RAW_DATA_NAME}/year={int(now.year)}/month={int(now.month)}/day={int(now.day)}\
        /weather_{now.strftime("%Y%m%d_%H%M%S")}.json"
    hook=S3Hook(aws_conn_id=CONN_ID)

    hook.load_string(
        string_data=json.dumps(weather_info),
        key=file_name,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    logger.info("Successfully loaded data into S3 Bucket.")

    return file_name


'''----------------------------------------------------------------------------'''

# TRANSFORM RAW JSON FILE
@task
def transform_weather_data(file_name):
    #get json from raw files in S3
    hook=S3Hook(aws_conn_id=CONN_ID)
    json_text=hook.read_key(key=file_name, bucket_name=BUCKET_NAME)
    jsondict=json.loads(json_text)
    
    # transform data
    df=transform_data(jsondict)


    #load to processed in S3
    now=datetime.now(timezone.utc)
    csv_file_name=f"{PROCESSED_FILE_NAME}/year={int(now.year)}/month={int(now.month)}/day={int(now.day)}\
        /weather_{now.strftime("%Y%m%d_%H%M%S")}.csv"
    
    content=StringIO()
    df.to_csv(content, index=False)

    hook.load_string(
        string_data=content.getvalue(),
        key=csv_file_name,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    return csv_file_name


'''----------------------------------------------------------------------------'''

# LOAD TO POSTGRES
@task
def load_to_postgres(csv_file_name):

    #get csv file from S3 processed files
    s3_hook=S3Hook(aws_conn_id=CONN_ID)
    content=s3_hook.read_key(key=csv_file_name, bucket_name=BUCKET_NAME)
    buffer=StringIO(content)
    df=pd.read_csv(buffer)

    #load to postgres
    hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df.to_sql(
        name="weather",
        con=hook.get_sqlalchemy_engine(),
        if_exists="append",
        index=False
    )


'''----------------------------------------------------------------------------'''

''' --- DEFINE DAG --- '''

@dag(
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    description="This DAG ingest data from a weather API and process it\
        through AWS databases to finally load it into postgres.",
    catchup=False
)
def weather_data_pipeline():
    
    fetch=fetch_and_store()
    transform=transform_weather_data(fetch)
    load=load_to_postgres(transform)

    chain(fetch, transform, load)

weather_data_pipeline()