from airflow.sdk import task, chain, dag
from datetime import datetime

# postgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

#s3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



''' --- DEFINE ALL TASKS --- '''

@task
def fetch_weather_data():
    pass

@task
def store_raw_s3():
    pass

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
    store=store_raw_s3()
    transform=transform_weather_data()
    load=load_to_postgres()

    chain(fetch, store, transform, load)

weather_data_pipeline()