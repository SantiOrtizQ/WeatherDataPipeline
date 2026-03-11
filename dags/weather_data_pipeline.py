from fileinput import filename

from airflow.sdk import task, chain, dag
from datetime import datetime
import requests
import logging
from io import StringIO
import pandas as pd
import json

# postgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

#s3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#variables from config
from include.config import CONN_ID, RAW_DATA_NAME, BUCKET_NAME, CITIES, API_key


#create logger
logger=logging.getLogger(__name__)
''' --- DEFINE ALL TASKS --- '''

@task
def fetch_weather_data():
    #create locations info dataframe
    locations=pd.DataFrame(columns=["City", "Latitude", "Longitude", "Country"])

    #get locations info from API
    for city in CITIES:
        #get response
        response=requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={API_key}")
        if response.status_code==200:
            #convert to json and extract element from the default list
            data=response.json()[0]

            #delete unwanted info
            data.pop('local_names', 0)
            data.pop("state", 0)
            
            # save into dataframe
            locations.loc[len(locations)]=data.values()
        else:
            raise TimeoutError("Data could not be retrieved from API.")
    
    logger.info("Successfully retrieved locations data.")


    # get weather info from API
    weather_info={}
    latlon=locations[["Latitude", "Longitude"]]     # extract latitude and longitude from dataframe
    size=len(latlon)
    for i in range(size):
        # get info
        response=requests.get(f"http://api.openweathermap.org/data/2.5/forecast?lat={float(latlon.iat[i, 0])}&lon={float(latlon.iat[i, 1])}&appid={API_key}")
        if response.status_code==200:
            logger.info("Weather info extracted correctly")
            # save weather data into dictionary
            weather_info[locations.iat[i, 0]]=response.json()
        else:
            raise TimeoutError("Weather data could not be extracted.")
    
    # return json file
    return json.dumps(weather_info)


@task
def store_raw_s3(data):
    hook=S3Hook(aws_conn_id=CONN_ID)

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