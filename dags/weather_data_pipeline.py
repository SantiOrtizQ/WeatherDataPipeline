from fileinput import filename

from airflow.sdk import task, chain, dag
from datetime import datetime
from more_itertools import bucket
import requests
import logging
import pandas as pd
import json
from io import StringIO

# postgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

#s3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#variables from config
from include.config import CONN_ID, RAW_DATA_NAME, BUCKET_NAME, CITIES, API_key, PROCESSED_FILE_NAME, POSTGRES_CONN_ID


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
        response=requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={API_key}&units=metric")
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
        response=requests.get(f"http://api.openweathermap.org/data/2.5/weather?lat={float(latlon.iat[i, 0])}&lon={float(latlon.iat[i, 1])}&appid={API_key}")
        if response.status_code==200:
            logger.info("Weather info extracted correctly")
            # save weather data into dictionary
            weather_response=response.json()
            weather_sum={"weather": weather_response["main"]}
            weather_sum["weather"]["timestamp"]=weather_response["dt"]
            weather_info[locations.iat[i, 0]]=weather_sum
        else:
            raise TimeoutError("Weather data could not be extracted.")
    
    # return json file
    return weather_info


@task
def store_raw_s3(data):
    file_name=str(RAW_DATA_NAME)
    hook=S3Hook(aws_conn_id=CONN_ID)

    hook.load_string(
        string_data=json.dumps(data),
        key=file_name,
        bucket_name=BUCKET_NAME,
        replace=True
    )

    logger.info("Successfully loaded data into S3 Bucket.")
    return file_name


#transform raw json data into csv
@task
def transform_weather_data(file_name):

    #get json from raw files in S3
    hook=S3Hook(aws_conn_id=CONN_ID)
    json_text=hook.read_key(key=file_name, bucket_name=BUCKET_NAME)
    jsondict=json.loads(json_text)

    # get columns for dataframe
    df_columns=["city"]+list(list(list(jsondict.values())[0].values())[0].keys())
    df=pd.DataFrame(columns=df_columns)
    for city in jsondict:
        # flat data
        data=[city]+list(jsondict[city]["weather"].values())
        # save data into df
        logger.info(f"Info:{data}")
        df.loc[len(df)]=data
    
    #converting timestamp
    df["timestamp"]=pd.to_datetime(df["timestamp"], unit='s', errors='raise', utc=True)

    #load to processed in S3
    csv_file_name=PROCESSED_FILE_NAME
    content=StringIO()
    df.to_csv(content, index=False)

    hook.load_string(
        string_data=content.getvalue(),
        key=csv_file_name,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    return csv_file_name



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




''' --- DEFINE DAG --- '''

@dag(
    start_date=datetime(2026, 3, 1),
    schedule="@hourly",
    description="This DAG ingest data from a weather API and process it\
        through AWS databases to finally load it into postgres.",
    catchup=False
)
def weather_data_pipeline():
    
    fetch=fetch_weather_data()
    store=store_raw_s3(fetch)
    transform=transform_weather_data(store)
    load=load_to_postgres(transform)

    chain(fetch, store, transform, load)

weather_data_pipeline()