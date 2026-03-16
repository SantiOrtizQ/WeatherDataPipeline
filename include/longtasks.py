import pandas as pd
from include.config import CITIES, API_key
import requests

def fetch_data(logger):
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

def transform_data(jsondict, logger):
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

    return df