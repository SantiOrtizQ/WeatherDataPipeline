from include.config import CITIES, API_key
from concurrent.futures import ThreadPoolExecutor
import requests
import pandas as pd
import time




def fetch_data(logger):
    #create locations info dataframe
    loc_columns=["City", "Latitude", "Longitude", "Country"]

    #get locations info from API with threadpoolexecutor (if high ammount of cities)

    def fetch_city(city):
        url=f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={API_key}"
        response=get_response(url)
        data=response.json()[0]

        #delete unwanted info
        data.pop('local_names', 0)
        data.pop("state", 0)

        return data.values()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        results=list(executor.map(fetch_city, CITIES))
    
    locations=pd.DataFrame(results, columns=loc_columns)


    logger.info("Successfully retrieved locations data.")



    # get weather info from API
    weather_info={}
    lat_lon=locations[["Latitude", "Longitude"]]     # extract latitude and longitude from dataframe
    size=len(lat_lon)
    for i in range(size):
        # get info
        response=get_response(f"http://api.openweathermap.org/data/2.5/weather?lat={float(lat_lon.iat[i, 0])}&lon={float(lat_lon.iat[i, 1])}&appid={API_key}")

        # save weather data into dictionary
        weather_response=response.json()
        weather_sum={"weather": weather_response["main"]}
        weather_sum["weather"]["timestamp"]=weather_response["dt"]
        weather_info[locations.iat[i, 0]]=weather_sum

        logger.info(f"Weather extracted for city {locations.iat[i, 0]}.")

    return weather_info




# ****** EXTRA FUNCTION: get responses properly *******
def get_response(url, retries=3, delay=2):
    for i in range(retries):
        try:
            response=requests.get(url, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if i==retries-1:
                raise e
            time.sleep(delay*(i+1))





# transform jsondict into data frame

def transform_data(jsondict):


    # get columns for dataframe
    df_columns=["city"]+list(list(list(jsondict.values())[0].values())[0].keys())

    rows=[]
    for city in jsondict:
        # flat data
        data=[city]+list(jsondict[city]["weather"].values())

        # saving into the rows
        rows.append(data)
    
    # save data into dataframe
    df=pd.DataFrame(rows, columns=df_columns)
    
    #converting timestamp
    df["timestamp"]=pd.to_datetime(df["timestamp"], unit='s', errors='raise', utc=True)

    return df