
# Collecting meteorological data for several cities in the 
# month of September 2022. 

import requests as r
import pandas as pd
import json

# Base URL for all NASA Power Project API endpoints
base_url = "https://power.larc.nasa.gov"

# Only going to pull data from one particular path in the api in this script
path = "/api/temporal/hourly/point"
api_endpoint = base_url + path

# List of cities that we need data for
#   -> Note: Could read city locations from a flat file into a python dict if the city list was big enough... 
city_locs = {
    "Chicago":(41.8781, -87.6298),
    "Denver":(39.7392, -104.9903 ),
    "Houston":(29.7604, -95.3698),
    "Los Angeles":(34.052235, -118.243683),
    "NYC":(40.7128, -74.0060)
}

# Time interval that we need data for
time_interval_start = "20220901"
time_interval_end = "20220930"  

# For each city in the city locatoins dictionary
All_City_Meteorological_Data = pd.DataFrame()
for city in city_locs:

    # API call for the current city in this iteration
    json_resp = r.get(

        url = api_endpoint,
        params = {
            "start":time_interval_start,
            "end":time_interval_end,
            "latitude":city_locs[city][0],
            "longitude":city_locs[city][1],
            "community":"ag",
            "parameters":"T2M,WD2M,WS10M,RH2M",
            "format":"json",
            "time-standard":"utc"
        }

    )
    json_resp = json.loads(json_resp.text)

    # Pull out each meteorological data time series from the JSON response

    # -> Temperature at 2 Meters
    T2M = pd.json_normalize(json_resp["properties"]["parameter"]["T2M"]).T
    T2M.rename( columns = {T2M.columns[0]:"Temperature at 2 Meters (Degrees Celsius)"}, inplace = True )
    T2M["Temperature at 2 Meters (Degrees Fahrenheit)"] = ( T2M["Temperature at 2 Meters (Degrees Celsius)"] * (9/5) ) + 32

    # -> Wind Direction at 10 Meters
    WD2M = pd.json_normalize(json_resp["properties"]["parameter"]["WD2M"]).T
    WD2M.rename( columns = {WD2M.columns[0]:"Wind Direction at 10 Meters"}, inplace = True )

    # -> Wind Speed at 10 Meters
    WS10M = pd.json_normalize(json_resp["properties"]["parameter"]["WS10M"]).T
    WS10M.rename( columns = {WS10M.columns[0]:"Wind Speed at 10 Meters"}, inplace = True )

    # -> Relative Humidity at 2 Meters
    RH2M = pd.json_normalize(json_resp["properties"]["parameter"]["RH2M"]).T
    RH2M.rename( columns = {RH2M.columns[0]:"Relative Humidity at 2 Meters"}, inplace = True )

    # Build a df for the current city
    city_meteorological_data = pd.merge(T2M, WD2M, left_index = True, right_index = True)
    city_meteorological_data = pd.merge(city_meteorological_data, WS10M, left_index = True, right_index = True)
    city_meteorological_data = pd.merge(city_meteorological_data, RH2M, left_index = True, right_index = True)
    city_meteorological_data["City"] = city 
    city_meteorological_data["Lat"] = city_locs[city][0]
    city_meteorological_data["Long"] = city_locs[city][1]

    # Re-order cols (so that the data makes a little more sense while reading from left to right)
    city_meteorological_data = city_meteorological_data[
        [
            "City",
            "Lat",
            "Long",
            "Temperature at 2 Meters (Degrees Celsius)",
            "Temperature at 2 Meters (Degrees Fahrenheit)",
            "Wind Direction at 10 Meters",
            "Wind Speed at 10 Meters",
            "Relative Humidity at 2 Meters"
        ]
    ]

    # Reset the index to make the datetimes the first column 
    city_meteorological_data.reset_index(inplace=True)
    city_meteorological_data.rename(columns={"index":"Datetime"}, inplace=True)

    # Convert the data type of the Datetime col so that it can be sorted
    city_meteorological_data["Datetime"] = pd.to_datetime(city_meteorological_data["Datetime"], format = "%Y%m%d%H")

    # Concatenate data set for the current city with the final data set holding 
    # meteorological data for all cities
    All_City_Meteorological_Data = pd.concat([All_City_Meteorological_Data,city_meteorological_data])

# Sort the data (will be in ascending order by default)
All_City_Meteorological_Data.sort_values(by = ["Datetime","Lat","Long"], inplace = True)

# Write all city meteorological data to CSV
All_City_Meteorological_Data.to_csv("All City Meteorological Data.csv", index= False)

# Pull data from this endpoint to test the code for transforming from JSON to tabular format (can see the JSON resp of this in the browser, as well)
#   -> https://power.larc.nasa.gov/api/temporal/hourly/point?start=20220901&end=20220930&latitude=39.7392&longitude=-104.9903&community=ag&parameters=T2M%2CWD2M%2CWS10M%2CRH2M&format=json&header=true&time-standard=utc