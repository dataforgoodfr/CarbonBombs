#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon April 17 09:19:23 2023

@author: Nicolle Mathieu
"""
import os
import pandas as pd
import numpy as np
import requests
from credentials import API_KEY



def determine_gps_coordinates_bomb_carbon(engine):
    """Determine GPS Coordinates for each carbon bombs listed in Kuhne paper
    """
    file_path = "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    df = pd.read_excel(file_path, sheet_name='Full Carbon Bombs List',\
                        engine='openpyxl', skipfooter = 4)
    # Filter column
    df = df[["Name","Country","Potential emissions (Gt CO2)","Fuel"]]
    # Add columns lat and long with dummy values and partial match value
    # Partial match value indicate whether location need to be manually check
    # See https://developers.google.com/maps/documentation/javascript/geocoding?hl=fr
    df["Latitude"] = 0.0
    df["Longitude"] = 0.0
    df["Partial_match"]=False
    # Iteration over rows
    for index, row in df.iterrows():
        if engine == "Google":
            address_call_api = "{0}, {1}".format(row["Name"],row["Country"])
            lat,long,match = get_coordinates_google_api(address_call_api)
        elif engine == "Wikipedia":
            try:
                title = row["Name"]
                lat,long = get_coordinates_wikipedia_api(title)
                match = True
            except:
                lat,long=np.nan,np.nan
                match= False
            
            
        else:
            print("Engine non supported, Please provide a value in the followign list : 'Wikipedia','Google'")
            sys.exit(0)
        df.loc[index,"Latitude"] = lat
        df.loc[index,"Longitude"] = long
        df.loc[index,"Partial_match"] = match
    # Save data to data_cleaned folder
    df.to_csv("data_cleaned/bomb_carbon_gps.csv",index=False)
    
    
def get_coordinates_google_api(address, api_key = API_KEY):
    """Get coordinates of an adress through an API call to Google Maps
    """
    url = f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == 'OK':
            latitude = data['results'][0]['geometry']['location']['lat']
            longitude = data['results'][0]['geometry']['location']['lng']
            if 'partial_match' in data["results"][0].keys():
                partial_match = True
            else:
                partial_match = False
        else:
            print(f"API Error for {address}")
            latitude=np.nan
            longitude=np.nan
            partial_match = True
    else:
        print(f"Request Error for {address}")
        latitude=np.nan
        longitude=np.nan
        partial_match = True
    return latitude,longitude,partial_match

def get_coordinates_wikipedia_api(title):
    # Define the Wikipedia API URL
    url = 'https://en.wikipedia.org/w/api.php'
    # Define the parameters for the API request
    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'coordinates',
        'titles': title,
        'formatversion': 2,
        'coprop': 'type|dim|globe|region|country'
    }
    # Make the API request
    response = requests.get(url, params=params)
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        # Get the coordinates from the JSON data
        try:
            coordinates = data['query']['pages'][0]['coordinates'][0]
            latitude = coordinates["lat"]
            longitude = coordinates["long"]
            return latitude, longitude
        except KeyError:
            print(f'Coordinates not found for the given title: {title}')
            return None
    else:
        print(f'Error while making the API request. Status code: {response.status_code}')
        return None
    
def urgewald_database():
    file_path = "./data_sources/urgewald_GOGEL2022V1.xlsx"
    df_upstream = pd.read_excel(file_path, sheet_name='UPSTREAM',\
                    engine='openpyxl', skiprows = 3)
    df_midstream = pd.read_excel(file_path, sheet_name='MIDSTREAM Expansion',\
                engine='openpyxl', skiprows = 3)
    # Drop NaN rows
    df_upstream.drop([0,1],axis=0, inplace = True)
    df_midstream.drop([0,1],axis=0, inplace = True)
    list_upstream = list(df_upstream["Company Name"])
    list_midstream = list(df_midstream["Company Name"])
    result_list = list_upstream + [x for x in list_midstream if x not in list_upstream]
    print(len(result_list))

    
def extract_carbon_bomb_from_research_project():
    file_path = "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    df = pd.read_excel(file_path, sheet_name='Full Carbon Bombs List',\
                    engine='openpyxl', skipfooter = 4)
    df = df[["New","Name","Country","Potential emissions (Gt CO2)","Fuel"]]
    # Make some adjustement on new project column
    df.rename(columns={ df.columns[0]: "New_project" }, inplace = True)
    df["New_project"].replace(np.nan, False, inplace= True)
    df.loc[df['New_project']=='*', 'New_project']= True
    # Define column types
    dtype_d = {
        "New_project": "bool",
        "Name": "string",
        "Country": "string",
        "Potential emissions (Gt CO2)": "float",
        "Fuel":"category"
        }
    df = df.astype(dtype_d)
    # Reset index and sort Potential emissions in descending order
    df.reset_index(inplace= True, names = "Original_index")
    df.sort_values("Potential emissions (Gt CO2)",ascending = False,inplace= True)
    df.reset_index(drop = True, inplace = True)
    df.to_csv("data_cleaned/bomb_carbon_list_orderedby_CO2_emissions.csv",index=False)
    return 
    
    

if __name__ == '__main__':
    # Main function
    #determine_gps_coordinates_bomb_carbon(engine = "Wikipedia")
    # Urgewald database 
    urgewald_database()
    # Kuhne database (research project)
    # extract_carbon_bomb_from_research_project()