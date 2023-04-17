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



def determine_gps_coordinates_bomb_carbon():
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
        address_call_api = "{0}, {1}".format(row["Name"],row["Country"])
        lat,long,match = get_coordinates(address_call_api)
        df.loc[index,"Latitude"] = lat
        df.loc[index,"Longitude"] = long
        df.loc[index,"Partial_match"] = match
    # Save data to data_cleaned folder
    df.to_csv("data_cleaned/bomb_carbon_gps.csv",index=False)
    
    
def get_coordinates(address, api_key = API_KEY):
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
    

    

if __name__ == '__main__':
    # Main function
    determine_gps_coordinates_bomb_carbon()