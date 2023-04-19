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

    
def load_urgewald_database(year, type = "UPSTREAM"):
    # Define file version in function of the year
    file_version = {"2021":"urgewald_GOGEL2021V2.xlsx",
                    "2022":"urgewald_GOGEL2022V1.xlsx",
                    }
    file_path = os.path.join("data_sources",file_version[str(year)])
    df= pd.read_excel(file_path, sheet_name=type,\
                    engine='openpyxl', skiprows = 3)
    # Drop NaN rows
    df.drop([0,1],axis=0, inplace = True)
    # Return dataframe
    return df


    
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