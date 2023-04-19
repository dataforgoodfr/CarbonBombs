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
import warnings
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

    
def load_urgewald_database_GOGEL(year, type = "UPSTREAM"):
    # Define file version in function of the year
    file_version = {"2021":"urgewald_GOGEL2021V2.xlsx",
                    "2022":"urgewald_GOGEL2022V1.xlsx",
                    }
    file_path = os.path.join("data_sources",file_version[str(year)])
    df = pd.read_excel(file_path, sheet_name=type,\
                    engine='openpyxl', skiprows = 3)
    # Drop NaN rows
    df.drop([0,1],axis=0, inplace = True)
    # Return dataframe
    return df

def load_urgewald_database_GCEL():
    file_path = "./data_sources/urgewald_GCEL_2022_download_0.xlsx"
    df = pd.read_excel(file_path, sheet_name = 'Output', engine='openpyxl')
    # Return dataframe
    return df
    
def load_carbon_bomb_database():
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
    return df
    
def load_coal_mine_gem_database():
    file_path = "./data_sources/Global-Coal-Mine-Tracker-April-2023.xlsx"
    df = pd.read_excel(file_path, sheet_name='Global Coal Mine Tracker',engine='openpyxl')
    return df

def load_gasoil_mine_gem_database():
    file_path = "./data_sources/Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx"
    # Line that must be passed before in order to avoid useless warning
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    df = pd.read_excel(file_path, sheet_name='Main data',engine='openpyxl')
    return df

def load_d4g_database():
    file_path = "./data_sources/Carbon_bomb_personalDB.csv"
    df = pd.read_csv(file_path,sep=";")
    return df
    
    
def clean_coal_mine_gem_database():
    # Load coal mine database from gem
    df = load_coal_mine_gem_database()
    # Filter dataframe based on Carbon bomb list 
    df_cb = load_carbon_bomb_database()
    # Only keep Carbon Bomb that corresponds to Coal in df_cb
    df_cb = df_cb[df_cb["Fuel"]=="Coal"]
    # Only keep Coal Mine which name corresponds to a Carbon Bomb
    list_coal_carbon_bomb = list(df_cb["Name"])
    print(len(list_coal_carbon_bomb))
    # Filter df rows based on this list
    df = df[df['Mine Name'].isin(list_coal_carbon_bomb)]
    print(df.shape)
    
    return df




if __name__ == '__main__':
    # Main function
    #determine_gps_coordinates_bomb_carbon(engine = "Wikipedia")
    # Urgewald database 
    urgewald_database()
    # Kuhne database (research project)
    # extract_carbon_bomb_from_research_project()