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
from fuzzywuzzy import fuzz
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
    
def load_carbon_bomb_list_database():
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

def load_carbon_bomb_coal_database():
    file_path = "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    df = pd.read_excel(file_path, sheet_name='Coal',\
                    engine='openpyxl', skipfooter = 3)
    # Filtering columns of interest
    df = df[["New","Project Name","Country","Potential emissions (GtCO2)","Fuel"]]
    # Make some adjustement on new project column
    df.rename(columns={ df.columns[0]: "New_project" }, inplace = True)
    df["New_project"].replace(np.nan, False, inplace= True)
    df.loc[df['New_project']=='*', 'New_project']= True
    # Define column types
    dtype_d = {
        "New_project": "bool",
        "Project Name": "string",
        "Country": "string",
        "Potential emissions (GtCO2)": "float",
        "Fuel":"category"
        }
    df = df.astype(dtype_d)
    # Change country name to correspond to GEM database (only for Russia)
    df['Country'] = df['Country'].replace({'Russian Federation': 'Russia', 'Turkey': 'Türkiye'})
    return df

def load_carbon_bomb_gasoil_database():
    file_path = "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    df = pd.read_excel(file_path, sheet_name='Oil&Gas',engine='openpyxl',
                     skipfooter = 4,skiprows=1)
    # Filtering columns of interest and reorganize them in order to match coal columns
    df = df[["New","Project","Country","Gt CO2"]]
    df.columns = ["New","Project Name","Country","Potential emissions (GtCO2)"]
    df["Fuel"] = "Oil&Gas"
    # Make some adjustement on new project column
    df.rename(columns={ df.columns[0]: "New_project" }, inplace = True)
    df["New_project"].replace(np.nan, False, inplace= True)
    df.loc[df['New_project']=='*', 'New_project']= True
    # Define column types
    dtype_d = {
        "New_project": "bool",
        "Project Name": "string",
        "Country": "string",
        "Potential emissions (GtCO2)": "float",
        "Fuel":"category"
        }
    df = df.astype(dtype_d)
    # Change country name to correspond to GEM database (only for Russia)
    df['Country'] = df['Country'].replace({'Russian Federation': 'Russia', 'Turkey': 'Türkiye'})
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
    df_cb = load_carbon_bomb_list_database()
    # Only keep Carbon Bomb that corresponds to Coal in df_cb
    df_cb = df_cb[df_cb["Fuel"]=="Coal"]
    # Only keep Coal Mine which name corresponds to a Carbon Bomb
    list_coal_carbon_bomb = list(df_cb["Name"])
    print(len(list_coal_carbon_bomb))
    # Filter df rows based on this list
    df = df[df['Mine Name'].isin(list_coal_carbon_bomb)]
    print(df.shape)
    return df

def create_carbon_bombs_table():
    # Load Dataframe from different sources 
    df_coal_carbon_bombs = load_carbon_bomb_coal_database()
    df_coal_gem_mines = load_coal_mine_gem_database()
    df_gasoil_carbon_bombs = load_carbon_bomb_gasoil_database()
    # Focus on merge for coal mines between GEM and CB database
    # Filter columns of interest from GEM database (WARNING : very restrictive)
    GEM_usefull_columns = [
        'Mine IDs',
        'Mine Name',
        'Country',
        'GEM Wiki Page (ENG)',
        'Latitude',
        'Longitude',
        ]
    df_coal_gem_mines = df_coal_gem_mines[GEM_usefull_columns]
    # Secondly only retain perfect match on Project Name between GEM & CB with a country verification
    df_coal_carbon_bombs["temp"] = df_coal_carbon_bombs["Project Name"]+"/"+df_coal_carbon_bombs["Country"]
    df_coal_gem_mines["temp"] = df_coal_gem_mines["Mine Name"]+"/"+df_coal_gem_mines["Country"]
    list_coal_carbon_bomb = list(df_coal_carbon_bombs["temp"])
    df_coal_gem_mines_perfect_match = df_coal_gem_mines[df_coal_gem_mines['temp'].isin(list_coal_carbon_bomb)]
    # Check how many duplicate in the filtered GEM database and handle them
    duplicates = df_coal_gem_mines_perfect_match[df_coal_gem_mines_perfect_match.duplicated(subset=['Mine Name'],keep=False)]
    non_duplicates = df_coal_gem_mines_perfect_match[~(df_coal_gem_mines_perfect_match.duplicated(subset=['Mine Name'],keep=False))]
    # WARNING TO BE MODIFY
    # Simple approach, quality check if on selected columns for GEM DB
    # duplicate rows really are duplicated on all properties
    full_duplicates = duplicates[duplicates.duplicated(keep=False)]
    partial_duplicates = duplicates[~(duplicates.duplicated(keep=False))]
    # WARNING Operations on partial duplicates data in order to erase difference 
    # when mine name and country are the same
    print("Cleaning Operations to be set")
    partial_duplicates_cleaned = partial_duplicates
    # WARNING We can setup a check for shape between full_duplicates and duplicates
    
    # Once cleaning operation on partial duplicates are done, merge data
    # For filtering duplicates list we use the inverse of the mask (see ~) because when
    # duplicates is more than twice we would still have duplicates 
    # (see doc https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.duplicated.html)
    full_duplicates_filtered = duplicates[~(duplicates.duplicated(keep="first"))]
    df_coal_gem_mines_filtered = pd.concat([non_duplicates,full_duplicates_filtered,partial_duplicates_cleaned])
    # Drop temp column from gem and cb dataframe
    df_coal_gem_mines_filtered.drop('temp', axis=1,inplace=True)
    df_coal_carbon_bombs.drop('temp', axis=1,inplace=True)
    
    
    
    # TEMP DEV
    list_gem_match = list(df_coal_gem_mines_filtered["Mine Name"])
    df_carbon_bombs_no_match =  df_coal_carbon_bombs[~(df_coal_carbon_bombs['Project Name'].isin(list_gem_match))]


    print(df_carbon_bombs_no_match.shape)
    
    # Iteration over rows to find the right match
    for index, row in df_carbon_bombs_no_match.iterrows():
        name, country, status = row["Project Name"], row["Country"], row["New_project"]
        match_name_gem = find_matching_name_for_GEM(name, country, status,df_coal_gem_mines)


    
    
    
    # Merge dataframe based on column Mine Name for GEM and Project Name 
    df_coal_merge = pd.merge(df_coal_carbon_bombs, df_coal_gem_mines_filtered, left_on='Project Name', right_on='Mine Name', how='left')
    df_coal_merge.drop(["Mine Name","Country_y"],axis=1,inplace=True)
    df_coal_merge.to_csv("output_coal_table.csv",index=False)
    # WARNING Here add a pytest that will compare equality between df_coal_carbon_bombs.shape[0] and df_coal_merge[0]
    # They should be equal even if we perform a left join because good cleaning on duplicates. 

    # Third step try to conciliate missing values 
    
    
    
def find_matching_name_for_GEM(name, country, status, df_gem):
    # WARNING To refactor because warning raise but not clean
    df_gem = df_gem.copy()
    # Filter line that correspond to the right country
    df_gem = df_gem[df_gem["Country"]==country]
    # Only keep first word of the name we want to match and the column Mine Name of GEM Database
    first_word_name = name.split()[0]
    df_gem["First_name"] = df_gem["Mine Name"].str.split().str[0]
    # Compare and look to how many match we have if we only look at the first word
    df_gem = df_gem[df_gem["First_name"]==first_word_name]
    # Depending on df_gem.shape various case scenario
    if df_gem.shape[0]==1:
        # Perfect match we retrieve GEM match name
        return df_gem["Mine Name"].iat[0]
    elif df_gem.shape[0]>1:
        df_gem["Fuzz_score"] = df_gem["Mine Name"].apply(lambda x: fuzz.ratio(x, name))
        print(f"The initial name was :{name}")
        print(df_gem[["Mine Name","Fuzz_score"]])
        print("\n\n")



        
            
    return "coucou"





if __name__ == '__main__':
    # Main function
    create_carbon_bombs_table()