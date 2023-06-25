#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script deals with Carbon Bombs database from K.Kühne research paper and 
GEM databasewebsite to build the csv file saved as 
carbon_bombs_informations.csv

Examples:
To use this script, simply run it from the command line:
$ python main.py
"""
import os
import sys
import re
import pandas as pd
import numpy as np
import warnings
import time
import requests
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
from data_sources.manual_match import manual_match_coal
from data_sources.manual_match import manual_match_gasoil

from data_sources.manual_data import manual_data_to_add


def load_carbon_bomb_list_database():
    """
    Load the Carbon Bomb List database.

    Returns:
    pandas.DataFrame: A dataframe containing the data from the database.

    Raises:
    FileNotFoundError: If the specified file path cannot be found.
    """
    file_path = "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    df = pd.read_excel(file_path, sheet_name='Full Carbon Bombs List',\
                    engine='openpyxl', skipfooter = 4)
    df=df.loc[:,["New","Name","Country","Potential emissions (Gt CO2)","Fuel"]]
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
    """
    Loads the carbon bomb coal database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the following columns:
        - New_project (bool): whether the project is new or not.
        - Project Name (str): name of the project.
        - Country (str): country where the project is located.
        - Potential emissions (GtCO2) (float): potential emissions of the 
        project.
        - Fuel (category): type of fuel used in the project.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path: 
    "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx".
    The sheet to be read is "Coal".
    The function filters the columns of interest, renames the 'New' column to 
    'New_project', changes its values to boolean and sets the desired data 
    types for the dataframe columns. It also replaces some country names to 
    correspond to GEM database.
    """
    file_path = "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    df = pd.read_excel(file_path, sheet_name='Coal',\
                    engine='openpyxl', skipfooter = 3)
    # Filtering columns of interest
    df = df.loc[:,["New","Project Name","Country",\
                    "Potential emissions (GtCO2)","Fuel"]]
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
    df['Country'] = df['Country'].replace({'Russian Federation': 'Russia',
                                           'Turkey': 'Türkiye'})
    return df

def load_carbon_bomb_gasoil_database():
    """
    Loads the carbon bomb coal database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the following columns:
        - New_project (bool): whether the project is new or not.
        - Project Name (str): name of the project.
        - Country (str): country where the project is located.
        - Potential emissions (GtCO2) (float): potential emissions of the 
        project.
        - Fuel (category): type of fuel used in the project.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path: 
    "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx".
    The sheet to be read is "Coal".
    The function filters the columns of interest, renames the 'New' column to 
    'New_project', changes its values to boolean and sets the desired data 
    types for the dataframe columns. It also replaces some country
    names to correspond to GEM database.
    """
    file_path = "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    df = pd.read_excel(file_path, sheet_name='Oil&Gas',engine='openpyxl',
                     skipfooter = 4,skiprows=1)
    # Filtering columns of interest and reorganize them in order to match 
    # coal columns
    df = df.loc[:,["New","Project","Country","Gt CO2"]]
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
    df['Country'] = df['Country'].replace({
        'Russian Federation': 'Russia',
        'Turkey': 'Türkiye',
        'Saudi-Arabia':'Saudi Arabia',
        'Kuwait-Saudi-Arabia-Neutral Zone':'Kuwait-Saudi Arabia'
        })
    # In case of project that have same name, rename temporarily project name 
    # with the format projectname_country
    # Renaming Will be undone after the manual matching process
    df.loc[df.duplicated(subset='Project Name', keep=False),"Project Name"] = (
        df["Project Name"]+"_"+df["Country"])
    return df
    
def load_coal_mine_gem_database():
    """
    Loads the Global Coal Mine Tracker database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the data from the "Global Coal Mine Tracker"
        sheet.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path: 
    "./data_sources/Global-Coal-Mine-Tracker-April-2023.xlsx".
    The sheet to be read is "Global Coal Mine Tracker".
    """
    file_path = "./data_sources/Global-Coal-Mine-Tracker-April-2023.xlsx"
    df = pd.read_excel(file_path, sheet_name='Global Coal Mine Tracker',
                       engine='openpyxl')
    return df

def load_gasoil_mine_gem_database():
    """
    Loads the Global Oil and Gas Extraction Tracker database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the data from the "Main data" sheet.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path: 
    "./data_sources/Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx".
    The sheet to be read is "Main data".
    """
    file_path = "./data_sources/Global-Oil-and-Gas-Extraction"\
                "-Tracker-Feb-2023.xlsx"
    # Line that must be passed before in order to avoid useless warning
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    df = pd.read_excel(file_path, sheet_name='Main data',engine='openpyxl')
    return df

def load_chatGPT_database():
    """
    Loads the ChatGPT database and returns a filtered and remapped DataFrame.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing carbon bomb data from the ChatGPT database.

    Notes:
    ------
    This function loads the ChatGPT database from a CSV file located at 
    "./data_sources/Data_chatGPT_carbon_bombs.csv". It then filters the 
    DataFrame to only keep columns that are useful for matching with the GEM 
    database. The country names in the "Country" column are remapped to ensure 
    consistency with the GEM database. Finally, the "Operator" column is 
    remapped to "Operators (GEM)" to ensure consistency with the GEM database.
    """
    file_path = "./data_sources/Data_chatGPT_carbon_bombs.csv"
    df = pd.read_csv(file_path,sep=";")
    # Filter dataframe usefull columns
    chatGPT_usefull_columns = [
        'Name',
        'Country',
        'Latitude',
        'Longitude',
        'Operator',
        ]
    df = df.loc[:,chatGPT_usefull_columns]
    # Remap some country name to ensure correspondance
    df['Country'] = df['Country'].replace({
        'Russian Federation': 'Russia',
        'Turkey': 'Türkiye',
        'Saudi-Arabia':'Saudi Arabia',
        'Kuwait-Saudi-Arabia-Neutral Zone':'Kuwait-Saudi Arabia'
        })
    # Remap some column name to ensure correspondance
    mapping_column = {
        "Operator":"Operators_source_GEM",
    }
    df.rename(columns=mapping_column,inplace=True)
    return df

def create_carbon_bombs_gasoil_table():
    """
    Combines data from the Global Oil and Gas Extraction Tracker and the Carbon
    Bomb Oil and Gas database to create a table of oil and gas mines matched to
    their corresponding carbon bombs.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with columns for the oil and gas mine's name, 
        country, potential emissions (GtCO2), fuel type, latitude, longitude, 
        operator, owner, parent, and the corresponding carbon bomb name.

    Raises
    ------
    FileNotFoundError:
        If one of the data files is not found in the specified path.
    ValueError:
        If one of the data files does not contain the expected sheet.

    Notes
    -----
    This function uses the load_carbon_bomb_gasoil_database() and
    load_gasoil_mine_gem_database() functions to read data from two Excel files.
    The expected file paths are:
    - "./data_sources/Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx"
    - "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    """
    df_gasoil_carbon_bombs = load_carbon_bomb_gasoil_database()
    df_gasoil_gem_mines = load_gasoil_mine_gem_database()
    # Focus on merge for coal mines between GEM and CB database
    # Filter columns of interest from GEM database (WARNING : very restrictive)
    GEM_usefull_columns = [
        'Unit ID',
        'Unit name',
        'Country',
        'Wiki URL',
        'Latitude',
        'Longitude',
        'Operator',
        'Owner',
        'Parent',
        ]
    df_gasoil_gem_mines = df_gasoil_gem_mines.loc[:,GEM_usefull_columns]
    # Only retain perfect match on Project Name between GEM & CB with a 
    # country verification
    df_gasoil_carbon_bombs["temp"] = (df_gasoil_carbon_bombs["Project Name"]+
                                      "/"+df_gasoil_carbon_bombs["Country"])
    df_gasoil_gem_mines["temp"] = (df_gasoil_gem_mines["Unit name"]+"/"+
                                   df_gasoil_gem_mines["Country"])
    list_gasoil_carbon_bomb = list(df_gasoil_carbon_bombs["temp"])
    df_gasoil_gem_perfect_match = (df_gasoil_gem_mines.loc
        [df_gasoil_gem_mines['temp'].isin(list_gasoil_carbon_bomb),:].copy())
    # Add gem mines that has no perfect match with carbon bomb
    list_gem_match = list(df_gasoil_gem_perfect_match['Unit name'])
    df_carbon_bombs_no_match = (df_gasoil_carbon_bombs
            [~(df_gasoil_carbon_bombs['Project Name'].isin(list_gem_match))])
    # Iteration over rows to find the right match
    index_gem_single_match = list()
    # Initiate multi match dataframe with an additional column to store 
    # Carbon Bombs names
    GEM_multi_match_columns = GEM_usefull_columns + ["CarbonBombName"]
    df_gasoil_gem_multi_match = pd.DataFrame(columns=GEM_multi_match_columns)
    for _, row in df_carbon_bombs_no_match.iterrows():
        name, country = row["Project Name"], row["Country"]
        index_gem, _ = find_matching_name_for_GEM_gasoil(name,
                                        country,df_gasoil_gem_mines)
        # 3 cases index_gem = 0 / value / list_values
        if index_gem == 0:
            continue
        elif isinstance(index_gem, list):
            concatenate_line = concatenate_multi_extraction_site(
                df_gasoil_gem_mines,GEM_multi_match_columns,index_gem, name)
            df_gasoil_gem_multi_match.loc\
                [df_gasoil_gem_multi_match.shape[0]] = concatenate_line
        else:
            index_gem_single_match.append(index_gem)
    # Once index list is complete extract those line from df_coal_gem_mines
    df_gasoil_gem_manual_match = df_gasoil_gem_mines.iloc\
                                    [index_gem_single_match,:].copy()
    # Before replace name by the one in carbon bombs in the extracted dataframe
    # in order to ensure join quality between GEM and CB database, we must make 
    # specific manipulation on manual_match_gasoil dictionary in order to 
    # separate dictionnary values that appears multiple time from the one that 
    # only appeared once. This manipulation is only relative to gasoil database
    # given the fact we had to associate the same GEM Mine to different carbon
    # bombs due to the lack of information on GEM Gas&Oil Tracker. 
    unique_values_dict = {}
    non_unique_values_dict = {}
    value_count = {}
    # Loop through the items in the manual_match_gasoil dictionary
    for key, value in manual_match_gasoil.items():
        # Keep track of how many times each value appears
        value_count[value] = value_count.get(value, 0) + 1
    # Fulfill unique_values_dict and non_unique_values_dict dictionnaries based 
    # on value_count dictionnary
    for key, value in manual_match_gasoil.items():
        # If the value has not been encountered before and only appears once,
        # add it to the unique_values_dict
        if value_count[value] == 1:
            unique_values_dict[key] = value
        # If the value has been encountered before, add it to the 
        # non_unique_values_dict
        else:
            non_unique_values_dict[key] = value
    # Replace name in df_gasoil_gem_manual_match by the one in carbon bomb
    # First only for unique_values_dict because unique values so simple
    inverse_manual_match_gasoil = ({value: key for key, value in \
                                                    unique_values_dict.items()})
    df_gasoil_gem_manual_match['Unit name'] = (df_gasoil_gem_manual_match
                            ['Unit name'].replace(inverse_manual_match_gasoil))
    # Secondly for non_unique_values_dict, more manipulation needed when 
    # inverting dictionnary and associated carbon bomb name to a site.
    inverted_dict = {}
    for key, value in non_unique_values_dict.items():
        if value in inverted_dict:
            if isinstance(inverted_dict[value], list):
                inverted_dict[value].append(key)
            else:
                inverted_dict[value] = [inverted_dict[value], key]
        else:
            inverted_dict[value] = key
    # Iterate throught all values of inverted_dict and remap each carbon bombs
    # name one by one (quick solution might be rework later)
    # Reset_index of df_gasoil_gem_manual_match before this operation
    df_gasoil_gem_manual_match.reset_index(inplace = True,drop = True)
    for gem_site in inverted_dict.keys():
        for carbon_bomb in inverted_dict[gem_site]:
            # Retrieve first index that corresponds to gem_site
            first_index = df_gasoil_gem_manual_match.loc[\
                df_gasoil_gem_manual_match['Unit name']==gem_site,:].index[0]
            # Modify only the first index of previous query with the carbon 
            # bomb name
            df_gasoil_gem_manual_match.loc[first_index,"Unit name"]=carbon_bomb
            # On the next iteration, as Unit Name has been modify, this 
            # instruction should modify the next one.            
    # Columns manipulation on df_multi_match to allow concatenation
    unit_name_list = list(df_gasoil_gem_multi_match["Unit name"])
    df_gasoil_gem_multi_match["Unit name"] = (df_gasoil_gem_multi_match
                                              ["CarbonBombName"])
    df_gasoil_gem_multi_match.loc[:,"Unit_concerned"] = unit_name_list
    # Add column "Unit_concerned" for the other df and drop useless columns
    df_gasoil_gem_perfect_match["Unit_concerned"]=""
    df_gasoil_gem_manual_match["Unit_concerned"]=""
    df_gasoil_gem_perfect_match.drop("temp",axis=1,inplace=True)
    df_gasoil_gem_manual_match.drop("temp",axis=1,inplace=True)
    df_gasoil_carbon_bombs.drop("temp",axis=1,inplace=True)
    df_gasoil_gem_multi_match.drop("CarbonBombName",axis=1,inplace=True)
    # Concat dataframes from match perfect/multi/manual
    df_gasoil_gem_matched = pd.concat([df_gasoil_gem_perfect_match,
                                       df_gasoil_gem_manual_match,
                                       df_gasoil_gem_multi_match,
                                       ])
    # Merge dataframe based on column Mine Name for GEM and Project Name 
    df_gasoil_merge = pd.merge(df_gasoil_carbon_bombs, 
                                df_gasoil_gem_matched,
                                left_on='Project Name',
                                right_on='Unit name',
                                how='left')
    df_gasoil_merge.drop(["Unit name","Country_y"],axis=1,inplace=True)
    return df_gasoil_merge

def ponderate_percentage(dict_percentage):
    """
    Ponderates the percentages in a dictionary such that their sum equals 100%.

    Parameters
    ----------
    dict_percentage : dict
        A dictionary where keys are company names and values are percentages.

    Returns
    -------
    dict:
        A new dictionary with the same keys as `dict_percentage` but with values
        that have been adjusted such that their sum equals 100%.

    Raises
    ------
    ValueError:
        If the values in `dict_percentage` do not add up to 100.

    Notes
    -----
    This function takes a dictionary where the values are percentages that may
    not add up to exactly 100. It calculates a new set of percentages that are
    adjusted such that their sum equals 100. The adjusted percentages are then
    rounded to one decimal point and added up to ensure that their sum equals
    exactly 100. The function returns a new dictionary with the same keys as
    the input dictionary but with adjusted percentages as values.
    """
    # Calculate sum of percentage to ponderate
    sum_percentage = sum(list(dict_percentage.values()))
    # Calculate new percentage based on this sum
    new_percentage = [elt/sum_percentage for elt in dict_percentage.values()]
    companies = dict_percentage.keys()
    # Round each element in the new_percentage list
    rounded_percentage = [round(x * 100, 1) for x in new_percentage]
    # Adjust the decimal of the last percentage in order to ensure a 100% sum
    diff = 100 - sum(rounded_percentage)
    rounded_percentage[-1] = round(rounded_percentage[-1] + diff,1)
    # Build new dictionary
    dict_percentage = dict()
    for company, percentage in zip(companies, rounded_percentage):
        dict_percentage[company] = percentage
    # Return dict with ponderate percentage
    return dict_percentage

def compute_percentage_multi_sites(raw_line):
    """
    Compute the percentage of involvement of each company mentioned in a given 
    line.

    Args:
        raw_line (str): A string that contains information about the companies 
        and their involvement.

    Returns:
        str: A clean line that contains the name of each company and their 
        corresponding percentage of involvement.

    Raises:
        TypeError: If the input `raw_line` is not a string.

    Examples:
        >>> compute_percentage_multi_sites("ABC (20%), XYZ (80%)")
        'ABC (20%); XYZ (80%)'
        
        >>> compute_percentage_multi_sites("ABC; XYZ; PQR")
        'ABC (33.33333333333333%); XYZ (33.33333333333333%); 
        PQR (33.33333333333333%)'
        
        >>> compute_percentage_multi_sites("")
        'No informations on company (100.0%)'

    Notes:
        The function can handle two possibilities:
        1) When the raw_line contains percentages, it calculates and merges the 
        percentage of involvement for each company and returns a clean line.
        2) When the raw_line does not contain percentages, it assumes each 
        company has an equal involvement and calculates the percentage 
        accordingly.

    The input raw_line should be in one of the following formats:
        1) <company_1> (percentage_1%), <company_2> (percentage_2%), ...
        2) <company_1>; <company_2>; <company_3>; ...

    If there is no information about the company, the output line will be 
    'No informations on company (100.0%)'.
    """
    # With raw_line content 2 possibilities : Percentage are indicated or not
    if "%" in raw_line:
        # Case where percentage are indicated
        companies = ["(".join(x.split("(")[:-1]).strip() for x in re.split("[;|,]", raw_line)]    
        percentages = re.findall(r"\(([\d\.]+)%\)", raw_line)
        # Merge percentage of same company into one
        combined_percentages = {}
        for company, percentage in zip(companies, percentages):
            if company in combined_percentages:
                combined_percentages[company] += float(percentage)
            else:
                combined_percentages[company] = float(percentage)
        # Once percentage are merge 3 possibilities based on the sum 
        sum_percentage = sum(list(combined_percentages.values()))
        # Percentage less than 100 percent (We complete by "Others"):
        if sum_percentage < 100.0 :
            left_percentage = 100.0 - sum_percentage
            combined_percentages["Others"] = left_percentage
        # Percentage equal to 100 percent (No action needed):    
        elif sum_percentage == 100.0:
            pass
        #Percentage more than 100 percent (We ponderate the results)
        else:
            combined_percentages = ponderate_percentage(combined_percentages)
    else: 
        # Case no percentage are indicated, we defined it based on number of 
        # compagnies defined into 
        companies = raw_line.split(";")
        if companies == ['']:
            companies = ['No informations on company']
        # Compute percentage considering each company have the same involvement
        percentages = [100.0/len(companies) for _ in companies]
        # Merge percentage of same company into one
        combined_percentages = {}
        for company, percentage in zip(companies, percentages):
            if company in combined_percentages:
                combined_percentages[company] += float(percentage)
            else:
                combined_percentages[company] = float(percentage)
    # Once combined percentage is defined for each case, defined a clean line
    clean_line = ""
    for keys in combined_percentages:
        company_line = f"{keys} ({combined_percentages[keys]}%);"
        clean_line = clean_line + company_line
    clean_line = clean_line[:-1] # Delete last ;
    return clean_line

def concatenate_multi_extraction_site(df_gem, list_columns, multi_index,
                                      project_name):
    """
    Concatenate values from multiple rows of a Pandas DataFrame for a given set 
    of columns.

    Args:
        df_gem (pandas.DataFrame): The Pandas DataFrame to extract values from.
        list_columns (list): A list of column names to concatenate values from.
        multi_index (tuple): A tuple of integer index values to use for row 
        selection.
        project_name (str): The name of the project to use for concatenation of 
        CarbonBombName.

    Returns:
        list: A list of concatenated values, one per column in the list_columns 
        parameter.

    Raises:
        SystemExit: If a column name in list_columns is not recognized for 
        concatenation.

    Example:
        >>> df = pd.DataFrame({'Country': ['USA', 'USA', 'Canada', 'Canada'],
        ...                    'CarbonBombName': ['Proj A', 'Proj A',
                                                    'Proj B', 'Proj B'],
        ...                    'Latitude': [34.05, 32.71, 45.50, 46.81],
        ...                    'Longitude': [-118.24, -117.16, -73.57, -71.19],
        ...                    'Unit ID': ['A1', 'A2', 'B1', 'B2'],
        ...                    'Unit name': ['Unit A', 'Unit A', 'Unit B', 
                                            'Unit B'],
        ...                    'Wiki URL': ['http://A', 'http://A', 'http://B',
                                            'http://B'],
        ...                    'Operator': ['Op A', 'Op A', 'Op B', 'Op B'],
        ...                    'Parent': ['P A', np.nan, np.nan, 'P B'],
        ...                    'Owner': ['Owner A', 'Owner A', 'Owner B', 
                                        'Owner B']})
        >>> concatenate_multi_extraction_site(df, ['Country', 'CarbonBombName',
        'Latitude', 'Longitude', 'Unit ID','Unit name', 'Wiki URL', 'Operator',
        'Parent', 'Owner'], (0, 1), 'Proj A')
        ['USA', 'Proj A', 34.05, -118.24, 'A1;A2', 'Unit A;Unit A',
        'http://A;http://A', 'Op A', 'P A', ['Owner A', 'Owner A']]
    """
    list_value_concat=list()
    for elt in list_columns:
        if elt == "Country":
            value = df_gem.loc[multi_index[0],elt]
            list_value_concat.append(value)
        elif elt == "CarbonBombName":
            list_value_concat.append(project_name)
        elif elt in ["Latitude","Longitude"]:
            value = df_gem.loc[multi_index[0],elt]
            list_value_concat.append(value)
        elif elt in ["Unit ID","Unit name","Wiki URL"]:
            value_concat = [df_gem.loc[index,elt] for index in multi_index]
            value = ";".join(value_concat)
            list_value_concat.append(value)
        elif elt =="Operator":
            # Ensure to delete all duplicate operator from this list
            value_concat = [df_gem.loc[index,elt] for index in multi_index]
            unique_list = list(set(value_concat))
            value = ";".join(unique_list)
            list_value_concat.append(value)
        elif elt =="Parent":
            value_concat = [df_gem.loc[index,elt] for index in multi_index]
            # Filtered nan from raw_percentage list    
            filtered_percentage = ([x for x in value_concat 
                                    if not isinstance(x,float)])
            value = ";".join(filtered_percentage)
            list_value_concat.append(value)
        elif elt =="Owner":
            value = [df_gem.loc[index,elt] for index in multi_index]
            list_value_concat.append(value)  
        else:
            print(f"No concatenation handle for column name {elt}\n"
                  "Please verfiy list_columns content\n"
                  "Exit the program")
            sys.exit()
    return list_value_concat
            
def find_matching_name_for_GEM_gasoil(name, country, df_gem):
    """
    Find the matching name(s) for a given Gas&Oil project in the GEM database.

    Args:
        name (str): The name of the Gas&Oil project.
        country (str): The country of the Gas&Oil project.
        df_gem (pandas.DataFrame): The GEM database as a pandas DataFrame.

    Returns:
        Tuple[Union[int, List[int]], Union[str, List[str]]]: A tuple of 
        index(es) and name(s) matching the Gas&Oil project in the GEM database. 
        If no matching name is found, the index will be 0 and the name will be 
        an empty string.

    Raises:
        None.

    Notes:
        - A copy of df_gem is made to avoid modification warnings.
        - Filtering is based on the "Country" column of df_gem.
        - For Gas&Oil projects, manual matching dictionary is mainly used due 
        to difficulties on matching Carbon Bombs and
          Extraction Site.
        - "$" in mine_name_gem means shale was identified in a list of 
        extraction sites.
        - If no match is found in the manual matching dictionary, the index 
        will be 0 and the name will be an empty string.
        - If a new project has no match in the GEM database, the index will be 
        0 and the name will be an empty string.
    """
    # Setup a copy to avoid warning 
    df_gem = df_gem.copy()
    # Filter line that correspond to the right country
    df_gem = df_gem[df_gem["Country"]==country]
    # For Gas&Oil we mainly use manual matching dictionary due 
    # to difficulties on matching Carbon Bombs and Extraction Site
    if name in manual_match_gasoil.keys():
        mine_name_gem = manual_match_gasoil[name]
    else:
        mine_name_gem = "None"
    # Identify special case for mine_name_gem
    if "$" in mine_name_gem:
        # Case where shale where identified in a list of extraction site    
        list_mine = mine_name_gem.split("$")
        index_gem = list()
        name_gem = list()
        for elt in list_mine:
            if df_gem.loc[df_gem["Unit name"]==elt].shape[0] > 0:
            # Bloc IF/ELSE to prevent error for Carbon Bomb with the same name 
            # but a different country (ex : Eagle Ford Shale in Mexico and US)
                index_gem.append(df_gem.loc[df_gem["Unit name"]==elt].index[0])
                name_gem.append(elt)
            else:
                index_gem = 0
                name_gem = ""
    elif mine_name_gem=="None":
        # No match associated in manual dictionary 
        index_gem = 0
        name_gem = ""
    elif mine_name_gem=="New":
        # No match associated in gem database due to new projects
        index_gem = 0
        name_gem = ""
    else:
        # Only one carbon bomb associated to one extraction site
        index_gem = df_gem.loc[df_gem["Unit name"]==mine_name_gem].index[0]
        name_gem = df_gem.loc[index_gem,"Unit name"]
    return index_gem, name_gem

def create_carbon_bombs_coal_table():
    """
    Creates a pandas DataFrame of coal carbon bombs data matched with 
    corresponding coal mines data from the GEM database.

    Args:
        None.

    Returns:
        pandas.DataFrame: A pandas DataFrame of coal carbon bombs data matched 
        with corresponding coal mines data from the GEM database.

    Raises:
        None.

    Notes:
        - Loads two pandas DataFrames from different sources: 
        df_coal_carbon_bombs and df_coal_gem_mines.
        - Filters columns of interest from df_coal_gem_mines.
        - Focuses on merge for coal mines between GEM and CB databases.
        - Only retains perfect match on Project Name between GEM and CB with a 
        country verification.
        - Drops duplicates based on "Mine Name" column from 
        df_coal_gem_mines_perfect_match DataFrame.
        - Concatenates non-duplicates and duplicates DataFrames from 
        df_coal_gem_mines_perfect_match.
        - Adds GEM mines that have no perfect match with carbon bomb.
        - Uses find_matching_name_for_GEM_coal function to find the right match 
        for the previous step.
        - Extracts lines from df_coal_gem_mines using the index list of 
        df_carbon_bombs_no_match DataFrame.
        - Replaces name in df_gem_no_match with the one in carbon bomb.
        - Merges df_coal_carbon_bombs and df_coal_gem_mines_matched DataFrames 
        based on "Project Name" and "Mine Name".
        - Drops temporary columns created in previous steps.
    """
    # Load Dataframe from different sources 
    df_coal_carbon_bombs = load_carbon_bomb_coal_database()
    df_coal_gem_mines = load_coal_mine_gem_database()
    # Focus on merge for coal mines between GEM and CB database
    # Filter columns of interest from GEM database (WARNING : very restrictive)
    GEM_usefull_columns = [
        'Mine IDs',
        'Mine Name',
        'Country',
        'GEM Wiki Page (ENG)',
        'Latitude',
        'Longitude',
        'Operators',
        'Owners',
        'Parent Company',
        ]
    df_coal_gem_mines = df_coal_gem_mines.loc[:,GEM_usefull_columns]
    # Only retain perfect match on Project Name between GEM & CB with a 
    # country verification
    df_coal_carbon_bombs["temp"] = (df_coal_carbon_bombs["Project Name"]+"/"+
                                    df_coal_carbon_bombs["Country"])
    df_coal_gem_mines["temp"] = (df_coal_gem_mines["Mine Name"]+"/"+
                                 df_coal_gem_mines["Country"])
    list_coal_carbon_bomb = list(df_coal_carbon_bombs["temp"])
    df_coal_gem_mines_perfect_match = df_coal_gem_mines.loc[\
        df_coal_gem_mines['temp'].isin(list_coal_carbon_bomb),:]
    # Check how many duplicate in the filtered GEM database and handle them
    duplicates = df_coal_gem_mines_perfect_match.loc\
        [df_coal_gem_mines_perfect_match.duplicated(subset=['Mine Name'],
                                                   keep=False),:].copy()
    non_duplicates = df_coal_gem_mines_perfect_match\
        [~(df_coal_gem_mines_perfect_match.duplicated(subset=['Mine Name'],
                                                      keep=False))]
    # For duplicates, drop duplicated line if data is the same on lat/long data
    # Very simple approach, should be modified in the future. 
    # No time to implement complex approach that gather results of duplicates
    # We choose this approach beacause difference on duplicated records always 
    # happened on Operator/Owner/Parent Company columns which is mainly due to 
    # typo errors.
    duplicates.drop_duplicates(subset=["Mine Name"],inplace = True)
    # Concat the different dataframe extract from GEM database :
    # non_duplicates = Perfect match just on the name/country.
    #                   No duplicates associated
    # duplicates = Duplicates match of name/country.
    #              Duplicates might be different on certain columns
    #              Those differences hasn't been handle due to a lack of time
    df_coal_gem_mines_filtered = pd.concat([non_duplicates,duplicates])
    # Second step add gem mines that has no perfect match with carbon bomb
    list_gem_match = list(df_coal_gem_mines_filtered["Mine Name"])
    df_carbon_bombs_no_match =  df_coal_carbon_bombs\
        [~(df_coal_carbon_bombs['Project Name'].isin(list_gem_match))]
    # Iteration over rows to find the right match
    index_gem_no_match = list()
    dict_gem_cb_names = dict()
    # Setup a copy of df_coal_gem_mines in order to avoid match
    # on the same project in GEM database (see Dananhu, China)
    df_coal_gem_mines_copy = df_coal_gem_mines.copy()
    for _, row in df_carbon_bombs_no_match.iterrows():
        name, country = row["Project Name"], row["Country"]
        index_gem, name_gem = find_matching_name_for_GEM_coal(name, country,
                                                    df_coal_gem_mines_copy)
        index_gem_no_match.append(index_gem)
        dict_gem_cb_names[name_gem] = name
        # Drop index_gem from df_coal_gem_mines_copy
        df_coal_gem_mines_copy.drop(index_gem,axis=0,inplace=True)
    # Once index list is complete extract those line from df_coal_gem_mines
    df_gem_no_match = df_coal_gem_mines.iloc[index_gem_no_match,:].copy()
    # Replace name in df_gem_no_match by the one in carbon bomb
    df_gem_no_match['Mine Name'] = df_gem_no_match['Mine Name'].replace(
                                                            dict_gem_cb_names)
    # Merge those rows with df_coal_gem_mines_filtered
    df_coal_gem_mines_matched = pd.concat([df_coal_gem_mines_filtered,
                                           df_gem_no_match])
    # Drop before merge temp column from gem cleaned and CB dataframe
    df_coal_gem_mines_matched.drop('temp', axis=1,inplace=True)
    df_coal_carbon_bombs.drop('temp', axis=1,inplace=True)
    # Merge dataframe based on column Mine Name for GEM and Project Name 
    df_coal_merge = pd.merge(df_coal_carbon_bombs,
                             df_coal_gem_mines_matched,
                             left_on='Project Name',
                             right_on='Mine Name',
                             how='left')
    df_coal_merge.drop(["Mine Name","Country_y"],axis=1,inplace=True)
    return df_coal_merge

def find_matching_name_for_GEM_coal(name, country, df_gem):
    """
    Finds the matching name for a given coal project in the GEM database.

    Args:
        name (str): The name of the coal project.
        country (str): The country of the coal project.
        df_gem (pandas.DataFrame): The GEM database as a pandas DataFrame.

    Returns:
        Tuple[int, str]: A tuple of index and name matching the coal project 
        in the GEM database.

    Raises:
        None.

    Notes:
        - A copy of df_gem is made to avoid modification warnings.
        - Filters lines that correspond to the right country.
        - Determines the column name to use based on df_gem.
        - Only keeps the first word of the name we want to match and the column 
        name of GEM Database.
        - Compares and looks for how many matches we have if we only look at 
        the first word.
        - Depending on df_gem.shape various case scenarios:
            - If only one match is found, retrieves GEM match index.
            - If numerous matches are found, needs to choose between them 
            through fuzzy wuzzy.
            - If no match is found based on the first word, uses manual_match 
            dictionary.
    """
    # Setup a copy to avoid warning 
    df_gem = df_gem.copy()
    # Setup column name to use with coal and gasoil
    if "Unit name" in df_gem.columns:
        # Gas and Oil database from GEM
        column_name = "Unit name"
    elif "Mine Name" in df_gem.columns:
        # Coal database from GEM
        column_name = "Mine Name"
    else:
        print("Error while checking column name for function "
              "find_matching_name_for_GEM")
        sys.exit()
    # Filter line that correspond to the right country
    df_gem = df_gem[df_gem["Country"]==country]
    # Only keep first word of the name we want to match and the column Mine 
    # Name of GEM Database
    first_word_name = name.split()[0]
    df_gem["First_name"] = df_gem[column_name].str.split().str[0]
    # Compare and look to how many match we have if we only look at the first 
    # word
    df_gem_filtered = df_gem.loc[df_gem["First_name"]==first_word_name,:].copy()
    # Depending on df_gem.shape various case scenario
    index_gem, name_gem = 0,0
    if df_gem_filtered.shape[0]==1:
        # Perfect match we retrieve GEM match index
        index_gem = df_gem_filtered.index[0]
        name_gem = df_gem_filtered.loc[index_gem,column_name]  
    elif df_gem_filtered.shape[0]>1:
        # Numerous match, need to choose between them throught fuzzy wuzzy
        # Calculate Fuzz_score for each line which as the same first word as 
        # the carbon bomb
        df_gem_filtered["Fuzz_score"] = df_gem_filtered[column_name].apply(
                                                lambda x: fuzz.ratio(x, name))
        index_gem = df_gem_filtered['Fuzz_score'].idxmax()
        name_gem = df_gem_filtered.loc[index_gem,column_name]
    else:
        # No match based on the first word, we use manual_match dictionary
        mine_name_gem = manual_match_coal[name]
        index_gem = df_gem.loc[df_gem[column_name]==mine_name_gem].index[0]
        name_gem = df_gem.loc[index_gem,column_name]
    return index_gem, name_gem

def add_chat_GPT_data(df):
    """
    Adds ChatGPT data to a DataFrame for rows that have no information from GEM.

    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to which ChatGPT data will be added.

    Returns:
    --------
    pandas.DataFrame
        The original DataFrame with ChatGPT data added.

    Notes:
    ------
    This function loads data from the ChatGPT database, and matches rows in the 
    input DataFrame with rows in the ChatGPT DataFrame based on a temporary 
    column created by concatenating the name of the carbon bomb with the 
    country it is in. It then adds information from the ChatGPT DataFrame to 
    the input DataFrame for rows that have no information from GEM, and adds a 
    column to indicate the source of the added information (either GEM or 
    ChatGPT).

    The columns that are added from ChatGPT are "Latitude", "Longitude", and 
    "Operators (GEM)".
    """
    df_chatgpt = load_chatGPT_database()
    # Once data from chat GPT is loaded fulfill df for rows with
    # no informations from GEM (i.e row with no value in GEM_ID (GEM) column)
    # We must create a temp column to avoid matching same project name in 
    # different country
    df_chatgpt["temp"] = df_chatgpt["Name"]+"/"+df_chatgpt["Country"]
    df["temp"] = (df["Carbon_bomb_name_source_CB"]+"/"+df["Country_source_CB"])
    # Filter df_chatgpt to only keep rows within list_na_carbon_bombs
    list_na_carbon_bombs = (df[df['GEM_id_source_GEM'].isna()]["temp"]
                            .unique())
    df_chatgpt = df_chatgpt.loc[df_chatgpt["temp"].isin(list_na_carbon_bombs),:]
    # Order both dataframe columns based on temp column in ascending order
    df_chatgpt.sort_values("temp",ascending = True, inplace = True)
    df.sort_values("temp",ascending = True, inplace = True)
    # Fulfil column with ChatGPT informations
    list_fulfill_columns =  [
        "Latitude",
        "Longitude",
        "Operators_source_GEM",
    ]
    for column in list_fulfill_columns:
        new_values = list(df_chatgpt[column])
        df.loc[df["temp"].isin(list_na_carbon_bombs),column] = new_values
    # Add columns lat/long/operator_source that define source either come from 
    # GEM database or ChatGPT
    df["Latitude_longitude_operator_source"] = ""
    df.loc[df['GEM_id_source_GEM'].isna(),\
        "Latitude_longitude_operator_source"] = "Chat GPT"
    df.loc[~(df['GEM_id_source_GEM'].isna()),\
        "Latitude_longitude_operator_source"] = "GEM"
    # Drop temp column from df 
    df.drop("temp",axis = 1, inplace = True)
    return df

def sort_values_if_not_null(value: str, separator=";") -> str:
    """Sort values in a string that has some values
    separated by a `separator`

    Parameters
    ----------
    value : str
        string with values inside
    separator : str, optional
        Separator to use when splitting string, by default ";"

    Returns
    -------
    str
        String with sorted values in each row

    Examples
    --------

    >>> sort_agg_values_in_column("B;C;A")
    A;B;C
    """
    if pd.notnull(value):
        return separator.join(
            list(
                sorted(value.split(separator))
            )
        )
    return value

def cancel_duplicated_rename(df):
    """
    Modifies the 'Project' column in a DataFrame to remove the _country
    set up to avoid duplication. 

    This function operates on rows where the 'Project' column matches the 
    pattern '<ProjectName>_<Country>', and the '<Country>' substring matches 
    the 'Country' column for that row. In these rows, it changes the 'Project' 
    value to '<ProjectName>', removing the duplicated country information.

    Args:
        df (pandas.DataFrame): The input DataFrame, which should contain 
            'Project' and 'Country' columns. 'Project' column values should 
            be strings in the format '<ProjectName>_<Country>'.

    Returns:
        pandas.DataFrame: The modified DataFrame. It is the same as the input 
            DataFrame, but in rows where the 'Project' and 'Country' matched 
            the pattern and condition, the 'Project' value is replaced by 
            '<ProjectName>'.
    """
    def match_pattern(row):
        if '_' in row['Project Name']:
            _, country = re.split('_', row['Project Name'])
            return country == row['Country_x']
        else:
            return False
    mask = df.apply(match_pattern, axis=1)
    df.loc[mask, 'Project Name'] = (
        df.loc[mask, 'Project Name'].apply(lambda x: x.split('_')[0]))
    return df
    


def cleanhtml(raw_html):
    """This function cleans html tag to extract only text.

    Args:
        raw_html: html code

    Returns:
        Cleaned text without html tag"""
    
    CLEAN = re.compile('<.*?>') 
    QUOTE = re.compile('\[[0-9]\]')
    cleantext = re.sub(CLEAN, '', raw_html)
    cleantext = re.sub(QUOTE, '', cleantext)
    cleantext = cleantext.strip('\n')
    return cleantext

def cleanyear(string_year):
    """This function cleans a string to extract only 4 digits years.

    Args:
        string_year: a string to clean

    Returns:
        Cleaned year with format YYYY"""
    
    YEAR = re.compile('[0-9]{4}')
    # if several years are displayed, I take the first: 2020/2021 => 2020
    string_year = re.findall(YEAR, string_year)[0]
    if string_year:
        return string_year
    else: 
        return 'No start year available'
    
def get_start_date_from_soup(soup):
    """This function extract the year field from GEM.

    Args:
        soup: html page

    Returns:
        The project start year if available. Else, string 'No start year available'."""
    
    for item in soup.find_all('li'):
        if 'start year' in str(item.text).lower() :
            start_year=str(item.text).split(':')[1]
            if start_year:
                start_year=cleanhtml(start_year)
            else:
                start_year='No start year available'
    return start_year

def get_description_from_soup(soup):
    """This function extract the year field from GEM.

    Args:
        soup: html page

    Returns:
        The project description if available. Else, string 'No description available'."""
    description = str(soup.find_all('div', attrs={'class':'mw-parser-output'})[0].find_all('p')[0])
    if description:
        description = cleanhtml(description)
    else:
        description = 'No description available'
    return description

def get_information_from_GEM(df):
    
    description = list()
    start_year = list()
    
    url = df['GEM_url_source_GEM'].tolist()
    
    for num, item in enumerate(url):
        time.sleep(0.2)
        if item in ['No informations available on GEM', 'New project']:
            description_ = 'No description available'
            start_year_  = 'No start year available'
        else:
            item = item.split(';')[0]
            r = requests.get(item)
            soup = BeautifulSoup(r.text, features="html.parser")
            try:
                description_ = get_description_from_soup(soup)
            except:
                description_ = 'No description available'
            try:
                start_year_ = get_start_date_from_soup(soup)
                if start_year_ != 'No start year available':
                    start_year_ = cleanyear(start_year_)
            except:
                start_year_   = 'No start year available'
        description.append(description_)
        start_year.append(start_year_)
        
    df['Carbon_bomb_description']=description
    df['Carbon_bomb_start_year']=start_year
    
    return df

def complete_GEM_with_ChatGPT(df):
    dtypes = {'Name':'string',
              'Start year':'string',
              'Description':'string'}								

    file_path = "./data_sources/Data_chatGPT_carbon_bombs.csv"
    gpt_df = pd.read_csv(file_path, sep=';', usecols=[0,7,8], dtype=dtypes)
    df = pd.merge(left=df, 
                  right=gpt_df, 
                  how='left', 
                  left_on='Carbon_bomb_name_source_CB',
                  right_on='Name',
                  suffixes=('', ''))
    df['Carbon_bomb_description_source'] = df.apply(lambda row: 'ChatGPT' if \
                                             (row['Carbon_bomb_description']=='No informations available on GEM'\
                                              or row['Carbon_bomb_description']=='New project') \
                                              else 'GEM', axis=1)
    df['Carbon_bomb_start_year_source'] = df.apply(lambda row: 'ChatGPT' if \
                                             row['Carbon_bomb_start_year'] == 'No start year available'\
                                             else 'GEM', axis=1)
    df['Carbon_bomb_start_year'] = df.apply(lambda row: row['Start year'] if \
                                            row['Carbon_bomb_start_year'] == 'No start year available' \
                                            else row['Carbon_bomb_start_year'], axis=1)
    df['Carbon_bomb_description'] = df.apply(lambda row: row['Description'] if \
                                             (row['Carbon_bomb_description']=='No informations available on GEM'\
                                              or \
                                              row['Carbon_bomb_description']=='New project') \
                                             else row['Carbon_bomb_description'], axis=1)
                                                    
    df=df.drop(labels=['Name', 'Start year', 'Description'], axis=1)
                                                    
    return df        

def create_carbon_bombs_table():
    """
    Creates a table of carbon bomb projects by merging coal and gas/oil tables,
    remapping columns, cleaning data, and filling missing values.

    Returns:
    --------
    pd.DataFrame:
        A pandas DataFrame with the following columns:
            - 'New_project (CB)': boolean indicating if the project is new or 
            not.
            - 'Carbon_Bomb_Name (CB)': name of the carbon bomb project.
            - 'Country (CB)': country where the carbon bomb project is located.
            - 'Potential_GtCO2 (CB)': potential emissions of the carbon bomb 
            project
              in gigatons of CO2.
            - 'Fuel_type (CB)': type of fuel used by the carbon bomb project.
            - 'GEM_ID (GEM)': identifier of the project in the Global Energy 
            Monitor
              (GEM) database.
            - 'GEM_source (GEM)': URL of the project page on the GEM database.
            - 'Latitude': geographic latitude of the project location.
            - 'Longitude': geographic longitude of the project location.
            - 'Operators (GEM)': operators of the project according to the GEM 
            database.
            - 'Parent_Company': parent company of the project, with percentage
              of ownership if available.
            - 'Multiple_unit_concerned (manual_match)': multiple unit concerned
              for coal projects only.

    Notes:
    ------
    This function relies on the following helper functions:
    - create_carbon_bombs_coal_table: creates the coal table of carbon bomb 
    projects.
    - create_carbon_bombs_gasoil_table: creates the gas/oil table of carbon 
    bomb projects.
    - compute_percentage_multi_sites: computes the percentage of ownership for
      parent companies that own several carbon bomb projects.
    - add_chat_GPT_data: adds ChatGPT data to carbon bomb projects that are not
      present in the GEM database.

    This function also uses the following mapping dictionaries to rename 
    columns:
    - name_mapping_coal: maps column names for the coal table.
    - name_mapping_gasoil: maps column names for the gas/oil table.
    - name_mapping_source: maps column names for the final merged table.
    """
    df_coal = create_carbon_bombs_coal_table()
    df_gasoil = create_carbon_bombs_gasoil_table()
    # Cancel renaming of project that have the same name but are located in 
    # different country (see function load_carbon_bomb_gasoil_database())
    # Only apply for the moment to gasoil dataframe
    df_gasoil = cancel_duplicated_rename(df_gasoil)
    # Add multiple unit concerned column to Coal Table
    df_coal["Multiple_unit_concerned"]=""
    name_mapping_coal = {
        "Project Name":"Carbon_Bomb_Name",
        "Country_x":"Country",
        "Potential emissions (GtCO2)" :"Potential_GtCO2",
        "Fuel":"Fuel_type",
        "Mine IDs":"GEM_ID",
        "GEM Wiki Page (ENG)":"GEM_source",
        "Operators":"Operators",
        "Owners":"Owners",
        "Parent Company":"Parent_Company",
    }
    name_mapping_gasoil = {
        "Project Name":"Carbon_Bomb_Name",
        "Country_x":"Country",
        "Potential emissions (GtCO2)" :"Potential_GtCO2",
        "Fuel":"Fuel_type",
        "Unit ID":"GEM_ID",
        "Wiki URL":"GEM_source",
        "Operator":"Operators",
        "Owner":"Owners",
        "Parent":"Parent_Company",
        "Unit_concerned":"Multiple_unit_concerned",
    }
    # Remap dataframe columns based on previous mapping
    df_coal.rename(columns=name_mapping_coal,inplace=True)
    df_gasoil.rename(columns=name_mapping_gasoil,inplace=True)
    # Merge dataframes
    df_carbon_bombs = pd.concat([df_coal,df_gasoil],axis=0)
    # Clean percentage in column Parent_company
    # Clean data into Parent company columns 
    df_carbon_bombs["Parent_Company"].fillna("",inplace=True)
    df_carbon_bombs["Parent_Company"] = df_carbon_bombs["Parent_Company"]\
                                        .apply(compute_percentage_multi_sites)
    # Drop column Owners (next to decision taken during GEM interview)
    df_carbon_bombs.drop("Owners", axis = 1, inplace = True)
    # Remap dataframe columns to display data source
    # Not efficient might be rework (no time for that right now)
    name_mapping_source = {
        "New_project":"New_project_source_CB",
        "Carbon_Bomb_Name":"Carbon_bomb_name_source_CB",
        "Country":"Country_source_CB",
        "Potential_GtCO2":"Potential_GtCO2_source_CB",
        "Fuel_type":"Fuel_type_source_CB",
        "GEM_ID": "GEM_id_source_GEM",
        "GEM_source": "GEM_url_source_GEM",
        "Latitude":"Latitude",
        "Longitude":"Longitude",
        "Operators":"Operators_source_GEM",
        "Parent_Company":"Parent_company_source_GEM",
        "Multiple_unit_concerned":"Multiple_unit_concerned_source_GEM",
    }
    df_carbon_bombs.rename(columns=name_mapping_source,inplace=True)
    # Add chatPGT data for Carbon Bombs that have not data extracted from GEM
    df_carbon_bombs = add_chat_GPT_data(df_carbon_bombs)
    # Reorganize column order after chatGPT 
    new_column_order = [
        "New_project_source_CB",
        "Carbon_bomb_name_source_CB",
        "Country_source_CB",
        "Potential_GtCO2_source_CB",
        "Fuel_type_source_CB",
        "GEM_id_source_GEM",
        "GEM_url_source_GEM",
        "Latitude",
        "Longitude",
        "Latitude_longitude_operator_source",
        "Operators_source_GEM",
        "Parent_company_source_GEM",
        "Multiple_unit_concerned_source_GEM",
    ]
    df_carbon_bombs = df_carbon_bombs[new_column_order]
    # Fulfill empty values for GEM_ID (GEM) and	GEM_source (GEM) columns
    # depending on project status defined in CB source
    # First need to fulfill empty values by np.NaN
    df_carbon_bombs.replace("", np.nan, inplace=True)
    # Secondly fulfill cell with New_project = True and empty GEM_ID value
    df_carbon_bombs.loc[(df_carbon_bombs["New_project_source_CB"]==True)\
                         & (df_carbon_bombs['GEM_id_source_GEM'].isna()),\
                         ["Parent_company_source_GEM"]] = "New project (100%)"
    df_carbon_bombs.loc[(df_carbon_bombs["New_project_source_CB"]==True)\
                         & (df_carbon_bombs['GEM_id_source_GEM'].isna()),\
                         ["GEM_id_source_GEM",
                          "GEM_url_source_GEM",
                          ]] = "New project"
    # Thirdly fulfill cell with New_project = False and empty GEM_ID value
    df_carbon_bombs.loc[(df_carbon_bombs["New_project_source_CB"]==False)\
                         & (df_carbon_bombs['GEM_id_source_GEM'].isna()),\
                         ["GEM_id_source_GEM","GEM_url_source_GEM"]] = (
                        "No informations available on GEM")
    # Add empty columns to receive info on Suppliers, Insurers and 
    # Subcontractors that will be generated later by ChatGPT (@Lou&Oriane)
    # No time to do it during mission time.
    df_carbon_bombs["Suppliers_source_chatGPT"] = ""
    df_carbon_bombs["Insurers_source_chatGPT"] = ""
    df_carbon_bombs["Subcontractors_source_chatGPT"] = ""

    # Post process dataset to sort columns with aggregated values
    agg_columns = [
        "GEM_id_source_GEM",
        "GEM_url_source_GEM",
        "Operators_source_GEM",
        "Parent_company_source_GEM",
        "Multiple_unit_concerned_source_GEM"
    ]
    df_carbon_bombs[agg_columns] = df_carbon_bombs[agg_columns].applymap(
                                                    sort_values_if_not_null)

    # Specific fix fort Khafji bomb that is set to Kuwait and Saudi Arabia
    # -> attribute this carbon bomb to Kuwait to insure a better repartition 
    # (Kuwait has 3 bombs and Saudi Arabia 23)
    df_carbon_bombs.loc[
        df_carbon_bombs.Carbon_bomb_name_source_CB == "Khafji", "Country_source_CB"
    ] = "Kuwait"
    df_carbon_bombs = get_information_from_GEM(df_carbon_bombs)
    df_carbon_bombs = complete_GEM_with_ChatGPT(df_carbon_bombs)

    # Add manualy EACOP carbon bomb
    df_carbon_bombs["Bomb_type"] = "Extraction"
    df_carbon_bombs = pd.concat(
        [df_carbon_bombs,  pd.DataFrame.from_dict(manual_data_to_add)]
    ).reset_index(drop=True)

    return df_carbon_bombs
    
if __name__ == '__main__':
    # Main function
    df = create_carbon_bombs_table()
    #df.to_csv("./data_cleaned/carbon_bombs_informations.csv",index=False)