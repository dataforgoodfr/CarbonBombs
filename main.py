#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon April 17 09:19:23 2023

@author: Nicolle Mathieu
"""
import os
import sys
import re
import pandas as pd
import numpy as np
import warnings
from fuzzywuzzy import fuzz
from manual_match import manual_match_coal
from manual_match import manual_match_gasoil

    
def load_urgewald_database_GOGEL(year, type = "UPSTREAM"):
    """
    Load the UrgeWald database from the GOGEL report for a given year and type.

    Args:
    year (int): The year of the report to load, either 2021 or 2022.
    type (str, optional): The type of data to load, either "UPSTREAM" or 
    "MIDSTREAM". Default is "UPSTREAM".

    Returns:
    pandas.DataFrame: A dataframe containing the data from the specified report.

    Raises:
    FileNotFoundError: If the specified file path cannot be found.
    ValueError: If an invalid year or type is specified.
    """
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
    """
    Load the UrgeWald database from the GCEL report.

    Returns:
    pandas.DataFrame: A dataframe containing the data from the report.

    Raises:
    FileNotFoundError: If the specified file path cannot be found.
    """
    file_path = "./data_sources/urgewald_GCEL_2022_download_0.xlsx"
    df = pd.read_excel(file_path, sheet_name = 'Output', engine='openpyxl')
    # Return dataframe
    return df
    
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
    return df
    
def load_coal_mine_gem_database():
    file_path = "./data_sources/Global-Coal-Mine-Tracker-April-2023.xlsx"
    df = pd.read_excel(file_path, sheet_name='Global Coal Mine Tracker',
                       engine='openpyxl')
    return df

def load_gasoil_mine_gem_database():
    file_path = "./data_sources/Global-Oil-and-Gas-Extraction"\
                "-Tracker-Feb-2023.xlsx"
    # Line that must be passed before in order to avoid useless warning
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    df = pd.read_excel(file_path, sheet_name='Main data',engine='openpyxl')
    return df

def load_d4g_database():
    file_path = "./data_sources/Carbon_bomb_personalDB.csv"
    df = pd.read_csv(file_path,sep=";")
    return df

def create_carbon_bombs_gasoil_table():
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
    df_gasoil_gem_mines = df_gasoil_gem_mines[GEM_usefull_columns]
    # Only retain perfect match on Project Name between GEM & CB with a 
    # country verification
    df_gasoil_carbon_bombs["temp"] = (df_gasoil_carbon_bombs["Project Name"]+
                                      "/"+df_gasoil_carbon_bombs["Country"])
    df_gasoil_gem_mines["temp"] = (df_gasoil_gem_mines["Unit name"]+"/"+
                                   df_gasoil_gem_mines["Country"])
    list_gasoil_carbon_bomb = list(df_gasoil_carbon_bombs["temp"])
    df_gasoil_gem_perfect_match = (df_gasoil_gem_mines
            [df_gasoil_gem_mines['temp'].isin(list_gasoil_carbon_bomb)])
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
    # WARNING raised on perfect match to be solved later
    df_gasoil_gem_perfect_match["Unit_concerned"]=""
    df_gasoil_gem_manual_match["Unit_concerned"]=""
    df_gasoil_gem_perfect_match.drop("temp",axis=1,inplace=True)
    df_gasoil_gem_manual_match.drop("temp",axis=1,inplace=True)
    df_gasoil_carbon_bombs.drop("temp",axis=1,inplace=True)
    df_gasoil_gem_multi_match.drop("CarbonBombName",axis=1,inplace=True)
    # Concat dataframes from match perfect/multi/manual
    """
    df_gasoil_gem_manual_match.to_csv("./working_documents/manual.csv")
    df_gasoil_gem_perfect_match.to_csv("./working_documents/perfect.csv")
    df_gasoil_gem_multi_match.to_csv("./working_documents/multi.csv")
    """
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
    df_gasoil_merge.to_csv("./data_cleaned/output_gasoil_table.csv",index=False)
    return df_gasoil_merge

def ponderate_percentage(dict_percentage):
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
    # With raw_line content 2 possibilities : Percentage are indicated or not
    if "%" in raw_line:
        # Case where percentage are indicated
        companies = re.findall(r"([\w\s\.\-]+)\s\(", raw_line)
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
    df_coal_gem_mines_perfect_match = df_coal_gem_mines[\
        df_coal_gem_mines['temp'].isin(list_coal_carbon_bomb)]
    # Check how many duplicate in the filtered GEM database and handle them
    duplicates = df_coal_gem_mines_perfect_match\
        [df_coal_gem_mines_perfect_match.duplicated(subset=['Mine Name'],
                                                   keep=False)]
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
    df_coal_merge.to_csv("./data_cleaned/output_coal_table.csv",index=False)
    return df_coal_merge

def find_matching_name_for_GEM_coal(name, country, df_gem):
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

def create_carbon_bombs_table():
    df_coal = create_carbon_bombs_coal_table()
    df_gasoil = create_carbon_bombs_gasoil_table()
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
    df_carbon_bombs.drop("Owners", axis = 1)
    df_carbon_bombs.to_csv("./data_cleaned/output_carbon_bombs.csv",index=False)
    return df_carbon_bombs
    
if __name__ == '__main__':
    # Main function
    df = create_carbon_bombs_table()
    print(df.shape)