#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon April 17 09:19:23 2023

@author: Nicolle Mathieu
"""

import re
import pandas as pd
from fuzzywuzzy import fuzz

def load_banking_climate_chaos():
    """
    Loads the banking on climate chaos data from an Excel file located at the 
    specified file path. Returns a pandas DataFrame containing the data.
    
    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the banking on climate chaos data.
        
    Notes
    -----
    - This function requires the pandas and openpyxl libraries to be installed.
    - The Excel file containing the data must be available at the specified 
      file path.
    - The sheet name containing the data must be 'Data'.
    """
    file_path = ("./data_sources/GROUP-Fossil_Fuel_Financing_by_Company_Banking"
                 "_on_Climate_Chaos_2023.xlsx")
    df = pd.read_excel(file_path, sheet_name = 'Data', engine='openpyxl')
    # Return dataframe
    return df

def load_carbon_bombs_database():
    """
    Loads the carbon bombs database from a CSV file located at the specified 
    file path. Returns a pandas DataFrame containing the data.
    
    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the carbon bombs database.
        
    Notes
    -----
    - This function requires the pandas library to be installed.
    - The CSV file containing the data must be available at the specified 
      file path.
    - The CSV file must be separated by a semicolon (;).
    """
    file_path = "./data_cleaned/output_carbon_bombs.csv"
    df = pd.read_csv(file_path,sep=";")
    return df 

def split_column_parent_company(row):
    """
    Splits the Parent Company column in the given row into multiple rows, each 
    row containing only one Parent Company value. Returns a list of 
    dictionaries, with each dictionary containing the Carbon Bomb Name and 
    Parent Company values.
    
    Parameters
    ----------
    row : pandas.Series
        A pandas Series containing the Carbon Bomb Name and Parent Company 
        values.
        
    Returns
    -------
    List[Dict[str, Any]]
        A list of dictionaries containing the Carbon Bomb Name and Parent 
        Company values after splitting the Parent Company column.
        
    Notes
    -----
    - This function requires the pandas library to be installed.
    """
    instance = row['Carbon_Bomb_Name']
    companies = row['Parent_Company'].split(',')
    carbon_bomb_list_company = ([{'Carbon_Bomb_Name': instance,
                                 'Parent_Company': company} 
                                 for company in companies])
    return carbon_bomb_list_company

def company_involvement_in_carbon_bombs():
    """
    Loads the carbon bombs database, extracts the Parent Company column and 
    splits it into multiple columns to provide detailed company participation 
    in carbon bombs. Removes the percentage and extra spaces from Parent 
    Company column and saves the result in a csv file. 
    
    Returns
    -------
    pandas.DataFrame
        A DataFrame containing detailed company participation in carbon bombs.
        
    Notes
    -----
    - This function requires the pandas library to be installed.
    - The carbon bombs database must be available in the current working 
    directory.
    """
    df_carbon_bombs = load_carbon_bombs_database()
    df_carbon_bombs_company = df_carbon_bombs.loc[:,["Carbon_Bomb_Name",
                                                     "Parent_Company"]]
    # Force type of column Parent_Company
    df_carbon_bombs_company["Parent_Company"] = (df_carbon_bombs_company.
                                                 loc[:,"Parent_Company"].
                                                 astype("str"))
    split_rows = df_carbon_bombs_company.apply(split_column_parent_company,
                                               axis=1)
    df_carbon_bombs_company_detailed = pd.concat(
        [pd.DataFrame(rows) for rows in split_rows])
    df_carbon_bombs_company_detailed.reset_index(drop=True, inplace=True)
    df_carbon_bombs_company_detailed['percentage'] = \
        df_carbon_bombs_company_detailed['Parent_Company']\
            .str.extract(r'(\d+)%', expand=False)
    # Remove the percentage and extra spaces from Parent Company column
    df_carbon_bombs_company_detailed['Parent_Company'] = \
        df_carbon_bombs_company_detailed['Parent_Company']\
            .str.replace(r'\s*\(\d+%\)','', regex=True).str.strip()
    df_carbon_bombs_company_detailed.to_csv(
        "./data_cleaned/carbon_bomb_company_participation.csv", index = False)
    return df_carbon_bombs_company_detailed

def clean(text):
    """
    Cleans the given text by removing certain words and characters.
    Returns the cleaned text in lowercase form.
    
    Parameters
    ----------
    text : str
        The text to be cleaned.
        
    Returns
    -------
    str
        The cleaned text in lowercase form.
        
    Notes
    -----
    - This function requires the re library to be installed.
    - The list of banned words can be modified as per requirement.
    """
    # Define a list of word to be cleaned before comparing company names
    list_ban_word = [
        "Co",
        "Ltd",
        "Limited",
        "Corp",
        "Inc",
        "Resources",
        "Group",
        "Corporation",
        "SA",
        "Holding",
        "Company",
        "Industry",
        "industry",
        "Investment",
        "investment",
        "Tbk",
        "PT",
        "Persero",
        "(CNPC)"
        "&",
    ]
    split_text = text.split()
    split_text_cleaned = [elt for elt in split_text if elt not in list_ban_word]
    text_cleaned = "".join(split_text_cleaned)
    text_cleaned = text_cleaned.lower()
    if "%" in text_cleaned:
        text_cleaned = re.sub(r'\(\s*\d+(\.\d+)?%\s*\)', '', text_cleaned)
    return text_cleaned


def test_link_record_BOCC_CB():
    # Define threshold value
    threshold = 90
    # Extract unique values from bocc
    df_bocc = load_banking_climate_chaos()
    list_bocc = df_bocc["Company"].unique()
    # Extract unique values from carbon bombs details
    df_cb = company_involvement_in_carbon_bombs()
    list_cb = df_cb["Parent_Company"].unique()

    df_list_bocc = pd.DataFrame()
    df_list_bocc["Company BOCC"] = list_bocc
    df_list_bocc["Company BOCC_cleaned"]=df_list_bocc["Company BOCC"].apply(clean)
    number = len(list_cb)
    print(f"Number of company to match from GEM DB = {number}")
    
    dict_resume = dict()
    for elt in list_cb:
        elt_cleaned = clean(elt)
        df_list_bocc['fuzzy_score'] = df_list_bocc["Company BOCC_cleaned"].apply(lambda x: fuzz.ratio(x, elt_cleaned))
        max_fuzz = df_list_bocc['fuzzy_score'].max()
        if max_fuzz < threshold:
            print(f"Initial string was :{elt}")
            print(f"Cleaned string was :{elt_cleaned}")
            print(df_list_bocc.sort_values("fuzzy_score",ascending = False).head())
        
        #if max_fuzz > threshold:
        #    dict_resume[elt] = df_list_bocc.loc[df_list_bocc['fuzzy_score']==max_fuzz,"Company BOCC"]
    
    #print(f"Number pos match = {len(dict_resume.keys())}")

if __name__ == '__main__':
    # Main function
    company_involvement_in_carbon_bombs()
