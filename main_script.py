#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script create all files present into ./data_cleaned
Examples:
To use this script, simply run it from the command line:
$ python main.py
"""

import os
import pandas as pd
from carbon_bomb import create_carbon_bombs_table
from connexion import main_connexion_function
from scrapper import main_scrapping_function, scrapping_company_location

CONCAT_DATA_FILE_PATH = "data_cleaned/carbon_bombs_all_datasets.xlsx"

def concat_dataframe_into_excel(fpath: str):
    """Generate an Excel file into `fpath` that takes all
    datasets path and put it into different sheets

    Parameters
    ----------
    fpath : str
        path to excel file
    """

    # Remove excel file if exists
    if os.path.exists(fpath):
        os.remove(fpath)
    
    cleaned_datasets_fpaths = [
        "data_cleaned/bank_informations.csv",
        "data_cleaned/carbon_bombs_informations.csv",
        "data_cleaned/company_informations.csv",
        "data_cleaned/connexion_bank_company.csv",
        "data_cleaned/connexion_carbonbombs_company.csv",
    ]
    # init writer to create excel file with
    writer = pd.ExcelWriter(fpath, engine="xlsxwriter", options={'strings_to_urls': False})

    for csv_fpath in cleaned_datasets_fpaths:
        if os.path.isfile(csv_fpath):
            df = pd.read_csv(csv_fpath)
            # Retrieve dataset name to set it as sheet name
            sheet_name = csv_fpath.split("/")[-1][:-4]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    writer.close()

if __name__ == '__main__':
    # Step1 : Carbon bombs table
    df = create_carbon_bombs_table()
    df.to_csv("./data_cleaned/carbon_bombs_informations.csv",index=False)
    print("carbon_bombs_informations.csv : done\n")

    # Step2 : Connexion between CarbonBombs and Company & Company and Bank
    df_cb,df_bank = main_connexion_function()
    df_cb.to_csv("./data_cleaned/connexion_carbonbombs_company.csv",
                 index = False)
    print("connexion_carbonbombs_company.csv : done\n")
    
    df_bank.to_csv("./data_cleaned/connexion_bank_company.csv",
              encoding='utf-8-sig',index = False)
    print("connexion_bank_company.csv : done\n")


    if os.path.isfile("./credentials.py"):
        # Step3 : Scrape bank informations
        URL = 'https://www.banktrack.org/banks'
        df_bank_info = main_scrapping_function(URL)
        df_bank_info.to_csv("./data_cleaned/bank_informations.csv",
                encoding='utf-8-sig',index = False)
        print("bank_informations.csv : done\n")
        
        # Step4 : Scrape company informations
        df_comp_info = scrapping_company_location()
        df_comp_info.to_csv("./data_cleaned/company_informations.csv",
                encoding='utf-8-sig',index = False)
        print("company_informations.csv : done\n")

    else:
        print("Create your own Google MAPS API KEY to scrap GPS coordinates.\n"
              "Skipping Step3 and Step4\n")
        
    concat_dataframe_into_excel(CONCAT_DATA_FILE_PATH)
