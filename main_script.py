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
from scrapper import main_scrapping_function, scrapping_company_location, saving_logos
from countries import create_country_table
from graph_database import purge_database, update_neo4j

CONCAT_DATA_FILE_PATH = "data_cleaned/carbon_bombs_all_datasets.xlsx"
DATA_SOURCES_PATH = './data_sources/'


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
        "data_cleaned/country_informations.csv",
    ]
    # init writer to create excel file with
    writer = pd.ExcelWriter(fpath,
                            engine="xlsxwriter",
                            engine_kwargs={
                                'options': {'strings_to_urls': False}
                                }
                            )

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
    df.to_csv("./data_cleaned/carbon_bombs_informations.csv",
              encoding='utf-8-sig',index=False)
    print("carbon_bombs_informations.csv : done\n")
    
    # Step2 : Connexion between CarbonBombs and Company & Company and Bank
    df_cb, df_bank = main_connexion_function()
    df_cb.to_csv("./data_cleaned/connexion_carbonbombs_company.csv",
                 encoding='utf-8-sig',index=False)
    print("connexion_carbonbombs_company.csv : done\n")
    df_bank.to_csv("./data_cleaned/connexion_bank_company.csv",
                   encoding='utf-8-sig', index=False)
    print("connexion_bank_company.csv : done\n")
    ### Once we made the connexion we can drop column temp_connexion_parent
    ### and temp connexion_owner from carbon_bombs database
    ### Will be rework later
    df = pd.read_csv("./data/carbon_bombs_informations.csv")
    df.drop(['temp_connexion_parent', 'temp_connexion_owner'],
            axis=1, inplace=True)
    df.to_csv("./data_cleaned/carbon_bombs_informations.csv",
              encoding='utf-8-sig',index=False)
    if os.path.isfile("./credentials.py"):
        # Step3 : Scrape bank informations
        URL = 'https://www.banktrack.org/banks'
        df_bank_info = main_scrapping_function(URL)
        # Filter Bank information based on unique values in BOCC
        bank_list = df_bank["Bank"].unique()
        df_bank_info = df_bank_info[df_bank_info['Bank Name'].isin(bank_list)]
        df_bank_info.to_csv("./data_cleaned/bank_informations.csv",
                            encoding='utf-8-sig', index=False)
        print("bank_informations.csv : done\n")

        # Step4 : Scrape company informations
        df_comp_info = scrapping_company_location()
        df_comp_info.to_csv("./data_cleaned/company_informations.csv",
                            encoding='utf-8-sig', index=False)
        print("company_informations.csv : done\n")
    else:
        print("Create your own Google MAPS API KEY to scrap GPS coordinates.\n"
              "Skipping Step3 and Step4\n")

#    # Step5 : Save companies' and banks' logos
#    # Include this code if logos need to be downloaded
#    csv_file_company = "./data_sources/company_url.csv"
#    company_url_field = ['Logo_OfficialWebsite',
#                         'Logo_Wikipedia_Large',
#                         'Logo_OtherSource'
#                        ]
#    company_name_field = 'Company_name'
#    company_logo_dest_folder = "./img/logo_company/"
#    saving_logos(csv_file_company, company_name_field, company_url_field, 
#                 company_logo_dest_folder)
#
#    csv_file_bank = "./data_cleaned/bank_informations.csv"
#    bank_url_field = 'Bank logo'
#    bank_name_field = 'Bank Name'
#    bank_logo_dest_folder = "./img/logo_bank/"
#    saving_logos(csv_file_bank, bank_name_field, bank_url_field, 
#                 bank_logo_dest_folder, file_extension='jpeg')

    # Step5 : Scrap countries informations
    df_countries = create_country_table(DATA_SOURCES_PATH)
    df_countries.to_csv("./data_cleaned/country_informations.csv",
                        encoding='utf-8-sig', index=False)
    print("country_informations.csv : done\n")
    # Step6 : Concatenate all csv files into a main Excel file
    concat_dataframe_into_excel(CONCAT_DATA_FILE_PATH)
    # Step7 : Update data into Neo4j folder and database
    #purge_database()
    #update_neo4j()
    
