#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script create all files present into ./data_cleaned
Examples:
To use this script, simply run it from the command line:
$ python main.py
"""

import os
from carbon_bomb import create_carbon_bombs_table
from connexion import main_connexion_function,filter_BOCC_database
from scrapper import main_scrapping_function, scrapping_company_location



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
        df = main_scrapping_function(URL)
        df.to_csv("./data_cleaned/bank_informations.csv",
                encoding='utf-8-sig',index = False)
        print("bank_informations.csv : done\n")
        # Step4 : Scrape company informations
        df = scrapping_company_location()
        df.to_csv("./data_cleaned/company_informations.csv",
                encoding='utf-8-sig',index = False)
        print("company_informations.csv : done\n")
    else:
        print("Create your own Google MAPS API KEY to scrap GPS coordinates.\n"
              "Skipping Step3 and Step4\n")