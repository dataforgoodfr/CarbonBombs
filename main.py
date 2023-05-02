#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script create all files present into ./data_cleaned
Examples:
To use this script, simply run it from the command line:
$ python main.py
"""


from carbon_bomb import create_carbon_bombs_table
from connexion import company_involvement_in_carbon_bombs,filter_BOCC_database
from scrapper import main_scrapping_function, scrapping_company_location



if __name__ == '__main__':
    # Step1 : Carbon bombs table
    df = create_carbon_bombs_table()
    df.to_csv("./data_cleaned/carbon_bombs_informations.csv",index=False)
    # Step2 : Connexion between CarbonBombs and Company
    df = company_involvement_in_carbon_bombs
    df.to_csv("./data_cleaned/connexion_carbonbombs_company.csv",index = False)
    # Step3 : Scrape bank informations
    URL = 'https://www.banktrack.org/banks'
    df = main_scrapping_function(URL)
    df.to_csv("./data_cleaned/bank_informations.csv",
              encoding='utf-8-sig',index = False)
    # Step4 : Scrape company informations
    df = scrapping_company_location()
    df.to_csv("./data_cleaned/company_informations.csv",
              encoding='utf-8-sig',index = False)
    # Step5 : Connexion between Company and Bank
    df = filter_BOCC_database()
    df.to_csv("./data_cleaned/connexion_bank_company.csv",
              encoding='utf-8-sig',index = False)