#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon April 17 09:19:23 2023

@author: Nicolle Mathieu
"""
import os
import sys
import logging
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz


def load_banking_climate_chaos():
    file_path = "./data_sources/GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx"
    df = pd.read_excel(file_path, sheet_name = 'Data', engine='openpyxl')
    # Return dataframe
    return df

def load_carbon_bombs_database():
    file_path = "./data_cleaned/output_carbon_bombs.csv"
    df = pd.read_csv(file_path,sep=";")
    
    # Fill NA value to avoid bug Voir si on le garde
    #df["Parent_Company"].fillna("None",inplace=True)
    return df 

def split_column_parent_company(row):
    instance = row['Carbon_Bomb_Name']
    companies = row['Parent_Company'].split(',')
    return [{'Carbon_Bomb_Name': instance, 'Parent_Company': company} for company in companies]

def company_involvement_in_carbon_bombs():
    df_carbon_bombs = load_carbon_bombs_database()
    df_carbon_bombs_company = df_carbon_bombs.loc[:,["Carbon_Bomb_Name","Parent_Company"]]
    # Force type of column Parent_Company
    df_carbon_bombs_company["Parent_Company"] = df_carbon_bombs_company.loc[:,"Parent_Company"].astype("str")
    split_rows = df_carbon_bombs_company.apply(split_column_parent_company, axis=1)
    df_carbon_bombs_company_detailed = pd.concat([pd.DataFrame(rows) for rows in split_rows])
    df_carbon_bombs_company_detailed.reset_index(drop=True, inplace=True)
    df_carbon_bombs_company_detailed['percentage'] = df_carbon_bombs_company_detailed['Parent_Company'].str.extract(r'(\d+)%', expand=False)
    # Remove the percentage and extra spaces from Parent Company column
    df_carbon_bombs_company_detailed['Parent_Company'] = df_carbon_bombs_company_detailed['Parent_Company'].str.replace(r'\s*\(\d+%\)','', regex=True).str.strip()
    df_carbon_bombs_company_detailed.to_csv("./data_cleaned/carbon_bomb_company_participation.csv", index = False)
    return df_carbon_bombs_company_detailed

def test_link_record_BOCC_CB():
    # Extract unique values from bocc
    df_bocc = load_banking_climate_chaos()
    list_bocc = df_bocc["Company"].unique()
    # Extract unique values from carbon bombs details
    df_cb = company_involvement_in_carbon_bombs()
    list_cb = df_cb["Parent_Company"].unique()
    print(len(list_cb))
    df_list_bocc = pd.DataFrame()
    df_list_bocc["Company BOCC"] = list_bocc
    for elt in list_cb:
        df_list_bocc['fuzzy_score'] = df_list_bocc["Company BOCC"].apply(lambda x: fuzz.ratio(x, elt))
        print(f"Initial string was :{elt}")
        print(df_list_bocc.sort_values("fuzzy_score",ascending = False).head())

def link_record_BOCC_CB():
    df_cb = company_involvement_in_carbon_bombs()
    df_bocc = load_banking_climate_chaos()
    indexer = recordlinkage.Index()
    indexer.block(left_on='Parent_Company', right_on='Company')
    candidate_pairs = indexer.index(df_cb, df_bocc)
    comparer = recordlinkage.Compare()
    comparer.string('Parent_Company', 'Company', method='levenshtein', label='Parent_Company_vs_Company')
    #comparer.string('column2_df1', 'column2_df2', method='jaro', label='column2')   
    comparison_results = comparer.compute(candidate_pairs, df_cb, df_bocc)
    # Define a threshold for the classification
    threshold = 0.75
    # Classify the pairs based on the threshold
    classified_pairs = comparison_results[comparison_results.sum(axis=1) > threshold]
    print(classified_pairs)
    # Get the indices of matched records
    #matched_indices = classified_pairs.index
    # Merge the matched records
    #matched_records = df_cb.loc[matched_indices.get_level_values(0)].reset_index(drop=True).join(
    #    df_bocc.loc[matched_indices.get_level_values(1)].reset_index(drop=True), lsuffix='_df1', rsuffix='_df2'
    #)
    #matched_records.to_csv("matched_records.csv", index=False)

if __name__ == '__main__':
    # Main function
    company_involvement_in_carbon_bombs()
    #link_record_BOCC_CB()
    #test_link_record_BOCC_CB()