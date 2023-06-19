#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script deals with the list of countries extracted from the carbon bomb
file previously created and UNSD databases website to build the csv file
saved as country_informations.csv

Examples:
To use this script, simply run it from the command line:
$ python main.py
"""
import os
import sys
import re
import pandas as pd


def load_CB_countries_database():
    """
    Loads the country column of the carbon bomb database.

    Returns:
    --------
    pd.DataFrame:
        A pandas DataFrame with the following columns:
            - 'Country (CB)': country where carbon bomb projects are located.

    Notes:
    ------
    The data file is expected to be in the following path:
    "./data_cleaned/carbon_bombs_informations.csv".
    """
    file_path = "./data_cleaned/carbon_bombs_informations.csv"
    cols = ["Country_source_CB"]
    dtypes = {"Country": "string"}
    df = pd.read_csv(file_path, usecols=cols, dtype=dtypes)
    # Split mulitple countries, separated by '-', in multiple countries
    df = df.assign(Country_source_CB=df.Country_source_CB.str.split("-")) \
           .explode("Country_source_CB")
    # Remove duplicates
    df.drop_duplicates(inplace=True)
    # Sort by increasing values
    df.sort_values(by="Country_source_CB", ascending=True, inplace=True)
    return df


def create_country_table():
    """
    Creates the table of countries extracted from the
    carbon_bombs_informations file.

    Args:
    -----
        None.

    Returns:
    --------
        pandas.DataFrame: A pandas DataFrame listing unique countries from the
        carbon_bombs_informations file.

    Raises:
    -------
        None.

    Notes:
    ------
        None.
    """
    # Loads Dataframe listing unique countries with identified carbon bombs
    df_CB_countries = load_CB_countries_database()
    # Loads Dataframe from different UNSD sources (next step)

    return df_CB_countries


if __name__ == "__main__":
    # Main function
    df_countries = create_country_table()
    # df_countries.to_csv("./data_cleaned/country_informations.csv",index=False)
