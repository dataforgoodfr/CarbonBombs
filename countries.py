#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script deals with the list of countries extracted from the carbon bomb
file previously created and with statistical tables from the UNSD databases
website (https://data.un.org/) to build the csv file saved as
country_informations.csv.
To access all datamarts, they are available from this link:
http://data.un.org/Explorer.aspx.

Examples:
To use this script, simply run it from the command line:
$ python main.py
"""
import os
import sys
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path


def scrapping_undata_file_url(url):
    """
    Scrapes the main page of the UN data website (https://data.un.org/)
    and extracts URLs of csv files (statistical tables).
    Useful to access directly (e.g. from a specific interface) to the list of
    all csv files and to download one or several files in one go.
    To access all datamarts, they are available from this link:
    http://data.un.org/Explorer.aspx.

    The list of tables provided by the UNdata website is decomposed into:
    - categories ("Population", "National accounts", ...)
    - subcategories ("Population, surface area and density", ...)
    - PDF and CSV files, and sometimes the last update date

    Args:
        url (str): The URL of the main page to be scraped.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns:
        - category (str): Name of the category.
        - subcategory (str): Name of the subcategory.
        - csv_url (str): URL pointing to corresponding CSV file.

    Raises:
        None: However, if the HTTP request fails, it prints an error message
              and terminates the script.
    """
    # Send an HTTP request to the target URL (timeout is set to 5 seconds)
    response = requests.get(url, timeout=5)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code != 200:
        print(f"Failed to fetch the page. HTTP status: {response.status_code}"
              "Please try again later")
        sys.exit()

    # Parse the HTML content using BeautifulSoup and focus on the part
    # containing relevant information
    soup = BeautifulSoup(response.content, 'html.parser')
    subsoup = soup.find("div", class_="DataSourceList")

    # Initiate dataframe to concatenate info
    df_undata = pd.DataFrame()

    nb_category = len(subsoup.find_all("p"))
    for category_i in range(nb_category):
        # Search the category
        category = subsoup.find_all("p")[category_i].text

        subsoup_category = subsoup.find_all("ul")[category_i]
        nb_subcategory = len(
            subsoup_category.find_all("li", class_="NoBullets"))

        # For each category...
        for subcategory_j in range(nb_subcategory):
            # ... search the subcategory ...
            subcategory = subsoup_category.find_all("li")[subcategory_j*2] \
                                          .text

            # ... and search the 2nd URL (URL of the CSV)
            subsoup_subcategory = subsoup_category.find_all("li")[
                subcategory_j*2+1]
            csv_url = subsoup_subcategory.find_all("a")[1].get('href')
            # Rebuilds the full URL
            csv_url = url + csv_url

            df_subcategory = pd.DataFrame()
            df_subcategory["category"] = [category]
            df_subcategory["subcategory"] = [subcategory]
            df_subcategory["csv_url"] = [csv_url]
            df_undata = pd.concat([df_undata, df_subcategory])
    return df_undata


def saving_file(url, folder_path):
    """
    Download and store a csv file located at a specific url.

    Args:
        url (str): The URL of the CSV file.
        folder_path (str): The destination path to store the csv file.

    Returns:
        CSV file with name starting with 'undata_' and located in the defined
        folder.

    Raises:
        None: However, if the HTTP request fails, it prints an error message
              and terminates the script.
    """
    # Send an HTTP request to the target URL (timeout is set to 5 seconds)
    myfile = requests.get(url, timeout=5)

    # Check if the request was successful (HTTP status code 200)
    if myfile.status_code != 200:
        print(f"Failed to fetch the page. HTTP status: {myfile.status_code} "
              "Please try again later")
        sys.exit()

    filename = os.path.basename(url)
    path = folder_path + 'undata_' + filename
    url_content = myfile.content
    # write the contents to a csv file
    csv_file = open(path, 'wb')
    csv_file.write(url_content)
    # close the file
    csv_file.close()


def scrapping_undata(file_paths):
    """
    Load all csv files downloaded from the UN data website.

    Args:
        file_paths (list of str): The list of csv file paths.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns:
        - Region_Country_Area_ID (int): A numerical value associated to the
          region/country/area.
        - Region_Country_Area_name (str): The concept refers to the country,
          geographical or political group of countries or regions within a
          country, as defined by the UN data website. The concept is subject
          to a variety of hierarchies, as countries comprise territorial
          entities that are States (as understood by international law and
          practice), regions and other territorial entities that are not
          States (such as Hong Kong) but for which statistical data are
          produced internationally on a separate and independent basis.
        - Year (int): Reference period of the statistical series.
        - Category (str): Group of statistical tables related to the same
          topic, displayed in the main page (http://data.un.org/Default.aspx).
          E.g. 'Population', 'National accounts', ...
        - Series (str): Statistical series.
        - Value (float): A unit of data for which the definition,
          identification, representation, and permissible values are specified
          by means of a set of attributes, as defined by the UN data website.
        - Footnotes (str): A note or other text located at the bottom of a
          page of text, manuscript, book or statistical tabulation that
          provides comment on or cites a reference for a designated part of
          the text or table.
        - Source (str): A specific data set, metadata set, database or
          metadata repository from where data or metadata are available.

    Raises:
        None: However, if the HTTP request fails, it prints an error message
              and terminates the script.
    """
    # Initiate Dataframe that will gather informations
    columns_dataframe = [
        "Region_Country_Area_ID",
        "Region_Country_Area_name",
        "Year",
        "Category",
        "Series",
        "Value",
        "Footnotes",
        "Source"
    ]
    df_output = pd.DataFrame()
    # Load csv files
    for file in file_paths:
        df = pd.read_csv(file, sep=",", header=1)
        # Retrieve the category in the first row of the csv file
        df_firstrow = pd.read_csv(file, sep=",", header=None, nrows=1)
        df.insert(3, 'Category', df_firstrow[1].values[0])
        # Concat dataframes
        df_output = pd.concat([df_output, df], ignore_index=True)
    df_output.columns = columns_dataframe
    return df_output


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


def create_country_table(undata_folder_path):
    """
    Creates the table of countries extracted from the
    carbon_bombs_informations file.

    Args:
    -----
        undata_folder_path (str): The folder path where csv files downloaded
        from the UN data website are stored.

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
    # Load Dataframe listing unique countries with identified carbon bombs
    df_CB_countries = load_CB_countries_database()

    # Load Dataframe from different UNSD sources
    # Change temporarily the working directory, to load 'undata_*.csv' files
    # 1- Save the current working directory
    pwd = os.getcwd()
    # 2- Changes the current working directory
    os.chdir(os.path.dirname(undata_folder_path))
    files = 'undata_*.csv'
    file_paths = Path.cwd().glob(files)
    # 3- Back to the initial working directory
    os.chdir(pwd)
    '''
    Alternative, without changing the working directory:
        file1 = r'undata_SYB65_1_202209_Population, Surface Area and Density.csv'
        file2 = r'undata_SYB65_230_202209_GDP and GDP Per Capita.csv'
        file3 = r'undata_SYB65_310_202209_Carbon Dioxide Emission Estimates.csv'
        file_paths = (destination_path+file1, destination_path+file2, destination_path+file3)
    '''
    df_undata = scrapping_undata(file_paths)

    # Map country names to get uniform country names between all files.
    # Rem: This mapping uses a dictionary, working for the current perimeter.
    #      If updates occur, new countries might need to be included to this
    #      dictionary.
    mapping_countries = {"Iran (Islamic Republic of)": "Iran",
                         "Dem. People's Rep. Korea": "North Korea",
                         "Russian Federation": "Russia",
                         "Syrian Arab Republic": "Syria",
                         "United Rep. of Tanzania": "Tanzania",
                         "United States of America": "United States",
                         "Venezuela (Boliv. Rep. of)": "Venezuela"
                         }
    df_undata['Region_Country_Area_name'].replace(
        mapping_countries, inplace=True)

    # Merge the 2 dataframes on country name column.
    df_countries = pd.merge(df_CB_countries, df_undata,
                            left_on='Country_source_CB',
                            right_on='Region_Country_Area_name',
                            how='inner',
                            sort=True)
    # Last cleaning: 2 columns removed
    columns_dataframe = [
        "Country_source_CB",
        "Year",
        "Category",
        "Series",
        "Value",
        "Footnotes",
        "Source"
    ]
    df_countries = df_countries.reindex(columns_dataframe, axis='columns')

    # Return cleaned dataframe
    return df_countries


if __name__ == "__main__":
    url_undata = "https://data.un.org/"
    destination_path = './data_sources/'
    # Step 1 - Display available statistical datasets (optional)
    df_undata_file = scrapping_undata_file_url(url_undata)
    # Step 2 - Download and save UN data CSV files (optional)
    # Subcategories required for dataviz
    subcategory1 = "Population, surface area and density"
    subcategory2 = "GDP and GDP per capita"
    subcategory3 = "CO2 emissions estimates"
    subcategories = (subcategory1, subcategory2, subcategory3)
    for subcat in subcategories:
        url = df_undata_file.query('subcategory == @subcat')['csv_url'][0]
        saving_file(url, destination_path)
    # Step 3 - Main function
    df_countries = create_country_table(destination_path)
    # df_countries.to_csv("./data_cleaned/country_informations.csv",index=False)
