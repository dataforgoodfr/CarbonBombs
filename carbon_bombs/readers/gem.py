"""
Functions to read the GEM datasets

Source for the datasets:
- Global-Coal-Mine-Tracker-April-2023.xlsx : The Global Coal Mine Tracker (GCMT) is a worldwide
  dataset of coal mines and proposed projects. The tracker provides asset-level details on ownership
  structure, development stage and status, coal type, production, workforce size, reserves and resources,
  methane emissions, geolocation, and over 30 other categories. This data will not be tracked under
  this repository as it Distributed under a Creative Commons Attribution 4.0 International License.
  It can be freely download through this page:
  https://globalenergymonitor.org/projects/global-coal-mine-tracker/download-data/
- Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx : The Global Oil and Gas Extraction Tracker (GOGET)
  is a global dataset of oil and gas resources and their development. GOGET includes information on discovered,
  in-development, and operating oil and gas units worldwide, including both conventional and unconventional
  assets. This data will not be tracked under this repository as it Distributed under a Creative Commons
  Attribution 4.0 International License. It can be freely download through this page:
  https://globalenergymonitor.org/projects/global-oil-gas-extraction-tracker/
"""
import re
import warnings

import pandas as pd
import requests
from bs4 import BeautifulSoup

from carbon_bombs.conf import FPATH_SRC_GEM_COAL
from carbon_bombs.conf import FPATH_SRC_GEM_GASOIL


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
    df = pd.read_excel(
        FPATH_SRC_GEM_COAL, sheet_name="Global Coal Mine Tracker", engine="openpyxl"
    )
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
    # Line that must be passed before in order to avoid useless warning
    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
    df = pd.read_excel(FPATH_SRC_GEM_GASOIL, sheet_name="Main data", engine="openpyxl")

    return df


def clean_html(raw_html):
    """This function cleans html tag to extract only text.

    Args:
        raw_html: html code

    Returns:
        Cleaned text without html tag"""

    CLEAN = re.compile("<.*?>")
    QUOTE = re.compile("\[[0-9]\]")
    clean_text = re.sub(CLEAN, "", raw_html)
    clean_text = re.sub(QUOTE, "", clean_text)
    clean_text = clean_text.strip("\n")

    return clean_text


def clean_year(string_year):
    """This function cleans a string to extract only 4 digits years.

    Args:
        string_year: a string to clean

    Returns:
        Cleaned year with format YYYY"""

    YEAR = re.compile("[0-9]{4}")
    # if several years are displayed, I take the first: 2020/2021 => 2020
    string_year = re.findall(YEAR, string_year)[0]
    if string_year:
        return string_year
    else:
        return "No start year available"


def get_start_date_from_soup(soup):
    """This function extract the year field from GEM.

    Args:
        soup: html page

    Returns:
        The project start year if available. Else, string 'No start year available'."""

    for item in soup.find_all("li"):
        if "start year" in str(item.text).lower():
            start_year = str(item.text).split(":")[1]
            if start_year:
                start_year = clean_html(start_year)
            else:
                start_year = "No start year available"

    return start_year


def get_description_from_soup(soup):
    """This function extract the year field from GEM.

    Args:
        soup: html page

    Returns:
        The project description if available. Else, string 'No description available'."""
    description = str(
        soup.find_all("div", attrs={"class": "mw-parser-output"})[0].find_all("p")[0]
    )
    if description:
        description = clean_html(description)
    else:
        description = "No description available"
    return description


def get_gem_wiki_details(item):
    r = requests.get(item)
    soup = BeautifulSoup(r.text, features="html.parser")
    try:
        description = get_description_from_soup(soup)
    except:
        description = "No description available"
    try:
        start_year = get_start_date_from_soup(soup)
        if start_year != "No start year available":
            start_year = clean_year(start_year)
    except:
        start_year = "No start year available"

    return description, start_year
