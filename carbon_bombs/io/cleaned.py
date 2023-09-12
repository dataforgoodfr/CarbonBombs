"""Functions to load cleaned datasets"""
import os

import pandas as pd

from carbon_bombs.conf import FPATH_OUT_ALL
from carbon_bombs.conf import FPATH_OUT_BANK
from carbon_bombs.conf import FPATH_OUT_CB
from carbon_bombs.conf import FPATH_OUT_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_BANK_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_CB_COMP
from carbon_bombs.conf import FPATH_OUT_COUNTRY
from carbon_bombs.conf import FPATH_SRC_METADATAS


def load_carbon_bombs_database() -> pd.DataFrame:
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
    df = pd.read_csv(FPATH_OUT_CB)

    return df


def load_banks_database() -> pd.DataFrame:
    """
    Loads the banks database from a CSV file located at the specified
    file path. Returns a pandas DataFrame containing the data.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the banks database.

    Notes
    -----
    - This function requires the pandas library to be installed.
    - The CSV file containing the data must be available at the specified
      file path.
    - The CSV file must be separated by a semicolon (;).
    """
    df = pd.read_csv(FPATH_OUT_BANK)

    return df


def load_company_database() -> pd.DataFrame:
    """
    Loads the company database from a CSV file located at the specified
    file path. Returns a pandas DataFrame containing the data.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the company database.

    Notes
    -----
    - This function requires the pandas library to be installed.
    - The CSV file containing the data must be available at the specified
      file path.
    - The CSV file must be separated by a semicolon (;).
    """
    df = pd.read_csv(FPATH_OUT_COMP)

    return df


def load_connexion_bank_company_database() -> pd.DataFrame:
    """
    Loads the connexion bank company database from a CSV file located at the specified
    file path. Returns a pandas DataFrame containing the data.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the connexion bank company database.

    Notes
    -----
    - This function requires the pandas library to be installed.
    - The CSV file containing the data must be available at the specified
      file path.
    - The CSV file must be separated by a semicolon (;).
    """
    df = pd.read_csv(FPATH_OUT_CONX_BANK_COMP)

    return df


def load_connexion_cb_company_database() -> pd.DataFrame:
    """
    Loads the connexion carbon bomb company database from a CSV file located at the specified
    file path. Returns a pandas DataFrame containing the data.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the connexion carbon bomb company database.

    Notes
    -----
    - This function requires the pandas library to be installed.
    - The CSV file containing the data must be available at the specified
      file path.
    - The CSV file must be separated by a semicolon (;).
    """
    df = pd.read_csv(FPATH_OUT_CONX_CB_COMP)

    return df


def concat_dataframe_into_excel():
    """Generate an Excel file into `fpath` that takes all
    datasets path and put it into different sheets

    Parameters
    ----------
    fpath : str
        path to excel file
    """

    # Remove excel file if exists
    if os.path.exists(FPATH_OUT_ALL):
        os.remove(FPATH_OUT_ALL)

    cleaned_datasets_fpaths = [
        FPATH_OUT_CB,
        FPATH_SRC_METADATAS,
        FPATH_OUT_COMP,
        FPATH_OUT_BANK,
        FPATH_OUT_CONX_BANK_COMP,
        FPATH_OUT_CONX_CB_COMP,
        FPATH_OUT_COUNTRY,
    ]

    # init writer to create excel file with
    writer = pd.ExcelWriter(
        FPATH_OUT_ALL,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )

    for csv_fpath in cleaned_datasets_fpaths:
        if os.path.isfile(csv_fpath):
            df = pd.read_csv(csv_fpath)
            # Retrieve dataset name to set it as sheet name
            sheet_name = csv_fpath.split("/")[-1][:-4]
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    writer.close()
