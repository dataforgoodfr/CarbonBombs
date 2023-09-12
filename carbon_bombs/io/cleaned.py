"""Functions to load cleaned datasets"""
import pandas as pd

from carbon_bombs.conf import FPATH_OUT_BANK
from carbon_bombs.conf import FPATH_OUT_CB
from carbon_bombs.conf import FPATH_OUT_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_BANK_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_CB_COMP


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
