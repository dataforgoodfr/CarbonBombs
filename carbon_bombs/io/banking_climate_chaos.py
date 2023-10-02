"""Functions to load BOCC dataset"""
import numpy as np
import pandas as pd

from carbon_bombs.conf import FPATH_SRC_BOCC
from carbon_bombs.utils.logger import LOGGER


def load_banking_climate_chaos():
    """
    Loads the banking on climate chaos data from an Excel file located at the
    specified file path. Returns a pandas DataFrame containing the data.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the banking on climate chaos data.

    Notes
    -----
    - This function requires the pandas and openpyxl libraries to be installed.
    - The Excel file containing the data must be available at the specified
      file path.
    - The sheet name containing the data must be 'Data'.
    """
    LOGGER.debug("Read Banking On Climate Chaos source")
    file_path = FPATH_SRC_BOCC
    df = pd.read_excel(file_path, sheet_name="Financing (USD)", engine="openpyxl")

    df["Company"] = np.where(
        (df["Parent-Level Company "].isna()) | (df["Parent-Level Company "] == 0),
        df["Company"],
        df["Parent-Level Company "],
    )
    df = df.drop(columns="Parent-Level Company ")

    # TODO : agg
    df = df.groupby(["Bank", "Company"]).sum().reset_index()

    return df
