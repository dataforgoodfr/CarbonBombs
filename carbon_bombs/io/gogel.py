"""Functions to read GOGEL dataset for LNG"""
import pandas as pd

from carbon_bombs.conf import FPATH_SRC_GOGEL_LNG
from carbon_bombs.utils.logger import LOGGER


def load_lng_database():
    """
    Load LNG database from GOGEL.

    Returns
    -------
    pandas.DataFrame:
        A dataframe containing the data from the database.
    """
    LOGGER.debug("Read GOGEL data: all LNG project")
    df = pd.read_excel(
        FPATH_SRC_GOGEL_LNG,
        sheet_name="LNG Liquefaction projects",
        engine="openpyxl",
        skiprows=2,
    )
    renamed_columns = {
        "Name (project)": "Project_name",
        "Export capacity (Mtpa)": "Export_capacity_in_Mtpa",
        "Status": "Project_status",
        "Country": "Country",
        "Companies involved": "Companies_involved",
    }
    # Only keep columns of interest for the project
    df = df.loc[:, renamed_columns.keys()]
    # Rename columns
    df = df.rename(columns=renamed_columns)
    return df
