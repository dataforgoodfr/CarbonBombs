"""Functions to read Rystad dataset for Gas & Oil projects"""
import pandas as pd

from carbon_bombs.conf import FPATH_SRC_RYSTAD_GASOIL
from carbon_bombs.utils.logger import LOGGER


def load_gasoil_database():
    """
    Load Gas & Oil database projects from Rystad.

    Returns
    -------
    pandas.DataFrame:
        A dataframe containing the data from the database.
    """
    LOGGER.debug("Read Rystad data: all Gas & Oil projects")
    df = pd.read_excel(
        FPATH_SRC_RYSTAD_GASOIL,
        sheet_name="Carbon_Bombs_Kenya",
        engine="openpyxl",
    )
    renamed_columns = {
        "Project name": "Project_name",
        "Country": "Country",
        "Latitude": "Latitude",
        "Longitude": "Longitude",
        "Start-up year min asset": "Start_year",
        "Total potential emissions (gtICO2)": "Potential_GtCO2_total",
        "Producing  - Potential emissions": "Potential_GtCO2_producing",
        "Short term expansion - Potential emissions": "Potential_GtCO2_short_term_expansion",
        "Long term expansion - Potential emissions": "Potential_GtCO2_long_term_expansion",
    }
    # Only keep columns of interest for the project
    df = df.loc[:, renamed_columns.keys()]
    # Rename columns
    df = df.rename(columns=renamed_columns)
    # Remove last line if it correponds to sum
    if df.iloc[-1]["Project_name"] == "Sum":
        return df.iloc[:-1]
    return df
