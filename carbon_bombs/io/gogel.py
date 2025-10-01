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
        FPATH_SRC_GOGEL_LNG, sheet_name="LNG Liquefaction projects", engine="openpyxl"
    )
    parent_cols = [f"Parent {i}" for i in range(1, 11)]

    df["Companies involved"] = df[parent_cols].apply(
        lambda row: ",".join(row.dropna().astype(str)), axis=1
    )

    renamed_columns = {
        "Project Name": "project_name",
        "Export capacity (Mtpa)": "export_capacity_in_mtpa",
        "Status": "project_status",
        "Start of commercial operations": "start_year",
        "Country": "country",
        "latitude": "latitude",
        "longitude": "longitude",
        "Companies involved": "companies_involved",
    }
    # Only keep columns of interest for the project
    df = df.loc[:, renamed_columns.keys()]
    # Rename columns
    df = df.rename(columns=renamed_columns)

    # Deprecated
    # # For duplicate name in project_name column we use other_name values
    # mask = df["project_name"].duplicated(keep=False) & df["other_name"].notna()
    # df.loc[mask, "project_name"] = df.loc[mask, "other_name"]

    # # If other_name is empty (NaN or ""), concatenate project_name and unit name
    # mask = df["project_name"].duplicated(keep=False)
    # df.loc[mask, "project_name"] = df["project_name"] + " " + df["unit"]

    # # Drop column other_name
    # df = df.drop(columns=["other_name", "unit"])
    # Replace UAE by United Arab Emirates in country column
    df["country"] = df["country"].replace("UAE", "United Arab Emirates")
    # Replace country Senegal/Mauritania for project Greater Tortue Ahmeyim - Phase 1 and Phase 2
    # To avoid warning on double country
    # See slack message : https://data-for-good.slack.com/archives/C08C639D8HM/p1755531343983969?thread_ts=1754847139.766209&cid=C08C639D8HM
    df["country"] = df["country"].replace("Senegal/Mauritania", "Senegal")
    return df
