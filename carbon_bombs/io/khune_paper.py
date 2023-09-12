"""
Functions to read the Khune Paper dataset
Source from the paper: https://www.sciencedirect.com/science/article/pii/S0301421522001756
"""
import numpy as np
import pandas as pd

from carbon_bombs.conf import FPATH_SRC_KHUNE_PAPER


def load_carbon_bomb_list_database():
    """
    Load the Carbon Bomb List database.

    Returns:
    pandas.DataFrame: A dataframe containing the data from the database.

    Raises:
    FileNotFoundError: If the specified file path cannot be found.
    """
    df = pd.read_excel(
        FPATH_SRC_KHUNE_PAPER,
        sheet_name="Full Carbon Bombs List",
        engine="openpyxl",
        skipfooter=4,
    )
    df = df.loc[:, ["New", "Name", "Country", "Potential emissions (Gt CO2)", "Fuel"]]

    # Make some adjustement on new project column
    df = df.rename(columns={df.columns[0]: "New_project"})

    # TODO: move
    df["New_project"] = np.where(df["New_project"] == "*", "not started", "operating")
    # Define column types
    dtype_d = {
        "New_project": "string",
        "Name": "string",
        "Country": "string",
        "Potential emissions (Gt CO2)": "float",
        "Fuel": "category",
    }
    df = df.astype(dtype_d)
    return df


def load_carbon_bomb_coal_database():
    """
    Loads the carbon bomb coal database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the following columns:
        - New_project (str): whether the project is operating or not started.
        - Project Name (str): name of the project.
        - Country (str): country where the project is located.
        - Potential emissions (GtCO2) (float): potential emissions of the
        project.
        - Fuel (category): type of fuel used in the project.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path:
    "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx".
    The sheet to be read is "Coal".
    The function filters the columns of interest, renames the 'New' column to
    'New_project', changes its values to string and sets the desired data
    types for the dataframe columns. It also replaces some country names to
    correspond to GEM database.
    """
    df = pd.read_excel(
        FPATH_SRC_KHUNE_PAPER, sheet_name="Coal", engine="openpyxl", skipfooter=3
    )
    # Filtering columns of interest
    df = df.loc[
        :, ["New", "Project Name", "Country", "Potential emissions (GtCO2)", "Fuel"]
    ]

    # Make some adjustement on new project column
    df = df.rename(columns={df.columns[0]: "New_project"})

    # TODO: move
    df["New_project"] = np.where(df["New_project"] == "*", "not started", "operating")

    # Define column types
    dtype_d = {
        "New_project": "string",
        "Project Name": "string",
        "Country": "string",
        "Potential emissions (GtCO2)": "float",
        "Fuel": "category",
    }
    df = df.astype(dtype_d)

    # Change country name to correspond to GEM database (only for Russia)
    df["Country"] = df["Country"].replace(
        {"Russian Federation": "Russia", "Turkey": "Türkiye"}
    )

    return df


def load_carbon_bomb_gasoil_database():
    """
    Loads the carbon bomb coal database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the following columns:
        - New_project (str): whether the project is operating or not started.
        - Project Name (str): name of the project.
        - Country (str): country where the project is located.
        - Potential emissions (GtCO2) (float): potential emissions of the
        project.
        - Fuel (category): type of fuel used in the project.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path:
    "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx".
    The sheet to be read is "Coal".
    The function filters the columns of interest, renames the 'New' column to
    'New_project', changes its values to string and sets the desired data
    types for the dataframe columns. It also replaces some country
    names to correspond to GEM database.
    """
    df = pd.read_excel(
        FPATH_SRC_KHUNE_PAPER,
        sheet_name="Oil&Gas",
        engine="openpyxl",
        skipfooter=4,
        skiprows=1,
    )

    # Filtering columns of interest and reorganize them in order to match coal columns
    df = df.loc[:, ["New", "Project", "Country", "Gt CO2"]]
    df.columns = ["New", "Project Name", "Country", "Potential emissions (GtCO2)"]

    # set fuel
    df["Fuel"] = "Oil&Gas"

    # Make some adjustement on new project column
    df = df.rename(columns={df.columns[0]: "New_project"})

    # TODO: move
    df["New_project"] = np.where(df["New_project"] == "*", "not started", "operating")

    # Define column types
    dtype_d = {
        "New_project": "string",
        "Project Name": "string",
        "Country": "string",
        "Potential emissions (GtCO2)": "float",
        "Fuel": "category",
    }
    df = df.astype(dtype_d)

    # Change country name to correspond to GEM database (only for Russia)
    df["Country"] = df["Country"].replace(
        {
            "Russian Federation": "Russia",
            "Turkey": "Türkiye",
            "Saudi-Arabia": "Saudi Arabia",
            "Kuwait-Saudi-Arabia-Neutral Zone": "Kuwait-Saudi Arabia",
        }
    )
    # In case of project that have same name, rename temporarily project name
    # with the format projectname_country
    # Renaming Will be undone after the manual matching process
    df.loc[df.duplicated(subset="Project Name", keep=False), "Project Name"] = (
        df["Project Name"] + "_" + df["Country"]
    )
    return df
