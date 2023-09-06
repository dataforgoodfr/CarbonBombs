
from carbon_bombs.readers.khune_paper import load_carbon_bomb_gasoil_database
from carbon_bombs.readers.gem import load_gasoil_mine_gem_database

from carbon_bombs.readers.manual_match import manual_match_gasoil
import pandas as pd
import numpy as np


def _handle_status_column(values):
    values = list(set(values) - {"None"})

    # handle case when there are multiple status
    # keep the first one to appear in this list
    remove_multiple_status = ["operating", "in development", "discovered"]
    for status in remove_multiple_status:
        if status in values:
            values = [status]
            break

    values = " & ".join(values)

    return values
    
def _handle_multiple_gem_mines(name, matched_gem_df, keep_first_cols, concat_cols, num_cols):
    matched_gem_df["tmp"] = ""
    group_df = matched_gem_df.fillna("None").groupby("tmp")

    # keep only first element
    tmp_merge = group_df[keep_first_cols].agg(lambda x: x.values[0])

    # concatenate and separate by a |
    tmp_merge = pd.concat(
        [tmp_merge, group_df[concat_cols].agg("|".join)], axis=1
    )

    tmp_merge = pd.concat(
        [tmp_merge, group_df["Status"].agg(_handle_status_column)], axis=1
    )
    
    tmp_merge[num_cols] = tmp_merge[num_cols].replace("None", np.NaN)
    
    return tmp_merge


def find_gasoil_gem_mines(row, df_gem):
    name, country = row["Project Name"], row["Country_cb"]
    
    # keep only GEM Units of the project country to avoid overlapping
    df_gem = df_gem.loc[df_gem["Country"] == country].copy()
    
    # Retrieve mine_name from manual match
    mine_name = manual_match_gasoil[name] if name in manual_match_gasoil else "None"
    
    matched_gem_df = pd.DataFrame(columns=df_gem.columns)

    if "$" in mine_name:
        for mine_split in mine_name.split("$"):
            mine_df = df_gem.loc[df_gem["Unit_concerned"] == mine_split]
            matched_gem_df = pd.concat([matched_gem_df, mine_df])
    else:
        matched_gem_df = df_gem.loc[df_gem["Unit_concerned"] == mine_name]

    if len(matched_gem_df) > 1:
        matched_gem_df = _handle_multiple_gem_mines(
            name,
            matched_gem_df,
            keep_first_cols=["Country","Latitude","Longitude"],
            concat_cols=["Unit ID","Unit_concerned","Wiki URL","Operator","Owner", "Parent"],
            num_cols=["Latitude","Longitude"]
        )

        return matched_gem_df

    if len(matched_gem_df) == 0:
        return pd.DataFrame(
            [[np.NaN] * len(df_gem.columns)], columns=df_gem.columns
        )

    return matched_gem_df

def create_carbon_bombs_gasoil_table():
    """
    Combines data from the Global Oil and Gas Extraction Tracker and the Carbon
    Bomb Oil and Gas database to create a table of oil and gas mines matched to
    their corresponding carbon bombs.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with columns for the oil and gas mine's name, 
        country, potential emissions (GtCO2), fuel type, latitude, longitude, 
        operator, owner, parent, and the corresponding carbon bomb name.

    Raises
    ------
    FileNotFoundError:
        If one of the data files is not found in the specified path.
    ValueError:
        If one of the data files does not contain the expected sheet.

    Notes
    -----
    This function uses the load_carbon_bomb_gasoil_database() and
    load_gasoil_mine_gem_database() functions to read data from two Excel files.
    The expected file paths are:
    - "./data_sources/Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx"
    - "./data_sources/1-s2.0-S0301421522001756-mmc2.xlsx"
    """
    df_gasoil_carbon_bombs = load_carbon_bomb_gasoil_database()
    df_gasoil_gem_mines = load_gasoil_mine_gem_database()

    # Focus on merge for coal mines between GEM and CB database
    # Filter columns of interest from GEM database (WARNING : very restrictive)
    GEM_usefull_columns = [
        'Unit ID',
        'Unit name',
        'Country',
        'Wiki URL',
        'Latitude',
        'Longitude',
        'Operator',
        'Owner',
        'Parent',
        'Status'
    ]
    df_gasoil_gem_mines = df_gasoil_gem_mines.loc[:, GEM_usefull_columns]

    # Only retain perfect match on Project Name between GEM & CB with a country verification
    df_gasoil_carbon_bombs["tmp_project_name"] = (
        df_gasoil_carbon_bombs["Project Name"] + "/" + df_gasoil_carbon_bombs["Country"]
    )
    df_gasoil_gem_mines["tmp_project_name"] = (
        df_gasoil_gem_mines["Unit name"] + "/" + df_gasoil_gem_mines["Country"]
    )

    df_gasoil_gem_mines = df_gasoil_gem_mines.rename(columns={"Unit name": "Unit_concerned"})

    df_gasoil = df_gasoil_carbon_bombs[
        ["tmp_project_name", "New_project", "Project Name", "Country", "Potential emissions (GtCO2)", "Fuel"]
    ].merge(
        df_gasoil_gem_mines, on="tmp_project_name", how="left", suffixes=("_cb", "")
    ).drop(columns="tmp_project_name")

    df_gasoil_gem_mines = df_gasoil_gem_mines.drop(columns="tmp_project_name")

    # Set at empty string to respect orignal format TODO: decide to keep or not
    df_gasoil["Unit_concerned"] = ""

    _filter_no_match = df_gasoil["Unit ID"].isna()

    no_match_df = pd.concat(
        df_gasoil.loc[_filter_no_match].apply(
            find_gasoil_gem_mines, axis=1, df_gem=df_gasoil_gem_mines
        ).values
    )

    df_gasoil.loc[_filter_no_match, no_match_df.columns] = no_match_df.values
    df_gasoil = df_gasoil.drop(columns=["Country_cb"])
    # df_gasoil = df_gasoil.rename(columns={"Country_cb": "Country_x"})

    return df_gasoil
