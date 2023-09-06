import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz

from carbon_bombs.readers.gem import load_coal_mine_gem_database
from carbon_bombs.readers.gem import load_gasoil_mine_gem_database
from carbon_bombs.readers.khune_paper import load_carbon_bomb_coal_database
from carbon_bombs.readers.khune_paper import load_carbon_bomb_gasoil_database
from carbon_bombs.readers.manual_match import manual_match_coal
from carbon_bombs.readers.manual_match import manual_match_gasoil


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


def _handle_multiple_gem_mines(matched_gem_df, keep_first_cols, concat_cols, num_cols):
    matched_gem_df["tmp"] = ""
    group_df = matched_gem_df.fillna("None").groupby("tmp")

    # keep only first element
    tmp_merge = group_df[keep_first_cols].agg(lambda x: x.values[0])

    # concatenate and separate by a |
    tmp_merge = pd.concat([tmp_merge, group_df[concat_cols].agg("|".join)], axis=1)

    tmp_merge = pd.concat(
        [tmp_merge, group_df["Status"].agg(_handle_status_column)], axis=1
    )

    tmp_merge[num_cols] = tmp_merge[num_cols].replace("None", np.NaN)

    return tmp_merge


def find_gem_mines(row, df_gem, used_mines, fuel="gasoil"):
    name, country = row["Project Name"], row["Country_cb"]

    # keep only GEM Units of the project country to avoid overlapping
    df_gem = df_gem.loc[df_gem["Country"] == country].copy()

    # For coal data remove used mines
    if fuel == "coal":
        df_gem = df_gem.loc[~df_gem["Unit ID"].isin(used_mines)]

    # Retrieve mine_name from manual match
    if fuel == "gasoil":
        mine_name = manual_match_gasoil[name] if name in manual_match_gasoil else "None"
    else:
        mine_name = manual_match_coal[name] if name in manual_match_coal else "None"

    matched_gem_df = pd.DataFrame(columns=df_gem.columns)

    if "$" in mine_name:
        for mine_split in mine_name.split("$"):
            mine_df = df_gem.loc[df_gem["Unit_concerned"] == mine_split]
            matched_gem_df = pd.concat([matched_gem_df, mine_df])
    else:
        # Perfect match on the mine_name
        matched_gem_df = df_gem.loc[df_gem["Unit_concerned"] == mine_name]

        # if no match then try to match it on the first name
        # ONLY FOR COAL DATA
        if len(matched_gem_df) == 0 and fuel == "coal":
            first_word_name = name.split()[0]
            df_gem["First_name"] = df_gem["Unit_concerned"].str.split().str[0]

            matched_gem_df = df_gem.loc[df_gem["First_name"] == first_word_name]

            # Numerous match, need to choose between them throught fuzzy wuzzy
            # Calculate Fuzz_score for each line which as the same first word as
            # the carbon bomb
            if len(matched_gem_df) > 1:
                matched_gem_df["Fuzz_score"] = matched_gem_df["Unit_concerned"].apply(
                    lambda x: fuzz.ratio(x, name)
                )
                matched_gem_df = matched_gem_df.loc[
                    [matched_gem_df["Fuzz_score"].idxmax()]
                ]
                matched_gem_df = matched_gem_df.drop(columns="Fuzz_score")

            matched_gem_df = matched_gem_df.drop(columns="First_name")
            df_gem = df_gem.drop(columns="First_name")

    if len(matched_gem_df) > 1:
        if fuel == "gasoil":
            concat_cols = [
                "Unit ID",
                "Unit_concerned",
                "Wiki URL",
                "Operator",
                "Owner",
                "Parent",
            ]
        else:
            concat_cols = [
                "Unit ID",
                "Unit_concerned",
                "GEM Wiki Page (ENG)",
                "Operators",
                "Owners",
                "Parent Company",
            ]

        matched_gem_df = _handle_multiple_gem_mines(
            matched_gem_df,
            keep_first_cols=["Country", "Latitude", "Longitude"],
            concat_cols=concat_cols,
            num_cols=["Latitude", "Longitude"],
        )

    elif len(matched_gem_df) == 0:
        matched_gem_df = pd.DataFrame(
            [[np.NaN] * len(df_gem.columns)], columns=df_gem.columns
        )

    # add used mines to avoid putting back the same mine for another project
    used_mines.extend(matched_gem_df["Unit ID"].astype(str).values[0].split("|"))

    return matched_gem_df


def _init_carbon_bombs_table(fuel):
    if fuel == "gasoil":
        df_cb = load_carbon_bomb_gasoil_database()
        df_gem = load_gasoil_mine_gem_database()

        # rename unit name to Unit_concerned to normalize with coal dataset
        df_gem = df_gem.rename(columns={"Unit name": "Unit_concerned"})

        # Focus on merge for coal mines between GEM and CB database
        # Filter columns of interest from GEM database (WARNING : very restrictive)
        GEM_usefull_columns = [
            "Unit ID",
            "Unit_concerned",
            "Country",
            "Wiki URL",
            "Latitude",
            "Longitude",
            "Operator",
            "Owner",
            "Parent",
            "Status",
        ]

    else:
        df_cb = load_carbon_bomb_coal_database()
        df_gem = load_coal_mine_gem_database()

        # rename unit name to Unit_concerned to normalize with gasoil dataset
        df_gem = df_gem.rename(columns={"Mine Name": "Unit_concerned"})
        df_gem = df_gem.rename(columns={"Mine IDs": "Unit ID"})

        GEM_usefull_columns = [
            "Unit ID",
            "Unit_concerned",
            "Country",
            "GEM Wiki Page (ENG)",
            "Latitude",
            "Longitude",
            "Operators",
            "Owners",
            "Parent Company",
            "Status",
        ]

    df_gem = df_gem.loc[:, GEM_usefull_columns]

    # create a key based on project name + country to get exact match
    df_cb["tmp_project_name"] = df_cb["Project Name"] + "/" + df_cb["Country"]
    df_gem["tmp_project_name"] = df_gem["Unit_concerned"] + "/" + df_gem["Country"]

    df_gem = df_gem.drop_duplicates(subset=["tmp_project_name"])

    # merge CB with exact match of GEM and keep not exact match with empty cells
    # for the GEM data
    df_merge = df_cb[
        [
            "tmp_project_name",
            "New_project",
            "Project Name",
            "Country",
            "Potential emissions (GtCO2)",
            "Fuel",
        ]
    ].merge(df_gem, on="tmp_project_name", how="left", suffixes=("_cb", ""))

    # Remove merge column
    df_merge = df_merge.drop(columns="tmp_project_name")
    df_gem = df_gem.drop(columns="tmp_project_name")

    # get filter for CB with no match
    _filter_no_match = df_merge["Unit_concerned"].isna()

    # retrieve data for CB with no match
    used_mines = []
    no_match_df = pd.concat(
        df_merge.loc[_filter_no_match]
        .apply(find_gem_mines, axis=1, df_gem=df_gem, used_mines=used_mines, fuel=fuel)
        .values
    )

    df_merge.loc[_filter_no_match, no_match_df.columns] = no_match_df.values
    df_merge = df_merge.drop(columns=["Country"])

    return df_merge


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
    return _init_carbon_bombs_table(fuel="gasoil")


def create_carbon_bombs_coal_table():
    """
    Creates a pandas DataFrame of coal carbon bombs data matched with
    corresponding coal mines data from the GEM database.

    Args:
        None.

    Returns:
        pandas.DataFrame: A pandas DataFrame of coal carbon bombs data matched
        with corresponding coal mines data from the GEM database.

    Raises:
        None.

    Notes:
        - Loads two pandas DataFrames from different sources:
        df_coal_carbon_bombs and df_coal_gem_mines.
        - Filters columns of interest from df_coal_gem_mines.
        - Focuses on merge for coal mines between GEM and CB databases.
        - Only retains perfect match on Project Name between GEM and CB with a
        country verification.
        - Drops duplicates based on "Mine Name" column from
        df_coal_gem_mines_perfect_match DataFrame.
        - Concatenates non-duplicates and duplicates DataFrames from
        df_coal_gem_mines_perfect_match.
        - Adds GEM mines that have no perfect match with carbon bomb.
        - Uses find_matching_name_for_GEM_coal function to find the right match
        for the previous step.
        - Extracts lines from df_coal_gem_mines using the index list of
        df_carbon_bombs_no_match DataFrame.
        - Replaces name in df_gem_no_match with the one in carbon bomb.
        - Merges df_coal_carbon_bombs and df_coal_gem_mines_matched DataFrames
        based on "Project Name" and "Mine Name".
        - Drops temporary columns created in previous steps.
    """
    return _init_carbon_bombs_table(fuel="coal")
