"""Function to process carbon bombs information"""
import re
from itertools import groupby

import awoc
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from geopy.geocoders import Nominatim

from carbon_bombs.conf import PROJECT_SEPARATOR
from carbon_bombs.conf import THRESHOLD_OPERATING_PROJECT
from carbon_bombs.io.gem import get_gem_wiki_details
from carbon_bombs.io.gem import load_coal_mine_gem_database
from carbon_bombs.io.gem import load_gasoil_mine_gem_database
from carbon_bombs.io.khune_paper import load_carbon_bomb_coal_database
from carbon_bombs.io.khune_paper import load_carbon_bomb_gasoil_database
from carbon_bombs.io.manual_match import manual_match_coal
from carbon_bombs.io.manual_match import manual_match_gasoil
from carbon_bombs.utils.logger import LOGGER
from carbon_bombs.utils.match_company_bocc import _get_companies_match_cb_to_bocc
from carbon_bombs.utils.match_company_bocc import save_uniform_company_names


def _match_gem_mines_using_fuzz(name: str, df_gem: pd.DataFrame) -> pd.DataFrame:
    """Try to match GEM mines following theses rules:
    - Match on first word in `name` and `'Unit_concerned'` column of `df_gem`
    - If one or zero line found then return this dataframe
    - Else keep only the line with the best fuzz score between `name`
      and `'Unit_concerned'` column values

    Parameters
    ----------
    name : str
        Project name to map GEM units with
    df_gem : pd.DataFrame
        GEM Dataframe with the following columns:
        `['GEM_ID', 'Unit_concerned', 'Country', 'GEM_source', 'Latitude',
        'Longitude', 'Operators', 'Owners', 'Parent_Company', 'Status']`

    Returns
    -------
    pd.DataFrame
        Matched dataframe with the following columns:
        `['GEM_ID', 'Unit_concerned', 'Country', 'GEM_source', 'Latitude',
        'Longitude', 'Operators', 'Owners', 'Parent_Company', 'Status']`

        It can only return one or zero line.
    """
    # copy dataframe to avoid
    df_gem = df_gem.copy()

    # get first word for the project name and the unit
    first_word_name = name.split()[0]
    df_gem["First_word"] = df_gem["Unit_concerned"].str.split().str[0]

    # try to match on the first word
    matched_gem_df = df_gem.loc[df_gem["First_word"] == first_word_name]

    # if more than one match found then need to choose between them throught fuzzy wuzzy
    # Calculate Fuzz_score for each line which as the same first word as the carbon bomb
    if len(matched_gem_df) > 1:
        matched_gem_df["Fuzz_score"] = matched_gem_df["Unit_concerned"].apply(
            lambda x: fuzz.ratio(x, name)
        )
        # keep only best score
        matched_gem_df = matched_gem_df.loc[[matched_gem_df["Fuzz_score"].idxmax()]]
        matched_gem_df = matched_gem_df.drop(columns="Fuzz_score")

    matched_gem_df = matched_gem_df.drop(columns="First_word")
    del df_gem

    return matched_gem_df


def _handle_status_column(values: list) -> str:
    """Handle status column case.
    If a project has different units with different status then:
    the status `operating` is used before `in development` which
    is also used before `discovered`

    >>> _handle_status_column(["operating", "in development", "discovered"])
    'operating'

    >>> _handle_status_column(["in development", "discovered"])
    'in development'

    Parameters
    ----------
    values : list
        List of string status to concatenate

    Returns
    -------
    str
        Concatenate string of kept status
    """
    values = list(values)
    # special rule to force operating if more than THRESHOLD units are operating
    if (values.count("operating") / len(values)) >= THRESHOLD_OPERATING_PROJECT:
        return "operating"

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


def _handle_multiple_gem_mines(
    matched_gem_df: pd.DataFrame,
    keep_first_cols: list,
    concat_cols: list,
    num_cols: list,
) -> pd.DataFrame:
    """Concatenate matched GEM units into one line.
    It apply the following rules:

    - For `keep_first_cols` columns: it only keep the first value
    - For `concat_cols` columns: it concatenates all values and separate them by a '|'
    - For `'Status'` column: it applies _handle_status_column function
    - For `num_cols` columns: it updates 'None' values by NaN

    Parameters
    ----------
    matched_gem_df : pd.DataFrame
        Dataframe with GEM units matched with a carbon bomb.
        It has the following columns:
        ['GEM_ID', 'Unit_concerned', 'Country', 'GEM_source', 'Latitude',
        'Longitude', 'Operators', 'Owners', 'Parent_Company', 'Status']
    keep_first_cols : list
        Columns which will only keep the first value
    concat_cols : list
        Columns which will concatenate all values together
    num_cols : list
        Numerical columns to replace "None" by NaN

    Returns
    -------
    pd.DataFrame
        Dataframe with one line containing merge values for each column
        It has the following columns:
        ['Country', 'Latitude', 'Longitude', 'GEM_ID', 'Unit_concerned',
        'GEM_source', 'Operators', 'Owners', 'Parent_Company', 'Status']
    """
    matched_gem_df["tmp"] = ""
    group_df = matched_gem_df.fillna("None").groupby("tmp")

    # keep only first element
    tmp_merge = group_df[keep_first_cols].agg(lambda x: x.values[0])

    # concatenate and separate by a |
    tmp_merge = pd.concat(
        [tmp_merge, group_df[concat_cols].agg(PROJECT_SEPARATOR.join)], axis=1
    )

    tmp_merge = pd.concat(
        [tmp_merge, group_df["Status"].agg(_handle_status_column)], axis=1
    )

    tmp_merge[num_cols] = tmp_merge[num_cols].replace("None", np.NaN)

    return tmp_merge


def _find_gem_mines(
    row: pd.Series, df_gem: pd.DataFrame, used_mines: list, fuel="gasoil"
) -> pd.DataFrame:
    """Find GEM Units / Mines for project that did not have a perfect match.

    For a given project (contained in `row`) it tries to find the units
    following theses rules:
    - For `gasoil` projects: use manual_match_gasoil and use it
    - For `coal` projects:
        - use manual_match_coal and if mines found then use it
        - If the project is not in manual_match_coal then try
          to find mines using _match_gem_mines_using_fuzz function

    Parameters
    ----------
    row : pd.Series
        Project row, it needs `'Project Name'` and `'Country_cb'` columns
    df_gem : pd.DataFrame
        GEM Dataframe with the following columns:
        `['GEM_ID', 'Unit_concerned', 'Country', 'GEM_source', 'Latitude',
        'Longitude', 'Operators', 'Owners', 'Parent_Company', 'Status']`
    used_mines : list
        List of used mines ID (only used for coal projects)
    fuel : str, optional
        Fuel type to see which source is used, by default "gasoil"
        Use "gasoil" or "coal"

    Returns
    -------
    pd.DataFrame
        Matched dataframe. It always has one line.
        If no GEM Units / Mines found then return dataframe
        with NaN values
    """
    name, country = row["Project Name"], row["Country_cb"]
    LOGGER.debug(f"{fuel}: {name} - get informations")

    # keep only GEM Units of the project country to avoid overlapping
    df_gem = df_gem.loc[df_gem["Country"] == country].copy()

    # For coal data remove used mines
    # TODO: check if OK with Matthieu
    if fuel == "coal":
        df_gem = df_gem.loc[~df_gem["GEM_ID"].isin(used_mines)]

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
            LOGGER.debug(f"{fuel}: {name} - match mine using fuzz score")
            matched_gem_df = _match_gem_mines_using_fuzz(name, df_gem)

    if len(matched_gem_df) > 1:
        LOGGER.debug(
            f"{fuel}: {name} - {len(matched_gem_df)} mines found for this project"
        )
        matched_gem_df = _handle_multiple_gem_mines(
            matched_gem_df,
            keep_first_cols=["Country", "Latitude", "Longitude"],
            concat_cols=[
                "GEM_ID",
                "Unit_concerned",
                "GEM_source",
                "Operators",
                # "Owners",
                "Parent_Company",
            ],
            num_cols=["Latitude", "Longitude"],
        )

    elif len(matched_gem_df) == 0:
        LOGGER.debug(f"{fuel}: {name} - no match found for this project")
        matched_gem_df = pd.DataFrame(
            [[np.NaN] * len(df_gem.columns)], columns=df_gem.columns
        )

    else:
        LOGGER.debug(f"{fuel}: {name} - one mine found for this project")

    # add used mines to avoid putting back the same mine for another project
    used_mines.extend(
        matched_gem_df["GEM_ID"].astype(str).values[0].split(PROJECT_SEPARATOR)
    )

    return matched_gem_df


def _init_carbon_bombs_table(fuel: str) -> pd.DataFrame:
    """Initialize the CB table for both gasoil and coal data.
    It merges the CB dataset and GEM dataset using some rules.

    - Try to match on Project Name / Country and Unit Name / Country
    - If no match then use _find_gem_mines function

    It also normalizes all columns for both coal and gasoil data

    Parameters
    ----------
    fuel : str
        Whether it loads `'gasoil'` or `'coal'` datasets

    Returns
    -------
    pd.DataFrame
        CB and GEM dataframe merged and normalized
    """
    LOGGER.debug(f"{fuel}: Start dataframe initialization")

    if fuel == "gasoil":
        df_cb = load_carbon_bomb_gasoil_database()
        df_gem = load_gasoil_mine_gem_database()

        # Keep specific GEM columns and rename it to normalize it with coal dataset
        GEM_cols_mapping = {
            "Unit ID": "GEM_ID",
            "Unit name": "Unit_concerned",
            "Country": "Country",
            "Wiki URL": "GEM_source",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Operator": "Operators",
            # "Owner": "Owners",
            "Parent": "Parent_Company",
            "Status": "Status",
            "Production start year": "Carbon_bomb_start_year",
        }

    else:
        df_cb = load_carbon_bomb_coal_database()
        df_gem = load_coal_mine_gem_database()

        # Keep specific GEM columns and rename it to normalize it with gasoil dataset
        GEM_cols_mapping = {
            "Mine IDs": "GEM_ID",
            "Mine Name": "Unit_concerned",
            "Country": "Country",
            "GEM Wiki Page (ENG)": "GEM_source",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Operators": "Operators",
            # "Owners": "Owners",
            "Parent Company": "Parent_Company",
            "Status": "Status",
            "Opening Year": "Carbon_bomb_start_year",
        }

    LOGGER.debug(f"{fuel}: CB and GEM dataframes loaded")

    df_gem = df_gem.loc[:, GEM_cols_mapping.keys()]
    df_gem = df_gem.rename(columns=GEM_cols_mapping)
    LOGGER.debug(
        f"{fuel}: keep only wanted columns and rename columns done for GEM dataframe"
    )

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
    LOGGER.debug(f"{fuel}: merge CB and GEM dataframes by name / country keys")

    # Remove merge column
    df_merge = df_merge.drop(columns="tmp_project_name")
    df_gem = df_gem.drop(columns="tmp_project_name")

    # get filter for CB with no match
    _filter_no_match = df_merge["Unit_concerned"].isna()

    # retrieve data for CB with no match
    LOGGER.debug(f"{fuel}: retrieve informations for projects with no match start...")
    used_mines = []
    no_match_df = pd.concat(
        df_merge.loc[_filter_no_match]
        .apply(_find_gem_mines, axis=1, df_gem=df_gem, used_mines=used_mines, fuel=fuel)
        .values
    )
    LOGGER.debug(f"{fuel}: retrieve informations for projects with no match done")

    df_merge.loc[_filter_no_match, no_match_df.columns] = no_match_df.values
    # drop country column (from GEM) since it can be null with the merge
    # we keep Country_cb that is always completed
    df_merge = df_merge.drop(columns=["Country"])

    # Rename columns with final format
    df_merge = df_merge.rename(
        columns={
            "New_project": "Status_source_CB",
            "Project Name": "Carbon_Bomb_Name",
            "Country_cb": "Country",
            "Potential emissions (GtCO2)": "Potential_GtCO2",
            "Fuel": "Fuel_type",
        }
    )

    return df_merge


def create_carbon_bombs_gasoil_table() -> pd.DataFrame:
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


def create_carbon_bombs_coal_table() -> pd.DataFrame:
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


def cancel_duplicated_rename(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modifies the 'Project' column in a DataFrame to remove the _country
    set up to avoid duplication.

    This function operates on rows where the 'Project' column matches the
    pattern '<ProjectName>_<Country>', and the '<Country>' substring matches
    the 'Country' column for that row. In these rows, it changes the 'Project'
    value to '<ProjectName>', removing the duplicated country information.

    Args:
        df (pandas.DataFrame): The input DataFrame, which should contain
            'Project' and 'Country' columns. 'Project' column values should
            be strings in the format '<ProjectName>_<Country>'.

    Returns:
        pandas.DataFrame: The modified DataFrame. It is the same as the input
            DataFrame, but in rows where the 'Project' and 'Country' matched
            the pattern and condition, the 'Project' value is replaced by
            '<ProjectName>'.
    """

    def match_pattern(row):
        if "_" in row["Carbon_Bomb_Name"]:
            _, country = re.split("_", row["Carbon_Bomb_Name"])
            return country == row["Country"]
        else:
            return False

    mask = df.apply(match_pattern, axis=1)
    df.loc[mask, "Carbon_Bomb_Name"] = df.loc[mask, "Carbon_Bomb_Name"].apply(
        lambda x: x.split("_")[0]
    )

    return df


def ponderate_percentage(dict_percentage: dict) -> dict:
    """
    Ponderates the percentages in a dictionary such that their sum equals 100%.

    Parameters
    ----------
    dict_percentage : dict
        A dictionary where keys are company names and values are percentages.

    Returns
    -------
    dict:
        A new dictionary with the same keys as `dict_percentage` but with values
        that have been adjusted such that their sum equals 100%.

    Raises
    ------
    ValueError:
        If the values in `dict_percentage` do not add up to 100.

    Notes
    -----
    This function takes a dictionary where the values are percentages that may
    not add up to exactly 100. It calculates a new set of percentages that are
    adjusted such that their sum equals 100. The adjusted percentages are then
    rounded to one decimal point and added up to ensure that their sum equals
    exactly 100. The function returns a new dictionary with the same keys as
    the input dictionary but with adjusted percentages as values.
    """
    # Calculate sum of percentage to ponderate
    sum_percentage = sum(list(dict_percentage.values()))
    # Calculate new percentage based on this sum
    new_percentage = [elt / sum_percentage for elt in dict_percentage.values()]
    companies = dict_percentage.keys()
    # Round each element in the new_percentage list
    rounded_percentage = [round(x * 100, 1) for x in new_percentage]
    # Adjust the decimal of the last percentage in order to ensure a 100% sum
    diff = 100 - sum(rounded_percentage)
    rounded_percentage[-1] = round(rounded_percentage[-1] + diff, 1)
    # Build new dictionary
    dict_percentage = dict()
    for company, percentage in zip(companies, rounded_percentage):
        dict_percentage[company] = percentage
    # Return dict with ponderate percentage
    return dict_percentage


def compute_clean_percentage(raw_line):
    """
    Compute the percentage of involvement of each company mentioned in a given
    line.

    Args:
        raw_line (str): A string that contains information about the companies
        and their involvement.

    Returns:
        str: A clean line that contains the name of each company and their
        corresponding percentage of involvement.

    Raises:
        TypeError: If the input `raw_line` is not a string.

    Examples:
        >>> compute_clean_percentage("ABC (20%), XYZ (80%)")
        'ABC (20%); XYZ (80%)'

        >>> compute_clean_percentage("ABC; XYZ; PQR")
        'ABC (33.33333333333333%); XYZ (33.33333333333333%);
        PQR (33.33333333333333%)'

        >>> compute_clean_percentage("")
        'No informations on company (100.0%)'

        >>> a = compute_clean_percentage("A|B|A")
        A (100.0%)|B (100.0%)|A (100.0%)

        >>> a = compute_clean_percentage("A (50%);B(50%)|A")
        A (50.0%);B (50.0%)|A (100.0%)

    Notes:
        The function can handle two possibilities:
        1) When the raw_line contains percentages, it calculates and merges the
        percentage of involvement for each company and returns a clean line.
        2) When the raw_line does not contain percentages, it assumes each
        company has an equal involvement and calculates the percentage
        accordingly.

    The input raw_line should be in one of the following formats:
        1) <company_1> (percentage_1%), <company_2> (percentage_2%), ...
        2) <company_1>; <company_2>; <company_3>; ...

    If there is no information about the company, the output line will be
    'No informations on company (100.0%)'.
    """

    def _clean_one_unit(raw_line):
        # With raw_line content 2 possibilities : Percentage are indicated or not
        if "%" in raw_line:
            # Case where percentage are indicated
            # replace "Fullwidth" char by basic char
            raw_line = raw_line.replace("，", ",").replace("）", ")").replace("（", "(")
            # remove useless spaces
            companies = (
                re.sub("  +", " ", raw_line).replace(" ;", ";").replace(" ,", ",")
            )

            # split at each percentage
            companies = [
                "(".join(x.split("(")[:-1]) for x in companies.split("%)")[:-1]
            ]
            companies = [re.sub(r"[,|;]", "", x).strip() for x in companies]
            percentages = re.findall(r"\(([\d\.]+)%\)", raw_line)
            # Merge percentage of same company into one
            combined_percentages = {}
            for company, percentage in zip(companies, percentages):
                if company in combined_percentages:
                    combined_percentages[company] += float(percentage)
                else:
                    combined_percentages[company] = float(percentage)
            # Once percentage are merge 3 possibilities based on the sum
            sum_percentage = sum(list(combined_percentages.values()))
            # Percentage less than 100 percent (We complete by "Others"):
            if sum_percentage < 100.0:
                left_percentage = 100.0 - sum_percentage
                combined_percentages["Others"] = left_percentage
            # Percentage equal to 100 percent (No action needed):
            elif sum_percentage == 100.0:
                pass
            # Percentage more than 100 percent (We ponderate the results)
            else:
                combined_percentages = ponderate_percentage(combined_percentages)
        else:
            # Case no percentage are indicated, we defined it based on number of
            # compagnies defined into
            companies = raw_line.split(";")
            if companies == [""]:
                companies = ["No informations on company"]
            # Compute percentage considering each company have the same involvement
            percentages = [100.0 / len(companies) for _ in companies]
            # Merge percentage of same company into one
            combined_percentages = {}
            for company, percentage in zip(companies, percentages):
                if company in combined_percentages:
                    combined_percentages[company] += float(percentage)
                else:
                    combined_percentages[company] = float(percentage)
        # Once combined percentage is defined for each case, defined a clean line
        clean_line = ""
        for keys in combined_percentages:
            company_line = f"{keys} ({combined_percentages[keys]}%);"
            clean_line = clean_line + company_line
        clean_line = clean_line[:-1]  # Delete last ;
        return clean_line

    clean_line = []
    for unit_text in raw_line.split("|"):
        clean_line.append(_clean_one_unit(unit_text))

    return PROJECT_SEPARATOR.join(clean_line)


def _add_companies_involved(df_carbon_bombs: pd.DataFrame) -> pd.DataFrame:
    """Add Companies_involved which takes the value of
    Parent_Company if not null else Operators

    Also it applies compute_clean_percentage on Parent_Company and Companies_involved
    """
    # Clean percentage in column Parent_company
    # Clean data into Parent company columns
    df_carbon_bombs["Parent_company_source_GEM"].fillna("", inplace=True)
    # update with operators only if no parent companies and only None
    df_carbon_bombs["Companies_involved_source_GEM"] = df_carbon_bombs.apply(
        lambda row: row["Operators_source_GEM"]
        if (row["Parent_company_source_GEM"] == "")
        or (
            row["Parent_company_source_GEM"]
            .replace(PROJECT_SEPARATOR, "")
            .replace("None", "")
            == ""
        )
        else row["Parent_company_source_GEM"],
        axis=1,
    )
    # format Parent_Company with normed separator and clean percentage
    df_carbon_bombs["Parent_company_source_GEM"] = df_carbon_bombs[
        "Parent_company_source_GEM"
    ].apply(compute_clean_percentage)

    df_carbon_bombs["Companies_involved_source_GEM"].fillna("", inplace=True)
    df_carbon_bombs["Companies_involved_source_GEM"] = df_carbon_bombs[
        "Companies_involved_source_GEM"
    ].apply(compute_clean_percentage)

    # uniform with BOCC companies name
    dict_match = _get_companies_match_cb_to_bocc(df_carbon_bombs)
    save_uniform_company_names(dict_match)

    def replace_comp(x):
        """Replace company names to normalize it"""
        new_comp = []
        for projects in x.split(PROJECT_SEPARATOR):
            new_comp_proj = []
            for comp_prct in projects.split(";"):
                comp, prct = comp_prct.rsplit(" (", 1)
                comp = dict_match[comp] if comp in dict_match else comp
                new_comp_proj.append(f"{comp} ({prct}")

            new_comp.append(";".join(new_comp_proj))

        return PROJECT_SEPARATOR.join(new_comp)

    df_carbon_bombs["Companies_involved_source_GEM"] = df_carbon_bombs[
        "Companies_involved_source_GEM"
    ].apply(replace_comp)

    return df_carbon_bombs


def _handle_missing_values_gem(df_carbon_bombs: pd.DataFrame) -> pd.DataFrame:
    """Replace missing values on Parent_company_source_GEM, GEM_id_source_GEM
    and GEM_url_source_GEM by text values
    """
    # Fulfill empty values for GEM_ID (GEM) and	GEM_source (GEM) columns
    # depending on project status defined in CB source
    # First need to fulfill empty values by np.NaN
    df_carbon_bombs = df_carbon_bombs.replace("", np.nan)

    # Secondly fulfill cell with New_project = True and empty GEM_ID value
    df_carbon_bombs.loc[
        (df_carbon_bombs["Status_source_CB"] == "operating")
        & (df_carbon_bombs["GEM_id_source_GEM"].isna()),
        ["Parent_company_source_GEM"],
    ] = "No informations on company (100.0%)"

    df_carbon_bombs.loc[
        (df_carbon_bombs["Status_source_CB"] == "operating")
        & (df_carbon_bombs["GEM_id_source_GEM"].isna()),
        [
            "GEM_id_source_GEM",
            "GEM_url_source_GEM",
        ],
    ] = "No informations available on GEM"

    # Thirdly fulfill cell with New_project = False and empty GEM_ID value
    df_carbon_bombs.loc[
        (df_carbon_bombs["Status_source_CB"] != "operating")
        & (df_carbon_bombs["GEM_id_source_GEM"].isna()),
        ["GEM_id_source_GEM", "GEM_url_source_GEM"],
    ] = "No informations available on GEM"

    return df_carbon_bombs


def _add_custom_status_columns(df_carbon_bombs: pd.DataFrame) -> pd.DataFrame:
    """Create 2 custom Status columns
    - Status_lvl_1: if Status_source_GEM is null then Status_source_CB else Status_source_GEM
    - Status_lvl_2: convert Status_lvl_1 values using the following:
        - operating : operating
        - not started : not started, in development, proposed, discovered, shelved
        - stopped : cancelled, mothballed, shut in
    """
    # create status column by using Status from GEM and Status from CB if first one is NaN
    df_carbon_bombs["Status_lvl_1"] = np.where(
        df_carbon_bombs["Status_source_GEM"].isna(),
        df_carbon_bombs["Status_source_CB"],
        df_carbon_bombs["Status_source_GEM"],
    )
    df_carbon_bombs["Status_lvl_2"] = df_carbon_bombs["Status_lvl_1"].replace(
        {
            "operating": "operating",
            "not started": "not started",
            "in development": "not started",
            "proposed": "not started",
            "discovered": "not started",
            "shelved": "not started",
            "cancelled": "stopped",
            "mothballed": "stopped",
            "shut in": "stopped",
        }
    )

    return df_carbon_bombs


def add_noise_lat_long(x: float) -> float:
    """Add a random noise on the 2nd decimal of a number"""
    return x + (np.random.choice([1, -1]) * np.random.rand() / 10)


def _add_country_lat_long_when_missing(df_carbon_bombs: pd.DataFrame) -> pd.DataFrame:
    """Set Latitude and Longitude of Country if this is null.
    It adds a random noise to avoid overlapping on the map for the webapp
    """
    df_carbon_bombs = df_carbon_bombs.reset_index(drop=True)

    # Add latitude and longitude if informations not present
    geolocator = Nominatim(user_agent="my_app")
    df_missing_coordinates = df_carbon_bombs[
        df_carbon_bombs["Latitude"].isnull() | df_carbon_bombs["Longitude"].isnull()
    ]
    for index, rows in df_missing_coordinates.iterrows():
        name = rows["Carbon_bomb_name_source_CB"]
        country = rows["Country_source_CB"]
        LOGGER.debug(
            f"No coordinates for `{name}` use latitude / longitude of {country}"
        )

        location = geolocator.geocode(country)
        latitude = location.latitude
        longitude = location.longitude
        df_carbon_bombs.loc[index, "Latitude"] = latitude
        df_carbon_bombs.loc[index, "Longitude"] = longitude
        df_carbon_bombs.loc[index, "Latitude_longitude_source"] = "Country CB"

    # add noise to dupplicated lat long
    # it's used for the website to avoid overlapping point on the map
    np.random.seed(42)
    lat_long_dup = (
        df_carbon_bombs[["Latitude", "Longitude"]].duplicated(keep=False).values
    )

    df_carbon_bombs.loc[lat_long_dup, "Latitude"] = df_carbon_bombs.loc[
        lat_long_dup, "Latitude"
    ].apply(add_noise_lat_long)
    df_carbon_bombs.loc[lat_long_dup, "Longitude"] = df_carbon_bombs.loc[
        lat_long_dup, "Longitude"
    ].apply(add_noise_lat_long)

    return df_carbon_bombs


def get_information_from_GEM(df: pd.DataFrame) -> pd.DataFrame:
    """Get description and start year from GEM wiki url"""
    description = list()
    start_year = list()

    url = df["GEM_url_source_GEM"].tolist()

    for _, item in enumerate(url):
        # time.sleep(0.2)
        if item in ["No informations available on GEM", "Qcoal"]:
            description_ = "No description available"
            start_year_ = "No start year available"
        else:
            item = item.replace(PROJECT_SEPARATOR, ";").split(";")[0]
            description_, start_year_ = get_gem_wiki_details(item)
        description.append(description_)
        start_year.append(start_year_)

    df["Carbon_bomb_description"] = description
    df["Carbon_bomb_start_year"] = np.where(
        df["Carbon_bomb_start_year"].isna(),
        start_year,
        np.where(
            df["Carbon_bomb_start_year"] == "TBD",
            "No start year available",
            df["Carbon_bomb_start_year"],
        ),
    )

    return df


def create_carbon_bombs_table() -> pd.DataFrame:
    """
    Creates a table of carbon bomb projects by merging coal and gas/oil tables,
    remapping columns, cleaning data, and filling missing values.

    Returns:
    --------
    pd.DataFrame:
        A pandas DataFrame with the following columns:
            - 'New_project (CB)': string indicating if the project is operating or
            not started.
            - 'Carbon_Bomb_Name (CB)': name of the carbon bomb project.
            - 'Country (CB)': country where the carbon bomb project is located.
            - 'Potential_GtCO2 (CB)': potential emissions of the carbon bomb
            project
              in gigatons of CO2.
            - 'Fuel_type (CB)': type of fuel used by the carbon bomb project.
            - 'GEM_ID (GEM)': identifier of the project in the Global Energy
            Monitor
              (GEM) database.
            - 'GEM_source (GEM)': URL of the project page on the GEM database.
            - 'Latitude': geographic latitude of the project location.
            - 'Longitude': geographic longitude of the project location.
            - 'Operators (GEM)': operators of the project according to the GEM
            database.
            - 'Parent_Company': parent company of the project, with percentage
              of ownership if available.
            - 'Multiple_unit_concerned (manual_match)': multiple unit concerned
              for coal projects only.

    Notes:
    ------
    This function relies on the following helper functions:
    - create_carbon_bombs_coal_table: creates the coal table of carbon bomb
    projects.
    - create_carbon_bombs_gasoil_table: creates the gas/oil table of carbon
    bomb projects.
    - compute_clean_percentage: computes the percentage of ownership for
      parent companies that own several carbon bomb projects.
    - add_chat_GPT_data: adds ChatGPT data to carbon bomb projects that are not
      present in the GEM database.

    This function also uses the following mapping dictionaries to rename
    columns:
    - name_mapping_coal: maps column names for the coal table.
    - name_mapping_gasoil: maps column names for the gas/oil table.
    - name_mapping_source: maps column names for the final merged table.
    """
    LOGGER.debug("Start creation of carbon bombs dataset")
    LOGGER.debug("Load dataframe coal and gasoil")
    df_coal = create_carbon_bombs_coal_table()
    df_gasoil = create_carbon_bombs_gasoil_table()

    # Cancel renaming of project that have the same name but are located in
    # different country (see function load_carbon_bomb_gasoil_database())
    # Only apply for the moment to gasoil dataframe
    LOGGER.debug("Cancel duplicated project name renaming for gasoil projects")
    df_gasoil = cancel_duplicated_rename(df_gasoil)

    # Merge dataframes
    LOGGER.debug("Merge coal and gasoil dataframes")
    df_carbon_bombs = pd.concat([df_coal, df_gasoil], axis=0)

    # Keep as comment in case we need to use it again
    # for Unit_concerned we only want to keep when there are more than one units
    # df_carbon_bombs["Unit_concerned"] = np.where(
    #     df_carbon_bombs["Unit_concerned"].str.contains(PROJECT_SEPARATOR, regex=False),
    #     df_carbon_bombs["Unit_concerned"],
    #     np.NaN,
    # )

    # Remap dataframe columns to display data source
    # Not efficient might be rework (no time for that right now)
    name_mapping_source = {
        "New_project": "Status_source_CB",
        "Carbon_Bomb_Name": "Carbon_bomb_name_source_CB",
        "Country": "Country_source_CB",
        "Potential_GtCO2": "Potential_GtCO2_source_CB",
        "Fuel_type": "Fuel_type_source_CB",
        "GEM_ID": "GEM_id_source_GEM",
        "GEM_source": "GEM_url_source_GEM",
        "Latitude": "Latitude",
        "Longitude": "Longitude",
        "Operators": "Operators_source_GEM",
        "Parent_Company": "Parent_company_source_GEM",
        "Unit_concerned": "GEM_project_name_source_GEM",
        "Status": "Status_source_GEM",
    }
    LOGGER.debug("Rename columns for CB dataframe")
    df_carbon_bombs = df_carbon_bombs.rename(columns=name_mapping_source)

    # Add companies involved column
    LOGGER.debug("Add companies involved column to CB dataframe")
    df_carbon_bombs = _add_companies_involved(df_carbon_bombs)

    # Set status to lower
    df_carbon_bombs["Status_source_GEM"] = df_carbon_bombs[
        "Status_source_GEM"
    ].str.lower()

    # Handle missing values by putting text instead
    LOGGER.debug("Update missing values for GEM columns")
    df_carbon_bombs = _handle_missing_values_gem(df_carbon_bombs)

    # create status column by using Status from GEM and Status from CB if first one is NaN
    LOGGER.debug("Add new status columns into CB dataframe")
    df_carbon_bombs = _add_custom_status_columns(df_carbon_bombs)

    # Add Lat long source
    df_carbon_bombs["Latitude_longitude_source"] = "GEM"
    # Handle Lat long of country when no lat long found
    LOGGER.debug("Add country latitude and longitude when no coordinate found")
    df_carbon_bombs = _add_country_lat_long_when_missing(df_carbon_bombs)

    # Retrieve description and start year from GEM wiki page
    LOGGER.debug("Add GEM description and start year columns into CB dataframe")
    df_carbon_bombs = get_information_from_GEM(df_carbon_bombs)

    # Specific fix fort Khafji bomb that is set to Kuwait and Saudi Arabia
    # -> attribute this carbon bomb to Kuwait to insure a better repartition
    # (Kuwait has 3 bombs and Saudi Arabia 23)
    df_carbon_bombs.loc[
        df_carbon_bombs.Carbon_bomb_name_source_CB == "Khafji", "Country_source_CB"
    ] = "Kuwait"

    # Add World Region associated to Headquarters country
    LOGGER.debug("Add world region column into CB dataframe")
    world_region = awoc.AWOC()
    df_carbon_bombs["World_region"] = (
        df_carbon_bombs["Country_source_CB"]
        .replace({"Türkiye": "Turkey"})
        .apply(world_region.get_country_continent_name)
    )

    # Reorder columns and sort by CB Name and country
    final_columns_order = [
        "Carbon_bomb_name_source_CB",
        "Country_source_CB",
        "World_region",
        "Potential_GtCO2_source_CB",
        "Fuel_type_source_CB",
        "GEM_id_source_GEM",
        "GEM_url_source_GEM",
        "Latitude",
        "Longitude",
        "Latitude_longitude_source",
        "Operators_source_GEM",
        "Parent_company_source_GEM",
        "Companies_involved_source_GEM",
        "GEM_project_name_source_GEM",
        "Carbon_bomb_description",
        "Carbon_bomb_start_year",
        "Status_source_CB",
        "Status_source_GEM",
        "Status_lvl_1",
        "Status_lvl_2",
    ]
    LOGGER.debug("Reorder CB dataframe columns and sort dataframe by name and country")
    df_carbon_bombs = df_carbon_bombs[final_columns_order].sort_values(
        by=["Carbon_bomb_name_source_CB", "Country_source_CB"], ascending=True
    )

    return df_carbon_bombs
