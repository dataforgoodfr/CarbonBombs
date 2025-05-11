"""Function to process carbon bombs information"""

import re
from itertools import groupby

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from geopy.geocoders import Nominatim
import country_converter as coco


from carbon_bombs.conf import PROJECT_SEPARATOR
from carbon_bombs.conf import THRESHOLD_OPERATING_PROJECT
from carbon_bombs.io.gem import get_gem_wiki_details
from carbon_bombs.io.gem import load_coal_mine_gem_database
from carbon_bombs.io.gem import load_gasoil_mine_gem_database
from carbon_bombs.io.khune_paper import load_carbon_bomb_coal_database
from carbon_bombs.io.khune_paper import load_carbon_bomb_gasoil_database
from carbon_bombs.io.manual_match import manual_match_coal
from carbon_bombs.io.manual_match import manual_match_gasoil
from carbon_bombs.io.manual_match import manual_match_lat_long
from carbon_bombs.io.rystad import load_rystad_cb_database
from carbon_bombs.utils.location import get_world_region
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
    group_df = matched_gem_df.astype(str).fillna("None").groupby("tmp")

    # keep only first element
    tmp_merge = group_df[keep_first_cols].agg(lambda x: x.values[0])

    # concatenate and separate by a |
    tmp_merge = pd.concat(
        [tmp_merge, group_df[concat_cols].agg(PROJECT_SEPARATOR.join)], axis=1
    )

    tmp_merge = pd.concat(
        [tmp_merge, group_df["Project_status"].agg(_handle_status_column)], axis=1
    )

    tmp_merge[num_cols] = tmp_merge[num_cols].replace("None", np.nan).astype(float)

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
    if fuel == "coal":
        df_gem = df_gem.loc[~df_gem["GEM_ID"].isin(used_mines)]

    # Retrieve mine_name from manual match
    if fuel == "gasoil":
        mine_name = (
            manual_match_gasoil[name] if name in manual_match_gasoil else "NOT_FOUND"
        )
    else:
        mine_name = (
            manual_match_coal[name] if name in manual_match_coal else "NOT_FOUND"
        )

    matched_gem_df = pd.DataFrame(columns=df_gem.columns)

    if "$" in mine_name:
        for mine_split in mine_name.split("$"):
            mine_df = df_gem.loc[df_gem["Unit_concerned"] == mine_split]

            if matched_gem_df.empty:
                matched_gem_df = mine_df.copy()
            else:
                matched_gem_df = pd.concat([matched_gem_df, mine_df])
    else:
        # Perfect match on the mine_name
        matched_gem_df = df_gem.loc[df_gem["Unit_concerned"] == mine_name]

        # if no match then try to match it on the first name
        # ONLY FOR COAL DATA
        if len(matched_gem_df) == 0 and fuel == "coal" and mine_name != "None":
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
                "Parent_Company",
            ],
            num_cols=["Latitude", "Longitude"],
        )

    elif len(matched_gem_df) == 0:
        LOGGER.debug(f"{fuel}: {name} - no match found for this project")
        matched_gem_df = pd.DataFrame(
            [[np.nan] * len(df_gem.columns)], columns=df_gem.columns
        )

    else:
        LOGGER.debug(f"{fuel}: {name} - one mine found for this project")

    # add used mines to avoid putting back the same mine for another project
    used_mines.extend(
        matched_gem_df["GEM_ID"].astype(str).values[0].split(PROJECT_SEPARATOR)
    )

    return matched_gem_df


def cancel_duplicated_rename(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modifies the 'Project' column in a DataFrame to remove the _country
    set up to avoid duplication.

    This function operates on rows where the 'Project' column matches the
    pattern '<ProjectName>_<Country>', and the '<Country>' substring matches
    the 'Country' column for that row. In these rows, it changes the 'Project'
    value to '<ProjectName>', removing the duplicated country information.

    Parameters
    ----------
    df: pandas.DataFrame
        The input DataFrame, which should contain
        'Project' and 'Country' columns. 'Project' column values should
        be strings in the format '<ProjectName>_<Country>'.

    Returns
    -------
    pandas.DataFrame:
        The modified DataFrame. It is the same as the input
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

    Parameters
    ----------
    raw_line: str
        A string that contains information about the companies
        and their involvement.

    Returns
    -------
    str:
        A clean line that contains the name of each company and their
        corresponding percentage of involvement.

    Examples
    --------
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

    Notes
    -----
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
            raw_line = (
                raw_line.replace("，", ",")
                .replace("）", ")")
                .replace("（", "(")
                .replace("]", ")")
                .replace("[", "(")
            )
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
    df_carbon_bombs["Parent_Company"] = df_carbon_bombs["Parent_Company"].fillna("")

    # format Parent_Company with normed separator and clean percentage
    df_carbon_bombs["Parent_Company"] = df_carbon_bombs["Parent_Company"].apply(
        compute_clean_percentage
    )

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

    df_carbon_bombs["Parent_Company"] = df_carbon_bombs["Parent_Company"].apply(
        replace_comp
    )

    return df_carbon_bombs


def _handle_missing_values_gem(df_carbon_bombs: pd.DataFrame) -> pd.DataFrame:
    """Replace missing values on Parent_company_source_GEM, GEM_id_source_GEM
    and GEM_url_source_GEM by text values
    """
    # Fulfill empty values for GEM_ID (GEM) and	GEM_source (GEM) columns
    # depending on project status defined in CB source
    # First need to fulfill empty values by np.nan
    df_carbon_bombs = df_carbon_bombs.replace("", np.nan)

    # Secondly fulfill cell with New_project = True and empty GEM_ID value
    df_carbon_bombs.loc[
        (df_carbon_bombs["GEM_ID"].isna()),
        ["Parent_Company"],
    ] = "No informations on company (100.0%)"

    df_carbon_bombs.loc[
        (df_carbon_bombs["GEM_ID"].isna()),
        ["GEM_ID"],
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
            "mothballed": "operating",
            "not started": "not started",
            "in development": "not started",
            "proposed": "not started",
            "discovered": "not started",
            "shelved": "not started",
            "cancelled": "stopped",
            "shut in": "stopped",
        }
    )

    return df_carbon_bombs


def add_noise_lat_long(x: float) -> float:
    """Add a random noise on the 2nd decimal of a number"""
    return x + (np.random.choice([1, -1]) * np.random.rand() / 10)


def _add_manual_matching_lat_long(df_carbon_bombs: pd.DataFrame) -> pd.DataFrame:
    """Set Latitude and Longitude with manual matching"""

    print(manual_match_lat_long)

    manual_match_lat_long = manual_match_lat_long.rename(
        columns={
            "Carbon_bomb_name_source_CB": "Project_name",
            "Country_source_CB": "Country",
        }
    )

    df_carbon_bombs = df_carbon_bombs.merge(
        manual_match_lat_long,
        on=["Project_name", "Country"],
        how="left",
        suffixes=("", "_TMP"),
    )

    df_carbon_bombs["Latitude"] = np.where(
        df_carbon_bombs["Latitude_TMP"].isna(),
        df_carbon_bombs["Latitude"],
        df_carbon_bombs["Latitude_TMP"],
    )
    df_carbon_bombs["Longitude"] = np.where(
        df_carbon_bombs["Longitude_TMP"].isna(),
        df_carbon_bombs["Longitude"],
        df_carbon_bombs["Longitude_TMP"],
    )
    df_carbon_bombs["Latitude_longitude_source"] = np.where(
        df_carbon_bombs["Longitude_TMP"].isna(),
        df_carbon_bombs["Latitude_longitude_source"],
        "Manual",
    )

    df_carbon_bombs = df_carbon_bombs.drop(columns=["Latitude_TMP", "Longitude_TMP"])

    return df_carbon_bombs


def _add_country_lat_long_when_missing(df_carbon_bombs: pd.DataFrame) -> pd.DataFrame:
    """Set Latitude and Longitude of Country if this is null.
    It adds a random noise to avoid overlapping on the map for the webapp
    """
    df_carbon_bombs = df_carbon_bombs.reset_index(drop=True)

    # Add latitude and longitude if informations not present
    # geolocator = Nominatim(user_agent="my_app")
    geolocator = Nominatim(user_agent="my-app_42")
    df_missing_coordinates = df_carbon_bombs[
        df_carbon_bombs["Latitude"].isnull() | df_carbon_bombs["Longitude"].isnull()
    ]
    for index, rows in df_missing_coordinates.iterrows():
        name = rows["Project_name"]
        country = rows["Country"]
        LOGGER.debug(
            f"No coordinates for `{name}` use latitude / longitude of {country}"
        )

        # location = geolocator.geocode({"country": country})
        # latitude = location.latitude
        # longitude = location.longitude

        country_iso3 = coco.convert(names=country, to="ISO3")

        from carbon_bombs.conf import DATA_SOURCE_PATH

        country_lat_long_df = pd.read_csv(f"{DATA_SOURCE_PATH}/longitude-latitude.csv")
        country_loc = country_lat_long_df.loc[
            country_lat_long_df["ISO-ALPHA-3"] == country_iso3
        ]

        if country_loc.empty:
            # TODO raise warning no country found
            LOGGER.info(f"country `{country}`: no lat, long found")
            latitude = 0
            longitude = 0
        else:
            latitude = country_loc["Latitude"].values[0]
            longitude = country_loc["Longitude"].values[0]

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
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Parent": "Parent_Company",
            "Status": "Project_status",
            "Production start year": "Start_year",
        }

    else:
        df_cb = load_carbon_bomb_coal_database()
        df_gem = load_coal_mine_gem_database()

        # Keep specific GEM columns and rename it to normalize it with gasoil dataset
        GEM_cols_mapping = {
            "GEM Mine ID": "GEM_ID",
            "Mine Name": "Unit_concerned",
            "Country": "Country",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Parent Company": "Parent_Company",
            "Status": "Project_status",
            "Opening Year": "Start_year",
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

    # get filter for CB with no match or if the project is in manual match keys
    _filter_no_match = df_merge["Unit_concerned"].isna() | df_merge[
        "Project Name"
    ].isin(manual_match_gasoil.keys())

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
            "Project Name": "Project_name",
            "Country_cb": "Country",
            "Potential emissions (GtCO2)": "Potential_GtCO2_total",
            "Fuel": "Fuel_type",
        }
    )

    LOGGER.debug("Add world region to dataframe")
    df_merge["World_region"] = (
        df_merge["Country"]
        # .replace("UAE", "United Arab Emirates") # UAE cannot be found in get_world_region
        .apply(get_world_region)
    )

    df_merge["Data_source"] = "GEM"

    # Replace Türkiye to Turkey
    df_merge = df_merge.replace({"Türkiye": "Turkey"})

    # Add companies involved column
    LOGGER.debug("Add companies involved column to CB dataframe")
    df_merge = _add_companies_involved(df_merge)

    # Set status to lower
    df_merge["Project_status"] = df_merge["Project_status"].str.lower()

    # Handle missing values by putting text instead
    LOGGER.debug("Update missing values for GEM columns")
    df_merge = _handle_missing_values_gem(df_merge)

    # create status column by using Status from GEM and Status from CB if first one is NaN
    # LOGGER.debug("Add new status columns into CB dataframe")
    # df_merge = _add_custom_status_columns(df_merge)

    # Add Lat long source
    df_merge["Latitude_longitude_source"] = "GEM"

    # Add manual matchin Lat long
    # LOGGER.debug("Add manual matching country latitude and longitude")
    # df_merge = _add_manual_matching_lat_long(df_merge)

    # Handle Lat long of country when no lat long found
    LOGGER.debug("Add country latitude and longitude when no coordinate found")
    df_merge = _add_country_lat_long_when_missing(df_merge)

    # Retrieve description and start year from GEM wiki page
    # LOGGER.debug("Add GEM description and start year columns into CB dataframe")
    # df_merge = get_information_from_GEM(df_merge)

    # Specific fix fort Khafji bomb that is set to Kuwait and Saudi Arabia
    # -> attribute this carbon bomb to Kuwait to insure a better repartition
    # (Kuwait has 3 bombs and Saudi Arabia 23)
    df_merge.loc[df_merge.Project_name == "Khafji", "Country"] = "Kuwait"

    # Reorder columns and sort by CB Name and country
    final_columns_order = [
        "Project_name",
        "Country",
        "World_region",
        "Latitude",
        "Longitude",
        "Start_year",
        "Project_status",
        "Potential_GtCO2_total",
        "Fuel_type",
        "Data_source",
        "GEM_ID",
        "Parent_Company",
        "Latitude_longitude_source",
    ]

    LOGGER.debug("Reorder CB dataframe columns and sort dataframe by name and country")
    df_merge = df_merge[final_columns_order].sort_values(
        by=["Project_name", "Country"], ascending=True
    )

    return df_merge


def create_carbon_bombs_gasoil_table() -> pd.DataFrame:
    """Combines data from the Global Oil and Gas Extraction Tracker and the Carbon
    Bomb Oil and Gas database to create a table of oil and gas mines matched to
    their corresponding carbon bombs.
    """
    rystad_df = load_rystad_cb_database()
    # Q: should we manual match? --> my opinion: no need we can use the Rystad name

    rystad_df["Start_year"] = rystad_df["Start_year"].astype(int)

    rystad_df["World_region"] = (
        rystad_df["Country"]
        .replace(
            "UAE", "United Arab Emirates"
        )  # UAE cannot be found in get_world_region
        .apply(get_world_region)
    )

    # define status
    rystad_df["Project_status"] = np.where(
        rystad_df["Potential_GtCO2_producing"] > 0,
        "producing",
        np.where(
            rystad_df["Potential_GtCO2_short_term_expansion"] > 0,
            "short term expansion",
            "long term expansion",
        ),
    )

    rystad_df["Fuel_type"] = "Oil&Gas"
    rystad_df["Data_source"] = "Rystad"
    rystad_df["Latitude_longitude_source"] = "Rystad"

    columns_sorted = [
        "Project_name",
        "Country",
        "World_region",
        "Latitude",
        "Longitude",
        "Latitude_longitude_source",
        "Start_year",
        "Project_status",
        "Potential_GtCO2_producing",
        "Potential_GtCO2_short_term_expansion",
        "Potential_GtCO2_long_term_expansion",
        "Potential_GtCO2_total",
        "Fuel_type",
        "Data_source",
    ]

    rystad_df = rystad_df[columns_sorted]

    return rystad_df


def create_carbon_bombs_coal_table() -> pd.DataFrame:
    """Creates a pandas DataFrame of coal carbon bombs data matched with
    corresponding coal mines data from the GEM database.
    """
    return _init_carbon_bombs_table(fuel="coal")


def create_carbon_bombs_table() -> pd.DataFrame:
    """
    Creates a table of carbon bomb projects by merging coal and gas/oil tables,
    remapping columns, cleaning data, and filling missing values.

    Returns
    -------
    pd.DataFrame:
        Carbon bombs dataframe. See metadatas to
        check out all the columns details
    """
    LOGGER.debug("Start creation of carbon bombs dataset")
    LOGGER.debug("Load dataframe coal and gasoil")

    df_coal = create_carbon_bombs_coal_table()
    df_gasoil = create_carbon_bombs_gasoil_table()

    cb_df = pd.concat([df_gasoil, df_coal]).reset_index(drop=True)

    final_columns = [
        "Project_name",
        "Country",
        "World_region",
        "Latitude",
        "Longitude",
        "Latitude_longitude_source",
        "Start_year",
        "Project_status",
        "Potential_GtCO2_total",
        "Potential_GtCO2_producing",
        "Potential_GtCO2_short_term_expansion",
        "Potential_GtCO2_long_term_expansion",
        "Fuel_type",
        "Data_source",
        # 'GEM_ID',
        "Parent_Company",
    ]

    cb_df = cb_df[final_columns]

    return cb_df
