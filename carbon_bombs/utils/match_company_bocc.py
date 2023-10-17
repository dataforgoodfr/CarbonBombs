"""Utils to match company names with BOCC names"""
import re

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz

from carbon_bombs.conf import PROJECT_SEPARATOR
from carbon_bombs.io.banking_climate_chaos import load_banking_climate_chaos
from carbon_bombs.io.cleaned import load_carbon_bombs_database
from carbon_bombs.io.manual_match import manual_match_company
from carbon_bombs.io.uniform_company_names import load_uniform_company_names
from carbon_bombs.io.uniform_company_names import save_uniform_company_names
from carbon_bombs.utils.logger import LOGGER


def split_column_parent_company(row):
    """
    Splits the Parent Company column in the given row into multiple rows, each
    row containing only one Parent Company value. Returns a list of
    dictionaries, with each dictionary containing the Carbon Bomb Name and
    Parent Company values.

    Parameters
    ----------
    row : pandas.Series
        A pandas Series containing the Carbon Bomb Name and Parent Company
        values.

    Returns
    -------
    List[Dict[str, Any]]
        A list of dictionaries containing the Carbon Bomb Name and Parent
        Company values after splitting the Parent Company column.

    Notes
    -----
    - This function requires the pandas library to be installed.
    """
    instance = row["Carbon_bomb_name_source_CB"]
    country = row["Country_source_CB"]
    # split by | for different unit and ; for different companies
    companies = (
        row["Companies_involved_source_GEM"].replace(PROJECT_SEPARATOR, ";").split(";")
    )
    carbon_bomb_list_company = [
        {
            "Carbon_bomb_name": instance,
            "Country": country,
            "Company": company,
        }
        for company in companies
    ]
    return carbon_bomb_list_company


def get_companies_involved_in_cb_df(df_carbon_bombs=None):
    """
    Loads the carbon bombs database, extracts the Parent Company column and
    splits it into multiple columns to provide detailed company participation
    in carbon bombs. Removes the percentage and extra spaces from Parent
    Company column and saves the result in a csv file.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing detailed company participation in carbon bombs.

    Notes
    -----
    - This function requires the pandas library to be installed.
    - The carbon bombs database must be available in the current working
    directory.
    """
    if df_carbon_bombs is None:
        df_carbon_bombs = load_carbon_bombs_database()
    df_carbon_bombs_company = df_carbon_bombs.loc[
        :,
        [
            "Carbon_bomb_name_source_CB",
            "Country_source_CB",
            "Companies_involved_source_GEM",
        ],
    ]

    # Force type of column Parent_Company
    df_carbon_bombs_company[
        "Companies_involved_source_GEM"
    ] = df_carbon_bombs_company.loc[:, "Companies_involved_source_GEM"].astype("str")

    split_rows = df_carbon_bombs_company.apply(split_column_parent_company, axis=1)

    # Create a dataframe with duplicates carbon bombs
    df = pd.concat([pd.DataFrame(rows) for rows in split_rows])
    df.reset_index(drop=True, inplace=True)

    # Remove percentage
    df["Company"] = df["Company"].apply(lambda x: "(".join(x.split("(")[:-1]))

    # Clean extra space from company column
    df["Company"] = df["Company"].str.strip()
    df["Company"] = np.where(df["Company"] == "None", "", df["Company"])

    # remove Others
    df = df.loc[~df["Company"].str.lower().str.contains("other")]

    # drop duplicates
    df = df.drop_duplicates()

    return df


def clean(text):
    """
    Cleans the given text by removing certain words and characters.
    Returns the cleaned text in lowercase form.

    Parameters
    ----------
    text : str
        The text to be cleaned.

    Returns
    -------
    str
        The cleaned text in lowercase form.

    Notes
    -----
    - This function requires the re library to be installed.
    - The list of banned words can be modified as per requirement.

    """
    # Define a list of word to be cleaned before comparing company names
    list_ban_word = [
        "Co",
        "Ltd",
        "Limited",
        "Corp",
        "Inc",
        "Resources",
        "Group",
        "Corporation",
        "SA",
        "Holding",
        "Company",
        "Industry",
        "industry",
        "Investment",
        "investment",
        "Tbk",
        "PT",
        "Persero",
        "(CNPC)" "&",
    ]
    split_text = text.split()
    split_text_cleaned = [word for word in split_text if word not in list_ban_word]
    text_cleaned = "".join(split_text_cleaned)
    text_cleaned = text_cleaned.lower()

    if "%" in text_cleaned:
        text_cleaned = re.sub(r"\(\s*\d+(\.\d+)?%\s*\)", "", text_cleaned)

    return text_cleaned


def _get_companies_match_cb_to_bocc(df_cb=None):
    """Link companies involved in carbon bombs with those in the banking
    sector from BOCC data source.

    Returns a dictionary with company names as keys and their corresponding
    matches as values.

    The function works by comparing a list of companies involved in
    "carbon bombs". The function uses fuzzy string matching to match
    company names in both lists and returns a dictionary of matched pairs.

    Parameters
    ----------
    df_cb: pd.DataFrame
        Carbon bombs dataframe. If None it loads from
        data_cleaned directory

    Returns
    -------
    dict:
        A dictionary with company names as keys and their corresponding
        matches as values.
    """
    df_cb = get_companies_involved_in_cb_df(df_cb)

    df_bocc = load_banking_climate_chaos()

    # Define threshold value in df_cb
    threshold = 90

    # Extract unique values from bocc
    list_bocc = df_bocc["Company"].unique()

    df_list_bocc = pd.DataFrame()
    df_list_bocc["Company BOCC"] = list_bocc
    df_list_bocc["Company BOCC_cleaned"] = df_list_bocc["Company BOCC"].apply(clean)

    dict_match = {}
    LOGGER.debug("Match CB company name with BOCC name with fuzzy score")
    for company in df_cb["Company"].unique():
        LOGGER.debug(f"{company}: try matching")
        company_cleaned = clean(company)
        df_list_bocc["fuzzy_score"] = df_list_bocc["Company BOCC_cleaned"].apply(
            lambda x: fuzz.ratio(x, company_cleaned)
        )
        max_fuzz = df_list_bocc["fuzzy_score"].max()

        if max_fuzz > threshold:
            company_matched = df_list_bocc.loc[
                df_list_bocc["fuzzy_score"] == max_fuzz, "Company BOCC"
            ].values[0]
            dict_match[company] = company_matched
            LOGGER.debug(f"{company}: match found with `{company_matched}`")
        else:
            LOGGER.debug(f"{company}: no match found")

    # Now we have dictionnary with auto matching, we had the manual match and
    # be cautious about not erasing key present in auto matching dict.
    for key, value in manual_match_company.items():
        # force manual matching
        if key in dict_match:
            dict_match[key] = value
        elif key in df_cb["Company"].unique():
            dict_match[key] = value

    return dict_match


def get_companies_match_cb_to_bocc(use_save_dict=False) -> dict:
    """Match company names from GEM Parent company column to
    BOCC company names.

    Parameters
    ----------
    use_save_dict : bool, optional
        Whether it use the saved uniform company
        names JSON file or not, by default False

    Returns
    -------
    dict
        Dictionary that match GEM company names (as key) and
        BOCC company names (as value)
    """
    if use_save_dict:
        dict_match = load_uniform_company_names()
    else:
        dict_match = _get_companies_match_cb_to_bocc()
        save_uniform_company_names(dict_match)

    return dict_match
