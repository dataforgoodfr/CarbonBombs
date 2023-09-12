"""Functions to load company dataset related"""
import pandas as pd

from carbon_bombs.conf import FPATH_SRC_COMP_ADDRESS
from carbon_bombs.conf import FPATH_SRC_COMP_LOGO
from carbon_bombs.io.uniform_company_names import load_uniform_company_names


def load_company_address_table() -> pd.DataFrame:
    """Return the company address table sourced by ChatGPT
    It contains the company name and the address from ChatGPT.

    Also it update company name with uniform_company_names created
    with connexion tables.

    In case of duplicates it keep the row with the address that has the biggest length.

    Returns
    -------
    pd.DataFrame
        Company address dataframe with the following columns:
        "Company" and "Address_source_chatGPT"
    """
    df_address = pd.read_csv(FPATH_SRC_COMP_ADDRESS, sep=";")

    match_dict = load_uniform_company_names()
    df_address["Company"] = df_address["Company"].replace(match_dict)

    df_address["column_len"] = (
        df_address["Address_source_chatGPT"].astype(str).apply(len)
    )
    df_address = df_address.sort_values(["Company", "column_len"])

    df_address = df_address.drop_duplicates("Company", keep="last")

    return df_address.drop(columns=["column_len"])


def load_company_logo() -> pd.DataFrame:
    """
    Select URL from URLs located in one or several column of the specified CSV
    file. Keep only rows with existing URLs.
    If URLs are located on several columns, this code prioritizes the URL of
    the first column provided by url_field, then the 2nd column if URL is
    missing in the first column, then the 3rd column...
    Example for url_field = ['Logo_Col1','Logo_Col2','Logo_Col3']:
    df['URL_logo'] = df['Logo_Col1'].fillna(df['Logo_Col2']) \
                                    .fillna(df['Logo_Col3'])

    Args:
        csv_file (str): The path and name of the CSV file.
        url_field (str or list): The column or list of columns
                                 (in priority order) where URLs are located.

    Returns:
        Dataframe with columns from csv_file and an additional column Logo_URL
        containing the selected URL of logos.
    """
    url_field = ["Logo_OfficialWebsite", "Logo_Wikipedia_Large", "Logo_OtherSource"]
    df = pd.read_csv(FPATH_SRC_COMP_LOGO, sep=",")

    # init logo url with first url field
    df["Logo_URL"] = df.loc[:, url_field[0]]

    # Fillna with next url_field
    for i in range(len(url_field) - 1):
        df["Logo_URL"] = df["Logo_URL"].fillna(df.loc[:, url_field[i + 1]])

    # drop rows with missing URL
    df = df.dropna(subset=["Logo_URL"])

    # drop duplicates and reset index
    df = df[["Company_name", "Logo_URL"]].drop_duplicates().reset_index(drop=True)

    return df
