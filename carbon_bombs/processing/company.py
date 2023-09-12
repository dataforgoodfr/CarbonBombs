"""Function to process companies information"""
import pandas as pd

from carbon_bombs.io.cleaned import load_company_database
from carbon_bombs.io.cleaned import load_connexion_cb_company_database
from carbon_bombs.io.company import load_company_address_table
from carbon_bombs.io.company import load_company_logo
from carbon_bombs.io.gmaps import get_coordinates_google_api
from carbon_bombs.processing.location_utils import get_country_from_geopy
from carbon_bombs.processing.location_utils import get_world_region


def _get_companies() -> pd.DataFrame:
    """Return dataframe with companies found in connexion carbon
    bombs / company. It also add a column "Carbon_bomb_name" to
    show all CB connected to the company

    Returns
    -------
    pd.DataFrame
        Company dataframe with following columns: "Company" and "Carbon_bomb_connected"
    """

    cnx_bank_comp = load_connexion_cb_company_database()

    df_companies = (
        cnx_bank_comp.groupby("Company")
        .agg(Carbon_bomb_connected=("Carbon_bomb_name", lambda x: ",".join(x)))
        .reset_index()
    )

    to_remove = [
        "Others",
        "other",
        "Other",
        "New project",
        "",
        "No informations on company",
    ]

    df_companies = df_companies.loc[~df_companies["Company"].isin(to_remove)]

    return df_companies


def create_company_table(check_old_df_address=False):
    """
    Scrapes the address information of companies using chatGPT and retrieves
    their corresponding geographic coordinates using the Google Maps API.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns:
        - Company_name (str): Name of the company
        - Address_headquarters_source_chatGPT (str): Address of the company
            headquarters obtained from chatGPT
        - Latitude (float): Latitude of the company headquarters obtained using
            Google Maps
        - Longitude (float): Longitude of the company headquarters obtained
            using Google Maps

    Raises:
        Exception: If the file path for the chatGPT data is invalid.
        Exception: If there is an error in obtaining coordinates from the
        Google Maps API.
    """
    df_companies = _get_companies()
    df_address = load_company_address_table()

    # If we want to check a change of the address then first we load old company dataframe
    if check_old_df_address:
        old_comp_df = load_company_database()

    # merge found companies with address
    df = df_companies.merge(df_address, on="Company", how="left")
    df.columns = [
        "Company_name",
        "Carbon_bomb_connected",
        "Address_headquarters_source_chatGPT",
    ]

    def _get_lat_long(row):
        address_maps = row["Address_headquarters_source_chatGPT"]

        if pd.isna(address_maps):
            return 0.0, 0.0

        # To avoid calling GMAPS API we check if old adress is equal to the new one
        # if so then we dont call GMAPS API
        if check_old_df_address:
            old_raw = old_comp_df.loc[
                old_comp_df["Company_name"] == row["Company_name"]
            ]
            # if no match then get lat, long from GMAPS
            if len(old_raw) == 0:
                return get_coordinates_google_api(address_maps)

            # if match then compare address and use old if addresses are equal
            else:
                old_raw = old_raw.iloc[0]
                old_address = old_raw["Address_headquarters_source_chatGPT"]

                if old_address == address_maps:
                    return old_raw["Latitude"], old_raw["Longitude"]

                else:
                    return get_coordinates_google_api(address_maps)

        return get_coordinates_google_api(address_maps)

    df[["Latitude", "Longitude"]] = df.apply(_get_lat_long, axis=1).tolist()
    df["Country"] = df.apply(
        lambda row: get_country_from_geopy(row["Latitude"], row["Longitude"]), axis=1
    )
    df["World_region"] = df["Country"].apply(get_world_region)

    # load logo dataframes
    df_logos = load_company_logo()

    # merge into company dataframe
    df = pd.merge(df, df_logos, on="Company_name", how="left")

    columns_order = [
        "Company_name",
        "Address_headquarters_source_chatGPT",
        "Latitude",
        "Longitude",
        "Carbon_bomb_connected",
        "Country",
        "World_region",
        "Logo_URL",
    ]
    # sort and order df
    df = df[columns_order].sort_values(by="Company_name", ascending=True)

    return df
