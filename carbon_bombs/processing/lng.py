"""Function to process lng informations from GOGEL"""
import pandas as pd
import numpy as np
import country_converter as coco
from carbon_bombs.io.gogel import load_lng_database
from carbon_bombs.conf import DATA_SOURCE_PATH
from carbon_bombs.utils.logger import LOGGER


def create_lng_table():
    """
    Load LNG data from GOGEL source, and add country location (latitude and longitude).

    Returns:
    pandas.DataFrame:
        A processed DataFrame with the following columns:
            - Project_name (str): Name of the LNG project.
            - Export_capacity_in_Mtpa (float): Export capacity of the project in million tonnes per annum (Mtpa).
            - Project_status (str): Current status of the project.
            - Country (str): Country where the project is located.
            - Companies_involved (str): Companies participating in the project.
            - Latitude (float): Latitude coordinate of the project's country location.
            - Longitude (float): Longitude coordinate of the project's country location.
    """
    LOGGER.debug("Read LNG source: LNG Liquefaction projects")
    df_lng = load_lng_database()
    country_lat_long_df = pd.read_csv(f"{DATA_SOURCE_PATH}/longitude-latitude.csv")

    LOGGER.debug("Add LNG project's country location")
    df_lng[["Latitude", "Longitude"]] = df_lng["Country"].apply(
        lambda x: pd.Series(_get_lat_long(x, country_lat_long_df))
    )

    # Add noise to duplicate lat/long
    np.random.seed(42)
    lat_long_dup = df_lng.duplicated(subset=["Latitude", "Longitude"], keep=False)
    df_lng.loc[lat_long_dup, "Latitude"] = df_lng.loc[lat_long_dup, "Latitude"].apply(
        _add_noise_lat_long
    )
    df_lng.loc[lat_long_dup, "Longitude"] = df_lng.loc[lat_long_dup, "Longitude"].apply(
        _add_noise_lat_long
    )
    LOGGER.debug("Success adding LNG project's country location")
    return df_lng


def _get_lat_long(country: str, country_lat_long_df: pd.DataFrame):
    """Retrieve latitude and longitude for a given country."""
    country_iso3 = coco.convert(names=country, to="ISO3")
    if isinstance(
        country_iso3, list
    ):  # Convert list to string if necessary (example Senegal/Mauritania)
        country_iso3 = country_iso3[0]
    country_loc = country_lat_long_df.loc[
        country_lat_long_df["ISO-ALPHA-3"] == country_iso3
    ]
    if country_loc.empty:
        LOGGER.info(f"Country `{country}`: no lat, long found")
        return 0, 0

    return country_loc["Latitude"].values[0], country_loc["Longitude"].values[0]


def _add_noise_lat_long(x: float) -> float:
    """Add a random noise on the 2nd decimal of a number."""
    return x + (np.random.choice([1, -1]) * np.random.rand() / 10)
