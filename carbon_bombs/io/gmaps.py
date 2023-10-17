"""Functions to call GMAPS API"""
import os

import requests
from dotenv import load_dotenv

from carbon_bombs.conf import REPO_PATH
from carbon_bombs.utils.logger import LOGGER


load_dotenv(f"{REPO_PATH}/.env")

if "GMAPS_API_KEY" in os.environ:
    API_KEY = os.environ["GMAPS_API_KEY"]
else:
    LOGGER.warning("No API key found in .env (GMAPS_API_KEY)")
    API_KEY = ""


def get_coordinates_google_api(address):
    """
    Get the latitude and longitude coordinates of an address using Google Maps
    API.

    Parameters
    ----------
    address: str
        The address to look up.
    api_key: str
        The Google Maps API key.

    Returns
    -------
    tuple:
        A tuple containing the latitude and longitude coordinates
        of the address.

    Examples
    --------
    >>> get_coordinates_google_api(
    ...     "1600 Amphitheatre Parkway, Mountain View, CA")
    (37.4224764, -122.0842499)
    """
    if address.startswith("None") or API_KEY == "":
        return 0.0, 0.0

    LOGGER.warning(f"Request on GMAPS API for: {address}")

    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_KEY}"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["status"] == "OK":
            loc = data["results"][0]["geometry"]["location"]
            latitude = loc["lat"]
            longitude = loc["lng"]
        else:
            LOGGER.error(f"API Error for {address}")
            latitude, longitude = 0.0, 0.0
    else:
        LOGGER.error(f"API Error for {address}")
        latitude, longitude = 0.0, 0.0

    return latitude, longitude
