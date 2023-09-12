import os

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["GMAPS_API_KEY"]


def get_coordinates_google_api(address):
    """
    Get the latitude and longitude coordinates of an address using Google Maps
    API.

    Args:
        address (str): The address to look up.
        api_key (str): The Google Maps API key.

    Returns:
        tuple: A tuple containing the latitude and longitude coordinates
               of the address.

    Raises:
        SystemExit: If an error occurs with the API call.

    Example:
        >>> get_coordinates_google_api(
        ...     "1600 Amphitheatre Parkway, Mountain View, CA")
        (37.4224764, -122.0842499)

    """
    if address.startswith("None"):
        return 0.0, 0.0

    print("API CALL for this address:", address)

    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_KEY}"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["status"] == "OK":
            loc = data["results"][0]["geometry"]["location"]
            latitude = loc["lat"]
            longitude = loc["lng"]
        else:
            print(f"API Error for {address}. Please try again later")
            latitude, longitude = 0.0, 0.0
    else:
        print(f"API Error for {address}. Please try again later")
        latitude, longitude = 0.0, 0.0

    return latitude, longitude
