"""Utils for location"""

# import awoc
import country_converter as coco
from geopy.geocoders import Nominatim


# load geolocator from geopy to get country based on lat, long
geolocator = Nominatim(user_agent="my_app")


def get_world_region(country: str) -> str:
    """Return the continent name of a specific country.
    If "None" pass then it returns "None"

    Parameters
    ----------
    country : str
        Country name

    Returns
    -------
    str
        Continent name
    """
    if country == "None":
        return "None"

    try:
        return coco.convert(names=country, to="Continent")

    # If country string does not exist then return empty string
    except NameError:
        return ""


def get_country_from_geopy(lat: float, long: float) -> str:
    """Return a country given a latitude and a longitude.
    It's based on geopy package

    Parameters
    ----------
    lat : float
        Latitude
    long : float
        Longitude

    Returns
    -------
    str
        Country name (if nothing found then return "")
    """
    # Add country associated to the coordinates
    if lat == 0 and long == 0:
        return ""
    location = geolocator.reverse([lat, long], exactly_one=True, language="en")

    if location is None:
        return ""

    address = location.raw["address"]
    country = address.get("country", "")
    return country
