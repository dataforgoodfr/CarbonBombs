"""Utils for location"""

# import awoc
import country_converter as coco
import re
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


def clean_project_names_with_iso(df, column_name="Project_name"):
    """
    Clean the project names by removing ISO codes,
    while preserving the original values in a new column.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing project names
    column_name : str, optional
        Name of the column containing project names, by default "Project_name"
    """
    # Match pattern: comma followed by optional space and 2 uppercase letters at the end
    iso_pattern = r",\s*[A-Z]{2}$"

    # Preserve original values
    df["Project_name_raw"] = df[column_name]

    # Clean the project name in place
    df[column_name] = df[column_name].apply(
        lambda x: re.sub(iso_pattern, "", x)
        if isinstance(x, str) and re.search(iso_pattern, x)
        else x
    )
