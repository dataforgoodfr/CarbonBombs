"""Function to process banks information"""
import pandas as pd

from carbon_bombs.io.banktracks import scrapping_description_bank_page
from carbon_bombs.io.banktracks import scrapping_main_page_bank_track
from carbon_bombs.io.cleaned import load_banks_database
from carbon_bombs.io.cleaned import load_connexion_bank_company_database
from carbon_bombs.io.gmaps import get_coordinates_google_api
from carbon_bombs.io.manual_match import manual_match_bank
from carbon_bombs.utils.location import get_world_region
from carbon_bombs.utils.logger import LOGGER


def process_raw_info(dict_info):
    """
    Process a dictionary of raw information and return a cleaned dictionary.

    Args:
        dict_info (dict): A dictionary containing raw information.

    Returns:
        dict: A dictionary containing cleaned information.

    Raises:
        SystemExit: If the function cannot recognize the raw information.

    Example:
        >>> dict_info = {"Website": "<a href='https://www.example.com'>Example</a>"}
        >>> process_raw_info(dict_info)
        {'Bank Website': 'Example'}

    """
    # Instanciate clean_dict that will contains info extracted from raw
    clean_dict = {
        "Bank Website": "None",
        "Headquarters address": "None",
        "Headquarters country": "None",
        "CEO Name": "None",
        "Board description": "None",
        "Supervisor Name": "None",
        "Supervisor Website": "None",
        "Shareholder structure source": "None",
    }

    if dict_info["Website"] != "None":
        clean_dict["Bank Website"] = dict_info["Website"].find("a").text

    if dict_info["Headquarters"] != "None":
        # Example for full_address
        # [<div>Gustav Mahlerlaan 10</div>, <div> 1082 PP Amsterdam</div>, <div>Netherlands</div>]
        full_address = dict_info["Headquarters"].find_all("div")

        address = f"{full_address[0].text.strip()},{full_address[1].text.strip()}"
        country = full_address[-1].text.strip()

        clean_dict["Headquarters address"] = address
        clean_dict["Headquarters country"] = country

    if dict_info["CEO/chair"] != "None":
        a_tag = dict_info["CEO/chair"].find("a")
        # if a_tag not empty
        if a_tag:
            # Extract the URL, see an example:
            # <a href="http://www.abnamro.nl/en/index.html" target="_blank">http://www.abnamro.nl/en/index.html</a>
            url_address = a_tag["href"]
            # Extract the CEO's name
            name_ceo = a_tag.text

            clean_dict["CEO Name"] = name_ceo
            clean_dict["Board description"] = url_address

    if dict_info["Supervisor"] != "None":
        # example of a wanted anchor
        # <a href="http://www.rba.gov.au/" target="_blank">Reserve Bank of Australia</a>
        anchor = dict_info["Supervisor"].find("a")

        if anchor and not isinstance(anchor, int):
            clean_dict["Supervisor Name"] = anchor.text
            clean_dict["Supervisor Website"] = anchor["href"]

    if dict_info["Ownership"] != "None":
        url = dict_info["Ownership"].find("a")
        # if url not empty
        if url:
            clean_dict["Shareholder structure source"] = url["href"]

    # Return cleaned dictionary
    return clean_dict


def create_banks_table(check_old_df_address=False):
    """
    Create a Pandas DataFrame from scraped information and return it.

    Args:
        url (str): The URL of the main page to scrape.

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing information on bank
        companies.

    Example:
        >>> url = "https://example.com"
        >>> create_bank_dataframe(url)
            Bank Name  Bank Website  Headquarters address  Headquarters country
        0    Example Bank www.example.com 123 Main St, Anytown  USA
        1    Another Bank www.anotherbank.com 456 Oak St, Anycity  USA
        ...
    """
    LOGGER.debug("Start creation of banks dataset")
    LOGGER.debug("Get banks name connected to companies")
    cnx_bank_comp = load_connexion_bank_company_database()
    banks_in_bocc = cnx_bank_comp["Bank"].unique()

    # Instanciate dataframe containing srapped info
    columns_dataframe = [
        "Bank Name",
        "Bank Website",
        "Headquarters address",
        "Headquarters country",
        "CEO Name",
        "Board description",
        "Supervisor Name",
        "Supervisor Website",
        "Shareholder structure source",
        "Source BankTrack",
        "Latitude",
        "Longitude",
    ]
    df = pd.DataFrame(columns=columns_dataframe)
    LOGGER.debug("Scrap all banks from banktracks website")
    bank_names, bank_list_url, bank_logos = scrapping_main_page_bank_track()

    # Make a remap of bank name based on manual_match_bank in order to have
    # coherent key values in BOCC and banking_informations.csv
    bank_names = [
        manual_match_bank[name] if name in manual_match_bank else name
        for name in bank_names
    ]

    # If we want to check a change of the address then first we load old bank dataframe
    if check_old_df_address:
        LOGGER.debug(
            "Load old bank dataset to avoid calling GMAPS API when no change in the address"
        )
        old_bank_df = load_banks_database()

    for bank_name, bank_url, logo in zip(bank_names, bank_list_url, bank_logos):
        # if bank name not in banks find in BOCC then dont scrap the content
        if bank_name not in banks_in_bocc:
            continue
        LOGGER.debug(f"{bank_name}: found in BOCC banks, scrap details from bank page")

        raw_info = scrapping_description_bank_page(bank_url)
        clean_info = process_raw_info(raw_info)

        address_maps = f"{clean_info['Headquarters address']}, {clean_info['Headquarters country']}"

        # To avoid calling GMAPS API we check if old adress is equal to the new one
        # if so then we dont call GMAPS API
        if check_old_df_address:
            old_raw = old_bank_df.loc[old_bank_df["Source BankTrack"] == bank_url]
            # if no match then get lat, long from GMAPS
            if len(old_raw) == 0:
                LOGGER.debug(f"{bank_name}: no match found on old bank dataset")
                lat, long = get_coordinates_google_api(address_maps)

            # if match then compare address and use old if addresses are equal
            else:
                old_raw = old_raw.iloc[0]
                old_address = f"{old_raw['Headquarters address']}, {old_raw['Headquarters country']}"

                if old_address == address_maps:
                    LOGGER.debug(
                        f"{bank_name}: same address found, use old latitude and longitude"
                    )
                    lat, long = old_raw["Latitude"], old_raw["Longitude"]

                else:
                    LOGGER.debug(f"{bank_name}: different address found, use GMAPS API")
                    lat, long = get_coordinates_google_api(address_maps)

        else:
            LOGGER.debug(f"{bank_name}: use GMAPS API to find latitude and longitude")
            lat, long = get_coordinates_google_api(address_maps)

        clean_info["Latitude"] = lat
        clean_info["Longitude"] = long
        clean_info["Bank Name"] = bank_name
        clean_info["Source BankTrack"] = bank_url
        clean_info["Bank logo"] = logo

        # Convert dictionary into Dataframe
        bank_data_df = pd.DataFrame(clean_info, index=[0])

        # Concat dataframes
        df = pd.concat([df, bank_data_df], ignore_index=True)

    # Remap some country name
    df["Headquarters country"] = df["Headquarters country"].replace(
        {
            "Taiwan, Republic of China": "Taiwan",
            "Russian Federation": "Russia",
        }
    )

    # Add World Region associated to Headquarters country
    LOGGER.debug("Get world region using Headquarters country column")
    df["World Region"] = df["Headquarters country"].apply(get_world_region)

    # sort df
    LOGGER.debug("Sort dataset by bank name")
    df = df.sort_values(by="Bank Name", ascending=True)

    # Return dataframe with all info on bank companies
    return df
