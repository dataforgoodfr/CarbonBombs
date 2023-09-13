import sys

import requests
from bs4 import BeautifulSoup

from carbon_bombs.utils.logger import LOGGER


BANKTRACKS_URL = "https://www.banktrack.org/banks"

# List of categories in section About
BANK_DESC_INFO_TAGS = [
    "Website",
    "Headquarters",
    "CEO/chair",
    "Supervisor",
    "Ownership",
]


def get_url_from_banktrack_div(div) -> str:
    """
    Return the url given a div found into banktrack website

    With this input:
    `<div
        class="bank-logo"
        style="background-image: url('https://www.banktrack.org/thumbimage.php?image=abn-logo.jpg;width=190');"
    ></div>`

    Return this:
    https://www.banktrack.org/thumbimage.php?image=abn-logo.jpg&amp
    """
    div = str(div)
    url = div.split("url('")[1]
    url = url.split(";")[0]

    return url


def scrapping_main_page_bank_track():
    """
    Scrapes the main page of bank track website and extracts URLs of individual
    bank description pages.

    Args:
        url (str): The URL of the main page to be scraped.

    Returns:
        list: A list of URLs (strings) pointing to individual bank description
              pages.
        list: A list of logo urls for each bank

    Raises:
        None: However, if the HTTP request fails, it prints an error message and
              terminates the script.

    Example:
        >>> scrapping_main_page_bank_track("https://www.examplebanktrack.org")
        ['https://www.examplebanktrack.org/bank/abc_bank',
         'https://www.examplebanktrack.org/bank/def_bank',
         'https://www.examplebanktrack.org/bank/ghi_bank']
    """
    # Send an HTTP request to the target URL
    LOGGER.debug(f"Request banktracks url: {BANKTRACKS_URL}")
    response = requests.get(BANKTRACKS_URL)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code != 200:
        print(
            f"Failed to fetch the page. HTTP status: {response.status_code} "
            "Please try again later"
        )
        sys.exit()

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    banks = soup.find_all("a", {"class": "bankprofile"})

    logos = [b.find("div", {"class", "bank-logo"}) for b in banks]
    logos = [get_url_from_banktrack_div(l) for l in logos]

    bank_description_url = [b.get("href") for b in banks]
    bank_names = [b.find("h1", {"class": "ddlist"}).text for b in banks]

    return bank_names, bank_description_url, logos


def scrapping_description_bank_page(url):
    """
    Scrapes the specified URL for bank information and returns the bank name
    and a dictionary containing details from the About section.

    Args:
        url (str): The URL of the bank's page to be scraped.

    Returns:
        dict: A dictionary with
               information from the About section. The dictionary has keys
               corresponding to the following categories: "Website",
               "Headquarters", "CEO/chair", "Supervisor", and "Ownership".
               If information is not available, the value is set to "None".

    Raises:
        None: However, if the HTTP request fails, it prints an error message and
              terminates the script.

    Example:
        >>> fetch_bank_info("https://www.examplebank.org/bank/abc_bank")
        {'Website': 'www.abcbank.com',
        'Headquarters': 'New York, NY',
        'CEO/chair': 'John Doe', 'Supervisor': 'Jane Smith',
        'Ownership': 'Private'}
    """
    LOGGER.debug(f"Request bank page details of {url}")

    # Send an HTTP request to the target URL
    response = requests.get(url)
    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Initiate dictionary to concatenate info
        dict_about_section = dict()

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        for info_name in BANK_DESC_INFO_TAGS:
            # Detect the tag
            tag_elt = soup.find("td", string=info_name)
            if tag_elt:
                # Retrieve content of the next td tag
                tag_content = tag_elt.find_next_sibling("td")
                # Add this element to dictionnary
                dict_about_section[info_name] = tag_content
            else:
                # No informations available in About section
                dict_about_section[info_name] = "None"
    else:
        LOGGER.error(
            f"Failed to fetch the page for {url}. HTTP status: {response.status_code}."
        )
        return None, {}

    return dict_about_section
