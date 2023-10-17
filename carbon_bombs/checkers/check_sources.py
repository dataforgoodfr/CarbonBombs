"""Function to check if sources were updated or not"""
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

from carbon_bombs.conf import FPATH_SRC_BOCC
from carbon_bombs.utils.logger import LOGGER


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
}

# BOCC informations
BOCC_URL = "https://www.bankingonclimatechaos.org/"
CURRENT_BOCC_DATA_URL = "https://www.bankingonclimatechaos.org/wp-content/themes/bocc-2021/inc/bcc-data-2023/GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx"
TMP_BOCC_DATA_FPATH = "tmp.xlsx"

# GEM informations
GEM_COAL_URL = (
    "https://globalenergymonitor.org/projects/global-coal-mine-tracker/download-data/"
)
GEM_COAL_WORD_TO_FIND = "October 2023"
GEM_GASOIL_URL = "https://globalenergymonitor.org/projects/global-oil-gas-extraction-tracker/download-data/"
GEM_GASOIL_WORD_TO_FIND = "July 2023"


def check_bocc_source_updated():
    """Check that BOCC data is up to date.

    Return a string explaining if something changed or not.
    """
    res_txt = ""

    response = requests.get(BOCC_URL, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # find url to download dataset
    fulldata = soup.find("section", {"id": "fulldata-panel"})
    url_data = fulldata.find("div", {"class": "download-data"}).find("a").get("href")

    # check if url changed
    if CURRENT_BOCC_DATA_URL != url_data:
        res_txt += f"⚠️BOCC check: URL changed\n"
        res_txt += (
            f"⚠️ BOCC check: Please download the new dataset... (url = {url_data})\n"
        )
        return res_txt

    try:
        s = requests.get(CURRENT_BOCC_DATA_URL, headers=headers).content
        with open(TMP_BOCC_DATA_FPATH, "wb") as f:
            f.write(s)

        # read new dataset
        df = pd.read_excel(
            TMP_BOCC_DATA_FPATH, sheet_name="Financing (USD)", engine="openpyxl"
        )
        os.remove(TMP_BOCC_DATA_FPATH)

        # read old dataset
        old_df = pd.read_excel(
            FPATH_SRC_BOCC, sheet_name="Financing (USD)", engine="openpyxl"
        )

        # compare them
        if not df.equals(old_df):
            comp = df.compare(old_df)
            res_txt += (
                f"⚠️ BOCC check: data was updated. (n rows concerned = {len(comp)})\n"
            )
            res_txt += f"⚠️ BOCC check: Please download the new dataset... (url = {CURRENT_BOCC_DATA_URL})\n"
            return res_txt

    except Exception as e:
        LOGGER.error(e)
        return f"⚠️ ERROR DURING BOCC CHECK: {e}\n"

    return "✅ BOCC check: DATA OK\n"


def _check_gem_source_updated(fuel):
    """Check that GEM data is up to date given a fuel type.

    Return a string explaining if something changed or not.
    """
    res_txt = ""

    if fuel == "coal":
        url = GEM_COAL_URL
        word_to_find = GEM_COAL_WORD_TO_FIND
    else:
        url = GEM_GASOIL_URL
        word_to_find = GEM_GASOIL_WORD_TO_FIND

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    main = soup.find("main", {"id": "main"})

    if word_to_find not in main.text:
        res_txt += f"⚠️ GEM {fuel} check: data was updated.\n"
        res_txt += f"⚠️ GEM {fuel} check: Please download the new dataset...\n"
    else:
        res_txt += f"✅ GEM {fuel} check: DATA OK\n"

    return res_txt


def check_gem_source_updated():
    """Check that GEM data is up to date.

    Return a string explaining if something changed or not.
    """
    res_txt = _check_gem_source_updated(fuel="coal")
    res_txt += _check_gem_source_updated(fuel="gasoil")

    return res_txt


def check_data_sources():
    """Check data sources freshness

    Return a string explaining if something changed or not.
    """
    res = "===== Check data sources =====\n"

    res += "\nCHECK BOCC SOURCE\n"
    res += check_bocc_source_updated()

    res += "\nCHECK GEM SOURCE\n"
    res += check_gem_source_updated()
    res += "\n"

    return res
