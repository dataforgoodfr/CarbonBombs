"""Function to check if manual match data are valid or not"""

import pandas as pd

from carbon_bombs.conf import FPATH_SRC_GEM_COAL
from carbon_bombs.conf import FPATH_SRC_GEM_GASOIL
from carbon_bombs.io.banking_climate_chaos import load_banking_climate_chaos
from carbon_bombs.io.khune_paper import load_carbon_bomb_coal_database
from carbon_bombs.io.khune_paper import load_carbon_bomb_gasoil_database
from carbon_bombs.io.manual_match import manual_match_bank
from carbon_bombs.io.manual_match import manual_match_coal
from carbon_bombs.io.manual_match import manual_match_company
from carbon_bombs.io.manual_match import manual_match_gasoil


# ========= #
# Load data #
# ========= #

# GEM source
# gem_coal_df = pd.read_excel(FPATH_SRC_GEM_COAL, sheet_name="Global Coal Mine Tracker")
gem_coal_df = pd.read_excel(
    FPATH_SRC_GEM_COAL, sheet_name="Global Coal Mine Tracker (Non-C"
)
gem_gasoil_df = pd.read_excel(
    FPATH_SRC_GEM_GASOIL, sheet_name="Main data", engine="openpyxl"
)

# paper source
gasoil = load_carbon_bomb_gasoil_database()
coal = load_carbon_bomb_coal_database()
cb_df = pd.concat([gasoil, coal])

# bank source
bocc_df = load_banking_climate_chaos()

# =============== #
# Check functions #
# =============== #


def _check_manual_match_gem_id(manual_match, fuel):
    """Check GEM units / mines manual match data validity.

    Return a string explaining if something is wrong or not.
    """
    res_txt = ""
    if fuel == "coal":
        names = gem_coal_df["Mine Name"].unique()
    else:
        names = gem_gasoil_df["Unit name"].unique()

    for cb, units in manual_match.items():
        for unit in units.split("$"):
            if unit not in names and unit not in ["None", ""]:
                res_txt += f"⚠️ GEM {fuel} check: `{cb}` - unit not found: `{unit}`\n"

    return res_txt


def check_manual_match_gem_id():
    """Check GEM units / mines manual match data validity.

    Return a string explaining if something is wrong or not.
    """
    res_txt = _check_manual_match_gem_id(manual_match_coal, fuel="coal")
    res_txt += _check_manual_match_gem_id(manual_match_gasoil, fuel="gasoil")

    return res_txt


def check_manual_match_banks():
    """Check banks manual match data validity.

    Return a string explaining if something is wrong or not.
    """
    res_txt = ""
    for value in set(manual_match_bank.values()):
        if value not in bocc_df["Bank"].unique():
            res_txt += f"⚠️ bank check: `{value}` not in BOCC Bank\n"

    return res_txt


def check_manual_match_companies():
    """Check companies manual match data validity

    Return a string explaining if something is wrong or not.
    """
    res_txt = ""
    for value in set(manual_match_company.values()):
        if value not in bocc_df["Company"].unique():
            res_txt += f"⚠️ company check: `{value}` not in BOCC Companies\n"

    return res_txt


def check_manual_match():
    """Check manual match data validity

    Return a string explaining if something is wrong or not.
    """
    res = "===== Check manual match data =====\n"

    res += "\nCHECK GEM MANUAL MATCH\n"
    res_gem = check_manual_match_gem_id()
    if res_gem:
        res += res_gem
    else:
        res += "✅ GEM check: everything OK\n"

    res += "\nCHECK BANK MANUAL MATCH\n"
    res_bank = check_manual_match_banks()
    if res_bank:
        res += res_bank
    else:
        res += "✅ bank check: everything OK\n"

    res += "\nCHECK COMPANY MANUAL MATCH\n"
    res_comp = check_manual_match_companies()
    if res_comp:
        res += res_comp
    else:
        res += "✅ company check: everything OK\n"

    res += "\n"

    return res
