"""Function to create connexion between carbon bombs and companies"""
from carbon_bombs.utils.logger import LOGGER
from carbon_bombs.utils.match_company_bocc import get_companies_involved_in_cb_df
from carbon_bombs.utils.match_company_bocc import get_companies_match_cb_to_bocc


def create_connexion_cb_company_table(use_save_dict=False):
    """Create the dataframe of the connexions between Carbon bombs and
    companies.

    Companies are from the Carbon bombs information source. Then it maps
    companies that appears in BOCC with the BOCC name to uniform between files.
    """
    LOGGER.debug(
        "Start creation of connexion between carbon bombs and companies dataset"
    )
    LOGGER.debug("Get companies involved in a CB")
    df_cb = get_companies_involved_in_cb_df()

    LOGGER.debug("Load matching company names in CB dataset with BOCC names")
    company_cb_bocc = get_companies_match_cb_to_bocc(use_save_dict=use_save_dict)

    LOGGER.debug("Replace company names that are in matching dictionary")
    df_cb["Company"] = df_cb["Company"].replace(company_cb_bocc)

    # sort df
    LOGGER.debug("Sort dataset by carbon bomb names")
    df_cb = df_cb.sort_values(by="Carbon_bomb_name", ascending=True)

    return df_cb
