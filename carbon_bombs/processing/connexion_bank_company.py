"""Function to create connexion between banks and companies"""
from carbon_bombs.io.banking_climate_chaos import load_banking_climate_chaos
from carbon_bombs.utils.logger import LOGGER
from carbon_bombs.utils.match_company_bocc import get_companies_match_cb_to_bocc


def create_connexion_bank_company_table(use_save_dict=False):
    """Create the dataframe of the connexions between banks and companies.

        Banks are found using Banking On Climate Chaos (BOCC) source.
    _get_companies_match_cb_to_bocc
        Companies are from the Carbon bombs information source. Then it maps
        companies that appears in BOCC with the BOCC name to uniform between files.
    """
    LOGGER.debug("Start creation of connexion between banks and companies dataset")
    # Load BOCC Database
    LOGGER.debug("Load BOCC dataset")
    df_bocc = load_banking_climate_chaos()

    LOGGER.debug("Load matching company names in CB dataset with BOCC names")
    company_cb_bocc = get_companies_match_cb_to_bocc(use_save_dict=use_save_dict)

    filtered_company = list(company_cb_bocc.values())
    LOGGER.debug("Keep only matching banks")
    df_bocc = df_bocc.loc[df_bocc["Company"].isin(filtered_company)]

    # sort df
    LOGGER.debug("Sort dataset by Bank name")
    df_bocc = df_bocc.sort_values(by="Bank", ascending=True)

    return df_bocc
