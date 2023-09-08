from carbon_bombs.processing.connexion_utils import get_companies_match_cb_to_bocc
from carbon_bombs.readers.banking_climate_chaos import load_banking_climate_chaos


def create_connexion_bank_company_table():
    # Load BOCC Database
    df_bocc = load_banking_climate_chaos()

    company_cb_bocc = get_companies_match_cb_to_bocc()

    filtered_company = list(company_cb_bocc.values())
    df_bocc = df_bocc.loc[df_bocc["Company"].isin(filtered_company)]

    return df_bocc
