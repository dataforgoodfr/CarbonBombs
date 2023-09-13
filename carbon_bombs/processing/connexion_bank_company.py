from carbon_bombs.io.banking_climate_chaos import load_banking_climate_chaos
from carbon_bombs.utils.connexion import get_companies_match_cb_to_bocc


def create_connexion_bank_company_table(use_save_dict=False):
    # Load BOCC Database
    df_bocc = load_banking_climate_chaos()

    company_cb_bocc = get_companies_match_cb_to_bocc(use_save_dict=use_save_dict)

    filtered_company = list(company_cb_bocc.values())
    df_bocc = df_bocc.loc[df_bocc["Company"].isin(filtered_company)]

    # sort df
    df = df.sort_values(by="Bank", ascending=True)

    return df_bocc
