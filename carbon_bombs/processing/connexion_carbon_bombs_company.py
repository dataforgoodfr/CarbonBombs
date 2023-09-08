from carbon_bombs.processing.connexion_utils import get_companies_involved_in_cb_df
from carbon_bombs.processing.connexion_utils import get_companies_match_cb_to_bocc


def create_connexion_cb_company_table():
    df_cb = get_companies_involved_in_cb_df()

    company_cb_bocc = get_companies_match_cb_to_bocc()

    df_cb["Company"] = df_cb["Company"].replace(company_cb_bocc)

    return df_cb
