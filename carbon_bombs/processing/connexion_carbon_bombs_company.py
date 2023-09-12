from carbon_bombs.processing.connexion_utils import get_companies_involved_in_cb_df
from carbon_bombs.processing.connexion_utils import get_companies_match_cb_to_bocc


def create_connexion_cb_company_table(use_save_dict=False):
    df_cb = get_companies_involved_in_cb_df()

    company_cb_bocc = get_companies_match_cb_to_bocc(use_save_dict=use_save_dict)

    df_cb["Company"] = df_cb["Company"].replace(company_cb_bocc)

    return df_cb
