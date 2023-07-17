import itertools
import pandas as pd

from carbon_bomb import create_carbon_bombs_table



def get_companies() -> list:
    """Retrieve all companies name from create_carbon_bombs_table function

    Returns
    -------
    list
        list of companies name
    """

    # Get all the carbon bombs
    df_cb = create_carbon_bombs_table()

    # Retrieve only the companies name without percentage
    companies = df_cb["Parent_company_source_GEM"].str.split(';').values
    companies = list(itertools.chain(*companies))
    companies = list(map(lambda x: "(".join(x.split('(')[:-1]).strip(), companies))

    companies = sorted(list(set([c for c in companies if c not in ["Others", ""]])))

    return companies


def create_hq_dataframe() -> pd.DataFrame:
    """Create the companies headquarter adress dataframe with
    all the companies and Address_source_chatGPT column initialized with empty strings

    Returns
    -------
    pd.DataFrame
        Initialized dataframe with Company column listing all companies
        retrieved and Address_source_chatGPT column initialized with empty strings
    """

    companies = get_companies()

    df = pd.DataFrame(companies, columns=["Company"])
    df["Address_source_chatGPT"] = ""

    return df


if __name__ == "__main__":
    df_comp_hq = create_hq_dataframe()
