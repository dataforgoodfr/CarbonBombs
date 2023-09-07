from os import pardir
from os.path import abspath
from os.path import join

# Path to the repository
REPO_PATH = abspath(join(join(__file__, pardir), pardir))

# Path to retrieve or store the data
DATA_SOURCE_PATH = f"{REPO_PATH}/data_sources"
DATA_CLEANED_PATH = f"{REPO_PATH}/data_cleaned"

# Files names of sources
FPATH_SRC_KHUNE_PAPER = f"{DATA_SOURCE_PATH}/1-s2.0-S0301421522001756-mmc2.xlsx"
FPATH_SRC_GEM_COAL = f"{DATA_SOURCE_PATH}/Global-Coal-Mine-Tracker-April-2023.xlsx"
FPATH_SRC_GEM_GASOIL = (
    f"{DATA_SOURCE_PATH}/Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx"
)
FPATH_SRC_BOCC = f"{DATA_SOURCE_PATH}/GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx"
FPATH_SRC_COMP_URL = f"{DATA_SOURCE_PATH}/company_url.csv"
FPATH_SRC_METADATAS = f"{DATA_SOURCE_PATH}/metadatas.csv"
FPATH_SRC_UNDATA_POPU = (
    f"{DATA_SOURCE_PATH}/undata_SYB65_1_202209_Population, Surface Area and Density.csv"
)
FPATH_SRC_UNDATA_GDP = (
    f"{DATA_SOURCE_PATH}/undata_SYB65_230_202209_GDP and GDP Per Capita.csv"
)
FPATH_SRC_UNDATA_CO2 = (
    f"{DATA_SOURCE_PATH}/undata_SYB65_310_202209_Carbon Dioxide Emission Estimates.csv"
)

# Files names of outputs
FPATH_OUT_BANK = f"{DATA_CLEANED_PATH}/bank_informations.csv"
FPATH_OUT_COMP = f"{DATA_CLEANED_PATH}/company_informations.csv"
FPATH_OUT_CB = f"{DATA_CLEANED_PATH}/carbon_bombs_informations.csv"
FPATH_OUT_CONX_BANK_COMP = f"{DATA_CLEANED_PATH}/connexion_bank_company.csv"
FPATH_OUT_CONX_CB_COMP = f"{DATA_CLEANED_PATH}/connexion_carbonbombs_company.csv"
FPATH_OUT_COUNTRY = f"{DATA_CLEANED_PATH}/country_informations.csv"
FPATH_OUT_ALL = f"{DATA_CLEANED_PATH}/carbon_bombs_all_datasets.xlsx"

# Specific characters for the project
PROJECT_SEPARATOR = "|"
