"""Configuration variables"""

from os import pardir
from os.path import abspath
from os.path import join

# Path to the repository
REPO_PATH = abspath(join(join(__file__, pardir), pardir))
# REPO_PATH = "."

# Path to retrieve or store the data
DATA_SOURCE_PATH = f"{REPO_PATH}/data_sources"
DATA_CLEANED_PATH = f"{REPO_PATH}/data_cleaned"
DATA_SAVE_OLD = f"{REPO_PATH}/data_save_tmp"
DATA_NEO4J_PATH = f"{REPO_PATH}/data_neo4j"

# File names of sources
FPATH_SRC_KHUNE_PAPER = f"{DATA_SOURCE_PATH}/1-s2.0-S0301421522001756-mmc2.xlsx"
# FPATH_SRC_GEM_COAL = f"{DATA_SOURCE_PATH}/Global-Coal-Mine-Tracker-October-2023.xlsx"
FPATH_SRC_GEM_COAL = f"{DATA_SOURCE_PATH}/Global-Coal-Mine-Tracker-April-2024.xlsx"
FPATH_SRC_GEM_GASOIL = (
    f"{DATA_SOURCE_PATH}/Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx"
)
FPATH_SRC_GOGEL_LNG = (
    f"{DATA_SOURCE_PATH}/LNG-Liquefaction-Projects-from-GOGEL-2024.xlsx"
)
FPATH_SRC_BOCC = f"{DATA_SOURCE_PATH}/GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx"
FPATH_SRC_COMP_URL = f"{DATA_SOURCE_PATH}/company_url.csv"
FPATH_SRC_COMP_ADDRESS = f"{DATA_SOURCE_PATH}/Data_chatGPT_company_hq_adress.csv"
FPATH_SRC_COMP_LOGO = f"{DATA_SOURCE_PATH}/company_url.csv"
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
FPATH_SRC_RYSTAD_CB = f"{DATA_SOURCE_PATH}/Carbon_Bombs_Projects.xlsx"
SHEETNAME_RYSTAD_CB_EMISSION_INFERIOR_1GT = "V1_method_0.1GT"
SHEETNAME_RYSTAD_CB_EMISSION = "Carbon_Bombs_1GT"
SHEETNAME_RYSTAD_CB_COMPANY = "Carbon_Bombs_1GT_Companies"
SHEETNAME_RYSTAD_GASOIL_EMISSION = "Carbon_Bombs_Projects"

# Manual matching source
FPATH_SRC_MANUAL_MATCHING = f"{DATA_SOURCE_PATH}/Manual matching.xlsx"
SHEETNAME_GEM_COAL = "macthGEMCoal"
SHEETNAME_GEM_GASOIL = "macthGEMGasoil"
SHEETNAME_COMPANIES = "Companies"
SHEETNAME_BANK = "Bank"
SHEETNAME_LAT_LONG = "Lat_Long"

# File names for output files used during the process
FPATH_SRC_UNIFORM_COMP_NAMES = f"{DATA_SOURCE_PATH}/uniform_company_names.json"

# File names of outputs
FPATH_OUT_BANK = f"{DATA_CLEANED_PATH}/bank_data.csv"
FPATH_OUT_COMP = f"{DATA_CLEANED_PATH}/company_data.csv"
FPATH_OUT_CB = f"{DATA_CLEANED_PATH}/carbon_bombs_data.csv"
FPATH_OUT_LNG = f"{DATA_CLEANED_PATH}/lng_data.csv"
FPATH_OUT_CONX_BANK_COMP = f"{DATA_CLEANED_PATH}/connection_bank_company.csv"
FPATH_OUT_CONX_CB_COMP = f"{DATA_CLEANED_PATH}/connection_carbonbombs_company.csv"
FPATH_OUT_COUNTRY = f"{DATA_CLEANED_PATH}/country_data.csv"
FPATH_OUT_ALL = f"{DATA_CLEANED_PATH}/carbon_bombs_all_datasets.xlsx"
FPATH_RESULT_CHECK = f"{REPO_PATH}/scripts/results/checks_results.txt"
FPATH_COMPARISON_DF = f"{REPO_PATH}/scripts/results/comparison.csv"

# File names of neo4j data
FPATH_NEO4J_BANK = f"{DATA_NEO4J_PATH}/banks_data.csv"
FPATH_NEO4J_COMP = f"{DATA_NEO4J_PATH}/companies_data.csv"
FPATH_NEO4J_CB = f"{DATA_NEO4J_PATH}/carbon_bombs_data.csv"
FPATH_NEO4J_COUNTRY = f"{DATA_NEO4J_PATH}/country_data.csv"
FPATH_NEO4J_CONX_BANK_COMP = f"{DATA_NEO4J_PATH}/connection_bank_company.csv"
FPATH_NEO4J_CONX_BANK_COUNTRY = f"{DATA_NEO4J_PATH}/connection_bank_country.csv"
FPATH_NEO4J_CONX_CB_COMP = f"{DATA_NEO4J_PATH}/connection_carbonbombs_company.csv"
FPATH_NEO4J_CONX_CB_COUNTRY = f"{DATA_NEO4J_PATH}/connection_carbonbombs_country.csv"
FPATH_NEO4J_CONX_COMP_COUNTRY = f"{DATA_NEO4J_PATH}/connection_company_country.csv"
FPATH_OUT_LOCAL_DATABASE = f"{DATA_NEO4J_PATH}/database.json"

# MD5 checksum file
FPATH_CHECKSUM = f"{DATA_CLEANED_PATH}/checksum"

# Specific characters for the project
PROJECT_SEPARATOR = "|"

# Threshold to put a project as operating
THRESHOLD_OPERATING_PROJECT = 0.3
