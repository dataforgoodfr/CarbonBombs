"""Functions to update and purge neo4j"""
import os

import pandas as pd
from dotenv import load_dotenv
from neo4j import basic_auth
from neo4j import GraphDatabase

from carbon_bombs.conf import FPATH_NEO4J_BANK
from carbon_bombs.conf import FPATH_NEO4J_CB
from carbon_bombs.conf import FPATH_NEO4J_COMP
from carbon_bombs.conf import FPATH_NEO4J_CONX_BANK_COMP
from carbon_bombs.conf import FPATH_NEO4J_CONX_BANK_COUNTRY
from carbon_bombs.conf import FPATH_NEO4J_CONX_CB_COMP
from carbon_bombs.conf import FPATH_NEO4J_CONX_CB_COUNTRY
from carbon_bombs.conf import FPATH_NEO4J_CONX_COMP_COUNTRY
from carbon_bombs.conf import FPATH_NEO4J_COUNTRY
from carbon_bombs.conf import FPATH_OUT_BANK
from carbon_bombs.conf import FPATH_OUT_CB
from carbon_bombs.conf import FPATH_OUT_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_BANK_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_CB_COMP
from carbon_bombs.conf import FPATH_OUT_COUNTRY
from carbon_bombs.utils.logger import LOGGER

load_dotenv()

NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USERNAME = os.environ["NEO4J_USERNAME"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

carbon_bombs_new_column = {
    "Carbon_bomb_name_source_CB": "name",
    "Country_source_CB": "country",
    "Potential_GtCO2_source_CB": "potential_gtco2",
    "Fuel_type_source_CB": "fuel_type",
    "GEM_id_source_GEM": "id",
    "GEM_url_source_GEM": "gem_source",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Latitude_longitude_source": "source_for_lat_long",
    "Operators_source_GEM": "operators",
    "Parent_company_source_GEM": "parent_company",
    "Companies_involved_source_GEM": "companies",
    "Carbon_bomb_start_year": "start_year",
    "Multiple_unit_concerned_source_GEM": "multiple_unit",
    "World_region": "world_region",
    "Status_source_CB": "status_cb",
    "Status_source_GEM": "status_gem",
    "Status_lvl_1": "status_lvl_1",
    "Status_lvl_2": "status_lvl_2",
}

banks_new_column = {
    "Bank Name": "name",
    "Bank Website": "website",
    "Headquarters country": "headquarters_country",
    "Headquarters address": "headquarters_address",
    "CEO Name": "ceo_name",
    "Board description": "board",
    "Supervisor Name": "supervisor",
    "Supervisor Website": "supervisor_website",
    "Shareholder structure source": "shareholder_source",
    "Source BankTrack": "source",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "World Region": "world_region",
    "Bank logo": "url_logo",
}

companies_new_column = {
    "Company_name": "name",
    "Address_headquarters_source_chatGPT": "headquarters_address",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Carbon_bomb_connected": "carbon_bomb_connected",
    "Logo_URL": "url_logo",
    "Country": "country",
    "World_region": "world_region",
}

country_new_column = {
    "Country_source_CB": "name",
    "Emissions_per_capita_tons_CO2": "emissions_per_capita_tons_co2",
    "Emissions_thousand_tons_CO2": "emissions_thousand_tons_co2",
    "GDP_millions_US_dollars": "gdp_millions_us_dollars",
    "GDP_per_capita_US_dollars": "gdp_per_capita_us_dollars",
    "Population_in_millions": "population_in_millions",
    "Surface_thousand_km2": "surface_thousand_km2",
    "Year_Surface_thousand_km2": "year_surface_thousand_km2",
}


def _update_dataset_for_neo4j(
    fpath_cleaned: str, fpath_neo4j: str, map_columns: dict
) -> pd.DataFrame:
    """Update a cleaned dataset for Neo4J"""
    data = pd.read_csv(fpath_cleaned)

    # Replace missing values
    data = data.fillna("None")

    # Keep only wanted columns and rename it to wanted format
    data = data[map_columns.keys()]
    data = data.rename(columns=map_columns)

    data.to_csv(fpath_neo4j, encoding="utf-8-sig", index=False)

    return data


def update_csv_nodes_neo4j():
    """Update nodes dataset for Neo4J and return formated datasets."""
    # update neo4j CSV for CB
    carbon_bombs = _update_dataset_for_neo4j(
        FPATH_OUT_CB, FPATH_NEO4J_CB, carbon_bombs_new_column
    )
    # update neo4j CSV for companies
    companies = _update_dataset_for_neo4j(
        FPATH_OUT_COMP, FPATH_NEO4J_COMP, companies_new_column
    )
    # update neo4j CSV for banks
    banks = _update_dataset_for_neo4j(
        FPATH_OUT_BANK, FPATH_NEO4J_BANK, banks_new_column
    )
    # update neo4j CSV for country
    countries = _update_dataset_for_neo4j(
        FPATH_OUT_COUNTRY, FPATH_NEO4J_COUNTRY, country_new_column
    )

    return carbon_bombs, companies, banks, countries


def write_nodes(driver, node_type, node_data):
    """Write all nodes given a node type (bank, country, carbon_bomb or company)
    and its data.
    """
    LOGGER.debug(f"{node_type} nodes: start writing...")

    def _create_nodes(tx, node_type, node_data):
        cypher_query_carbon_bombs = f"MERGE (n:{node_type} " + "{"

        # Dynamically build the query string that sets the attributes
        for key in node_data.keys():
            cypher_query_carbon_bombs += f"{key}: ${key},"

        # Remove the trailing comma and add closing brackets
        cypher_query_carbon_bombs = cypher_query_carbon_bombs[:-1] + "})"
        tx.run(cypher_query_carbon_bombs, **node_data)

    with driver.session(database="neo4j") as session:
        for _, row in node_data.iterrows():
            LOGGER.debug(f"{node_type} nodes: write `{row.iloc[0]}`")
            session.execute_write(_create_nodes, node_type, row)

    LOGGER.debug(f"{node_type} nodes: writing done")


def _write_connexions_cb_companies(driver):
    """Write connexions between CB nodes and company nodes"""
    ### Carbon Bombs and Companies relationship
    LOGGER.debug("Start writing connexions between carbon bombs and companies")
    carbonbombs_companies = pd.read_csv(FPATH_OUT_CONX_CB_COMP)
    carbonbombs_companies.to_csv(
        FPATH_NEO4J_CONX_CB_COMP, encoding="utf-8-sig", index=False
    )

    query_cb_company = """
        MATCH (cb:carbon_bomb {name: $carbon_bomb, country: $country})
        MATCH (c:company {name: $company})
        MERGE (c)-[:OPERATES {weight: $weight}]->(cb)
    """

    def create_interaction_cb_companies(tx, cb, company, country, weight):
        tx.run(
            query_cb_company,
            carbon_bomb=cb,
            company=company,
            country=country,
            weight=weight,
        )

    with driver.session(database="neo4j") as session:
        for _, row in carbonbombs_companies.iterrows():
            carbon_bomb = row["Carbon_bomb_name"]
            company = row["Company"]
            country = row["Country"]
            LOGGER.debug(
                f"Connexion CB to COMPANY: write `{carbon_bomb}` - `{company}`"
            )
            weight = 1  # row['Percentage']
            session.execute_write(
                create_interaction_cb_companies, carbon_bomb, company, country, weight
            )
    LOGGER.debug("Writing connexions between carbon bombs and companies done")


def _write_connexions_bank_companies(driver):
    """Write connexions between bank nodes and company nodes"""
    LOGGER.debug("Start writing connexions between banks and companies")
    ### Banks and Companies relationship
    banks_companies = pd.read_csv(FPATH_OUT_CONX_BANK_COMP)
    banks_companies.to_csv(
        FPATH_NEO4J_CONX_BANK_COMP, encoding="utf-8-sig", index=False
    )
    query_bank_company = """
        MATCH (c:company {name: $company})
        MATCH (b:bank {name: $bank})
        MERGE (b)-[:FINANCES {
            total: $total,
            year_2016: $year_2016,
            year_2017: $year_2017,
            year_2018: $year_2018,
            year_2019: $year_2019,
            year_2020: $year_2020,
            year_2021: $year_2021,
            year_2022: $year_2022
            }]->(c)
    """

    def create_interaction_banks_companies(
        tx,
        bank,
        company,
        year_2016,
        year_2017,
        year_2018,
        year_2019,
        year_2020,
        year_2021,
        year_2022,
        total,
    ):
        tx.run(
            query_bank_company,
            bank=bank,
            company=company,
            year_2016=year_2016,
            year_2017=year_2017,
            year_2018=year_2018,
            year_2019=year_2019,
            year_2020=year_2020,
            year_2021=year_2021,
            year_2022=year_2022,
            total=total,
        )

    with driver.session(database="neo4j") as session:
        for _, row in banks_companies.iterrows():
            bank = row["Bank"]
            company = row["Company"]
            year_2016 = row["2016"]
            year_2017 = row["2017"]
            year_2018 = row["2018"]
            year_2019 = row["2019"]
            year_2020 = row["2020"]
            year_2021 = row["2021"]
            year_2022 = row["2022"]
            total = row["Grand Total"]
            LOGGER.debug(f"Connexion BANK to COMPANY: write `{bank}` - `{company}`")
            session.execute_write(
                create_interaction_banks_companies,
                bank,
                company,
                year_2016,
                year_2017,
                year_2018,
                year_2019,
                year_2020,
                year_2021,
                year_2022,
                total,
            )
    LOGGER.debug("Writing connexions between banks and companies done")


def _write_connexions_cb_country(driver):
    """Write connexions between CB nodes and country nodes"""
    LOGGER.debug("Start writing connexions between carbon bombs and countries")
    ### Carbon Bombs and Country relationship
    carbonbombs_informations = pd.read_csv(FPATH_OUT_CB)

    filtered_columns = ["Carbon_bomb_name_source_CB", "Country_source_CB"]
    carbonbombs_countries = carbonbombs_informations[filtered_columns]
    carbonbombs_countries.columns = ["Carbon_bomb", "Country"]

    carbonbombs_countries.to_csv(
        FPATH_NEO4J_CONX_CB_COUNTRY, encoding="utf-8-sig", index=False
    )
    query_cb_country = """
        MATCH (cb:carbon_bomb {name: $carbon_bomb, country: $country})
        MATCH (c:country {name: $country})
        MERGE (cb)-[:IS_LOCATED]->(c)
    """

    def create_interaction_cb_countries(tx, cb, country):
        tx.run(query_cb_country, carbon_bomb=cb, country=country)

    with driver.session(database="neo4j") as session:
        for _, row in carbonbombs_countries.iterrows():
            carbon_bomb = row["Carbon_bomb"]
            country = row["Country"]
            LOGGER.debug(
                f"Connexion CB to COUNTRY: write `{carbon_bomb}` - `{country}`"
            )
            session.execute_write(create_interaction_cb_countries, carbon_bomb, country)

    LOGGER.debug("Writing connexions between carbon bombs and countries done")


def _write_connexions_companies_country(driver):
    """Write connexions between company nodes and company nodes"""
    LOGGER.debug("Start writing connexions between companies and countries")
    ### Company and Country relationship
    company_informations = pd.read_csv(FPATH_OUT_COMP)

    filtered_columns = ["Company_name", "Country"]
    companies_countries = company_informations[filtered_columns]
    companies_countries.columns = ["Company", "Country"]

    companies_countries.to_csv(
        FPATH_NEO4J_CONX_COMP_COUNTRY, encoding="utf-8-sig", index=False
    )

    query_company_country = """
        MATCH (cp:company {name: $company, country: $country})
        MATCH (c:country {name: $country})
        MERGE (cp)-[:IS_LOCATED]->(c)
    """

    def create_interaction_companies_countries(tx, company, country):
        tx.run(query_company_country, company=company, country=country)

    with driver.session(database="neo4j") as session:
        for _, row in companies_countries.iterrows():
            company = row["Company"]
            country = row["Country"]
            LOGGER.debug(
                f"Connexion COMPANY to COUNTRY: write `{company}` - `{country}`"
            )
            session.execute_write(
                create_interaction_companies_countries, company, country
            )

    LOGGER.debug("Writing connexions between companies and countries done")


def _write_connexions_bank_country(driver):
    """Write connexions between bank nodes and country nodes"""
    LOGGER.debug("Start writing connexions between banks and countries")
    ### Bank and Country relationship
    bank_informations = pd.read_csv(FPATH_OUT_BANK)

    filtered_columns = ["Bank Name", "Headquarters country"]
    banks_countries = bank_informations[filtered_columns]
    banks_countries.columns = ["Bank", "Country"]

    banks_countries.to_csv(
        FPATH_NEO4J_CONX_BANK_COUNTRY, encoding="utf-8-sig", index=False
    )

    query_bank_country = """
        MATCH (b:bank {name: $bank, country: $country})
        MATCH (c:country {name: $country})
        MERGE (b)-[:IS_LOCATED]->(c)
    """

    def create_interaction_banks_countries(tx, bank, country):
        tx.run(query_bank_country, bank=bank, country=country)

    with driver.session(database="neo4j") as session:
        for _, row in banks_countries.iterrows():
            bank = row["Bank"]
            country = row["Country"]
            LOGGER.debug(f"Connexion BANK to COUNTRY: write `{bank}` - `{country}`")
            session.execute_write(create_interaction_banks_countries, bank, country)

    LOGGER.debug("Writing connexions between banks and countries done")


def write_connexions(driver):
    """Write all connexions between nodes into Neo4J"""
    _write_connexions_cb_companies(driver)
    _write_connexions_bank_companies(driver)
    _write_connexions_cb_country(driver)
    _write_connexions_companies_country(driver)
    _write_connexions_bank_country(driver)


def update_neo4j():
    """Update CSV for Neo4J, create nodes and connexions"""
    LOGGER.debug("Update cleaned csv for neo4j and save them")
    carbon_bombs, companies, banks, countries = update_csv_nodes_neo4j()

    LOGGER.debug("Connect to driver...")
    driver = GraphDatabase.driver(
        NEO4J_URI, auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD)
    )
    LOGGER.debug("Driver connected")

    # Define dict to iterate over three types node creation
    dict_nodes = {
        "carbon_bomb": carbon_bombs,
        "company": companies,
        "bank": banks,
        "country": countries,
    }

    # Iterate throught dictionnary to create each node type
    LOGGER.debug("Write nodes")
    for node_type, node_data in dict_nodes.items():
        write_nodes(driver, node_type, node_data)

    # Define connexion between nodes
    LOGGER.debug("Write connexions")
    write_connexions(driver)

    driver.close()


def purge_database():
    """Purge all Neo4J database"""
    LOGGER.debug("Connect to driver...")
    driver = GraphDatabase.driver(
        NEO4J_URI, auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD)
    )
    LOGGER.debug("Driver connected")

    def delete_all(tx):
        tx.run("MATCH (n) DETACH DELETE n")

    LOGGER.debug("Start NEO4J purge...")
    with driver.session() as session:
        session.execute_write(delete_all)
    LOGGER.debug("NEO4J purge done")

    driver.close()
