#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
To use this script, simply run it from the command line:
$ python graph_database.py
"""

import os
import pandas as pd
from neo4j import GraphDatabase, basic_auth
from credentials import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD

def load_renamed_columns():
    # create a mapping dictionary for carbon bombs columns
    carbon_bombs_new_column = {
        'New_project_source_CB': 'new_project',
        'Carbon_bomb_name_source_CB': 'name',
        'Country_source_CB': 'country',
        'Potential_GtCO2_source_CB':'potential_gtco2',
        'Fuel_type_source_CB':'fuel_type',
        'GEM_id_source_GEM':'id',
        'GEM_url_source_GEM':'gem_source',
        'Latitude':'latitude',
        'Longitude':'longitude',
        'Latitude_longitude_operator_source':'source_for_lat_long_operator',
        'Operators':'operators',
        'Parent_company_source_GEM':'parent_company',
        'Carbon_bomb_start_year':'start_year',
        'Multiple_unit_concerned_source_GEM':'multiple_unit',
        'World_region' : 'world_region'
    }
    banks_new_column = {
        'Bank Name':'name',
        'Bank Website':'website',
        'Headquarters country':'headquarters_country',
        'Headquarters address':'headquarters_address',
        'CEO Name':'ceo_name',
        'Board description':'board',
        'Supervisor Name':'supervisor',
        'Supervisor Website':'supervisor_website',
        'Shareholder structure source':'shareholder_source',
        'Source BankTrack':'source',
        'Latitude':'latitude',
        'Longitude':'longitude',
        'World Region' : 'world_region',
        'Bank logo' : 'url_logo',
    }
    companies_new_column = {
        'Company_name':'name',
        'Address_headquarters_source_chatGPT':'headquarters_address',
        'Latitude':'latitude',
        'Longitude':'longitude',
        'Carbon_bomb_connected':'carbon_bomb_connected',
        'Logo_URL':'url_logo',
        'Country':'country',
        'World_region':'world_region'
    }
    country_new_column = {
        'Country_source_CB':'name',
        'Emissions_per_capita_tons_CO2':'emissions_per_capita_tons_co2',
        'Emissions_thousand_tons_CO2':'emissions_thousand_tons_co2',
        'GDP_millions_US_dollars':'gdp_millions_us_dollars',
        'GDP_per_capita_US_dollars':'gdp_per_capita_us_dollars',
        'Population_in_millions':'population_in_millions',
        'Surface_thousand_km2':'surface_thousand_km2',
        'Year_Surface_thousand_km2':'year_surface_thousand_km2',
    }
    
    return (
        carbon_bombs_new_column,
        companies_new_column,
        banks_new_column,
        country_new_column,
    )
        

def update_csv_nodes_neo4j():
    # Load Data Carbon Bombs / Banks / Company (nodes) and connexion (relations)
    carbon_bombs = pd.read_csv("./data_cleaned/carbon_bombs_informations.csv")
    banks = pd.read_csv("./data_cleaned/bank_informations.csv")
    companies = pd.read_csv("./data_cleaned/company_informations.csv")
    countries = pd.read_csv("./data_cleaned/country_informations.csv")
    # Replace NaN value by 'None' in order to avoid neo4j error in each df
    carbon_bombs.fillna('None',inplace = True)
    banks.fillna('None',inplace = True)
    companies.fillna('None',inplace = True)
    countries.fillna('None',inplace = True)
    # Load dictionnary to remap column name
    carbon_bombs_column, companies_column, banks_column, countries_column = (
        load_renamed_columns())
    # Drop column that are not renamed with the above dictionnary
    # Used for logo column of Banks informations (mainly)
    carbon_bombs = carbon_bombs[list(carbon_bombs_column.keys())]
    companies = companies[list(companies_column.keys())]
    banks = banks[list(banks_column.keys())]
    countries = countries[list(countries_column.keys())]
    # Remap column name of each df
    carbon_bombs.rename(columns=carbon_bombs_column, inplace=True)
    companies.rename(columns=companies_column, inplace=True)
    banks.rename(columns=banks_column, inplace=True)
    countries.rename(columns=countries_column, inplace=True)
    # Verify presence of data_neo4j and create it if needed
    if not os.path.exists('data_neo4j'):
        os.makedirs('data_neo4j')
    # Save dataframe into neo4j csv folder
    countries.to_csv("./data_neo4j/country_informations.csv",
                    encoding='utf-8-sig', index=False)
    carbon_bombs.to_csv("./data_neo4j/carbon_bombs_informations.csv",
                encoding='utf-8-sig', index=False)
    companies.to_csv("./data_neo4j/companies_informations.csv",
                encoding='utf-8-sig', index=False)
    banks.to_csv("./data_neo4j/banks_informations.csv",
            encoding='utf-8-sig', index=False)
    return carbon_bombs,companies,banks,countries
    
def purge_database():
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD)
        )
    def delete_all(tx):
        tx.run("MATCH (n) DETACH DELETE n")
    with driver.session() as session:
        session.execute_write(delete_all)
    driver.close()
    
def write_nodes(driver,node_type,node_data):
    def create_nodes(tx, node_type, node_data):
        cypher_query_carbon_bombs = f"MERGE (n:{node_type} " + "{"
        # Dynamically build the query string that sets the attributes
        for key in node_data.keys():
            cypher_query_carbon_bombs += f"{key}: ${key},"
        # Remove the trailing comma and add closing brackets
        cypher_query_carbon_bombs = cypher_query_carbon_bombs[:-1] + "})"
        tx.run(cypher_query_carbon_bombs, **node_data)
    with driver.session(database="neo4j") as session:
        for _, row in node_data.iterrows():
            session.execute_write(create_nodes, node_type, row)
    
def write_connexions(driver):
    ### Carbon Bombs and Companies relationship
    carbonbombs_companies = pd.read_csv(
        "./data_cleaned/connexion_carbonbombs_company.csv")
    carbonbombs_companies.to_csv(
        "./data_neo4j/connexion_carbonbombs_company.csv",
        encoding='utf-8-sig',
        index=False)
    query_cb_company = '''
        MATCH (cb:carbon_bomb {name: $carbon_bomb, country: $country})
        MATCH (c:company {name: $company})
        MERGE (c)-[:OPERATES {weight: $weight}]->(cb)
    '''
    def create_interaction_cb_companies(tx,cb, company, country, weight):
        tx.run(query_cb_company,
               carbon_bomb = cb,
               company = company,
               country = country,
               weight = weight)
    with driver.session(database="neo4j") as session:
        for _, row in carbonbombs_companies.iterrows():
            carbon_bomb = row['Carbon_bomb_name']
            company = row['Company']
            country = row['Country']
            weight = row['Percentage']
            session.execute_write(create_interaction_cb_companies,
                                  carbon_bomb, company, country, weight) 
                            
    ### Banks and Companies relationship 
    banks_companies = pd.read_csv("./data_cleaned/connexion_bank_company.csv")
    banks_companies.to_csv(
        "./data_neo4j/connexion_bank_company.csv",
        encoding='utf-8-sig',
        index=False)
    query_bank_company = '''
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
    '''   
    def create_interaction_banks_companies(tx, bank, company,year_2016,
                                           year_2017,year_2018,year_2019,
                                           year_2020,year_2021,year_2022,total):
        tx.run(
            query_bank_company,
            bank = bank,
            company = company,
            year_2016 = year_2016,
            year_2017 = year_2017,
            year_2018 = year_2018,
            year_2019 = year_2019,
            year_2020 = year_2020,
            year_2021 = year_2021,
            year_2022 = year_2022,
            total = total,
            )
    with driver.session(database="neo4j") as session:
        for _, row in banks_companies.iterrows():
            bank = row['Bank']
            company = row['Company']
            year_2016 = row['2016']
            year_2017 = row['2017']
            year_2018 = row['2018']
            year_2019 = row['2019']
            year_2020 = row['2020']  
            year_2021 = row['2021']
            year_2022 = row['2022'] 
            total = row['Grand Total']
            session.execute_write(create_interaction_banks_companies,
                                  bank,
                                  company,
                                  year_2016,
                                  year_2017,
                                  year_2018,
                                  year_2019,
                                  year_2020,
                                  year_2021,
                                  year_2022,
                                  total)
    ### Carbon Bombs and Country relationship
    carbonbombs_informations = pd.read_csv(
        "./data_cleaned/carbon_bombs_informations.csv")
    filtered_columns = ['Carbon_bomb_name_source_CB','Country_source_CB']
    carbonbombs_countries = carbonbombs_informations[filtered_columns]
    carbonbombs_countries.columns = ["Carbon_bomb","Country"]
    carbonbombs_countries.to_csv(
        "./data_neo4j/connexion_carbonbombs_country.csv",
        encoding='utf-8-sig',
        index=False)
    query_cb_country = '''
        MATCH (cb:carbon_bomb {name: $carbon_bomb, country: $country})
        MATCH (c:country {name: $country})
        MERGE (cb)-[:IS_LOCATED]->(c)
    '''
    def create_interaction_cb_countries(tx, cb, country):
        tx.run(query_cb_country, carbon_bomb = cb, country = country)
    with driver.session(database="neo4j") as session:
        for _, row in carbonbombs_countries.iterrows():
            carbon_bomb = row['Carbon_bomb']
            country = row['Country']
            session.execute_write(create_interaction_cb_countries,
                                  carbon_bomb, country)

    ### Company and Country relationship
    company_informations = pd.read_csv(
        "./data_cleaned/company_informations.csv")
    filtered_columns = ['Company_name','Country']
    companies_countries = company_informations[filtered_columns]
    companies_countries.columns = ["Company","Country"]
    companies_countries.to_csv(
        "./data_neo4j/connexion_company_country.csv",
        encoding='utf-8-sig',
        index=False)
    query_company_country = '''
        MATCH (cp:company {name: $company, country: $country})
        MATCH (c:country {name: $country})
        MERGE (cp)-[:IS_LOCATED]->(c)
    '''
    def create_interaction_companies_countries(tx, company, country):
        tx.run(query_company_country, company = company, country = country)
    with driver.session(database="neo4j") as session:
        for _, row in companies_countries.iterrows():
            company = row['Company']
            country = row['Country']
            session.execute_write(create_interaction_companies_countries,
                                  company, country)
    ### Bank and Country relationship
    bank_informations = pd.read_csv(
        "./data_cleaned/bank_informations.csv")
    filtered_columns = ['Bank Name','Headquarters country']
    banks_countries = bank_informations[filtered_columns]
    banks_countries.columns = ["Bank","Country"]
    banks_countries.to_csv(
        "./data_neo4j/connexion_bank_country.csv",
        encoding='utf-8-sig',
        index=False)
    query_bank_country = '''
        MATCH (b:bank {name: $bank, country: $country})
        MATCH (c:country {name: $country})
        MERGE (b)-[:IS_LOCATED]->(c)
    '''
    def create_interaction_banks_countries(tx, bank, country):
        tx.run(query_bank_country, bank = bank, country = country)
    with driver.session(database="neo4j") as session:
        for _, row in banks_countries.iterrows():
            bank = row['Bank']
            country = row['Country']
            session.execute_write(create_interaction_banks_countries,
                                  bank, country)
    
def update_neo4j():
    carbon_bombs, companies, banks, countries = update_csv_nodes_neo4j()
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD))
    # Define dict to iterate over three types node creation
    dict_nodes = {
        "carbon_bomb":carbon_bombs,
        "company":companies,
        "bank":banks,
        "country":countries,
    }
    # Iterate throught dictionnary to create each node type
    for node_type, node_data in dict_nodes.items():
        write_nodes(driver,node_type,node_data)
    # Define connexion between nodes 
    write_connexions(driver)
    driver.close()
    
if __name__ == '__main__':
    purge_database()
    update_neo4j()
