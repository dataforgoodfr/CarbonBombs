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
        'New_project_source_CB': 'New_project',
        'Carbon_bomb_name_source_CB': 'Name',
        'Country_source_CB': 'Country',
        'Potential_GtCO2_source_CB':'Potential_GTCO2',
        'Fuel_type_source_CB':'Fuel_type',
        'GEM_id_source_GEM':'ID',
        'GEM_url_source_GEM':'GEM_source',
        'Latitude':'Latitude',
        'Longitude':'Longitude',
        'Latitude_longitude_operator_source':'SourceForLatLongAndOperator',
        'Operators_source_GEM':'Operators',
        'Parent_company_source_GEM':'Parent_company',
        'Multiple_unit_concerned_source_GEM':'Multiple_unit',
        'Suppliers_source_chatGPT':'Suppliers',
        'Insurers_source_chatGPT':'Insurers',
        'Subcontractors_source_chatGPT':'Subcontractors',
    }
    banks_new_column = {
        'Bank Name':'Name',
        'Bank Website':'Website',
        'Headquarters country':'Headquarters_country',
        'Headquarters address':'Headquarters_address',
        'CEO Name':'CEO_name',
        'Board description':'Board',
        'Supervisor Name':'Supervisor',
        'Supervisor Website':'Supervisor_Website',
        'Shareholder structure source':'Shareholder_source',
        'Source BankTrack':'Source',
        'Latitude':'Latitude',
        'Longitude':'Longitude',
    }
    companies_new_column = {
        'Company_name':'Name',
        'Address_headquarters_source_chatGPT':'Headquarters_address',
        'Latitude':'Latitude',
        'Longitude':'Longitude',
        'Carbon_bomb_connected':'Carbon_bomb_connected'
    }
    country_new_column = {
        'Country_source_CB':'Name',
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
    driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD))
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
    query_cb_company = '''
        MATCH (cb:Carbon_bomb {Name: $carbon_bomb})
        MATCH (c:Company {Name: $company})
        MERGE (c)-[:OPERATES {weight: $weight}]->(cb)
    '''
    def create_interaction_cb_companies(tx,cb, company, weight):
        tx.run(query_cb_company, carbon_bomb = cb, company = company,
               weight = weight)
    with driver.session(database="neo4j") as session:
        for _, row in carbonbombs_companies.iterrows():
            carbon_bomb = row['Carbon_bomb_name']
            company = row['Company']
            weight = row['Percentage']
            session.execute_write(create_interaction_cb_companies,
                                  carbon_bomb, company, weight)                          
    ### Banks and Companies relationship 
    banks_companies = pd.read_csv("./data_cleaned/connexion_bank_company.csv")
    query_bank_company = '''
        MATCH (c:Company {Name: $company})
        MATCH (b:Bank {Name: $bank})
        MERGE (b)-[:FINANCES {weight: $weight}]->(c)
    '''
    def create_interaction_banks_companies(tx, bank, company, weight):
        tx.run(query_bank_company, bank = bank, company = company, weight = 1)
    with driver.session(database="neo4j") as session:
        for _, row in banks_companies.iterrows():
            bank = row['Bank']
            company = row['Company']
            weight = row['Grand Total']
            session.execute_write(create_interaction_banks_companies,
                                  bank, company, weight)
    ### Carbon Bombs and Country relationship
    carbonbombs_informations = pd.read_csv(
        "./data_cleaned/carbon_bombs_informations.csv")
    filtered_columns = ['Carbon_bomb_name_source_CB','Country_source_CB']
    carbonbombs_countries = carbonbombs_informations[filtered_columns]
    carbonbombs_countries.columns = ["Carbon_bomb","Country"]
    query_cb_country = '''
        MATCH (cb:Carbon_bomb {Name: $carbon_bomb})
        MATCH (c:Country {Name: $country})
        MERGE (cb)-[:IS_LOCATED {weight: $weight}]->(c)
    '''
    def create_interaction_cb_countries(tx, cb, country, weight):
        tx.run(query_cb_country, carbon_bomb = cb, country = country, weight = 1)
    with driver.session(database="neo4j") as session:
        for _, row in carbonbombs_countries.iterrows():
            carbon_bomb = row['Carbon_bomb']
            country = row['Country']
            weight = 1
            session.execute_write(create_interaction_cb_countries,
                                  carbon_bomb, country, weight)
    
def update_neo4j():
    carbon_bombs, companies, banks, countries = update_csv_nodes_neo4j()
    driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USERNAME, NEO4J_PASSWORD))
    # Define dict to iterate over three types node creation
    dict_nodes = {
        "Carbon_bomb":carbon_bombs,
        "Company":companies,
        "Bank":banks,
        "Country":countries,
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
