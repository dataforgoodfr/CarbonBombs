#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
To use this script, simply run it from the command line:
$ python graph_database.py
"""

import os
import pandas as pd
from neo4j import GraphDatabase, basic_auth

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
    return carbon_bombs_new_column,companies_new_column,banks_new_column
        
def purge_current_database():
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "password"))
    def delete_all(tx):
        tx.run("MATCH (n) DETACH DELETE n")
    with driver.session() as session:
        session.execute_write(delete_all)
    driver.close()
    
    
def carbon_bombs_graph_database():
    # Define driver to connect to neo4j Database
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "password"))
    ### Nodes creation
    # Load Data Carbon Bombs / Banks / Company (nodes) and connexion (relations)
    carbon_bombs = pd.read_csv("./data_cleaned/carbon_bombs_informations.csv")
    banks = pd.read_csv("./data_cleaned/bank_informations.csv")
    companies = pd.read_csv("./data_cleaned/company_informations.csv")
    # Replace NaN value by 'None' in order to avoid neo4j error in each df
    carbon_bombs.fillna('None',inplace = True)
    banks.fillna('None',inplace = True)
    companies.fillna('None',inplace = True)
    # Load dictionnary to remap column name and remap column name of each df
    carbon_bombs_column, companies_column, banks_column = load_renamed_columns()
    carbon_bombs.rename(columns=carbon_bombs_column, inplace=True)
    companies.rename(columns=companies_column, inplace=True)
    banks.rename(columns=banks_column, inplace=True)
    # Define dict to iterate over three types node creation
    dict_nodes = {
        "Carbon_bomb":carbon_bombs,
        "Company":companies,
        "Bank":banks,
    }
    for node_type, node_data in dict_nodes.items():
        write_nodes(driver,node_type,node_data)
    ### Relationship creation between Carbon Bombs and Companies
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
    ### Relationship creation between Banks and Companies
    banks_companies = pd.read_csv("./data_cleaned/connexion_bank_company.csv")
    query_bank_company = '''
        MATCH (c:Company {Name: $company})
        MATCH (b:Bank {Name: $bank})
        MERGE (b)-[:FINANCE {weight: $weight}]->(c)
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
    # Close connection to database
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


if __name__ == '__main__':
    purge_current_database()
    carbon_bombs_graph_database()
    """
    Cypher Query examples
    MATCH (n:Carbon_bomb) RETURN n LIMIT 25
    MATCH (n:Company) RETURN n LIMIT 25
    """