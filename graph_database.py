#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
To use this script, simply run it from the command line:
$ python graph_database.py
"""

import os
import pandas as pd
from neo4j import GraphDatabase, basic_auth

def tutorial_movies_neo4j():
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "password"))
    imdb = pd.read_csv('./data_graphDB/imdb.csv')
    # Cypher query to write nodes and relationships into DB
    cypher_query = '''
        MERGE (m:Movie {id: $id, title: $title, year: $year, rating: $rating, votes: $votes})
        MERGE (d:Director {name: $director})
        MERGE (d)-[:HAS_DIRECTED]->(m)
        MERGE (a1:Actor {name: $star1})
        MERGE (a2:Actor {name: $star2})
        MERGE (a3:Actor {name: $star3})
        MERGE (a4:Actor {name: $star4})
        MERGE (a1)-[:HAS_ACTED_IN]->(m)
        MERGE (a2)-[:HAS_ACTED_IN]->(m)
        MERGE (a3)-[:HAS_ACTED_IN]->(m)
        MERGE (a4)-[:HAS_ACTED_IN]->(m)
    '''

    # Function that runs the query by passing some properties
    def create_movie(tx, id, title, year, rating, votes, director, star1, star2, star3, star4):
        tx.run(cypher_query, id = id, title = title, year = year, rating = rating, votes = votes, director = director,
            star1 = star1, star2 = star2, star3 = star3, star4 = star4)

    # Open connection with DB, loop over the DataFrame's rows and write nodes/relationships into DB
    with driver.session(database="neo4j") as session:
        for i, row in imdb.iterrows():
            id = i
            title = row['Series_Title']
            year = row['Released_Year']
            rating = row['IMDB_Rating']
            votes = row['No_of_Votes']
            director = row['Director']
            star1 = row['Star1']
            star2 = row['Star2']
            star3 = row['Star3']
            star4 = row['Star4']
            session.execute_write(create_movie, id, title, year, rating, votes, director, 
                                    star1, star2, star3, star4)

    driver.close()

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
        'Bank Name':'Bank',
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

def tutorial_got_neo4j():
    # Read list of characters (nodes)
    nodes = pd.read_csv("./data_graphDB/asoiaf-all-nodes.csv")
    # Read list of interactions (relationships)
    edges = pd.read_csv("./data_graphDB/asoiaf-all-edges.csv")
    # Connect to the database
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "password"))
    # Write nodes into DB
    cypher_query = '''
        MERGE (n:Character {id: $id, name: $name})
        RETURN n.id, n.name as person
    '''

    def create_character(tx, id, name):
        tx.run(cypher_query, id = id, name = name)

    with driver.session(database="neo4j") as session:
        for i, row in nodes.iterrows():
            id = row['Id']
            name = row['Label']
            session.execute_write(create_character, id, name)

    driver.close()
    # Write relationships into DB
    cypher_query = '''
        MATCH (c1:Character {id: $id1})
        MATCH (c2:Character {id: $id2})
        MERGE (c1)-[:INTERACTS_WITH {weight: $weight}]->(c2)
    '''

    def create_interaction(tx, id1, id2, weight):
        tx.run(cypher_query, id1 = id1, id2 = id2, weight = weight)

    with driver.session(database="neo4j") as session:
        for i, row in edges.iterrows():
            id1 = row['Source']
            id2 = row['Target']
            weight = row['weight']
            session.execute_write(create_interaction, id1, id2, weight)

    driver.close()

def carbon_bombs_graph_database():
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
    # Define driver database from neo4j
    # Connect to the database
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "password"))
    # Define dict to iterate over three types node creation
    dict_nodes = {
        "Carbon_Bombs":carbon_bombs,
        "Companies":companies,
        "Banks":banks,
    }
    for node_type, node_data in dict_nodes.items():
        write_nodes(driver,node_type,node_data)

            
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
    #tutorial_movies_neo4j()
    #tutorial_got_neo4j()
    purge_current_database()
    carbon_bombs_graph_database()


    """
    MATCH (n:Carbon_Bombs) RETURN n LIMIT 25
    """