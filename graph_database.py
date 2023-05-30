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
    # Read list of Carbon Bombs / Banks / Company (nodes)
    carbon_bombs = pd.read_csv("./data_cleaned/carbon_bombs_informations.csv")
    banks = pd.read_csv("./data_cleaned/bank_informations.csv")
    companies = pd.read_csv("./data_cleaned/company_informations.csv")
    # Read list of interactions (relationships)
    #conn_banks_companies = pd.read_csv("./data_cleaned/connexion_bank_company.csv")
    #conn_companies_cb = pd.read_csv("./data_cleaned/connexion_carbonbombs.csv")
    # Connect to the database
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=basic_auth("neo4j", "password"))
    # Write carbon bombs nodes into DB
    write_carbon_bombs_nodes(driver,carbon_bombs)


def write_carbon_bombs_nodes(driver,carbon_bombs):
    cypher_query_carbon_bombs = '''
        MERGE (n:Carbon_Bombs {
            New_project: $new_project,
            Name: $name,
            Country: $country,
            Potential_GtCO2_emissions: $CO2_emissions,
            Fuel_type: $fuel_type
            })
    '''
    def create_carbon_bombs(tx, carbon_bombs_values):
        tx.run(cypher_query_carbon_bombs,
               new_project = carbon_bombs_values['New_project_source_CB'],
               name = carbon_bombs_values['Carbon_bomb_name_source_CB'],
               country = carbon_bombs_values['Country_source_CB'],
               CO2_emissions = carbon_bombs_values['Potential_GtCO2_source_CB'],
               fuel_type = carbon_bombs_values['Fuel_type_source_CB']
               )
    with driver.session(database="neo4j") as session:
        for i, row in carbon_bombs.iterrows():
            session.execute_write(create_carbon_bombs, row)














if __name__ == '__main__':
    #tutorial_movies_neo4j()
    #tutorial_got_neo4j()
    purge_current_database()
    carbon_bombs_graph_database()


    """
    MATCH (n:Carbon_Bombs) RETURN n LIMIT 25
    """