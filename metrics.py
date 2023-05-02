#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon April 17 09:19:23 2023

@author: Nicolle Mathieu
"""
import os
import sys
import logging
import pandas as pd
import numpy as np

def log_metrics():
    logging.info("This is an info message")


if __name__ == '__main__':
    # Main function
    # Configure logging to output to a file
    logging.basicConfig(filename='metrics.log',
                        filemode='w',
                        level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    # Log metrics
    log_metrics()
    
    
    
    
    # Liste of idea applicable uniquement sur les fichiers dans dossier data_cleaned
    # Nombre de carbon bombs selon leur type
    # Nombre de carbon bombs sourcer dans GEM 
    # Nombre de carbon Bombs rempli par chat GPT
    # Nombre de banque dans BOCC
    # Nombre de banques matché avec ce que l'on a nous 
    # Nombre d'entreprises unique lié au carbon bombs 
    # Parmis celle-ci celle que l'on arrive à matcher avec BOCC 
    # Donner un chiffre de coverage vis a vis des carbon bombs. 
    # Quantifier les infos New None Multi match and SIngle Match.
    # quantifier le nombre de carbon bomb ou on n'arrive vraiment pas à relier à un seul financement