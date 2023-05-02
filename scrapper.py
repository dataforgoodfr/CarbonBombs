#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script scrapes the BankTrack website to collect information about banks and
stores it in a Pandas DataFrame. It retrieves information such as the bank name,
website, headquarters address and country, CEO name, board description,
supervisor name and website, shareholder structure source, and the latitude and
longitude coordinates of the headquarters. The resulting DataFrame is saved to a
CSV file.

Examples:
To use this script, simply run it from the command line:
$ python scrapper.py
"""

import sys
from credentials import API_KEY
import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from manual_match import manual_match_bank

# Define the target URL of Bank Track
# URL = 'https://www.banktrack.org/banks'

def scrapping_main_page_bank_track(url):
    """
    Scrapes the main page of bank track website and extracts URLs of individual 
    bank description pages.

    Args:
        url (str): The URL of the main page to be scraped.

    Returns:
        list: A list of URLs (strings) pointing to individual bank description
              pages.

    Raises:
        None: However, if the HTTP request fails, it prints an error message and
              terminates the script.

    Example:
        >>> scrapping_main_page_bank_track("https://www.examplebanktrack.org")
        ['https://www.examplebanktrack.org/bank/abc_bank',
         'https://www.examplebanktrack.org/bank/def_bank',
         'https://www.examplebanktrack.org/bank/ghi_bank']
    """
    # Send an HTTP request to the target URL
    response = requests.get(url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        bank_description_url = list()
        for link in BeautifulSoup(response.content,parse_only=SoupStrainer('a'),
                                features="html.parser"):
            # Only keep link corresponding to bank description link
            if link.has_attr('href') and len(link['href'].split('/'))>=4:
                if link['href'].split('/')[3]=="bank":
                    bank_description_url.append(link['href'])
    else:
        print(f"Failed to fetch the page. HTTP status: {response.status_code} "
            "Please try again later")
        sys.exit()
    return bank_description_url

def scrapping_description_bank_page(url):
    """
    Scrapes the specified URL for bank information and returns the bank name
    and a dictionary containing details from the About section.

    Args:
        url (str): The URL of the bank's page to be scraped.

    Returns:
        tuple: A tuple containing the bank name (str) and a dictionary with
               information from the About section. The dictionary has keys
               corresponding to the following categories: "Website",
               "Headquarters", "CEO/chair", "Supervisor", and "Ownership".
               If information is not available, the value is set to "None".

    Raises:
        None: However, if the HTTP request fails, it prints an error message and
              terminates the script.

    Example:
        >>> fetch_bank_info("https://www.examplebank.org/bank/abc_bank")
        ('ABC Bank', {'Website': 'www.abcbank.com',
                      'Headquarters': 'New York, NY',
                      'CEO/chair': 'John Doe', 'Supervisor': 'Jane Smith',
                      'Ownership': 'Private'})
    """
    # List of categories in section About
    list_info_about = [
        "Website",
        "Headquarters",
        "CEO/chair",
        "Supervisor",
        "Ownership",
    ]
    # Send an HTTP request to the target URL
    response = requests.get(url)
    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Initiate dictionary to concatenate info
        dict_about_section = dict()
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        # Extract bank name that is contains in bank title
        title_page = soup.title.string
        bank_name = title_page.split('–')[-1].strip()
        for elt in list_info_about:
            # Detect the tag 
            tag_elt = soup.find('td', string=elt)
            if tag_elt:
                # Retrieve content of the next td tag
                tag_content = tag_elt.find_next_sibling('td')
                # Add this element to dictionnary
                dict_about_section[elt] = tag_content  
            else:
                # No informations available in About section
                dict_about_section[elt] = "None"
    else:
        print(f"Failed to fetch the page. HTTP status: {response.status_code} "
            "Please try again later")
        sys.exit()
    return bank_name, dict_about_section
 
    
def process_raw_info(dict_info):
    """
    Process a dictionary of raw information and return a cleaned dictionary.

    Args:
        dict_info (dict): A dictionary containing raw information.

    Returns:
        dict: A dictionary containing cleaned information.

    Raises:
        SystemExit: If the function cannot recognize the raw information.

    Example:
        >>> dict_info = {"Website": "<a href='https://www.example.com'>Example</a>"}
        >>> process_raw_info(dict_info)
        {'Bank Website': 'Example'}

    """
    # Instanciate clean_dict that will contains info extracted from raw
    clean_dict = dict()
    for elt in dict_info.keys():
        if elt == "Website":
            clean_dict["Bank Website"] = dict_info[elt].find('a').text
        elif elt == "Headquarters":
            full_address = dict_info[elt].find_all('div')
            address = ','.join([full_address[0].text.strip(),
                                full_address[1].text.strip()])
            country = full_address[-1].text.strip()
            clean_dict["Headquarters address"] = address
            clean_dict["Headquarters country"] = country
        elif elt == "CEO/chair":
            a_tag = dict_info[elt].find('a')
            if a_tag: # a_tag not of NoneType
                # Extract the URL
                url_address = a_tag['href']
                # Extract the CEO's name
                name_ceo = a_tag.text
                clean_dict["CEO Name"] = name_ceo
                clean_dict["Board description"] = url_address
            else:
                clean_dict["CEO Name"] = "None"
                clean_dict["Board description"] = "None"
        elif elt == "Supervisor":
            anchor = dict_info[elt].find('a')
            if anchor and not isinstance(anchor,int):
                clean_dict["Supervisor Name"] = anchor.text
                clean_dict["Supervisor Website"] = anchor['href']
            else : 
                # No information available, fulfill with None 
                clean_dict["Supervisor Name"] = "None"
                clean_dict["Supervisor Website"] = "None"               
        elif elt == "Ownership":
            url = dict_info[elt].find('a')
            if url: # url not of NoneType
                clean_dict["Shareholder structure source"] = url['href']
            else:
                clean_dict["Shareholder structure source"] = "None"
        else:
            print("Error recognizing about informations\n"
                  "Please look into source code")
            sys.exit()
    # Return cleaned dictionary
    return clean_dict

def get_coordinates_google_api(address, api_key = API_KEY):
    """
    Get the latitude and longitude coordinates of an address using Google Maps
    API.

    Args:
        address (str): The address to look up.
        api_key (str): The Google Maps API key.

    Returns:
        tuple: A tuple containing the latitude and longitude coordinates
               of the address.

    Raises:
        SystemExit: If an error occurs with the API call.

    Example:
        >>> get_coordinates_google_api(
        ...     "1600 Amphitheatre Parkway, Mountain View, CA")
        (37.4224764, -122.0842499)

    """
    url = (f'https://maps.googleapis.com/maps/api/geocode/json?'
           f'address={address}&key={api_key}')
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == 'OK':
            latitude = data['results'][0]['geometry']['location']['lat']
            longitude = data['results'][0]['geometry']['location']['lng']
        else:
            print(f"API Error for {address}. Please try again later")
            latitude, longitude = 0.0,0.0
    else:
        print(f"API Error for {address}. Please try again later")
        latitude, longitude = 0.0,0.0
    return latitude,longitude

def main_scrapping_function(url):
    """
    Create a Pandas DataFrame from scraped information and return it.

    Args:
        url (str): The URL of the main page to scrape.

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing information on bank
        companies.

    Example:
        >>> url = "https://example.com"
        >>> create_bank_dataframe(url)
            Bank Name  Bank Website  Headquarters address  Headquarters country
        0    Example Bank www.example.com 123 Main St, Anytown  USA
        1    Another Bank www.anotherbank.com 456 Oak St, Anycity  USA
        ...
    """
    # Instanciate dataframe containing srapped info
    columns_dataframe = [
        "Bank Name",
        "Bank Website",
        "Headquarters address",
        "Headquarters country",
        "CEO Name",
        "Board description",
        "Supervisor Name",
        "Supervisor Website",
        "Shareholder structure source",
        "Source BankTrack",
        "Latitude",
        "Longitude",
    ]
    df = pd.DataFrame(columns = columns_dataframe)
    bank_list_url = scrapping_main_page_bank_track(url)
    for bank_url in bank_list_url:
        bank_name, raw_info = scrapping_description_bank_page(bank_url)
        clean_info = process_raw_info(raw_info)
        address = clean_info["Headquarters address"]
        country = clean_info["Headquarters country"]
        address_maps = (f"{address}, {country}")
        lat,long = get_coordinates_google_api(address_maps)
        clean_info["Latitude"] = lat
        clean_info["Longitude"] = long
        clean_info["Bank Name"] = bank_name
        clean_info["Source BankTrack"] = bank_url
        # Convert dictionary into Dataframe
        bank_data_df = pd.DataFrame(clean_info, index=[0])
        # Concat dataframes
        df = pd.concat([df, bank_data_df], ignore_index=True)
    # Make a remap of bank name based on manual_match_bank in order to have 
    # coherent key values in BOCC and banking_informations.csv
    df['Bank Name'] = df['Bank Name'].replace(manual_match_bank)
    # Return dataframe with all info on bank companies
    return df

def scrapping_company_location():
    """
    Scrapes the address information of companies using chatGPT and retrieves 
    their corresponding geographic coordinates using the Google Maps API.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns:
        - Company_name (str): Name of the company
        - Address_headquarters_source_chatGPT (str): Address of the company 
            headquarters obtained from chatGPT
        - Latitude (float): Latitude of the company headquarters obtained using 
            Google Maps
        - Longitude (float): Longitude of the company headquarters obtained 
            using Google Maps

    Raises:
        Exception: If the file path for the chatGPT data is invalid.
        Exception: If there is an error in obtaining coordinates from the 
        Google Maps API.
    """
    # Initiate Dataframe that will gather informations
    columns_dataframe = [
        "Company_name",
        "Address_headquarters_source_chatGPT",
        "Latitude",
        "Longitude",
    ]
    df_output = pd.DataFrame(columns = columns_dataframe)
    # Load list company and adress generated with chatGPT
    file_path = "./data_sources/Data_chatGPT_company_hq_adress.csv"
    df = pd.read_csv(file_path,sep = ";")
    for _,row in df.iterrows():
        # Create dict relative to one company
        company_info = dict()
        # Construct Google maps request API
        address_maps = row["Address_source_chatGPT"]
        if address_maps == "N/A":
            lat,long = 0.0,0.0
        else :
            # Get answer from Google maps
            lat,long = get_coordinates_google_api(address_maps)
        # Store info in company_info dict
        company_info["Company_name"] = row["Company"]
        company_info["Address_headquarters_source_chatGPT"] = row\
                                                    ["Address_source_chatGPT"]
        company_info["Latitude"] = lat
        company_info["Longitude"] = long
        # Convert dictionary into Dataframe
        company_df = pd.DataFrame(company_info, index=[0])
        # Concat dataframes
        df_output = pd.concat([df_output, company_df], ignore_index=True)
    # Add column Carbon_bombs_connexion
    df_output = add_column_carbon_bombs_connexion(df_output)
    return df_output

def add_column_carbon_bombs_connexion(df_company):
    """
    Add a column to the input dataframe containing a comma-separated string of 
    carbon bomb names.

    This function reads a CSV file containing company names and their 
    associated carbon bombs. It then creates a new column in the input 
    dataframe, 'Carbon_bomb_connected', which contains the carbon bomb
    names associated with each company as a comma-separated string.

    Args:
        df_company (pd.DataFrame): Input dataframe containing company names 
        in a column called 'Company_name'.

    Returns:
        pd.DataFrame: The input dataframe with an additional column, 
                    'Carbon_bomb_connected', containing the carbon bomb names 
                    associated with each company as a comma-separated string.

    """
    # Load dataframe with carbon_bombs_connexion
    df = pd.read_csv("./data_cleaned/connexion_carbonbombs_company.csv")
    # Create a dictionary to store the Carbon_bomb_name for each company
    company_bombs = {}
    # Iterate through the rows of df
    for _, row in df.iterrows():
        company = row['Company']
        bomb_name = row['Carbon_bomb_name']
        if company not in company_bombs:
            company_bombs[company] = [bomb_name]
        else:
            if bomb_name not in company_bombs[company]:
                company_bombs[company].append(bomb_name)
    # Create a new column in df_company to store the Carbon_bomb_names
    df_company['Carbon_bomb_connected'] = (df_company['Company_name']
                                           .map(company_bombs))
    # Convert column format to a real string not a list
    df_company['Carbon_bomb_connected'] = (\
        df_company['Carbon_bomb_connected'].fillna(''))
    df_company['Carbon_bomb_connected'] = (\
        df_company['Carbon_bomb_connected'].apply(lambda x: ','.join(x)))
    return df_company


if __name__ == '__main__':
    # Main function scrapping Bank Track informations
    df = main_scrapping_function(URL)
    # Main function scrapping company location
    df = scrapping_company_location()