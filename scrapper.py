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
import awoc
import requests
from bs4 import BeautifulSoup, SoupStrainer
from geopy.geocoders import Nominatim
from data_sources.manual_match import manual_match_bank
from data_sources.uniform_company_name import uniform_company_name


# Define the target URL of Bank Track
# URL = 'https://www.banktrack.org/banks'

def get_url_from_banktrack_div(div) -> str:
    """
    Return the url given a div found into banktrack website
    
    With this input:
    `<div class="bank-logo" style="background-image: url('https://www.banktrack.org/thumbimage.php?image=abn-logo.jpg;width=190');"></div>`

    Return this:
    https://www.banktrack.org/thumbimage.php?image=abn-logo.jpg&amp
    """
    div = str(div)
    url = div.split("url('")[1]
    url = url.split(";")[0]
    
    return url


def select_logos(csv_file, url_field):
    """
    Select URL from URLs located in one or several column of the specified CSV
    file. Keep only rows with existing URLs.
    If URLs are located on several columns, this code prioritizes the URL of
    the first column provided by url_field, then the 2nd column if URL is
    missing in the first column, then the 3rd column...
    Example for url_field = ['Logo_Col1','Logo_Col2','Logo_Col3']:
    df['URL_logo'] = df['Logo_Col1'].fillna(df['Logo_Col2']) \
                                    .fillna(df['Logo_Col3'])

    Args:
        csv_file (str): The path and name of the CSV file.
        url_field (str or list): The column or list of columns
                                 (in priority order) where URLs are located.

    Returns:
        Dataframe with columns from csv_file and an additional column Logo_URL
        containing the selected URL of logos.

    Raises:
        None.
    """
    df = pd.read_csv(csv_file, sep=",")

    # Identification of the URL
    if len(url_field)==1 or isinstance(url_field, str):
        df['Logo_URL'] = df.loc[:,url_field]
    else:
        df['Logo_URL'] = df.loc[:,url_field[0]]
        for i in range(len(url_field)-1):
            df['Logo_URL'] = df['Logo_URL'].fillna(df.loc[:,url_field[i+1]])

    # Rows with missing URL removed
    df.dropna(subset = ['Logo_URL'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df

##############################################################################
# The 2 following def are currently not in use
def saving_image(url, output_filename, output_folder_path):
    """
    Download and store an image located at a specific url.

    Args:
        url (str): The URL of the image.
        output_filename (str): The filename of the image to be saved.
        output_folder_path (str): The destination path to store the image.

    Returns:
        Image located in the defined folder.

    Raises:
        None: However, if the HTTP request fails, it prints an error message
              and terminates the script.
    """
    # Send an HTTP request to the target URL (timeout is set to 60 seconds)
    myfile = requests.get(url, timeout=60)

    # Check if the request was successful (HTTP status code 200)
    if myfile.status_code != 200:
        print(
            f"Failed to fetch the page. HTTP status: {myfile.status_code} "
            "Please try again later"
        )
        sys.exit()

    path = output_folder_path + output_filename
    url_content = myfile.content
    # write the contents to a csv file
    output_file = open(path, "wb")
    output_file.write(url_content)
    # close the file
    output_file.close()

def saving_logos(csv_file, entity_name_field, url_field, destination_folder, 
                 file_extension=""):
    """
    Download and store all images from URLs located in the specified CSV file.

    Args:
        csv_file (str): The path and name of the CSV file.
        entity_name_field (str): The column providing entity names.
        url_field (str or list): The column or list of columns
                                 (in priority order) where URLs are located.
        destination_folder (str): The destination path to store images.
        file_extension (str): Extension of the output file.
                              By default, extension of the downloaded file.

    Returns:
        Images located in the defined folder.
        By convention, images will be saved with the following name:
        entity_name.extension where:
        - 'entity_name' is the entity name
        - 'extension' is the file extension provided by the URL.

    Raises:
        None: However, if the HTTP request fails, it prints an error message
              and terminates the script.
    """
    df = select_logos(csv_file, url_field)

    for i in range(len(df)):
        entity_logo_url = df.loc[i,'Logo_URL']
        # File extension for record i
        if file_extension == "":
            #file_extension = df.loc[i,'File_extension']
            file_extension_i = entity_logo_url.rsplit('.', 1)[-1] \
                                              .split('?', 1)[0]
        else:
            file_extension_i = file_extension
        # File name for record i
        entity_filename = df.loc[i,entity_name_field].replace('/', '_') + '.' + file_extension_i
        try:
            saving_image(entity_logo_url, entity_filename, destination_folder)
        except:
            print(f"The image for the company {df.loc[i,entity_name_field]} "
                  "couldn't be downloaded. "
                  "URL: {entity_logo_url}")
##############################################################################


def scrapping_main_page_bank_track(url):
    """
    Scrapes the main page of bank track website and extracts URLs of individual 
    bank description pages.

    Args:
        url (str): The URL of the main page to be scraped.

    Returns:
        list: A list of URLs (strings) pointing to individual bank description
              pages.
        list: A list of logo urls for each bank

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
    if response.status_code != 200:
        print(f"Failed to fetch the page. HTTP status: {response.status_code} "
            "Please try again later")
        sys.exit()
        
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    banks = soup.find_all("a", {"class": "bankprofile"})
    
    logos = [b.find("div", {"class", "bank-logo"}) for b in banks]
    logos = [get_url_from_banktrack_div(l) for l in logos]
    
    bank_description_url = [b.get("href") for b in banks]

    return bank_description_url, logos
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
    bank_list_url, bank_logos = scrapping_main_page_bank_track(url)
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
    # Create columns bank logos
    df["Bank logo"] = bank_logos
    # Remap some country name
    df['Headquarters country'].replace({'Taiwan, Republic of China': 'Taiwan',
                                        'Russian Federation': 'Russia',
                                        },inplace = True)
    # Add World Region associated to Headquarters country
    world_region = awoc.AWOC()
    df["World Region"] = df["Headquarters country"].apply(
        world_region.get_country_continent_name)
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
        if address_maps == "N/A" or address_maps != address_maps:
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
    # Rename company column with uniformed Name
    df_output['Company_name'] = (df_output['Company_name'].
                                 replace(uniform_company_name))
    # Drop perfect duplicates companies
    df_output = df_output.drop_duplicates().reset_index(drop=True)
    # Isolate duplicated companies that will be droped
    duplicates = df_output.loc[df_output.duplicated('Company_name',
                                                    keep=False)].copy()
    # Calculates len of adress column
    duplicates["column_len"]=(duplicates["Address_headquarters_source_chatGPT"]
                              .apply(len))
    # Order by Carbon_bomb_connected first (NaN will always be last record) and
    # column_len (We keep the simpliest address as there is some doubt)
    duplicates.sort_values(["Company_name","Carbon_bomb_connected","column_len"]
                           ,inplace=True)
    duplicates.reset_index(inplace=True)
    # Get index of column to drop in the main dataframe df_output
    drop_index = list(duplicates.drop_duplicates("Company_name",keep="last")
                      ["index"])
    # Drop those index from df_output
    df_output.drop(drop_index,inplace = True)
    # Add country associated to the coordinates
    def get_country(lat, long):
        if lat == 0 and long == 0:
            pass
        location = geolocator.reverse([lat, long],
                                      exactly_one=True,
                                      language='en')
        address = location.raw['address']
        country = address.get('country', '')
        return country
    
    geolocator = Nominatim(user_agent="geoapiExercises")
    df_output['Country'] = df_output.apply(lambda row: get_country(
        row['Latitude'],row['Longitude']), axis=1)
    # Add World Region associated to Headquarters country
    world_region = awoc.AWOC()
    df_output["World_region"] = ""
    for index, row in df_output.iterrows():
        if row["Latitude"] == 0 and row["Longitude"] == 0:
            continue
        else:
            df_output.at[index, 'World_region'] = world_region.get_country_continent_name(row['Country'])

    # Add logo URLs
    # Load list company and logo URL
    csv_file_company = "./data_sources/company_url.csv"
    company_url_field = ['Logo_OfficialWebsite',
                         'Logo_Wikipedia_Large',
                         'Logo_OtherSource'
                        ]
    df_logos = select_logos(csv_file_company, company_url_field)
    # Add logo to the output dataframe
    df_logos = df_logos.reindex(columns=['Company_name','Address_headquarters_source_chatGPT','Logo_URL'])
    df_output = pd.merge(df_output, df_logos,
                         on=['Company_name','Address_headquarters_source_chatGPT'],
                         how='left')
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