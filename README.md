# CarbonBombs

![Screenshot](docs/Carbon_bombs.jpg)
*(Image source = <https://www.theguardian.com/environment/ng-interactive/2022/may/11/fossil-fuel-carbon-bombs-climate-breakdown-oil-gas>)*

425 fossil fuel projects around the world will generate more that 1 gigatonne of CO2 emissions during their lifetime.
<https://www.carbonbombs.org/> proposed a representation of those CarbonBombs and their link between Banks and Company that operate them.
This repository contains all informations needed to reproduce the data used on <https://www.carbonbombs.org/>

# Acronym used in this repository

- BOCC = Banking On Climate Chaos
- CB = Carbon Bombs (also corresponds to Carbon Bombs research paper)
- GEM = Global Energy Monitor

# Data sources

## Input data

Files located in data_sources folder consitute the source data associated to the construction of our database :

- 1-s2.0-S0301421522001756-mmc2.xlsx : Data associated to research of Kühne and al. : Carbon Bombs - Mapping key fossil fuel projects that can be accessed at the following adress : <https://www.sciencedirect.com/science/article/pii/S0301421522001756>
- GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx : Data containing banking transaction for the financement of Climate Bombs. Data can freely be download at <https://www.bankingonclimatechaos.org/>. The download link is the following (it might change over time): <https://www.bankingonclimatechaos.org/wp-content/themes/bocc-2021/inc/bcc-data-2023/GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx>
- Global-Coal-Mine-Tracker-April-2023.xlsx : The Global Coal Mine Tracker (GCMT) is a worldwide dataset of coal mines and proposed projects. The tracker provides asset-level details on ownership structure, development stage and status, coal type, production, workforce size, reserves and resources, methane emissions, geolocation, and over 30 other categories. This data will not be tracked under this repository as it Distributed under a Creative Commons Attribution 4.0 International License. It can be freely download through this page : <https://globalenergymonitor.org/projects/global-coal-mine-tracker/download-data/>
- Global-Oil-and-Gas-Extraction-Tracker-July-2023.xlsx : The Global Oil and Gas Extraction Tracker (GOGET) is a global dataset of oil and gas resources and their development. GOGET includes information on discovered, in-development, and operating oil and gas units worldwide, including both conventional and unconventional assets. This data will not be tracked under this repository as it Distributed under a Creative Commons Attribution 4.0 International License. It can be freely download through this page : <https://globalenergymonitor.org/projects/global-oil-gas-extraction-tracker/>
- undata_*.csv : Statistical datasets downloaded from the UNSD databases website (<https://data.un.org/>)
- company_url.csv : URLs of website and logos of companies found manually.
- Data_chatGPT_company_hq_adress.csv : This file includes the addresses of headquarters sourced from ChatGPT. Each address has been manually reviewed. In cases where we were uncertain about the specific location, we provided only the country. There may still be occasional inaccuracies in the exact addresses of the company headquarters. However, these addresses are primarily used to associate fossil fuel companies with a particular country. Please reach out to us if you notice any discrepancies.
- manual_match.py : This script establishes key-value pair correspondences to facilitate alignment between different databases. For more details, please refer to the docstring file.
- metadatas.csv : This file provides details about the significance of each column in every output file. This CSV is directly appended to the final Excel file.
- uniform_company_names.json : File generated during script execution that provides standardized names for fossil fuel companies.

## Output data

The files located in the "data_cleaned" folder constitute the database we use to construct the visualization tool available at <https://www.carbonbombs.org/>. The following files are included:

- bank_data.csv: This file was generated through web scraping of <https://www.banktrack.org/>. More specifically, we retrieved information from all the banks listed at <https://www.banktrack.org/banks>, focusing on the "About" section (i.e., Website, Headquarters, CEO/Chair, Supervisor, Ownership).
  - Source of the data: <https://www.banktrack.org/>
- carbon_bombs_data.csv: This file contains information on Carbon Bombs extracted from two different data sources: the CB research paper and the GEM database. The data source is specified in the column title when it is common to all columns (e.g., GEM). When data sources differ within a column, a "column1/column2_source" column provides details on the data source for each row.
  - Source of the data:
    - Global-Coal-Mine-Tracker-April-2023.xlsx (Coal Mine)
    - Global-Oil-and-Gas-Extraction-Tracker-July-2023.xlsx (Oil & Gas Extraction site)
    - 1-s2.0-S0301421522001756-mmc2.xlsx (Carbon Bombs research paper)
- company_data.csv: This file contains the approximative address, associated coordinates and the list of connected Carbon Bombs for each company. Approximative address has been generated with ChatGPT and verified manually.
  - Source of the data: ./data_cleaned/Data_chatGPT_company_hq_adress.csv
- connection_carbonbombs_company.csv : This file contains the connections between each company and various Carbon Bombs.
  - Source of the data: ./data_cleaned/connection_carbonbombs_company.csv
- connection_bank_company.csv : This file contains information on the amount financed by each bank to companies involved in Carbon Bombs. It primarily consists of a filtered version of the BOCC database, including only companies present in the "Parent_company" field of the GEM database.
  - Source of the data: GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx
- country_data.csv : This file contains the deduplicated list of countries where Carbon Bombs have been detected. It includes also additional statistical data of each country, extracted from the UN data website (<https://data.un.org/>)
  - Source of the data:
    - carbon_bombs_informations.csv (created hereabove and located in the data_cleaned folder)
    - undata_*.csv
- carbon_bombs_all_datasets.xlsx : This file contains all previous files and metadata csv file but stored into different tabs. Tabs name match the dataset name.

The 'img' folder stores all images, in particular:
- bank logos in subfolder 'logo_bank'.
  - Source of the data: <https://www.banktrack.org/>
- company logos in subfolder 'logo_company'. The ID_tmp provides a temporary ID for each studied company. The list of companies is based on the file company_informations.csv.
  - Source of the data: Official websites and Wikipedia. Additional sources can also be used and are referenced in the file company_url.csv

# Install and generate data

To install use this command:

```bash
pip install -e .
```

Then you can generate dataset using this command:

```bash
python scripts/generate_dataset.py
```

# Code Documentation

Code documentation has been generated using Sphinx Library.
To consult it, you can enter following command in your terminal :<br>
**open ./docs/_build/html/index.html**<br>
It will open documentation locally into your Web Browser.


# Precautions when manipulating our data

In this repository, our goal was to reconcile different data sources to establish a more comprehensive view of the organization around the exploitation of these carbon bombs. Some precautions regarding our data manipulation process are detailed below :

- To establish the link between the names of extractive companies used by BOCC and those used in the GEM database, some extractive companies had to be manually associated using the dictionaries presented in manual_match. Some errors or inaccuracies may be present in this manual association. We invite you to directly consult the manual_match file to observe the closeness in the connections we were able to establish. In case of an error, please contact the repository owner to submit a correction: <hellodataforgood@gmail.com>
- To establish the link between the names of Carbon Bombs by K.Kühne in his research paper and the names used in the GEM database, some Carbon Bombs had to be manually associated using the dictionaries presented in manual_match. Some errors or inaccuracies may be present in this manual association. We invite you to directly consult the manual_match file to observe the closeness in the connections we were able to establish. In case of an error, please contact the repository owner to make a correction: <hellodataforgood@gmail.com>
- In the GEM and CB research paper databases, the description of extraction sites is provided with different levels of granularity. This mainly concerns Gas & Oil carbon bombs, where in the CB research paper database, the information is presented at the level of a geological zone (Shale/OffShore/OnShore), while in the GEM database, the information remains presented at the level of each extraction site. A concrete example of this difference in granularity can be represented by the Carbon Bomb designated "Yucatan Platform Offshore" in the CB research paper, which consists of several extraction sites such as Akal, Nohoch, Chac, and Kutz, as described on the OnePetro oil company's website: <https://onepetro.org/OTCONF/proceedings-abstract/01OTC/All-01OTC/OTC-13177-MS/34073><br>
In these cases, we have tried to gather the extraction sites within a geological area as much as possible (see manual_match dictionary where Extraction sites within a geological area are separated by "$"). It should be noted that, as with the previous point, some errors or inaccuracies may be present in this manual association. In case of an error, please contact the repository owner to make a correction: <hellodataforgood@gmail.com><br>
It is also important to note that the Latitude and Longitude columns only contain the GPS coordinates of the first extraction site to simplify data extraction.
- Data sourced from ChatGPT carries a significant degree of uncertainty. It's essential to note that addresses of headquarters generated using ChatGPT are primarily utilized to link fossil fuel companies to a specific country, rather than to provide precise addresses.

## Others details on our work

- For the `Khafji` carbon bomb, [the ownership is splitted equally for both Kuwait and Saudi Arabia](https://www.sciencedirect.com/science/article/pii/S0301421522001756). Since we don't want to merge two countries and we don't want to duplicate the bomb, we'll attribute this carbon bomb to Kuwait to insure a better repartition (Kuwait has 3 bombs and Saudi Arabia 23).
- Please note that within the GEM database, when the starting year spans two consecutive years, only the earlier year is recorded. For instance, if the starting year is denoted as 2022-2023 in the GEM database, we would only record it as 2022 (Example : Saharpur Jamarpani Coal mine).
