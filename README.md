# CarbonBombs



# Data sources
Each md5sum associated to source data has been generated in md5sum_values.<br>
Data Source associated to the construction of our database are constitued of the following files :
- 1-s2.0-S0301421522001756-mmc2.xlsx : Data associated to research of Kühne and al. : Carbon Bombs - Mapping key fossil fuel projects that can be accessed at the following adress : https://www.sciencedirect.com/science/article/pii/S0301421522001756
- urgewald_GOGEL2022V1.xlsx : Data containing Fossil fuels company (Gas & Oil only) that are directly related to Carbon Bombs. This data can be directly access on https://gogel.org/. You must create a personal account (https://gogel.org/user/register) to get access to the data.
- urgewald_GOGEL2021V2.xlsx : Same data as previously for year 2021.
- urgewald_GCEL_2022_download_0.xlsx : Data containing Fossil fuels company (Coal only) that are directly related to Carbon Bombs. This data can be directly access on https://www.coalexit.org/welcome. You must create a personal account to get access to the data. the download link is the following : https://www.coalexit.org/sites/default/files/download_private/urgewald%20GCEL%202022%20download_0.xlsx
- GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx : Data containing banking transaction for the financement of Climate Bombs. Data can freely be download at https://www.bankingonclimatechaos.org/. The download link is the following (it might change over time): https://www.bankingonclimatechaos.org/wp-content/themes/bocc-2021/inc/bcc-data-2023/GROUP-Fossil_Fuel_Financing_by_Company_Banking_on_Climate_Chaos_2023.xlsx
- Global-Coal-Mine-Tracker-April-2023.xlsx : The Global Coal Mine Tracker (GCMT) is a worldwide dataset of coal mines and proposed projects. The tracker provides asset-level details on ownership structure, development stage and status, coal type, production, workforce size, reserves and resources, methane emissions, geolocation, and over 30 other categories. This data will not be tracked under this repository as it Distributed under a Creative Commons Attribution 4.0 International License. It can be freely download through this page : https://globalenergymonitor.org/projects/global-coal-mine-tracker/download-data/
- Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx : The Global Oil and Gas Extraction Tracker (GOGET) is a global dataset of oil and gas resources and their development. GOGET includes information on discovered, in-development, and operating oil and gas units worldwide, including both conventional and unconventional assets. This data will not be tracked under this repository as it Distributed under a Creative Commons Attribution 4.0 International License. It can be freely download through this page : https://globalenergymonitor.org/projects/global-oil-gas-extraction-tracker/


### Update Sphinx documentation

When releasing the new version of a project please update Sphinx documentation with the following steps :
- 1: Naviguate into docs folder with the following command : cd ./docs
- 2 : Update release number of project with release variable of conf.py file
- 3 : Delete .rst file in order to ensure actualisation of the documentation with the following comand: rm -rf *.rst
- 3 : Update new .rst file with the following command : sphinx-apidoc -o . ..
- 4 : Update html documentation with the following command : make html

