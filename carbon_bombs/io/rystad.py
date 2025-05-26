"""Functions to read Rystad dataset for Carbon Bombs and Gasoil projects"""
import pandas as pd

from carbon_bombs.conf import FPATH_SRC_RYSTAD_CB
from carbon_bombs.conf import SHEETNAME_RYSTAD_CB_EMISSION
from carbon_bombs.conf import SHEETNAME_RYSTAD_CB_COMPANY
from carbon_bombs.conf import SHEETNAME_RYSTAD_GASOIL_EMISSION
from carbon_bombs.conf import SHEETNAME_RYSTAD_CB_EMISSION_INFERIOR_1GT
from carbon_bombs.utils.logger import LOGGER
from carbon_bombs.utils.location import clean_project_names_with_iso


def load_rystad_emission_database(sheet_name: str) -> pd.DataFrame:
    """
    Load Rystad Carbon Bombs and simple gasoil projects emissions data from Excel.

    Parameters
    ----------
    sheet_name : str
        Name of the sheet to load.

    Returns
    -------
    pandas.DataFrame
        A dataframe containing the data from the specified sheet.
    """
    if sheet_name == SHEETNAME_RYSTAD_CB_EMISSION:
        renamed_columns = {
            "Project name": "Project_name",
            "Country": "Country",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Start-up year min asset": "Start_up_year",
            "Producing  - Potential emissions (GTCO2)": "Producing_potential_emissions_in_GTCO2",
            "Short term expansion - Potential emissions (GTCO2)": "Short_term_expansion_potential_emissions_in_GTCO2",
            "Long term expansion - Potential emissions (GTCO2)": "Long_term_expansion_potential_emissions_in_GTCO2",
            "Total potential emissions (GTCO2)": "Total_potential_emissions_in_GTCO2",
        }
        log_message = "Read Rystad data: all Carbon Bombs project emissions > 1 GtCO2"
    elif sheet_name == SHEETNAME_RYSTAD_CB_EMISSION_INFERIOR_1GT:
        renamed_columns = {
            "Project name": "Project_name",
            "Country": "Country",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Start-up year min asset": "Start_up_year",
            "Producing  - Potential emissions (GTCO2)": "Producing_potential_emissions_in_GTCO2",
            "Short term expansion - Potential emissions (GTCO2)": "Short_term_expansion_potential_emissions_in_GTCO2",
            "Long term expansion - Potential emissions (GTCO2)": "Long_term_expansion_potential_emissions_in_GTCO2",
            "Total potential emissions (GTCO2)": "Total_potential_emissions_in_GTCO2",
        }
        log_message = "Read Rystad data: all Carbon Bombs project emissions < 1 GtCO2"
    elif sheet_name == SHEETNAME_RYSTAD_GASOIL_EMISSION:
        renamed_columns = {
            "Project name": "Project_name",
            "Country": "Country",
            "Latitude": "Latitude",
            "Longitude": "Longitude",
            "Start-up year min asset": "Start_up_year",
            "Producing  - Potential emissions": "Producing_potential_emissions",
            "Short term expansion - Potential emissions": "Short_term_expansion_potential_emissions",
            "Long term expansion - Potential emissions": "Long_term_expansion_potential_emissions",
            "Total potential emissions (mtCO2)": "Total_potential_emissions",
        }
        log_message = "Read Rystad data: all Gasoil project emissions > 5MTCO2"
    else:
        raise ValueError(f"Unsupported sheet name: {sheet_name}")

    LOGGER.debug(log_message)

    df = pd.read_excel(
        FPATH_SRC_RYSTAD_CB,
        sheet_name=sheet_name,
        engine="openpyxl",
    )
    # Only keep relevant columns
    df = df.loc[:, renamed_columns.keys()]
    # Rename columns
    df = df.rename(columns=renamed_columns)
    # Remove total row if applicable
    df = df[df["Project_name"] != "SUMS"]
    # Clean project names
    clean_project_names_with_iso(df)
    return df


def load_rystad_cb_company_database():
    """
    Load Carbon Bombs database companies from Rystad.

    Returns
    -------
    pandas.DataFrame:
        A dataframe containing the data from the database.
    """
    LOGGER.debug("Read Rystad data: all Carbon Bombs project companies")
    df = pd.read_excel(
        FPATH_SRC_RYSTAD_CB,
        sheet_name=SHEETNAME_RYSTAD_CB_COMPANY,
        engine="openpyxl",
    )
    renamed_columns = {
        "Project name": "Project_name",
        "Country": "Country",
        "Company (ranking from the highest to the lowest paricipations in the project)": "Company_involved",
        "Company's headquarters country": "Company_headquarters_country",
        "Potential emissions (GTCO2)": "Potential_emissions_in_GTCO2",
    }
    # Only keep columns of interest for the project
    df = df.loc[:, renamed_columns.keys()]
    # Rename columns
    df = df.rename(columns=renamed_columns)
    # Remove row if Project_name = "SUMS"
    df = df[df["Project_name"] != "SUMS"]
    # Clean project names
    clean_project_names_with_iso(df)

    # Split companies and duplicate rows while maintaining order
    companies_split = df["Company_involved"].str.split("\n")
    df_expanded = df.loc[df.index.repeat(companies_split.str.len())]

    # Get the flattened list of companies
    companies_flat = [company for companies in companies_split for company in companies]
    df_expanded["Company_involved"] = companies_flat

    # Create involvement rank for each company in their project
    involvement_ranks = []
    for companies in companies_split:
        # Add ranks from 1 to N for each company in the project
        involvement_ranks.extend(range(1, len(companies) + 1))
    df_expanded["Company_involvement_rank"] = involvement_ranks

    # Split and expand headquarters countries to match companies
    headquarters_split = df["Company_headquarters_country"].str.split("\n")
    headquarters_flat = []
    for idx, companies in enumerate(companies_split):
        countries = headquarters_split.iloc[idx]
        # If we have the same number of countries as companies, use them
        if len(countries) == len(companies):
            headquarters_flat.extend(countries)
        # If numbers don't match, use the first country for all companies (fallback)
        else:
            LOGGER.warning(
                f"Project {df['Project_name'].iloc[idx]} has a mismatch between number of companies and headquarters countries."
            )
            headquarters_flat.extend([countries[0]] * len(companies))

    df_expanded["Company_headquarters_country"] = headquarters_flat
    df_expanded = df_expanded.reset_index(drop=True)

    return df_expanded
