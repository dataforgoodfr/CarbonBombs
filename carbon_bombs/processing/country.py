"""Function to process countries information"""
import pandas as pd

from carbon_bombs.io.cleaned import load_carbon_bombs_database
from carbon_bombs.io.undata import load_undata
from carbon_bombs.utils.logger import LOGGER


def _get_countries():
    """
    Loads the country column of the carbon bomb database.

    Returns:
    --------
    pd.DataFrame:
        A pandas DataFrame with the following columns:
            - 'Country (CB)': country where carbon bomb projects are located.

    Notes:
    ------
    The data file is expected to be in the following path:
    "./data_cleaned/carbon_bombs_informations.csv".
    """
    df = load_carbon_bombs_database()[["Country_source_CB"]]

    # Split mulitple countries, separated by '-', in multiple countries
    df = df.assign(Country_source_CB=df.Country_source_CB.str.split("-")).explode(
        "Country_source_CB"
    )

    # Remove duplicates
    df = df.drop_duplicates()

    # Sort by increasing values
    df = df.sort_values(by="Country_source_CB", ascending=True)

    return df


def format_serie_values(val):
    """Format string as interger or float"""
    if not isinstance(val, str):
        return val

    val = val.replace(",", "")

    if "." in val:
        return float(val)

    return int(val)


def format_countries_df_with_wanted_series(df_countries: pd.DataFrame) -> pd.DataFrame:
    """Format raw countries dataframe with a row by country
    and wanted KPI series (see `columns_map` dict in function).

    Parameters
    ----------
    df_countries : pd.DataFrame
        Dataframe with country names, series value, year for each
        serie. It's the merging of Country from CB data and
        UN datasets

    Returns
    -------
    pd.DataFrame
        Countries dataframe with one row per country and
        wanted KPI series as columns with a year column for
        each KPI
    """
    columns_map = {
        "Population mid-year estimates (millions)": "Population_in_millions",
        "Surface area (thousand km2)": "Surface_thousand_km2",
        "GDP in current prices (millions of US dollars)": "GDP_millions_US_dollars",
        "GDP per capita (US dollars)": "GDP_per_capita_US_dollars",
        "Emissions (thousand metric tons of carbon dioxide)": "Emissions_thousand_tons_CO2",
        "Emissions per capita (metric tons of carbon dioxide)": "Emissions_per_capita_tons_CO2",
    }
    LOGGER.debug(
        f"Keep only the following informations from UNData: {list(columns_map.keys())}"
    )

    # Keep only some wanted KPI
    df_countries_filtered = df_countries.loc[
        df_countries["Series"].isin(columns_map.keys())
    ]

    # get max years by country and serie
    country_year_max = df_countries_filtered.groupby(
        ["Country_source_CB", "Series"]
    ).agg(year_max=("Year", "max"))
    country_year_max_df = country_year_max.merge(
        df_countries,
        left_on=["Country_source_CB", "Series", "year_max"],
        right_on=["Country_source_CB", "Series", "Year"],
    ).drop(columns=["year_max"])

    # Change serie name to wanted format
    country_year_max_df["Series"] = country_year_max_df["Series"].replace(columns_map)

    # Init final countries df
    final_countries_df = df_countries[["Country_source_CB"]].drop_duplicates()

    LOGGER.debug("Add last year found for each information")
    for serie, serie_df in country_year_max_df.groupby("Series"):
        # Pivot dataframe to get values and year into the same row
        serie_df = serie_df.pivot(
            index=["Country_source_CB"], columns=["Series"], values=["Value", "Year"]
        ).reset_index()

        # Format year dtype
        serie_df["Year"] = serie_df["Year"].astype(int)

        # Change columns name
        serie_df.columns = ["Country_source_CB", serie, f"Year_{serie}"]

        # Format values
        serie_df[serie] = serie_df[serie].apply(format_serie_values)

        # merge to add KPI for each countries with the last year available for this metric
        final_countries_df = final_countries_df.merge(
            serie_df, on=["Country_source_CB"]
        )

    return final_countries_df


def create_country_table():
    """
    Creates the table of countries extracted from the
    carbon_bombs_informations file.

    Args:
    -----
        undata_folder_path (str): The folder path where csv files downloaded
        from the UN data website are stored.

    Returns:
    --------
        pandas.DataFrame: A pandas DataFrame listing unique countries from the
        carbon_bombs_informations file.

    Raises:
    -------
        None.

    Notes:
    ------
        None.
    """
    LOGGER.debug("Start creation of countries dataset")
    # Load Dataframe listing unique countries with identified carbon bombs
    LOGGER.debug("Get country from Carbon bombs dataset")
    df_cb_countries = _get_countries()

    # Load UNData with wanted information of countries
    LOGGER.debug("Load UNData dataset")
    df_undata = load_undata()

    # Merge the 2 dataframes on country name column.
    LOGGER.debug("Merge country from CB with UNData dataframes")
    df_countries = df_cb_countries.merge(
        df_undata,
        left_on="Country_source_CB",
        right_on="Region_Country_Area_name",
        how="inner",
        sort=True,
    )

    # Format countries df to get on row per country
    LOGGER.debug("Keep only wanted information from UNData")
    df_countries = format_countries_df_with_wanted_series(df_countries)

    # sort df
    LOGGER.debug("Sort dataset by country")
    df_countries = df_countries.sort_values(by="Country_source_CB")

    # Return cleaned dataframe
    return df_countries
