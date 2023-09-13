import pandas as pd

from carbon_bombs.conf import FPATH_SRC_UNDATA_CO2
from carbon_bombs.conf import FPATH_SRC_UNDATA_GDP
from carbon_bombs.conf import FPATH_SRC_UNDATA_POPU
from carbon_bombs.utils.logger import LOGGER


def load_undata():
    """
    Load all csv files downloaded from the UN data website.

    Args:
        file_paths (list of str): The list of csv file paths.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns:
        - Region_Country_Area_ID (int): A numerical value associated to the
          region/country/area.
        - Region_Country_Area_name (str): The concept refers to the country,
          geographical or political group of countries or regions within a
          country, as defined by the UN data website. The concept is subject
          to a variety of hierarchies, as countries comprise territorial
          entities that are States (as understood by international law and
          practice), regions and other territorial entities that are not
          States (such as Hong Kong) but for which statistical data are
          produced internationally on a separate and independent basis.
        - Year (int): Reference period of the statistical series.
        - Category (str): Group of statistical tables related to the same
          topic, displayed in the main page (http://data.un.org/Default.aspx).
          E.g. 'Population', 'National accounts', ...
        - Series (str): Statistical series.
        - Value (float): A unit of data for which the definition,
          identification, representation, and permissible values are specified
          by means of a set of attributes, as defined by the UN data website.
        - Footnotes (str): A note or other text located at the bottom of a
          page of text, manuscript, book or statistical tabulation that
          provides comment on or cites a reference for a designated part of
          the text or table.
        - Source (str): A specific data set, metadata set, database or
          metadata repository from where data or metadata are available.

    Raises:
        None: However, if the HTTP request fails, it prints an error message
              and terminates the script.
    """
    LOGGER.error("Read UNData files...")
    # Initiate Dataframe that will gather informations
    columns_dataframe = [
        "Region_Country_Area_ID",
        "Region_Country_Area_name",
        "Year",
        "Category",
        "Series",
        "Value",
        "Footnotes",
        "Source",
    ]
    file_paths = [
        FPATH_SRC_UNDATA_CO2,
        FPATH_SRC_UNDATA_GDP,
        FPATH_SRC_UNDATA_POPU,
    ]

    df_undata = pd.DataFrame()
    # Load csv files
    for file in file_paths:
        LOGGER.error(f"Read UNData file: {file}")
        df = pd.read_csv(file, sep=",", header=1)

        # Retrieve the category in the first row of the csv file
        df_firstrow = pd.read_csv(file, sep=",", header=None, nrows=1)
        df.insert(3, "Category", df_firstrow[1].values[0])

        # Concat dataframes
        df_undata = pd.concat([df_undata, df], ignore_index=True)

    df_undata.columns = columns_dataframe

    # Map country names to get uniform country names between all files.
    # Rem: This mapping uses a dictionary, working for the current perimeter.
    #      If updates occur, new countries might need to be included to this
    #      dictionary.
    mapping_countries = {
        "Iran (Islamic Republic of)": "Iran",
        "Dem. People's Rep. Korea": "North Korea",
        "Russian Federation": "Russia",
        "Syrian Arab Republic": "Syria",
        "United Rep. of Tanzania": "Tanzania",
        "United States of America": "United States",
        "Venezuela (Boliv. Rep. of)": "Venezuela",
    }
    df_undata["Region_Country_Area_name"] = df_undata[
        "Region_Country_Area_name"
    ].replace(mapping_countries)

    return df_undata
