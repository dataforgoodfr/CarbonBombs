"""
Functions to read the GEM datasets

Source for the datasets:
- Global-Coal-Mine-Tracker-April-2023.xlsx : The Global Coal Mine Tracker (GCMT) is a worldwide
  dataset of coal mines and proposed projects. The tracker provides asset-level details on ownership
  structure, development stage and status, coal type, production, workforce size, reserves and resources,
  methane emissions, geolocation, and over 30 other categories. This data will not be tracked under
  this repository as it Distributed under a Creative Commons Attribution 4.0 International License.
  It can be freely download through this page:
  https://globalenergymonitor.org/projects/global-coal-mine-tracker/download-data/
- Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx : The Global Oil and Gas Extraction Tracker (GOGET)
  is a global dataset of oil and gas resources and their development. GOGET includes information on discovered,
  in-development, and operating oil and gas units worldwide, including both conventional and unconventional
  assets. This data will not be tracked under this repository as it Distributed under a Creative Commons
  Attribution 4.0 International License. It can be freely download through this page:
  https://globalenergymonitor.org/projects/global-oil-gas-extraction-tracker/
"""
import warnings
import pandas as pd

from carbon_bombs.conf import FPATH_SRC_GEM_COAL
from carbon_bombs.conf import FPATH_SRC_GEM_GASOIL

    
def load_coal_mine_gem_database():
    """
    Loads the Global Coal Mine Tracker database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the data from the "Global Coal Mine Tracker"
        sheet.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path:
    "./data_sources/Global-Coal-Mine-Tracker-April-2023.xlsx".
    The sheet to be read is "Global Coal Mine Tracker".
    """
    df = pd.read_excel(
        FPATH_SRC_GEM_COAL,
        sheet_name='Global Coal Mine Tracker',
        engine='openpyxl'
    )
    return df

def load_gasoil_mine_gem_database():
    """
    Loads the Global Oil and Gas Extraction Tracker database from an Excel file.

    Returns
    -------
    pd.DataFrame:
        A pandas dataframe with the data from the "Main data" sheet.

    Raises
    ------
    FileNotFoundError:
        If the data file is not found in the specified path.
    ValueError:
        If the data file does not contain the expected sheet.

    Notes
    -----
    The data file is expected to be in the following path:
    "./data_sources/Global-Oil-and-Gas-Extraction-Tracker-Feb-2023.xlsx".
    The sheet to be read is "Main data".
    """
    # Line that must be passed before in order to avoid useless warning
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    df = pd.read_excel(
        FPATH_SRC_GEM_GASOIL, sheet_name='Main data',engine='openpyxl'
    )

    return df
