import pandas as pd

from carbon_bombs.conf import FPATH_OUT_CB


def load_carbon_bombs_database():
    """
    Loads the carbon bombs database from a CSV file located at the specified
    file path. Returns a pandas DataFrame containing the data.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the carbon bombs database.

    Notes
    -----
    - This function requires the pandas library to be installed.
    - The CSV file containing the data must be available at the specified
      file path.
    - The CSV file must be separated by a semicolon (;).
    """
    df = pd.read_csv(FPATH_OUT_CB)

    return df
