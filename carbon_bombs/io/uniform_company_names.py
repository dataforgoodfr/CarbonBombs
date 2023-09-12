import json

from carbon_bombs.conf import FPATH_SRC_UNIFORM_COMP_NAMES


def load_uniform_company_names() -> dict:
    """_summary_

    Returns
    -------
    dict
        _description_
    """
    with open(FPATH_SRC_UNIFORM_COMP_NAMES, "r") as f:
        data = json.load(f)

    return data


def save_uniform_company_names(data: dict):
    """_summary_

    Parameters
    ----------
    fpath : str
        _description_
    """
    with open(FPATH_SRC_UNIFORM_COMP_NAMES, "w") as f:
        f.write(json.dumps(data, indent=4))
