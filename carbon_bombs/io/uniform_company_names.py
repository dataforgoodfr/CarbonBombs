import json

from carbon_bombs.conf import FPATH_SRC_UNIFORM_COMP_NAMES


def load_uniform_company_names() -> dict:
    """Load uniform company names with GEM company names
    as keys and BOCC company names as values.

    Returns
    -------
    dict
        uniform company names dict
    """
    with open(FPATH_SRC_UNIFORM_COMP_NAMES, "r") as f:
        data = json.load(f)

    return data


def save_uniform_company_names(data: dict):
    """Save uniform company names with GEM company names
    as keys and BOCC company names as values.

    Parameters
    ----------
    data: dict
        uniform company names dict
    """
    with open(FPATH_SRC_UNIFORM_COMP_NAMES, "w") as f:
        f.write(json.dumps(data, indent=4))
