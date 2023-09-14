"""Function to generate md5 checksum"""
import hashlib
import os

from carbon_bombs.conf import DATA_CLEANED_PATH
from carbon_bombs.conf import FPATH_CHECKSUM


def md5(fname: str):
    """Generate MD5 checksum for a file

    Function found here: https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
    """
    hash_md5 = hashlib.md5()

    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def generate_checksum_cleaned_datasets():
    """Create a checksum file with all csv md5 checksum for cleaned datasets"""
    csv_files = [f for f in os.listdir(DATA_CLEANED_PATH) if f.endswith("csv")]
    md5_str = ""

    for fname in csv_files:
        checksum = md5(f"{DATA_CLEANED_PATH}/{fname}")
        md5_str += f"{fname.ljust(32, ' ')}\t{checksum}\n"

    with open(FPATH_CHECKSUM, "w") as f:
        f.write(md5_str)


generate_checksum_cleaned_datasets()
