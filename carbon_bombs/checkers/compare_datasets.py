"""Check to compare old cleaned dataframe with new one"""
import shutil
from os.path import isdir

import pandas as pd

from carbon_bombs.conf import DATA_CLEANED_PATH
from carbon_bombs.conf import DATA_SAVE_OLD
from carbon_bombs.conf import FPATH_COMPARISON_DF
from carbon_bombs.conf import FPATH_OUT_BANK
from carbon_bombs.conf import FPATH_OUT_CB
from carbon_bombs.conf import FPATH_OUT_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_BANK_COMP
from carbon_bombs.conf import FPATH_OUT_CONX_CB_COMP
from carbon_bombs.conf import FPATH_OUT_COUNTRY


def compare_dataframes(df1, df2, key_col, df_name=""):
    """Compare 2 dataframes using a key col to merge compare them"""
    res = ""

    if not isinstance(key_col, list):
        df1 = df1.loc[(~df1[key_col].isna()) & (df1[key_col] != "None")]
        df2 = df2.loc[~df2[key_col].isna() & (df2[key_col] != "None")]
        key_col = [key_col]

    # create a key column to merge old and new dataframe
    df1["key_col"] = df1.apply(
        lambda x: (" - ".join([str(x[k]) for k in key_col])), axis=1
    )
    df2["key_col"] = df2.apply(
        lambda x: (" - ".join([str(x[k]) for k in key_col])), axis=1
    )

    df1 = df1.sort_values(by="key_col").reset_index(drop=True).fillna("None")
    df2 = df2.sort_values(by="key_col").reset_index(drop=True).fillna("None")

    diff = set(df1.columns) - set(df2.columns)
    if diff:
        res += f"⚠️ {df_name}: New df has new columns: {diff}\n"

    diff = set(df2.columns) - set(df1.columns)
    if diff:
        res += f"⚠️ {df_name}: New df misses columns: {diff}\n"

    uniq_keys1 = set(df1["key_col"])
    uniq_keys2 = set(df2["key_col"])

    # check uniq keys
    if uniq_keys1.symmetric_difference(uniq_keys2):
        if uniq_keys1 - uniq_keys2:
            res += f"⚠️ {df_name}: Found new keys: {uniq_keys1 - uniq_keys2}\n"
        if uniq_keys2 - uniq_keys1:
            res += f"⚠️ {df_name}: Missing keys: {uniq_keys2 - uniq_keys1}\n"

        # keep only common keys to allow comparison
        common_keys = uniq_keys1.intersection(uniq_keys2)
        df1 = (
            df1.loc[df1["key_col"].isin(common_keys)]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        df2 = (
            df2.loc[df2["key_col"].isin(common_keys)]
            .drop_duplicates()
            .reset_index(drop=True)
        )

    full_comp = []

    for col in df2.columns:
        if (col == "key_col") or (col not in df1):
            continue

        if col not in key_col:
            df1_ = df1.set_index(key_col).copy()
            df2_ = df2.set_index(key_col).copy()
        else:
            df1_ = df1
            df2_ = df2

        comp = (
            df1_[col]
            .fillna("None")
            .replace("", "None")
            .compare(
                df2_[col].fillna("None").replace("", "None"),
                result_names=("new", "old"),
            )
            .reset_index()
        )
        comp["column"] = col

        comp["key_col"] = ""

        if "index" not in comp:
            if len(comp):
                comp["key_col"] = comp.apply(
                    lambda x: (" - ".join([str(x[k]) for k in key_col])), axis=1
                )

        if len(comp) == 0:
            res += f"✅ {df_name} -- {col} -- no changes\n"
        else:
            res += f"⚠️  {df_name} -- {col} -- some rows changed (n={len(comp)}) see details in comparison csv\n"

        full_comp += [comp]

    full_comp = pd.concat(full_comp).reset_index(drop=True)

    return full_comp, res


def compare_cleaned_datasets():
    """Compare all cleaned datasets"""
    res = "\n===== Compare old cleaned datasets with new =====\n"

    cleaned_datasets_fpaths = [
        FPATH_OUT_CB,
        FPATH_OUT_COMP,
        FPATH_OUT_BANK,
        FPATH_OUT_CONX_BANK_COMP,
        FPATH_OUT_CONX_CB_COMP,
        FPATH_OUT_COUNTRY,
    ]

    key_cols_map = {
        "carbon_bombs_data.csv": ["Carbon_bomb_name_source_CB", "Country_source_CB"],
        "company_data.csv": "Company_name",
        "bank_data.csv": "Bank Name",
        "connection_bank_company.csv": ["Bank", "Company"],
        "connection_carbonbombs_company.csv": [
            "Carbon_bomb_name",
            "Company",
            "Country",
        ],
        "country_data.csv": "Country_source_CB",
    }

    full_comp = []

    for fpath in cleaned_datasets_fpaths:
        fname = fpath.split("/")[-1]
        save_path = f"{DATA_SAVE_OLD}/{fname}"

        new_df = pd.read_csv(fpath)
        old_df = pd.read_csv(save_path)

        comp, res_comp = compare_dataframes(new_df, old_df, key_cols_map[fname], fname)
        comp["file"] = fname
        comp = comp[["file", "column", "key_col", "new", "old"]]
        full_comp += [comp]

        res += res_comp

    full_comp = pd.concat(full_comp).reset_index(drop=True)

    full_comp.to_csv(FPATH_COMPARISON_DF, index=False)

    res += "\n"

    return res


def remove_old_cleaned_datasets():
    """Remove old cleaned datasets folders saved for comparison"""
    if isdir(DATA_SAVE_OLD):
        shutil.rmtree(DATA_SAVE_OLD)


def copy_old_cleaned_datasets():
    """Copy old cleaned datasets folders saved for comparison"""
    if not isdir(DATA_SAVE_OLD):
        shutil.copytree(DATA_CLEANED_PATH, DATA_SAVE_OLD)
