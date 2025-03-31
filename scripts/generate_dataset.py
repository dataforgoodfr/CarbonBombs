import click

from carbon_bombs.checkers.check_cleaned_datasets import check_cleaned_datasets
from carbon_bombs.checkers.check_manual_match import check_manual_match
from carbon_bombs.checkers.check_sources import check_data_sources
from carbon_bombs.checkers.compare_datasets import compare_cleaned_datasets
from carbon_bombs.checkers.compare_datasets import copy_old_cleaned_datasets
from carbon_bombs.checkers.compare_datasets import remove_old_cleaned_datasets
from carbon_bombs.conf import FPATH_RESULT_CHECK
from carbon_bombs.io.cleaned import save_bank_table
from carbon_bombs.io.cleaned import save_carbon_bombs_table
from carbon_bombs.io.cleaned import save_company_table
from carbon_bombs.io.cleaned import save_lng_table
from carbon_bombs.io.cleaned import save_connexion_bank_company_table
from carbon_bombs.io.cleaned import save_connexion_cb_company_table
from carbon_bombs.io.cleaned import save_country_table
from carbon_bombs.io.cleaned import save_dataframes_into_excel
from carbon_bombs.io.gmaps import API_KEY
from carbon_bombs.io.md5 import generate_checksum_cleaned_datasets
from carbon_bombs.processing.banks import create_banks_table
from carbon_bombs.processing.carbon_bombs_info import create_carbon_bombs_table
from carbon_bombs.processing.company import create_company_table
from carbon_bombs.processing.lng import create_lng_table
from carbon_bombs.processing.connexion_bank_company import (
    create_connexion_bank_company_table,
)
from carbon_bombs.processing.connexion_carbon_bombs_company import (
    create_connexion_cb_company_table,
)
from carbon_bombs.processing.country import create_country_table
from carbon_bombs.utils.logger import get_logger


@click.command()
@click.option("-v", "--verbose", default=20, help="Verbosity level")
@click.option("--start-at-step", default=0, help="start at step")
def generate_dataset(verbose, start_at_step):
    """"""
    LOGGER = get_logger(verbose=verbose, name="carbon_bombs", log=True)
    LOGGER.info("Start generate dataset script")

    LOGGER.info("Copy data cleaned datasets for comparison at the end")
    copy_old_cleaned_datasets()

    # Step 0 : Check data sources and manual match
    LOGGER.info("Step 0 - START")
    LOGGER.info("Step 0 - create carbon bombs table started")
    check_txt = check_data_sources()
    check_txt += check_manual_match()
    LOGGER.info(f"Check data sources and manual match result:\n{check_txt}")
    LOGGER.info("Step 0 - DONE")

    # Step 1 : CB table
    if start_at_step <= 1:
        LOGGER.info("Step 1 - START")
        LOGGER.info("Step 1 - create carbon bombs table started")
        data_cb = create_carbon_bombs_table()
        LOGGER.info("Step 1 - carbon bombs table created")
        save_carbon_bombs_table(data_cb)
        LOGGER.info("Step 1 - carbon bombs table saved")
        LOGGER.info("Step 1 - DONE")
    else:
        LOGGER.info("Step 1 - skipped (create carbon bombs table)")

    # Step 2 : Connexion bank to company table
    if start_at_step <= 2:
        LOGGER.info("Step 2 - START")
        LOGGER.info("Step 2 - create connexion bank - company table started")
        data_cnx_bank_comp = create_connexion_bank_company_table(use_save_dict=True)
        LOGGER.info("Step 2 - connexion bank - company table created")
        save_connexion_bank_company_table(data_cnx_bank_comp)
        LOGGER.info("Step 2 - connexion bank - company table saved")
        LOGGER.info("Step 2 - DONE")
    else:
        LOGGER.info("Step 2 - skipped (create connexion bank - company table)")

    # Step 3 : Connexion CB to company table
    if start_at_step <= 3:
        LOGGER.info("Step 3 - START")
        LOGGER.info("Step 3 - create connexion carbon bombs - company table started")
        data_cnx_cb_comp = create_connexion_cb_company_table(use_save_dict=True)
        LOGGER.info("Step 3 - connexion carbon bombs - company table created")
        save_connexion_cb_company_table(data_cnx_cb_comp)
        LOGGER.info("Step 3 - connexion carbon bombs - company table saved")
        LOGGER.info("Step 3 - DONE")
    else:
        LOGGER.info("Step 3 - skipped (create connexion carbon bombs - company table)")

    # If API_KEY found then can execute tables created with scrapping and GMAPS API
    if API_KEY != "":
        # Step 4 : bank table
        if start_at_step <= 4:
            LOGGER.info("Step 4 - START")
            LOGGER.info("Step 4 - create bank table started")
            data_bank = create_banks_table(check_old_df_address=True)
            LOGGER.info("Step 4 - bank table created")
            save_bank_table(data_bank)
            LOGGER.info("Step 4 - bank table saved")
            LOGGER.info("Step 4 - DONE")
        else:
            LOGGER.info("Step 4 - skipped (create bank table)")

        # Step 5 : company table
        if start_at_step <= 5:
            LOGGER.info("Step 5 - START")
            LOGGER.info("Step 5 - create company table started")
            data_comp = create_company_table(check_old_df_address=True)
            LOGGER.info("Step 5 - company table created")
            save_company_table(data_comp)
            LOGGER.info("Step 5 - company table saved")
            LOGGER.info("Step 5 - DONE")
        else:
            LOGGER.info("Step 5 - skipped (create company table)")

    # Step 6 : country table
    if start_at_step <= 6:
        LOGGER.info("Step 6 - START")
        LOGGER.info("Step 6 - create country table started")
        data_country = create_country_table()
        LOGGER.info("Step 6 - country table created")
        save_country_table(data_country)
        LOGGER.info("Step 6 - country table saved")
        LOGGER.info("Step 6 - DONE")
    else:
        LOGGER.info("Step 6 - skipped (create country table)")

    # Step 7 : LNG table
    if start_at_step <= 7:
        LOGGER.info("Step 7 - START")
        LOGGER.info("Step 7 - create LNG table started")
        data_lng = create_lng_table()
        LOGGER.info("Step 7 - LNG table created")
        save_lng_table(data_lng)
        LOGGER.info("Step 7 - LNG table saved")
        LOGGER.info("Step 7 - DONE")
    else:
        LOGGER.info("Step 7 - skipped (create LNG table)")

    # Step 8 : concat all datasets in the same excel + metadatas
    LOGGER.info("Step 8 - START")
    LOGGER.info("Step 8 - save all dataframes into concatenate excel")
    save_dataframes_into_excel()
    LOGGER.info("Step 8 - generate checksums")
    generate_checksum_cleaned_datasets()
    LOGGER.info("Step 8 - DONE")

    # Step 9 : check cleaned datasets and compare with old ones
    LOGGER.info("Step 9 - START")
    LOGGER.info("Step 9 - check cleaned datasets and compare with old")
    check_txt_end = check_cleaned_datasets()
    check_txt_end += compare_cleaned_datasets()
    LOGGER.info(f"Check cleaned datasets and comparison:\n{check_txt_end}")

    check_txt += check_txt_end
    with open(FPATH_RESULT_CHECK, "w", encoding="utf-8-sig") as f:
        f.write(check_txt)

    remove_old_cleaned_datasets()
    LOGGER.info("Step 9 - DONE")

    LOGGER.info("Generate dataset script - DONE")


if __name__ == "__main__":
    generate_dataset()
