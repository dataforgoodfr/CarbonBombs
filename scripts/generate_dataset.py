import click

from carbon_bombs.io.cleaned import save_bank_table
from carbon_bombs.io.cleaned import save_carbon_bombs_table
from carbon_bombs.io.cleaned import save_company_table
from carbon_bombs.io.cleaned import save_connexion_bank_company_table
from carbon_bombs.io.cleaned import save_connexion_cb_company_table
from carbon_bombs.io.cleaned import save_country_table
from carbon_bombs.io.cleaned import save_dataframes_into_excel
from carbon_bombs.io.gmaps import API_KEY
from carbon_bombs.processing.banks import create_banks_table
from carbon_bombs.processing.carbon_bombs_info import create_carbon_bombs_table
from carbon_bombs.processing.company import create_company_table
from carbon_bombs.processing.connexion_bank_company import (
    create_connexion_bank_company_table,
)
from carbon_bombs.processing.connexion_carbon_bombs_company import (
    create_connexion_cb_company_table,
)
from carbon_bombs.processing.country import create_country_table
from carbon_bombs.utils.logger import get_logger


@click.command()
@click.option("-v", "--verbose", default=50, help="Verbosity level")
@click.option("--start-at-step", default=0, help="start at step")
def generate_dataset(verbose, start_at_step):
    """"""
    LOGGER = get_logger(verbose=verbose, name="carbon_bombs")
    LOGGER.info("Start generate dataset script")

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
        data_cnx_bank_comp = create_connexion_bank_company_table(use_save_dict=False)
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

    # Step 7 : concat all datasets in the same excel + metadatas
    LOGGER.info("Step 7 - START")
    LOGGER.info("Step 7 - save all dataframes into concatenate excel")
    save_dataframes_into_excel()
    LOGGER.info("Step 7 - DONE")


if __name__ == "__main__":
    generate_dataset()
