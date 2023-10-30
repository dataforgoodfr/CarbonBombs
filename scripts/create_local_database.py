import click

from carbon_bombs.io.neo4j import create_local_database
from carbon_bombs.utils.logger import get_logger


@click.command()
@click.option("-v", "--verbose", default=50, help="Verbosity level")
def create_local_database_script(verbose):
    """"""
    LOGGER = get_logger(verbose=verbose, name="carbon_bombs", log=True)
    LOGGER.info("Start create local database script")

    create_local_database()

    LOGGER.info("Create local database script - DONE")


if __name__ == "__main__":
    create_local_database_script()
