import click

from carbon_bombs.io.neo4j import purge_database
from carbon_bombs.io.neo4j import update_neo4j
from carbon_bombs.utils.logger import get_logger


@click.command()
@click.option("-v", "--verbose", default=50, help="Verbosity level")
def reload_neo4j(verbose):
    """"""
    LOGGER = get_logger(verbose=verbose, name="carbon_bombs", log=True)
    LOGGER.info("Start reload neo4j script")

    # Step 1: purge all data from neo4j
    LOGGER.info("Step 1 - Purge database start")
    purge_database()
    LOGGER.info("Step 1 - Purge database done")

    # Step 2: Update neo4j data and load it into database
    LOGGER.info("Step 2 - Update neo4j data and load it into database start")
    update_neo4j()
    LOGGER.info("Step 2 - Update neo4j data and load it into database done")

    LOGGER.info("Reload neo4j script - DONE")


if __name__ == "__main__":
    reload_neo4j()
