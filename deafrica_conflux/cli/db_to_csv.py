import click

import deafrica_conflux.stack
from deafrica_conflux.cli.logs import logging_setup


@click.command("db-to-csv", no_args_is_help=True)
@click.option(
    "--output-directory",
    type=click.Path(),
    required=True,
    help="Output directory for Waterbodies-style CSVs",
)
@click.option("-v", "--verbose", count=True)
@click.option(
    "--jobs",
    "-j",
    default=8,
    help="Number of workers",
)
@click.option(
    "--index-num",
    "-i",
    type=click.INT,
    default=0,
    help="The waterbodies ID chunks index after split overall waterbodies ID list by split-num.",
)
@click.option(
    "--split-num",
    type=click.INT,
    default=1,
    help="Number of chunks after split overall waterbodies ID list.",
)
@click.option(
    "--remove-duplicated-data/--no-remove-duplicated-data",
    default=True,
    help="Remove timeseries duplicated data if applicable. Default True",
)
def db_to_csv(output_directory, verbose, jobs, index_num, split_num, remove_duplicated_data):
    """
    Output Waterbodies-style CSVs from a database.
    """
    logging_setup(verbose)

    deafrica_conflux.stack.stack_waterbodies_db_to_csv(
        output_directory=output_directory,
        verbose=verbose > 0,
        remove_duplicated_data=remove_duplicated_data,
        n_workers=jobs,
        index_num=index_num,
        split_num=split_num,
    )
