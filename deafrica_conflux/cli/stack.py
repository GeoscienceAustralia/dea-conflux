import click

import deafrica_conflux.stack
from deafrica_conflux.cli.logs import logging_setup


@click.command("stack", no_args_is_help=True)
@click.option(
    "--parquet-path",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the Parquet directory.",
)
@click.option(
    "--pattern",
    required=False,
    default=".*\.pq", # noqa W605
    help="Regular expression for filename matching.",
)
@click.option(
    "--output-directory",
    type=click.Path(),
    required=False,
    help="Output directory for waterbodies-style stack",
)
@click.option(
    "--mode",
    type=click.Choice(["waterbodies", "waterbodies_db"]),
    default="waterbodies",
    required=False,
)
@click.option("-v", "--verbose", count=True)
@click.option(
    "--drop/--no-drop", default=False, help="Drop database if applicable. Default False"
)
@click.option(
    "--remove-duplicated-data/--no-remove-duplicated-data",
    default=True,
    help="Remove timeseries duplicated data if applicable. Default True",
)
def stack(
    parquet_path,
    pattern,
    output_directory,
    mode,
    verbose,
    drop,
    remove_duplicated_data
):
    """
    Stack outputs of deafrica-conflux into other formats.
    """
    logging_setup(verbose)

    # Convert mode to StackMode
    mode_map = {
        "waterbodies": deafrica_conflux.stack.StackMode.WATERBODIES,
        "waterbodies_db": deafrica_conflux.stack.StackMode.WATERBODIES_DB,
    }

    kwargs = {}
    if mode == "waterbodies":
        kwargs["output_directory"] = output_directory
        kwargs["remove_duplicated_data"] = remove_duplicated_data
    elif mode == "waterbodies_db":
        kwargs["drop"] = drop

    deafrica_conflux.stack.stack_parquet(path=parquet_path,
                                         pattern=pattern,
                                         mode=mode_map[mode],
                                         verbose=verbose,
                                         **kwargs)

    return 0
