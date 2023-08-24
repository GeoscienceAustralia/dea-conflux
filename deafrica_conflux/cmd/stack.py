import click

from ._cli_common import main, logging_setup

import deafrica_conflux.stack


@main.command("stack", no_args_is_help=True)
@click.option(
    "--parquet-path",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the Parquet directory.",
)
@click.option(
    "--output",
    type=click.Path(),
    required=False,
    help="Output directory for waterbodies-style stack",
)
@click.option(
    "--pattern",
    required=False,
    default=".*",
    help="Regular expression for filename matching.",
)
@click.option(
    "--mode",
    type=click.Choice(["waterbodies", "waterbodies_db", "wit_tooling"]),
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
def stack(parquet_path, output, pattern, mode, verbose, drop, remove_duplicated_data):
    """
    Stack outputs of deafrica-conflux into other formats.
    """
    logging_setup(verbose)

    # Convert mode to StackMode
    mode_map = {
        "waterbodies": deafrica_conflux.stack.StackMode.WATERBODIES,
        "waterbodies_db": deafrica_conflux.stack.StackMode.WATERBODIES_DB,
        "wit_tooling": deafrica_conflux.stack.StackMode.WITTOOLING,
    }

    kwargs = {}
    if mode == "waterbodies" or mode == "wit_tooling":
        kwargs["output_dir"] = output
        kwargs["remove_duplicated_data"] = remove_duplicated_data
    elif mode == "waterbodies_db":
        kwargs["drop"] = drop
        kwargs["remove_duplicated_data"] = remove_duplicated_data

    deafrica_conflux.stack.stack(
        parquet_path,
        pattern,
        mode_map[mode],
        verbose=verbose,
        **kwargs,
    )

    return 0
