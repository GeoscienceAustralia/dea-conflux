import click

from ._cli_common import main, logging_setup

import deafrica_conflux.stack


@main.command("package-delivery", no_args_is_help=True)
@click.option(
    "--csv-path",
    type=click.Path(),
    required=True,
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the polygon base (CSV files) result directory.",
)
@click.option(
    "--output",
    type=click.Path(),
    required=True,
    help="REQUIRED. Output directory for single file delivery (single CSV and single parquet)",
)
@click.option(
    "--precision",
    type=int,
    default=4,
    help="Reduce the precision of the data to given value. E.g. given 4, then round WIT result to 0.0001.",
)
@click.option("-v", "--verbose", count=True)
def package_delivery(csv_path, output, precision, verbose):
    """
    Concatenate all polygon base CSV files to a single CSV and single parquet file for QLD delivery.
    """
    logging_setup(verbose)

    # use the kwargs to pass the precision value
    kwargs = {}
    kwargs["precision"] = precision
    kwargs["output_dir"] = output

    deafrica_conflux.stack.stack(
        csv_path,
        ".*",
        deafrica_conflux.stack.StackMode.WITTOOLING_SINGLE_FILE_DELIVERY,
        verbose=verbose,
        **kwargs,
    )

    return 0
