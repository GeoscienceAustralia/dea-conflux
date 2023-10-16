import logging
import sys

import click


def logging_setup(verbose: int = 1):
    """
    Setup logging to print to stdout with default logging level being INFO.
    """

    if verbose == 0:
        level = logging.WARNING
    elif verbose == 1:
        level = logging.INFO
    elif verbose == 2:
        level = logging.DEBUG
    else:
        raise click.ClickException("Maximum verbosity is -vv")

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    # Suppress all (other than CRITICAL) errors for boto3
    # logging.getLogger('botocore').setLevel(logging.WARNING)
    # logging.getLogger('boto3').setLevel(logging.WARNING)
