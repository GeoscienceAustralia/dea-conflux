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


# Keeping this here incase its needed.
def setup_logging(verbose: int):
    """
    Set up logging.

    Arguments
    ---------
    verbose : int
        Verbosity level (0, 1, 2).
    """
    loggers = [
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if not name.startswith("fiona")
        and not name.startswith("sqlalchemy")
        and not name.startswith("boto")
    ]
    # For compatibility with docker+pytest+click stack...
    stdout_hdlr = logging.StreamHandler(sys.stdout)
    for logger in loggers:
        if verbose == 0:
            logging.basicConfig(level=logging.WARNING)
        elif verbose == 1:
            logging.basicConfig(level=logging.INFO)
        elif verbose == 2:
            logging.basicConfig(level=logging.DEBUG)
        else:
            raise click.ClickException("Maximum verbosity is -vv")
        logger.addHandler(stdout_hdlr)
        logger.propagate = False
