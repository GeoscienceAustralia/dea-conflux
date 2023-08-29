import click

import deafrica_conflux.__version__

from .get_ids import get_ids


@click.version_option(package_name="deafrica_conflux", version=deafrica_conflux.__version__)
@click.group(help="Run deafrica-conflux.")
def main():
    pass


main.add_command(get_ids)
