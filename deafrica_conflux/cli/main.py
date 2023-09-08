import click

import deafrica_conflux.__version__

from deafrica_conflux.cli.get_dataset_ids import get_dataset_ids
from deafrica_conflux.cli.run_from_list import run_from_list
from deafrica_conflux.cli.run_from_txt import run_from_txt
from deafrica_conflux.cli.make_queue import make_sqs_queue
from deafrica_conflux.cli.run_from_queue import run_from_queue
from deafrica_conflux.cli.stack import stack


@click.version_option(package_name="deafrica_conflux", version=deafrica_conflux.__version__)
@click.group(help="Run deafrica-conflux.")
def main():
    pass


main.add_command(get_dataset_ids)
main.add_command(run_from_list)
main.add_command(run_from_txt)
main.add_command(make_sqs_queue)
main.add_command(run_from_queue)
main.add_command(stack)
main.add
