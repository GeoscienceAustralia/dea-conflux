import logging

import click
import boto3

import deafrica_conflux.queues
from deafrica_conflux.cli.logs import logging_setup


@click.command("push-to-sqs-queue", no_args_is_help=True)
@click.option(
    "--text-file-path",
    type=click.Path(),
    required=True,
    help="REQUIRED. Path to text file to push to queue.",
)
@click.option("--queue-name", required=True, help="REQUIRED. Queue name to push to.")
@click.option("-v", "--verbose", count=True)
def push_to_sqs_queue(text_file_path, queue_name, verbose):
    """
    Push lines of a text file to a SQS queue.
    """
    # Cribbed from datacube-alchemist
    logging_setup(verbose)
    _log = logging.getLogger(__name__) # noqa F841

    # Create an sqs client.
    sqs_client = boto3.client("sqs")

    deafrica_conflux.queues.push_to_queue_from_txt(text_file_path=text_file_path,
                                                   queue_name=queue_name,
                                                   sqs_client=sqs_client)
