import boto3
import time
import click
import logging
import datacube

from ._cli_common import main, logging_setup
from ._vector_file_utils import get_crs, guess_id_field, load_and_reproject_shapefile

import deafrica_conflux.drill


@main.command("filter-from-queue", no_args_is_help=True)
@click.option("--input-queue", "-iq", help="Queue to read all IDs.")
@click.option("--output-queue", "-oq", help="Queue to save filtered IDs.")
@click.option(
    "--shapefile",
    "-s",
    type=click.Path(),
    help="REQUIRED. Path to the polygon " "shapefile to filter datasets.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in shapefile.",
)
@click.option(
    "--timeout",
    default=60 * 60,
    help="The seconds of a received SQS msg is invisible.",
)
@click.option(
    "--num-worker",
    type=int,
    help="The number of processes to filter datasets.",
    default=4,
)
@click.option("-v", "--verbose", count=True)
def filter_from_queue(
    input_queue, output_queue, shapefile, use_id, timeout, num_worker, verbose
):
    """
    Run deafrica-conflux filter dataset based on scene ids from a queue.
    Then submit the filter result to another queue.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Guess the ID field.
    id_field = guess_id_field(shapefile, use_id)
    _log.debug(f"Guessed ID field: {id_field}")

    # Load and reproject the shapefile.
    shapefile = load_and_reproject_shapefile(
        shapefile,
        id_field,
        get_crs(shapefile),
    )

    sqs = boto3.resource("sqs")
    input_queue_instance = sqs.get_queue_by_name(QueueName=input_queue)
    input_queue_url = input_queue_instance.url

    output_queue_instance = sqs.get_queue_by_name(QueueName=output_queue)

    # setup 10 retries to make sure no drama from SQS
    message_retries = 10

    while message_retries > 0:
        response = input_queue_instance.receive_messages(
            AttributeNames=["All"],
            MaxNumberOfMessages=10,
            VisibilityTimeout=timeout,
        )

        messages = []

        # if nothing back from SQS, minus 1 retry
        if len(response) == 0:
            message_retries = message_retries - 1
            time.sleep(1)
            _log.info(f"No msg in {input_queue} now")
            continue
        # if we get anything back from SQS, reset retry
        else:
            message_retries = 10
            uuids = [e.body for e in response]

            _log.info(f"Before filter {' '.join(uuids)}")

            ids = [dc.index.datasets.get(uuid) for uuid in uuids]

            uuids = deafrica_conflux.drill.filter_dataset(
                ids, shapefile, worker_num=num_worker
            )

            _log.info(f"After filter {' '.join(uuids)}")

            for id in uuids:
                message = {
                    "Id": str(id),
                    "MessageBody": str(id),
                }

                messages.append(message)

            if len(messages) != 0:
                output_queue_instance.send_messages(Entries=messages)

            input_entries = [
                {"Id": msg.message_id, "ReceiptHandle": msg.receipt_handle}
                for msg in response
            ]

            resp = input_queue_instance.delete_messages(
                QueueUrl=input_queue_url,
                Entries=input_entries,
            )

            if len(resp["Successful"]) != len(input_entries):
                raise RuntimeError(f"Failed to delete message from: {input_queue_url}")

            messages = []

    return 0
