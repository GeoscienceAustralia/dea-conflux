import logging
import os
import time

import boto3
import click
import datacube

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.hopper import check_ds_region
from deafrica_conflux.io import find_parquet_files
from deafrica_conflux.queues import (
    delete_batch_with_retry,
    get_queue_url,
    receive_messages,
    send_batch_with_retry,
)


@click.command("filter-from-sqs-queue", no_args_is_help=True)
@click.option("-v", "--verbose", count=True)
@click.option("--input-queue", help="SQS queue to read all dataset IDs.")
@click.option("--output-queue", help="SQS Queue to save filtered dataset IDs.")
@click.option(
    "--polygons-split-by-region-directory",
    type=str,
    help="Path to the directory containing the parquet files which contain polygons grouped by product region.",
)
@click.option(
    "--use-id",
    type=str,
    default="",
    help="Optional. Unique key id polygons vector file.",
)
@click.option(
    "--visibility-timeout",
    default=60 * 60,
    help="The duration (in seconds) that a received SQS message is hidden from "
    "subsequent retrieve requests after being retrieved by a ReceiveMessage request.",
)
def filter_from_queue(
    verbose,
    input_queue,
    output_queue,
    polygons_split_by_region_directory,
    use_id,
    visibility_timeout,
    num_worker,
):
    """
    Run deafrica-conflux filter dataset based on scene ids from a queue.
    Then submit the filter result to another queue.
    """
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Support pathlib paths.
    polygons_split_by_region_directory = str(polygons_split_by_region_directory)

    # Get the region codes from the polygon files.
    pq_files = find_parquet_files(path=polygons_split_by_region_directory, pattern=".*")
    region_codes = [os.path.splitext(os.path.basename(i))[0] for i in pq_files]
    _log.info(f"Found {len(region_codes)} regions.")
    _log.debug(f"Found regions: {region_codes}")

    # Connect to the datacube.
    dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Create the service client.
    sqs_client = boto3.client("sqs")

    # Input queue should have a dead letter queue configured in its RedrivePolicy.
    input_queue_url = get_queue_url(queue_name=input_queue, sqs_client=sqs_client)

    output_queue_url = get_queue_url(queue_name=output_queue, sqs_client=sqs_client)

    # Maximum number of retries to get messages from the input queue.
    message_retries = 10
    while message_retries > 0:
        # Get a maximum of 10 dataset ids from the input queue.
        retrieved_messages = receive_messages(
            queue_url=input_queue_url,
            max_retries=message_retries,
            visibility_timeout=visibility_timeout,
            max_no_messages=10,
            sqs_client=sqs_client,
        )

        # If no messages are received from the queue, subtract 1 from the number
        # of retries.
        if retrieved_messages is None:
            time.sleep(1)
            _log.info(f"No messages received from queue {input_queue_url}")
            message_retries -= 1
            continue
        else:
            # If a message(s) is received reset the message retries
            # back to the maximum number of retries.
            message_retries = 10

        # Get the dataset ids from the input queue.
        dict_keys = [(msg["MessageId"], msg["ReceiptHandle"]) for msg in retrieved_messages]
        dict_values = [msg["Body"] for msg in retrieved_messages]
        dataset_ids = dict(zip(dict_keys, dict_values))
        _log.info(f"Before filter {' '.join([v for k,v in dataset_ids.items()])}")

        # Get the Datasets using the dataset ids.
        dss = {k: dc.index.datasets.get(v) for k, v in dataset_ids.items()}

        # Filter the found datasets using the region code.
        filtered_dataset_ids_ = {k: check_ds_region(region_codes, v) for k, v in dss.items()}
        filtered_dataset_ids = {k: v for k, v in filtered_dataset_ids_.items() if v}
        _log.info(f"Filter by region code removed {len(dss) - len(filtered_dataset_ids)} datasets.")
        _log.info(f"After filter {' '.join([v for k,v in filtered_dataset_ids.items()])}")

        for entry, dataset_id in filtered_dataset_ids.items():
            # Send each dataset id to the output queue.
            entry_to_delete = [{"Id": entry[0], "ReceiptHandle": entry[1]}]

            successful, failed = send_batch_with_retry(
                queue_url=output_queue_url,
                messages=[dataset_id],
                max_retries=10,
                sqs_client=sqs_client,
            )
            if successful:
                # Delete the succesffuly sent message from the input queueue
                delete_batch_with_retry(
                    queue_url=input_queue_url, entries=entry_to_delete, sqs_client=sqs_client
                )
