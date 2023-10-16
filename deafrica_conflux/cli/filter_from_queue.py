import logging
import time

import boto3
import click
import datacube
import geopandas as gpd

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.drill import filter_datasets
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.queues import delete_batch, get_queue_url, send_batch_with_retry


@click.command("filter-from-sqs-queue", no_args_is_help=True)
@click.option("--input-queue", "-iq", help="SQS queue to read all dataset IDs.")
@click.option("--output-queue", "-oq", help="SQS Queue to save filtered dataset IDs.")
@click.option(
    "--polygons-vector-file",
    type=click.Path(),
    help="Path to the vector file defining the polygon(s) to run polygon drill on to filter datasets.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id polygons vector file.",
)
@click.option(
    "--visibility-timeout",
    default=60 * 60,
    help="The duration (in seconds) that a received SQS message is hidden from "
    "subsequent retrieve requests after being retrieved by a ReceiveMessage request.",
)
@click.option(
    "--num-worker",
    type=int,
    help="The number of processes to filter datasets.",
    default=4,
)
@click.option("-v", "--verbose", count=True)
def filter_from_queue(
    input_queue, output_queue, polygons_vector_file, use_id, visibility_timeout, num_worker, verbose
):
    """
    Run deafrica-conflux filter dataset based on scene ids from a queue.
    Then submit the filter result to another queue.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Read the vector file.
    try:
        polygons_gdf = gpd.read_file(polygons_vector_file)
    except Exception as error:
        _log.exception(f"Could not read file {polygons_vector_file}")
        raise error

    # Guess the ID field.
    id_field = guess_id_field(polygons_gdf, use_id)
    _log.debug(f"Guessed ID field: {id_field}")

    # Set the ID field as the index.
    polygons_gdf.set_index(id_field, inplace=True)

    sqs_client = boto3.client("sqs")

    # Input queue should have a dead letter queue configured in its RedrivePolicy.
    input_queue_url = get_queue_url(queue_name=input_queue, sqs_client=sqs_client)

    output_queue_url = get_queue_url(queue_name=output_queue, sqs_client=sqs_client)

    # Maximum number of retries to get messages from the input queue.
    message_retries = 10
    while message_retries > 0:
        receive_message_response = sqs_client.receive_message(
            QueueUrl=input_queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=10,
            VisibilityTimeout=visibility_timeout,
        )
        retrieved_messages = receive_message_response["Messages"]

        # If no messages are received from the queue, subtract 1 from the number
        # of retries.
        if len(retrieved_messages) == 0:
            time.sleep(1)
            _log.info("No messages received from queue {input_queue_url}")
            message_retries -= 1
            continue
        else:
            # If a message(s) is received reset the message retries
            # back to the maximum number of retries.
            message_retries = 10

        # Get the receipt handle for each of the retrieved messages.
        retrieved_receipt_handles = [msg["ReceiptHandle"] for msg in retrieved_messages]

        # Get the dataset ids from the input queue.
        dataset_ids = [msg["Body"] for msg in retrieved_messages]
        _log.info(f"Before filter {' '.join(dataset_ids)}")

        # Get a list of Datasets using the dataset ids.
        dss = [dc.index.datasets.get(dataset_id) for dataset_id in dataset_ids]

        # Filter the Datasets.
        filtered_dataset_ids = filter_datasets(
            dss=dss, polygons_gdf=polygons_gdf, worker_num=num_worker
        )
        _log.info(f"After filter {' '.join(filtered_dataset_ids)}")

        # Send the filtered dataset ids to the output queue in batches of 10.
        messages_to_send = []
        for idx, filtered_dataset_id in enumerate(filtered_dataset_ids):
            messages_to_send.append(filtered_dataset_id)
            if (idx + 1) % 10 == 0:
                successful, failed = send_batch_with_retry(
                    queue_url=output_queue_url,
                    messages=messages_to_send,
                    max_retries=10,
                    sqs_client=sqs_client,
                )
                # Delete the sucessfully sent messages from the input queue.
                messages_to_delete = [
                    retrieved_receipt_handles[dataset_ids.index(msg)] for msg in successful
                ]
                delete_batch(
                    queue_url=input_queue_url,
                    receipt_handles=messages_to_delete,
                    sqs_client=sqs_client,
                )
                # Reset the messages to send list.
                messages_to_send = []

        # Send the remaining messages if there are any.
        successful, failed = send_batch_with_retry(
            queue_url=output_queue_url,
            messages=messages_to_send,
            max_retries=10,
            sqs_client=sqs_client,
        )
        # Delete the sucessfully sent messages from the input queue.
        messages_to_delete = [
            retrieved_receipt_handles[dataset_ids.index(msg)] for msg in successful
        ]
        delete_batch(
            queue_url=input_queue_url, receipt_handles=messages_to_delete, sqs_client=sqs_client
        )
