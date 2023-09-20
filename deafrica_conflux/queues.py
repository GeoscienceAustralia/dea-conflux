"""Queue functions.

Matthew Alger, Alex Leith
Geoscience Australia
2021
"""
import json
import logging
import time

import boto3
import click
import fsspec
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_sqs.client import SQSClient

import deafrica_conflux.io

_log = logging.getLogger(__name__)


# From the AWS Code Examples Repository
# https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/python/example_code/sqs#code-examples
def get_queue_url(queue_name: str, sqs_client: SQSClient = None) -> str:
    """
    Get the URL of an existing Amazon SQS queue by name, e.g., alex-really-secret-queue

    Parameters
    ----------
    queue_name : str
        The name that was used to create the SQS queue.
    sqs_client: SQSClient
        A low-level client representing Amazon Simple Queue Service (SQS), by default None
    Returns
    -------
    str
        URL of the Amazon SQS queue, if it exists.
    """
    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Get the queue name.
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
    except ClientError as error:
        _log.exception(f"Couldn't get URL for queue named {queue_name}.")
        raise error
    else:
        queue_url = response["QueueUrl"]
        _log.info(f"Got queue named {queue_name} with URL={queue_url}")
        return queue_url


def get_queue_attribute(queue_name: str, attribute_name: str, sqs_client: SQSClient = None) -> str:
    """
    Get the attribute value for the specified queue and attribute name.

    Parameters
    ----------
    queue_name : str
        The name that was used to create the SQS queue.
    attribute_name : str
        The attribute for which to retrieve information.
    sqs_client : SQSClient, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    Returns
    -------
    str
        The attribute value for the specified queue and attribute name.

    """

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Get the queue URL.
    queue_url = get_queue_url(queue_name, sqs_client)

    # Get the queue attribute.
    try:
        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=[attribute_name]
        )
    except ClientError as error:
        _log.exception(f"Couldn't get attribute {attribute_name} for queue named {queue_name}.")
        raise error
    else:
        queue_attribute_value = response["Attributes"][attribute_name]
        return queue_attribute_value


def verify_queue_name(queue_name: str):
    """
    Validate the name of an SQS queue.

    Parameters
    ----------
    queue_name : str
        Name of the SQS queue to validate.
    """
    if not queue_name.startswith("waterbodies_"):
        _log.error(f"Queue name {queue_name} does not start with waterbodies_", exc_info=True)
        raise click.ClickException("DE Africa conflux queues must start with waterbodies_")


def make_source_queue(
    queue_name: str,
    dead_letter_queue_name: str,
    timeout: int = 2 * 60,
    retries: int = 5,
    retention_period: int = 60,
    sqs_client: SQSClient = None,
):
    """
    Creates an Amazon SQS queue.

    Parameters
    ----------
    queue_name : str
        Name of the SQS queue to create.
    dead_letter_queue_name : str
        Name of the dead-letter SQS queue to target for messages that can't be
        processed sucessfully by the source SQS queue created.
    timeout : float, optional
        The visibility timeout for the queue, in seconds, by default 2*60
    retries : float, optional
        Maximum number of retry attempts, by default 5
    retention_period: int, optional
        The length of time, in seconds, for which Amazon SQS retains a message, by default 5
    sqs_client : SQSClient, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    """

    verify_queue_name(queue_name)

    # Retry configuration.
    retry_config = {"max_attempts": 10, "mode": "standard"}

    # Create SQS client
    if sqs_client is None:
        sqs_client = boto3.client("sqs", config=Config(retries=retry_config))
    else:
        sqs_client.meta.config.retries = retry_config

    # Queue attributes.
    queue_attributes = dict(
        VisibilityTimeout=str(timeout), MessageRetentionPeriod=str(retention_period)
    )

    if dead_letter_queue_name:
        try:
            # Get the Amazon Resource Name (ARN) of the dead-letter queue.
            dead_letter_queue_arn = get_queue_attribute(
                dead_letter_queue_name, "QueueArn", sqs_client
            )
        except Exception:
            _log.exception(f"Failed to get ARN for dead-letter queue {dead_letter_queue_name}.")
            _log.info(f"Creating dead-letter queue {dead_letter_queue_name}")
            verify_queue_name(dead_letter_queue_name)
            # Create dead-letter queue.
            try:
                response = sqs_client.create_queue(QueueName=dead_letter_queue_name)
            except ClientError as error:
                _log.exception(f"Couldn't create dead-letter queue {dead_letter_queue_name}")
                raise error
            else:
                # Get the Amazon Resource Name (ARN) of the dead-letter queue.
                dead_letter_queue_arn = get_queue_attribute(
                    dead_letter_queue_name, "QueueArn", sqs_client
                )

        # Parameters for the dead-letter queue functionality of the source
        # queue as a JSON object.
        redrive_policy = {"deadLetterTargetArn": dead_letter_queue_arn, "maxReceiveCount": "10"}
        queue_attributes["RedrivePolicy"] = json.dumps(redrive_policy)

    # Create the queue.
    try:
        response = sqs_client.create_queue(QueueName=queue_name, Attributes=queue_attributes)
    except ClientError as error:
        _log.exception(f"Could not create the queue {queue_name}.")
        raise error
    else:
        assert response


def delete_queue(queue_name: str, sqs_client: SQSClient = None):
    """
    Deletes a queue, regardless of the queue's contents.

    Parameters
    ----------
    queue_name : str
        Name of the SQS queue to delete.
    sqs_client : botocore.client.SQS, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None
    """

    verify_queue_name(queue_name)

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Get the queue URL.
    queue_url = get_queue_url(queue_name, sqs_client)

    # Delete the queue.
    try:
        response = sqs_client.delete_queue(QueueUrl=queue_url)  # noqa F841
    except ClientError as error:
        _log.exception(f"Couldn't delete the queue {queue_name}.")
        raise error
    else:
        _log.info("Deletion process in progress...")
        time.sleep(60)
        _log.info(f"Queue {queue_name} deleted.")


def move_to_deadletter_queue(
    deadletter_queue_name: str, message_body: str, sqs_client: SQSClient = None
):
    """
    Deliver a message to the dead-letter SQS queue.

    Parameters
    ----------
    deadletter_queue_name : str
        The name of the deadletter SQS queue to receive the message.
    message_body : str
        The body text of the message.
    sqs_client : SQSClient, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None
    """

    verify_queue_name(deadletter_queue_name)

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    deadletter_queue_url = get_queue_url(deadletter_queue_name, sqs_client)

    # Only send one message, so 1 is OK as the identifier for a message in this
    # batch used to communicate the result.
    entry = {"Id": "1", "MessageBody": str(message_body)}
    # Send message to SQS queue
    try:
        response = sqs_client.send_message_batch(  # noqa F841
            QueueUrl=deadletter_queue_url, Entries=[entry]
        )
    except ClientError as error:
        _log.exception(f"Send message failed: {str(message_body)}")
        raise error


def push_to_queue_from_txt(text_file_path: str, queue_name: str, sqs_client: SQSClient = None):
    """
    Push lines of a text file to a SQS queue.

    Parameters
    ----------
    text_file_path : str
        File path (s3 or local) of the text file to push to the specified
        SQS queue.
    queue_name : str
        Name of the SQS queue to push the lines of the text file to.
    sqs_client : SQSClient, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None
    """

    # Check if the text file exists.
    is_s3_uri = deafrica_conflux.io.check_if_s3_uri(text_file_path)

    try:
        if is_s3_uri:
            deafrica_conflux.io.check_s3_object_exists(text_file_path, error_if_exists=False)
        else:
            deafrica_conflux.io.check_local_file_exists(text_file_path, error_if_exists=False)
    except FileNotFoundError as error:
        _log.exception(f"Could not find text file {text_file_path}!")
        raise error
    except PermissionError as error:
        _log.exception(f"You do not have sufficient permissions to access {text_file_path}!")
        raise error

    # Read the text file.
    with fsspec.open(text_file_path, "rb") as file:
        dataset_ids = [line.decode().strip() for line in file]

    verify_queue_name(queue_name)

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Get the queue url.
    queue_url = get_queue_url(queue_name, sqs_client)

    # Send the dataset ids to the queue.
    messages_to_send = []
    for idx, dataset_id in enumerate(dataset_ids):
        messages_to_send.append(dataset_id)
        if (idx + 1) % 10 == 0:
            successful, failed = send_batch_with_retry(
                queue_url=queue_url,
                messages=messages_to_send,
                max_retries=10,
                sqs_client=sqs_client,
            )
            # Reset the messages to send list.
            messages_to_send = []

    # Send the remaining messages if there are any.
    successful, failed = send_batch_with_retry(
        queue_url=queue_url, messages=messages_to_send, max_retries=10, sqs_client=sqs_client
    )


def send_batch_with_retry(
    queue_url: str, messages: list[str], max_retries: int = 10, sqs_client: SQSClient | None = None
) -> tuple[list | None, list | None]:
    """
    Sends a batch of upto 10 messages in a single request to an SQS queue.
    Retry to send failed messages `max_retries` number of times.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue to send the messages to.
    messages : list[str]
        The messages to send to the queue.
    max_retries: int
        Maximum number of times to retry to send a batch of messages.
    sqs_client : SQSClient | None, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    Returns
    -------
    tuple[list, list]
        A list of the successfully sent messages and a list of the failed messages.

    """
    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Ensure the number of messages being sent in the batch is 10 or less.
    assert len(messages) <= 10

    retries = 0
    sucessful = []
    while retries <= max_retries:
        sucessful_msgs, messages = send_batch(
            queue_url=queue_url, messages=messages, sqs_client=sqs_client
        )
        sucessful.extend(sucessful_msgs)
        if messages is None:
            break
        else:
            retries += 1

    return sucessful, messages


def send_batch(
    queue_url: str, messages: list[str], sqs_client: SQSClient | None = None
) -> tuple[list | None, list | None]:
    """
    Sends a batch of upto 10 messages in a single request to an SQS queue.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue to send the messages to.
    messages : list[str]
        The messages to send to the queue.
    sqs_client : SQSClient | None, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    Returns
    -------
    tuple[list, list]
        A list of the successfully sent messages and a list of the failed messages.

    """

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Ensure the number of messages being sent in the batch is 10 or less.
    assert len(messages) <= 10

    entries = []
    for idx, msg in enumerate(messages):
        entry = {"Id": str(idx), "MessageBody": str(msg)}
        entries.append(entry)

    try:
        send_message_batch_response = sqs_client.send_message_batch(
            QueueUrl=queue_url, Entries=entries
        )
    except ClientError as error:
        _log.exception(f"Failed to send messages {entries} to queue: {queue_url}.")
        raise error
    else:
        successful = send_message_batch_response.get("Successful", None)
        failed = send_message_batch_response.get("Failed", None)

        if successful is not None:
            successful_messages = [messages[int(msg_meta["Id"])] for msg_meta in successful]
            _log.info(f"Successfully sent messages {successful_messages} to queue {queue_url}")
        else:
            successful_messages = None

        if failed is not None:
            failed_messages = [messages[int(msg_meta["Id"])] for msg_meta in failed]
            _log.error(f"Failed to send messages {failed_messages} to queue {queue_url}")
        else:
            failed_messages = None

        return successful_messages, failed_messages


def delete_batch(queue_url: str, receipt_handles: list[str], sqs_client: SQSClient | None = None):
    """
    Deletes a batch of upto 10 messages in a single request to an SQS queue.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue from which messages are deleted.
    receipt_handles : list[str]
        A list of the receipt handles for the messages to be deleted.
    sqs_client : SQSClient | None, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    """

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Ensure the number of receipt handles for the messages to be deleted is 10 or less.
    assert len(receipt_handles) <= 10

    entries = []
    for idx, receipt_handle in enumerate(receipt_handles):
        entry = {"Id": str(idx), "ReceiptHandle": str(receipt_handle)}
        entries.append(entry)

    try:
        delete_message_batch_response = sqs_client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )
    except ClientError as error:
        _log.exception(f"Failed to delete messages from queue: {queue_url}.")
        raise error
    else:
        successful = delete_message_batch_response.get("Successful", None)
        failed = delete_message_batch_response.get("Failed", None)

        if successful is not None:
            _log.info(f"Succefully deleted {len(successful)} messages from queue {queue_url}.")

        if failed is not None:
            _log.error(f"Failed to delete {len(failed)} messages from queue {queue_url}.")
