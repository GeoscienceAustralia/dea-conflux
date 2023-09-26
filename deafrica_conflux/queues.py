"""Queue functions.

Matthew Alger, Alex Leith
Geoscience Australia
2021
"""
import json
import logging
import math
import time
from pathlib import Path

import boto3
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


# From https://stackoverflow.com/a/312464
def batch_messages(messages: list, n: int = 10):
    """
    Helper function to group a list of messages into batches of n messages.

    Parameters
    ----------
    messages : list
        A list of messages to be grouped into batches of n messages.
    n : int, optional
        Maximum number of messages in a single batch, by default 10

    Returns
    -------
    list[list, list]
        A list of lists containing the batched messages.
    """
    batched_messages = [messages[i: i + n] for i in range(0, len(messages), n)]

    assert len(batched_messages) == math.ceil(len(messages) / n)

    return batched_messages


def send_batch(
    queue_url: str, messages: list[str], sqs_client: SQSClient | None = None
) -> tuple[list | None, list | None]:
    """
    Sends a batch of messages in a single request to an SQS queue.

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

    entries = []
    for idx, msg in enumerate(messages):
        entry = {"Id": str(idx), "MessageBody": str(msg)}
        entries.append(entry)

    batched_entries = batch_messages(messages=entries, n=10)

    successful_messages_list = []
    failed_messages_list = []
    for batch in batched_entries:
        assert len(batch) <= 10
        try:
            send_message_batch_response = sqs_client.send_message_batch(
                QueueUrl=queue_url, Entries=batch
            )
        except ClientError as error:
            _log.exception(f"Failed to send messages {batch} to queue: {queue_url}.")
            raise error
        else:
            successful = send_message_batch_response.get("Successful", None)
            failed = send_message_batch_response.get("Failed", None)

            if successful is not None:
                successful_messages = [messages[int(msg_meta["Id"])] for msg_meta in successful]
                successful_messages_list.extend(successful_messages)
                _log.info(f"Successfully sent messages {successful_messages} to queue {queue_url}")
            else:
                successful_messages = None

            if failed is not None:
                failed_messages = [messages[int(msg_meta["Id"])] for msg_meta in failed]
                failed_messages_list.extend(failed_messages)
                _log.error(f"Failed to send messages {failed_messages} to queue {queue_url}")
            else:
                failed_messages = None

    return successful_messages_list, failed_messages_list


def send_batch_with_retry(
    queue_url: str, messages: list[str], max_retries: int = 10, sqs_client: SQSClient | None = None
) -> tuple[list | None, list | None]:
    """
    Sends a batch of messages in a single request to an SQS queue.
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


def push_to_queue_from_txt(
    text_file_path: str | Path, queue_name: str, max_retries: int = 10, sqs_client: SQSClient = None
):
    """
    Push lines of a text file to a SQS queue.

    Parameters
    ----------
    text_file_path : str | Path
        File path (s3 or local) of the text file to push to the specified
        SQS queue.
    queue_name : str
        Name of the SQS queue to push the lines of the text file to.
    max_retries: int
        Maximum number of times to retry to send a batch of messages.
    sqs_client : SQSClient, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None
    """
    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # "Support" pathlib Paths.
    text_file_path = str(text_file_path)

    # Check if the text file exists.
    if not deafrica_conflux.io.check_file_exists(text_file_path):
        _log.error(f"Could not find text file {text_file_path}!")
        raise FileNotFoundError(f"Could not find text file {text_file_path}!")

    if deafrica_conflux.io.check_if_s3_uri(text_file_path):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    # Read the text file.
    with fs.open(text_file_path, "r") as file:
        dataset_ids = [line.strip() for line in file]

    # Get the queue url.
    queue_url = get_queue_url(queue_name, sqs_client)

    # Batch the dataset ids into batches of 10.
    messages_to_send = batch_messages(dataset_ids, n=10)

    failed_to_send_list = []
    for batch in messages_to_send:
        successfully_sent, failed_to_send = send_batch_with_retry(
            queue_url=queue_url, messages=batch, max_retries=max_retries, sqs_client=sqs_client
        )
        if failed_to_send is not None:
            failed_to_send_list.extend(failed_to_send)

    if failed_to_send_list:
        _log.error(f"Failed to send {failed_to_send_list} to queue {queue_url}")


def receive_batch(
    queue_url: str,
    max_retries: int = 10,
    visibility_timeout: int = 3600,  # 1 hour
    sqs_client: SQSClient | None = None,
) -> tuple[list | None, list | None]:
    """
    Receive all messages from an SQS Queue.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue to receive messages from.
    max_retries : int, optional
        Maximum number of times to retry to receive a batch of messages., by default 10
    visibility_timeout : int, optional
         The duration (in seconds) that the received messages are hidden from subsequent
         retrieve requests after being retrieved by a `ReceiveMessage` request., by default 3600

    Returns
    -------
    list
        A list of messages from the queue.
    """

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Read messages from the queue.
    received_messages_list = []

    retries = 0
    while retries <= max_retries:
        receive_response = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=10,
            VisibilityTimeout=visibility_timeout,
        )
        received_messages = receive_response.get("Messages", None)
        if received_messages is not None:
            received_messages_list.extend(received_messages)
        else:
            retries += 1
            _log.info(f"No messages present in queue {queue_url}")

    return received_messages_list


def delete_batch(
    queue_url: str,
    receipt_handles: list[str],
    max_retries: int = 10,
    sqs_client: SQSClient | None = None,
):
    """
    Deletes a batch of messages in a single request to an SQS queue.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue from which messages are deleted.
    receipt_handles : list[str]
        A list of the receipt handles for the messages to be deleted.
    max_retries : int, optional
        Maximum number of times to retry to delete a batch of messages., by default 10
    sqs_client : SQSClient | None, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    """

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    entries = []
    for idx, receipt_handle in enumerate(receipt_handles):
        entry = {"Id": str(idx), "ReceiptHandle": str(receipt_handle)}
        entries.append(entry)

    batched_entries = batch_messages(messages=entries, n=10)

    successful_entries_list = []
    failed_entries_list = []
    for batch in batched_entries:
        assert len(batch) <= 10

        retries = 0
        batch_success = []
        while retries <= max_retries:
            try:
                delete_message_batch_response = sqs_client.delete_message_batch(
                    QueueUrl=queue_url, Entries=batch
                )
            except ClientError as error:
                _log.exception(f"Failed to delete messages from queue: {queue_url}.")
                raise error
            else:
                successful = delete_message_batch_response.get("Successful", None)
                failed = delete_message_batch_response.get("Failed", None)

                if successful is not None:
                    batch_success.extend(successful)

                if failed is not None:
                    retries += 1
                else:
                    break

        if batch_success:
            successful_entries_list.extend(batch_success)

        if failed:
            failed_entries_list.extend(failed)

    if failed_entries_list:
        _log.error(f"Failed to delete {len(failed_entries_list)} messages from queue {queue_url}.")
