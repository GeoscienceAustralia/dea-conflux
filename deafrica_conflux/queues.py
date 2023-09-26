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
    Get the URL of an existing SQS queue by name, e.g., alex-really-secret-queue

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
    Creates a SQS queue.

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

    if not dead_letter_queue_name:
        dead_letter_queue_name = f"{queue_name}-deadletter"

    try:
        # Get the Amazon Resource Name (ARN) of the dead-letter queue.
        dead_letter_queue_arn = get_queue_attribute(dead_letter_queue_name, "QueueArn", sqs_client)
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
    Deletes a SQS queue, regardless of the queue's contents.

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
    batched_messages = [messages[i : i + n] for i in range(0, len(messages), n)]

    assert len(batched_messages) == math.ceil(len(messages) / n)

    _log.info(f"Grouped {len(messages)} messages into {len(batched_messages)} batches.")

    return batched_messages


def send_batch(
    queue_url: str, messages: list[str], sqs_client: SQSClient | None = None
) -> tuple[list | None, list | None]:
    """
    Sends messages to a SQS queue.

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
        A list of  the successfully sent messages
        and a list of the failed messages.

    """
    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Create a list of SendMessageBatchRequestEntry items.
    entries = [{"Id": str(idx), "MessageBody": str(msg)} for idx, msg in enumerate(messages)]

    # Batch the messages into groups of 10 because
    # SendMessageBatch can only send up to 10 messages at a time.
    batched_entries = batch_messages(messages=entries, n=10)

    successful_msgs_list = []
    failed_msgs_list = []
    for batch in batched_entries:
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
                # ID in the send_message_batch_response matches ID in the batch.
                # Use ID to identify successfully sent entries in the batch.
                successful_ids = [msg["Id"] for msg in successful]
                successful_entries = [entry for entry in batch if entry["Id"] in successful_ids]
                successful_messages = [entry["MessageBody"] for entry in successful_entries]
                _log.info(f"Successfully sent messages {successful_messages} to queue {queue_url}")
                successful_msgs_list.extend(successful_messages)
            if failed is not None:
                failed_ids = [msg["Id"] for msg in failed]
                failed_entries = [entry for entry in batch if entry["Id"] in failed_ids]
                failed_messages = [entry["MessageBody"] for entry in failed_entries]
                _log.error(f"Failed to send messages {failed_messages} to queue {queue_url}")
                failed_msgs_list.extend(failed_messages)

    if successful_msgs_list:
        _log.info(
            f"Successfully sent {len(successful_msgs_list)} out of {len(messages)} messages to queue {queue_url}"
        )

    if failed_msgs_list:
        _log.error(
            f"Failed to send messages {len(failed_msgs_list)} out of {len(messages)} messages to queue {queue_url}"
        )

    return successful_msgs_list, failed_msgs_list


def send_batch_with_retry(
    queue_url: str, messages: list[str], max_retries: int = 10, sqs_client: SQSClient | None = None
) -> tuple[list | None, list | None]:
    """
    Sends messages to a SQS queue and retry a maximum a number of `max_retries` times
    to send failed messages.

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

    successfully_sent = []
    retries = 0
    while retries <= max_retries:
        successful, messages = send_batch(
            queue_url=queue_url, messages=messages, sqs_client=sqs_client
        )

        successfully_sent.extend(successful)
        # If there are failed messages increase the number of retries by 1.
        if messages:
            retries += 1
        else:  # If there are no failed messages break the loop.
            break

    if messages:
        _log.error(
            f"Failed to send messages {messages} to queue {queue_url} after {max_retries} retries."
        )

    return successfully_sent, messages


def move_to_dead_letter_queue(
    dead_letter_queue_url: str,
    message_body: str,
    max_retries: int = 10,
    sqs_client: SQSClient = None,
):
    """
    Deliver a message to the dead-letter SQS queue.

    Parameters
    ----------
    dead_letter_queue_url : str
        URL of the dead-letter SQS queue to receive the message.
    message_body : str
        The body text of the message.
    max_retries : int, optional
        Maximum number of times to try to resend a message to the dead-letter SQS queue.
    sqs_client : SQSClient, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None
    """
    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    message = [str(message_body)]
    # Send message to SQS queue
    send_batch_with_retry(
        queue_url=dead_letter_queue_url,
        messages=message,
        max_retries=max_retries,
        sqs_client=sqs_client,
    )


def push_dataset_ids_to_queue_from_txt(
    text_file_path: str | Path, queue_name: str, max_retries: int = 10, sqs_client: SQSClient = None
) -> list:
    """
    Push dataset ids from lines of a text file to a SQS queue.

    Parameters
    ----------
    text_file_path : str | Path
        File path (s3 or local) of the text file containing the dataset ids to be
        push to the specified SQS queue.
    queue_name : str
        Name of the SQS queue to push the lines of the text file to.
    max_retries: int
        Maximum number of times to retry to push a dataset id to the SQS queue.
    sqs_client : SQSClient, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    list:
        A list of the dataset-ids that failed to be pushed to the queue.

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

    # Get the queue url.
    queue_url = get_queue_url(queue_name, sqs_client)

    # Process each line to prevent loading the entire file into memory.
    failed_to_push = []
    with fs.open(text_file_path, "r") as file:
        for line in file:
            dataset_id = line.strip()
            # Push the dataset id to the sqs queue.
            _, failed = send_batch_with_retry(
                queue_url=queue_url,
                messages=[dataset_id],
                max_retries=max_retries,
                sqs_client=sqs_client,
            )
            if failed:
                failed_to_push.extend(failed)

    if failed_to_push:
        _log.error(f"Failed to push ids {failed_to_push} ")

    return failed_to_push


def delete_batch(
    queue_url: str,
    entries: list[dict[str, str]],
    sqs_client: SQSClient | None = None,
):
    """
    Deletes messages from a SQS queue.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue to delete messages from.
    entries : list[dict[str, str]]
        A list of the messages to be deleted. The entries must contain an "Id"
        and "ReceiptHandle".
    sqs_client : SQSClient | None, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    Returns
    -------
    tuple[list, list]
        A list of the successfully deleted messages and a list of the failed messages.

    """

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Assert each message contains the required properties.
    for entry in entries:
        assert "Id" in entry.keys()
        assert "ReceiptHandle" in entry.keys()

    # Batch the entries into groups of 10.
    batched_entries = batch_messages(messages=entries, n=10)

    # Assert each message contains the required properties.
    for entry in entries:
        assert "Id" in entry.keys()
        assert "ReceiptHandle" in entry.keys()

    # Batch the entries into groups of 10.
    batched_entries = batch_messages(messages=entries, n=10)

    successful_entries_list = []
    failed_entries_list = []
    for batch in batched_entries:
        try:
            delete_response = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=batch)
        except ClientError as error:
            _log.exception(f"Failed to delete messages {batch} from queue: {queue_url}.")
            raise error
        else:
            successful = delete_response.get("Successful", None)
            failed = delete_response.get("Failed", None)

            if successful is not None:
                # ID in the delete_response matches ID in the batch.
                # Use ID to identify successfully deleted entries in the batch.
                successful_ids = [msg["Id"] for msg in successful]
                successful_entries = [entry for entry in batch if entry["Id"] in successful_ids]
                _log.info(
                    f"Successfully deleted messages {successful_entries} from queue {queue_url}"
                )
                successful_entries_list.extend(successful_entries)
            if failed is not None:
                failed_ids = [msg["Id"] for msg in failed]
                failed_entries = [entry for entry in batch if entry["Id"] in failed_ids]
                _log.error(f"Failed to delete messages {failed_entries} from queue {queue_url}")
                failed_entries_list.extend(failed_entries)

    if successful_entries_list:
        _log.info(
            f"Successfully deleted {len(successful_entries_list)} out of {len(entries)} messages from queue {queue_url}"
        )

    if failed_entries_list:
        _log.error(
            f"Failed to delete messages {len(failed_entries_list)} out of {len(entries)} messages from queue {queue_url}"
        )

    return successful_entries_list, failed_entries_list


def delete_batch_with_retry(
    queue_url: str,
    entries: list[dict[str, str]],
    max_retries: int = 10,
    sqs_client: SQSClient | None = None,
) -> tuple[list | None, list | None]:
    """
    Delete messages from an SQS queue and retry a maximum a number of `max_retries` of times
    to delete failed messages.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue to delete messages from.
    entries : list[dict[str, str]]
        A list of the messages to be deleted. The entries must contain an "Id"
        and "ReceiptHandle".
    max_retries : int, optional
        Maximum number of times to retry to delete a batch of messages., by default 10
    sqs_client : SQSClient | None, optional
        A low-level client representing Amazon Simple Queue Service (SQS), by default None

    Returns
    -------
    tuple[list, list]
        A list of the successfully deleted messages and a list of the failed messages.

    """
    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    successfully_deleted = []
    retries = 0
    while retries <= max_retries:
        successful, entries = delete_batch(
            queue_url=queue_url, entries=entries, sqs_client=sqs_client
        )

        successfully_deleted.extend(successful)
        # If there are failed messages increase the number of retries by 1.
        if entries:
            retries += 1
        else:  # If there are no failed messages break the loop.
            break

    if entries:
        _log.error(
            f"Failed to delete messages {entries} from queue {queue_url} after {max_retries} retries."
        )

    return successfully_deleted, entries


def receive_a_message(
    queue_url: str,
    max_retries: int = 10,
    visibility_timeout: int = 3600,  # 1 hour
    sqs_client: SQSClient | None = None,
) -> dict:
    """
    Receive a single message from an SQS Queue.

    Parameters
    ----------
    queue_url : str
        URL of the SQS queue to receive message from.
    max_retries : int, optional
        Maximum number of times to retry to receive a messages., by default 10
    visibility_timeout : int, optional
         The duration (in seconds) that the received message is hidden from subsequent
         retrieve requests after being retrieved by a `ReceiveMessage` request., by default 3600

    Returns
    -------
    dict
        A single message from the queue.
    """

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    retries = 0
    while retries <= max_retries:
        try:
            # Retrieve a single message from the queue.
            receive_response = sqs_client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["All"],
                MaxNumberOfMessages=1,
                VisibilityTimeout=visibility_timeout,
            )
        except ClientError as error:
            _log.exception(f"Could not receive a message from queue {queue_url}")
            raise error
        else:
            received_message = receive_response.get("Messages", None)

            if received_message is None:
                retries += 1
            else:
                break  # Reset the count

    if received_message is not None:
        assert len(received_message) == 1
        # Get the message body from the retrieved message.
        message = received_message[0]
        _log.info(f"Received message {message} from queue {queue_url}")
        return message
    else:
        _log.info(f"Received no message from queue {queue_url}")
        return None
