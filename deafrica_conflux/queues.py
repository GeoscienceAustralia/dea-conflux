"""Queue functions.

Matthew Alger, Alex Leith
Geoscience Australia
2021
"""
import json
import boto3
import click
import fsspec
import logging
import time
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_sqs.client import SQSClient

from deafrica_conflux.io import check_if_s3_uri, check_s3_object_exists, check_local_file_exists

_log = logging.getLogger(__name__)


# From the AWS Code Examples Repository
# https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/python/example_code/sqs#code-examples
def get_queue_url(queue_name: str,
                  sqs_client: SQSClient = None) -> str:
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
    

def get_queue_attribute(queue_name: str,
                        attribute_name: str,
                        sqs_client: SQSClient = None) -> str:
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
        response = sqs_client.get_queue_attributes(QueueUrl=queue_url,
                                                   AttributeNames=[attribute_name])
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
        sqs_client: SQSClient = None):
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
    retry_config = {'max_attempts': 10,
                    'mode': 'standard'}

    # Create SQS client
    if sqs_client is None:
        sqs_client = boto3.client("sqs",
                                  config=Config(retries=retry_config))
    else:
        sqs_client.meta.config.retries = retry_config

    # Queue attributes.
    queue_attributes = dict(VisibilityTimeout=str(timeout),
                            MessageRetentionPeriod=str(retention_period))

    if dead_letter_queue_name:
        try:
            # Get the Amazon Resource Name (ARN) of the dead-letter queue.
            dead_letter_queue_arn = get_queue_attribute(dead_letter_queue_name, 'QueueArn', sqs_client)
        except Exception:
            _log.info(f"Creating dead-letter queue {dead_letter_queue_name}")
            verify_queue_name(dead_letter_queue_name)
            # Create dead-letter queue.
            response = sqs_client.create_queue(QueueName=dead_letter_queue_name)
            # Get the Amazon Resource Name (ARN) of the dead-letter queue.
            dead_letter_queue_arn = get_queue_attribute(dead_letter_queue_name, 'QueueArn', sqs_client)

        # Parameters for the dead-letter queue functionality of the source
        # queue as a JSON object.
        redrive_policy = {'deadLetterTargetArn': dead_letter_queue_arn,
                          'maxReceiveCount': '10'}
        queue_attributes["RedrivePolicy"] = json.dumps(redrive_policy)

    # Create the queue.
    try:
        response = sqs_client.create_queue(QueueName=queue_name,
                                           Attributes=queue_attributes)
    except ClientError as error:
        _log.exception(f"Couldn't create the queue {queue_name}.")
        raise error
    else:
        assert response

    return 0


def delete_queue(queue_name: str,
                 sqs_client: SQSClient = None):
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
        response = sqs_client.delete_queue(QueueUrl=queue_url) # noqa F841
    except ClientError as error:
        _log.exception(f"Couldn't delete the queue {queue_name}.")
        raise error
    else:
        _log.info("Deletion process in progress...")
        time.sleep(60)
        _log.info(f"Queue {queue_name} deleted.")


def move_to_deadletter_queue(
        deadletter_queue_name: str,
        message_body: str,
        sqs_client: SQSClient = None):
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
    entry = {"Id": "1",
             "MessageBody": str(message_body)}
    # Send message to SQS queue
    try:
        response = sqs_client.send_message_batch(QueueUrl=deadletter_queue_url, # noqa F841
                                                 Entries=[entry])
    except ClientError as error:
        _log.exception(f"Send message failed: {str(message_body)}")
        raise error


def _post_messages_batch(
        sqs_client: SQSClient,
        queue_url: str,
        messages: list,
        count: int):
    """
    Helper function for `push_to_queue_from_txt` function.
    Pushes a batch of 10 or less messages to the queue.

    Parameters
    ----------
    sqs_client : SQSClient
        A low-level client representing Amazon Simple Queue Service (SQS)
    queue_url : str
        URL of the queue to push the messages to.
    messages : list
        A list of messages to send.
    count : int
        Message count

    Returns
    -------
    list
        Empty list.
    """
    
    # Ensure the number of messages is 10 or less.
    assert len(messages) <= 10
    # Send the messages to the queue.
    response = sqs_client.send_message_batch(QueueUrl=queue_url, # noqa F841
                                             Entries=messages)
    _log.info(f"Added {count} messages...")
    return []
    

def push_to_queue_from_txt(
        text_file_path: str,
        queue_name: str,
        sqs_client: SQSClient = None):
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
    is_s3_uri = check_if_s3_uri(text_file_path)

    try:
        if is_s3_uri:
            check_s3_object_exists(text_file_path, error_if_exists=False)
        else:
            check_local_file_exists(text_file_path, error_if_exists=False)
    except FileNotFoundError as error:
        _log.exception(f"Could not find text file {text_file_path}!")
        raise error
    except PermissionError as error:
        _log.exception(f"You do not have sufficient permissions to access {text_file_path}!")
        raise error
    
    # Read the text file.
    with fsspec.open(text_file_path, "rb") as file:
        ids = [line.decode().strip() for line in file]
    
    verify_queue_name(queue_name)

    # Get the service client.
    if sqs_client is None:
        sqs_client = boto3.client("sqs")

    # Get the queue url.
    queue_url = get_queue_url(queue_name, sqs_client)

    # Send the ids from the text file as messages in
    # batches of 10.
    _log.debug(f'Adding IDs {ids}')

    count = 0
    messages = []
    for id_ in ids:
        message = {"Id": str(count),
                   "MessageBody": str(id_)}
        messages.append(message)
        count += 1
        if count % 10 == 0:
            messages = _post_messages_batch(sqs_client=sqs_client,
                                            queue_url=queue_url,
                                            messages=messages,
                                            count=count)
            
    # Post the remaining messages, if there are any.
    if len(messages) > 0:
        _post_messages_batch(sqs_client=sqs_client,
                             queue_url=queue_url,
                             messages=messages,
                             count=count)
