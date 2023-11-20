import json
import os
from pathlib import Path

import boto3
import fsspec
import pytest
from moto import mock_sqs

from deafrica_conflux.queues import (
    delete_batch_with_retry,
    delete_queue,
    get_queue_attribute,
    get_queue_url,
    make_source_queue,
    move_to_dead_letter_queue,
    push_dataset_ids_to_queue_from_txt,
    receive_messages,
    send_batch,
    send_batch_with_retry,
)

TEST_SOURCE_QUEUE = "test_waterbodies_queue"
TEST_DEADLETTER_QUEUE = "test_waterbodies_queue_deadletter"

HERE = Path(__file__).parent.resolve()
TEST_TEXT_FILE = HERE / "data" / "test_push_to_queue_from_txt.txt"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "af-south-1"


@pytest.fixture(scope="function")
def sqs_client(aws_credentials):
    with mock_sqs():
        yield boto3.client("sqs", region_name="af-south-1")


@mock_sqs
def test_get_queue_url_with_existing_queue(sqs_client):
    # Create the queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    # Get queue url.
    try:
        queue_url = get_queue_url(queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client)
    except Exception:
        assert False
    else:
        assert TEST_SOURCE_QUEUE in queue_url


@mock_sqs
def test_get_queue_url_with_not_existing_queue(sqs_client):
    # Get queue url.
    try:
        queue_url = get_queue_url(queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client)  # noqa F841
    except sqs_client.exceptions.QueueDoesNotExist:
        assert True
    else:
        assert False


@mock_sqs
def test_get_queue_attribute(sqs_client):
    # Create the queue.
    queue_attributes = {"VisibilityTimeout": "43200"}
    response = sqs_client.create_queue(  # noqa F841
        QueueName=TEST_SOURCE_QUEUE, Attributes=queue_attributes
    )
    # Get attribute from the queue.
    try:
        attribute = get_queue_attribute(
            queue_name=TEST_SOURCE_QUEUE, attribute_name="VisibilityTimeout", sqs_client=sqs_client
        )
    except Exception:
        assert False
    else:
        assert attribute == "43200"


@mock_sqs
def test_make_source_queue_with_existing_deadletter_queue(sqs_client):
    # Create the deadletter queue.
    sqs_client.create_queue(QueueName=TEST_DEADLETTER_QUEUE)

    try:
        make_source_queue(
            queue_name=TEST_SOURCE_QUEUE,
            dead_letter_queue_name=TEST_DEADLETTER_QUEUE,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        source_queue_url = get_queue_url(queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client)
        redrive_policy_attribute = get_queue_attribute(
            queue_name=TEST_SOURCE_QUEUE, attribute_name="RedrivePolicy", sqs_client=sqs_client
        )
        redrive_policy_attribute = json.loads(redrive_policy_attribute)

        assert TEST_DEADLETTER_QUEUE in redrive_policy_attribute["deadLetterTargetArn"]
        assert TEST_SOURCE_QUEUE in source_queue_url


@mock_sqs
def test_make_source_queue_with_no_existing_deadletter_queue(sqs_client):
    try:
        make_source_queue(
            queue_name=TEST_SOURCE_QUEUE,
            dead_letter_queue_name=TEST_DEADLETTER_QUEUE,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        source_queue_url = get_queue_url(queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client)
        deadletter_queue_url = get_queue_url(
            queue_name=TEST_DEADLETTER_QUEUE, sqs_client=sqs_client
        )

        assert TEST_SOURCE_QUEUE in source_queue_url
        assert TEST_DEADLETTER_QUEUE in deadletter_queue_url


@mock_sqs
def test_delete_empty_sqs_queue(sqs_client):
    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)

    # Delete queue.
    try:
        delete_queue(queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client)
    except Exception:
        assert False
    else:
        try:
            queue_url = get_queue_url(  # noqa F841
                queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client
            )
        except sqs_client.exceptions.QueueDoesNotExist:
            assert True
        else:
            assert False


@mock_sqs
def test_send_batch(sqs_client):
    # Create messages to send.
    messages_to_send = [str(i) for i in list(range(0, 42))]

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = get_queue_url(queue_name=TEST_SOURCE_QUEUE)

    try:
        successful_messages, failed_messages = send_batch(
            queue_url=queue_url, messages=messages_to_send, sqs_client=sqs_client
        )
    except Exception:
        assert False
    else:
        assert successful_messages == messages_to_send


@mock_sqs
def test_send_batch_with_retry(sqs_client):
    max_retries = 10

    # Create messages to send.
    messages_to_send = [str(i) for i in list(range(0, 97))]

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = get_queue_url(TEST_SOURCE_QUEUE)

    try:
        successful_messages, failed_messages = send_batch_with_retry(
            queue_url=queue_url,
            messages=messages_to_send,
            max_retries=max_retries,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        assert messages_to_send == successful_messages


@mock_sqs
def test_move_to_deadletter_queue(sqs_client):
    message_body = "Test move to deadletter queue"
    max_retries = 10

    # Create the deadletter queue.
    sqs_client.create_queue(QueueName=TEST_DEADLETTER_QUEUE)
    deadletter_queue_url = get_queue_url(TEST_DEADLETTER_QUEUE)

    try:
        move_to_dead_letter_queue(
            dead_letter_queue_url=deadletter_queue_url,
            message_body=message_body,
            max_retries=max_retries,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        response = sqs_client.receive_message(QueueUrl=deadletter_queue_url)
        assert response["Messages"][0]["Body"] == message_body


@mock_sqs
def test_push_dataset_ids_to_queue_from_local_txt(sqs_client):
    max_retries = 10
    # Create messages to send.
    dataset_ids = [str(i) for i in list(range(0, 365))]

    # Write the messages to a local text file
    fs = fsspec.filesystem("file")
    with fs.open(TEST_TEXT_FILE, "w") as file:
        for dataset_id in dataset_ids:
            file.write(f"{dataset_id}\n")

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)

    # Push ids to sqs queues
    try:
        push_dataset_ids_to_queue_from_txt(
            text_file_path=TEST_TEXT_FILE,
            queue_name=TEST_SOURCE_QUEUE,
            max_retries=max_retries,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        assert True


def test_delete_batch_with_retry(sqs_client):
    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = get_queue_url(TEST_SOURCE_QUEUE)

    # Create the messages to send.
    messages = [str(i) for i in list(range(1, 10))]

    # Push the messages to the queue.
    send_batch(queue_url=queue_url, messages=messages, sqs_client=sqs_client)

    # Receive messages from queue
    receive_response = sqs_client.receive_message(
        QueueUrl=queue_url, AttributeNames=["All"], MaxNumberOfMessages=10
    )

    receive_messages = receive_response["Messages"]
    entries_to_delete = [
        {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]} for msg in receive_messages
    ]

    # Delete messages
    successfully_deleted, failed = delete_batch_with_retry(
        queue_url=queue_url, entries=entries_to_delete, max_retries=10, sqs_client=sqs_client
    )

    assert successfully_deleted == entries_to_delete


def test_receive_messages(sqs_client):
    max_retries = 10
    visibility_timeout = 3600

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = get_queue_url(TEST_SOURCE_QUEUE)

    # Create the messages to send.
    messages = [str(i) for i in list(range(1, 10))]

    # Send the messages to the queue.
    send_batch(queue_url=queue_url, messages=messages)

    # Retrieve a single message.
    try:
        message = receive_messages(
            queue_url=queue_url,
            max_retries=max_retries,
            visibility_timeout=visibility_timeout,
            sqs_client=sqs_client,
            max_no_messages=1,
        )
    except Exception:
        assert False
    else:
        assert message is not None
