import json
import os
from pathlib import Path
import boto3
import fsspec
import pytest
from moto import mock_sqs

import deafrica_conflux.queues

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
        queue_url = deafrica_conflux.queues.get_queue_url(
            queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client
        )
    except Exception:
        assert False
    else:
        assert TEST_SOURCE_QUEUE in queue_url


@mock_sqs
def test_get_queue_url_with_not_existing_queue(sqs_client):
    # Get queue url.
    try:
        queue_url = deafrica_conflux.queues.get_queue_url(  # noqa F841
            queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client
        )
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
        attribute = deafrica_conflux.queues.get_queue_attribute(
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
        deafrica_conflux.queues.make_source_queue(
            queue_name=TEST_SOURCE_QUEUE,
            dead_letter_queue_name=TEST_DEADLETTER_QUEUE,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        source_queue_url = deafrica_conflux.queues.get_queue_url(
            queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client
        )
        redrive_policy_attribute = deafrica_conflux.queues.get_queue_attribute(
            queue_name=TEST_SOURCE_QUEUE, attribute_name="RedrivePolicy", sqs_client=sqs_client
        )
        redrive_policy_attribute = json.loads(redrive_policy_attribute)

        assert TEST_DEADLETTER_QUEUE in redrive_policy_attribute["deadLetterTargetArn"]
        assert TEST_SOURCE_QUEUE in source_queue_url


@mock_sqs
def test_make_source_queue_with_no_existing_deadletter_queue(sqs_client):
    try:
        deafrica_conflux.queues.make_source_queue(
            queue_name=TEST_SOURCE_QUEUE,
            dead_letter_queue_name=TEST_DEADLETTER_QUEUE,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        source_queue_url = deafrica_conflux.queues.get_queue_url(
            queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client
        )
        deadletter_queue_url = deafrica_conflux.queues.get_queue_url(
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
        deafrica_conflux.queues.delete_queue(queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client)
    except Exception:
        assert False
    else:
        try:
            queue_url = deafrica_conflux.queues.get_queue_url(  # noqa F841
                queue_name=TEST_SOURCE_QUEUE, sqs_client=sqs_client
            )
        except sqs_client.exceptions.QueueDoesNotExist:
            assert True
        else:
            assert False


@mock_sqs
def test_move_to_deadletter_queue(sqs_client):
    message_body = "Test move to deadletter queue"

    # Create the deadletter queue.
    sqs_client.create_queue(QueueName=TEST_DEADLETTER_QUEUE)
    deadletter_queue_url = deafrica_conflux.queues.get_queue_url(TEST_DEADLETTER_QUEUE)

    try:
        deafrica_conflux.queues.move_to_deadletter_queue(
            deadletter_queue_name=TEST_DEADLETTER_QUEUE,
            message_body=message_body,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        response = sqs_client.receive_message(QueueUrl=deadletter_queue_url)
        assert response["Messages"][0]["Body"] == message_body


@mock_sqs
def test_send_batch(sqs_client):
    # Create messages to send.
    messages_to_send = list(range(0, 100))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = deafrica_conflux.queues.get_queue_url(queue_name=TEST_SOURCE_QUEUE)

    try:
        successful_messages, failed_messages = deafrica_conflux.queues.send_batch(
            queue_url=queue_url, messages=messages_to_send, sqs_client=sqs_client
        )
    except Exception:
        assert False
    else:
        assert len(successful_messages) == len(messages_to_send)


@mock_sqs
def test_send_batch_with_retry(sqs_client):
    max_retries = 10

    # Create messages to send.
    messages_to_send = list(range(0, 97))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = deafrica_conflux.queues.get_queue_url(TEST_SOURCE_QUEUE)

    try:
        successful_messages, failed_messages = deafrica_conflux.queues.send_batch_with_retry(
            queue_url=queue_url,
            messages=messages_to_send,
            max_retries=max_retries,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        assert len(messages_to_send) == len(successful_messages)


@mock_sqs
def test_receive_batch(sqs_client):
    max_retries = 10
    visibility_timeout = 3600

    # Create messages to send.
    messages_to_send = list(range(1, 23))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = deafrica_conflux.queues.get_queue_url(TEST_SOURCE_QUEUE)

    deafrica_conflux.queues.send_batch(
        queue_url=queue_url, messages=messages_to_send, sqs_client=sqs_client
    )
    try:
        received_messages = deafrica_conflux.queues.receive_batch(
            queue_url=queue_url,
            max_retries=max_retries,
            visibility_timeout=visibility_timeout,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        assert len(messages_to_send) == len(received_messages)


@mock_sqs
def test_push_to_queue_from_txt(sqs_client):
    max_retries = 10

    # Write messages to text file.
    messages_to_send = list(range(0, 43))
    messages_to_send = [str(i) for i in messages_to_send]

    fs = fsspec.filesystem("file")
    with fs.open(TEST_TEXT_FILE, "w") as f:
        for msg in messages_to_send:
            f.write(f"{msg}\n")

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = deafrica_conflux.queues.get_queue_url(TEST_SOURCE_QUEUE)

    # Push to queue from text file.
    try:
        deafrica_conflux.queues.push_to_queue_from_txt(
            text_file_path=TEST_TEXT_FILE,
            queue_name=TEST_SOURCE_QUEUE,
            max_retries=max_retries,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        received_messages = deafrica_conflux.queues.receive_batch(
            queue_url=queue_url,
            max_retries=max_retries,
            visibility_timeout=3600,
            sqs_client=sqs_client,
        )
        assert len(messages_to_send) == len(received_messages)
        # Remove text file.
        fs.rm(TEST_TEXT_FILE)


@mock_sqs
def test_delete_batch(sqs_client):
    max_retries = 10

    # Send messages to queue.
    messages_to_send = list(range(0, 50))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=TEST_SOURCE_QUEUE)
    queue_url = deafrica_conflux.queues.get_queue_url(TEST_SOURCE_QUEUE)

    # Push messages to queue.
    deafrica_conflux.queues.send_batch(
        queue_url=queue_url, messages=messages_to_send, sqs_client=sqs_client
    )

    # Receive messages from queue.
    received_messages = deafrica_conflux.queues.receive_batch(
        queue_url=queue_url, max_retries=max_retries, visibility_timeout=3600, sqs_client=sqs_client
    )
    receipt_handles = [msg["ReceiptHandle"] for msg in received_messages]

    # Delete messages.
    try:
        deafrica_conflux.queues.delete_batch(
            queue_url=queue_url,
            receipt_handles=receipt_handles,
            max_retries=max_retries,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        received_messages = deafrica_conflux.queues.receive_batch(
            queue_url=queue_url,
            max_retries=max_retries,
            visibility_timeout=3600,
            sqs_client=sqs_client,
        )
        assert received_messages == []
