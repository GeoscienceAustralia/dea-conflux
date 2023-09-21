import json
import os

import boto3
import click
import pytest
from moto import mock_sqs

import deafrica_conflux.queues


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def sqs_client(aws_credentials):
    return boto3.client("sqs", region_name="af-south-1")


@mock_sqs
def test_get_queue_url_with_existing_queue(sqs_client):
    queue_name = "test_queue"
    # Create the queue.
    sqs_client.create_queue(QueueName=queue_name)
    # Get queue url.
    try:
        queue_url = deafrica_conflux.queues.get_queue_url(
            queue_name=queue_name, sqs_client=sqs_client
        )
    except Exception:
        assert False
    else:
        assert queue_name in queue_url


@mock_sqs
def test_get_queue_url_with_not_existing_queue(sqs_client):
    queue_name = "test_queue"
    # Get queue url.
    try:
        queue_url = deafrica_conflux.queues.get_queue_url(  # noqa F841
            queue_name=queue_name, sqs_client=sqs_client
        )
    except sqs_client.exceptions.QueueDoesNotExist:
        assert True
    else:
        assert False


@mock_sqs
def test_get_queue_attribute(sqs_client):
    # Create the queue.
    queue_name = "test_queue"
    queue_attributes = {"VisibilityTimeout": "43200"}
    response = sqs_client.create_queue(  # noqa F841
        QueueName=queue_name, Attributes=queue_attributes
    )
    # Get attribute from the queue.
    try:
        attribute = deafrica_conflux.queues.get_queue_attribute(
            queue_name=queue_name, attribute_name="VisibilityTimeout", sqs_client=sqs_client
        )
    except Exception:
        assert False
    else:
        assert attribute == "43200"


def test_verify_queue_name_with_correct_queue_name():
    queue_name = "waterbodies_test_queue"
    try:
        deafrica_conflux.queues.verify_queue_name(queue_name=queue_name)
    except click.ClickException:
        assert False
    else:
        assert True


def test_verify_queue_name_with_incorrect_queue_name():
    queue_name = "test_queue"
    try:
        deafrica_conflux.queues.verify_queue_name(queue_name=queue_name)
    except click.ClickException:
        assert True
    else:
        assert False


@mock_sqs
def test_make_source_queue_with_existing_deadletter_queue(aws_credentials):
    sqs_client = boto3.client("sqs", region_name="af-south-1")

    source_queue_name = "waterbodies_conflux_test_queue"
    deadletter_queue_name = "waterbodies_conflux_test_queue_deadletter"

    # Create the deadletter queue.
    sqs_client.create_queue(QueueName=deadletter_queue_name)

    try:
        deafrica_conflux.queues.make_source_queue(
            queue_name=source_queue_name,
            dead_letter_queue_name=deadletter_queue_name,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        source_queue_url = deafrica_conflux.queues.get_queue_url(
            queue_name=source_queue_name, sqs_client=sqs_client
        )
        redrive_policy_attribute = deafrica_conflux.queues.get_queue_attribute(
            queue_name=source_queue_name, attribute_name="RedrivePolicy", sqs_client=sqs_client
        )
        redrive_policy_attribute = json.loads(redrive_policy_attribute)

        assert deadletter_queue_name in redrive_policy_attribute["deadLetterTargetArn"]
        assert source_queue_name in source_queue_url


@mock_sqs
def test_make_source_queue_with_no_existing_deadletter_queue(sqs_client):
    source_queue_name = "waterbodies_conflux_test_queue"
    deadletter_queue_name = "waterbodies_conflux_test_queue_deadletter"

    try:
        deafrica_conflux.queues.make_source_queue(
            queue_name=source_queue_name,
            dead_letter_queue_name=deadletter_queue_name,
            sqs_client=sqs_client,
        )
    except Exception:
        assert False
    else:
        source_queue_url = deafrica_conflux.queues.get_queue_url(
            queue_name=source_queue_name, sqs_client=sqs_client
        )
        deadletter_queue_url = deafrica_conflux.queues.get_queue_url(
            queue_name=deadletter_queue_name, sqs_client=sqs_client
        )

        assert source_queue_name in source_queue_url
        assert deadletter_queue_name in deadletter_queue_url


@mock_sqs
def test_delete_empty_sqs_queue(sqs_client):
    # Create queue.
    queue = "waterbodies_conflux_test_delete_queue"
    sqs_client.create_queue(QueueName=queue)

    # Delete queue.
    try:
        deafrica_conflux.queues.delete_queue(queue_name=queue, sqs_client=sqs_client)
    except Exception:
        assert False
    else:
        try:
            queue_url = deafrica_conflux.queues.get_queue_url(  # noqa F841
                queue_name=queue, sqs_client=sqs_client
            )
        except sqs_client.exceptions.QueueDoesNotExist:
            assert True
        else:
            assert False


@mock_sqs
def test_move_to_deadletter_queue(sqs_client):
    deadletter_queue_name = "waterbodies_test_queue_deadletter"
    message_body = "Test move to deadletter queue"

    # Create the deadletter queue.
    sqs_client.create_queue(QueueName=deadletter_queue_name)
    deadletter_queue_url = deafrica_conflux.queues.get_queue_url(deadletter_queue_name)

    try:
        deafrica_conflux.queues.move_to_deadletter_queue(
            deadletter_queue_name=deadletter_queue_name,
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
    queue_name = "waterbodies_conflux_test"

    # Create messages to send.
    messages_to_send = list(range(0, 100))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=queue_name)
    queue_url = deafrica_conflux.queues.get_queue_url(queue_name=queue_name)

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
    queue_name = "waterbodies_conflux_test"
    max_retries = 10

    # Create messages to send.
    messages_to_send = list(range(0, 97))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=queue_name)
    queue_url = deafrica_conflux.queues.get_queue_url(queue_name)

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
    queue_name = "waterbodies_conflux_test"
    max_retries = 10
    visibility_timeout = 3600

    # Create messages to send.
    messages_to_send = list(range(1, 23))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=queue_name)
    queue_url = deafrica_conflux.queues.get_queue_url(queue_name)

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
    queue_name = "waterbodies_conflux_test"
    text_file_path = "test_push_to_queue_from_txt.txt"
    max_retries = 10
    # Write messages to text file.
    messages_to_send = list(range(0, 43))
    messages_to_send = [str(i) for i in messages_to_send]

    with open(text_file_path, "w") as f:
        for msg in messages_to_send:
            f.write(f"{msg}\n")

    # Create queue.
    sqs_client.create_queue(QueueName=queue_name)
    queue_url = deafrica_conflux.queues.get_queue_url(queue_name)

    # Push to queue from text file.
    try:
        deafrica_conflux.queues.push_to_queue_from_txt(
            text_file_path=text_file_path,
            queue_name=queue_name,
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
        os.remove(text_file_path)


@mock_sqs
def test_delete_batch(sqs_client):
    queue_name = "waterbodies_conflux_test"
    max_retries = 10

    # Send messages to queue.
    messages_to_send = list(range(0, 50))
    messages_to_send = [str(i) for i in messages_to_send]

    # Create queue.
    sqs_client.create_queue(QueueName=queue_name)
    queue_url = deafrica_conflux.queues.get_queue_url(queue_name)

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
