"""Queue functions.

Matthew Alger, Alex Leith
Geoscience Australia
2021
"""

import boto3
import click


def get_queue(queue_name: str):
    """
    Return a queue resource by name, e.g., alex-really-secret-queue

    Cribbed from odc.algo.
    """
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue


def verify_name(name):
    if (not name.startswith("waterbodies_")) and (not name.startswith("wit_")):
        raise click.ClickException(
            "DE Africa conflux queues must start with waterbodies_ or wit_"
        )


def move_to_deadletter_queue(dl_queue_name, message_body):
    verify_name(dl_queue_name)

    dl_queue = get_queue(dl_queue_name)

    # only send one message, so 1 is OK as the identifier for a message in this
    # batch used to communicate the result.
    dl_queue.send_messages(Entries=[{"Id": "1", "MessageBody": str(message_body)}])
