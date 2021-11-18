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
    if not name.startswith("waterbodies_"):
        raise click.ClickException("Waterbodies queues must start with waterbodies_")


def move_to_deadletter_queue(dl_queue_name, message_body):
    verify_name(dl_queue_name)

    dl_queue = get_queue(dl_queue_name)

    dl_queue.send_messages(Entries=[{"MessageBody": str(message_body)}])
