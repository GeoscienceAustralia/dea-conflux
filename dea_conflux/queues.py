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
    if not name.startswith('waterbodies_'):
        raise click.ClickException(
            'Waterbodies queues must start with waterbodies_')
