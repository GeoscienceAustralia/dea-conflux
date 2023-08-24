import click
import boto3

from .common import main

import deafrica_conflux.queues


@main.command("delete-queue", no_args_is_help=True)
@click.argument("name")
def delete_queue(name):
    """
    Delete a queue.
    """
    
    deafrica_conflux.queues.verify_name(name)

    sqs = boto3.resource("sqs")

    queue = sqs.get_queue_by_name(QueueName=name)
    arn = queue.attributes["QueueArn"]
    queue.delete()

    deadletter = name + "_deadletter"
    dl_queue = sqs.get_queue_by_name(QueueName=deadletter)
    dl_arn = dl_queue.attributes["QueueArn"]

    # check deadletter is empty or not
    # if empty, delete it
    response = dl_queue.receive_messages(
        AttributeNames=["All"],
        MaxNumberOfMessages=1,
    )

    if len(response) == 0:
        dl_queue.delete()
        arn = ",".join([arn, dl_arn])

    return arn
