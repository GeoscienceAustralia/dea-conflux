import click
import boto3
import json
from botocore.config import Config

from ._cli_common import main

import deafrica_conflux.queues


@main.command("make-queue", no_args_is_help=True)
@click.argument("name")
@click.option(
    "--timeout", type=int, help="Visibility timeout in seconds", default=18 * 60
)
@click.option(
    "--retention-period",
    type=int,
    help="The length of time, in seconds before retains a message.",
    default=7 * 24 * 3600,
)
@click.option("--retries", type=int, help="Number of retries", default=5)
def make(name, timeout, retries, retention_period):
    """
    Make a queue.
    """

    deafrica_conflux.queues.verify_name(name)

    deadletter = name + "_deadletter"

    sqs_client = boto3.client(
        "sqs",
        config=Config(
            retries={
                "max_attempts": retries,
            }
        ),
    )

    # create deadletter queue
    dl_queue_response = sqs_client.create_queue(QueueName=deadletter)

    # Get ARN from deadletter queue name.
    dl_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dl_queue_response["QueueUrl"], AttributeNames=["All"]
    )

    # create the queue attributes form
    attributes = dict(VisibilityTimeout=str(timeout))
    attributes["RedrivePolicy"] = json.dumps(
        {
            "deadLetterTargetArn": dl_attrs["Attributes"]["QueueArn"],
            "maxReceiveCount": 10,
        }
    )

    attributes["MessageRetentionPeriod"] = str(retention_period)

    queue = sqs_client.create_queue(QueueName=name, Attributes=attributes)

    assert queue
    return 0
