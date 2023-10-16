import boto3
import click

from deafrica_conflux.queues import delete_queue, get_queue_attribute, get_queue_url


@click.command("delete-sqs-queue", no_args_is_help=True)
@click.option("queue-name", help="Name of the SQS Queue to delete")
def delete_sqs_queue(queue_name):
    """
    Delete a SQS queue.
    """

    sqs_client = boto3.client("sqs")

    # Get the Amazon Resource Name (ARN) of the source queue.
    source_queue_arn = get_queue_attribute(
        queue_name=queue_name, attribute_name="QueueArn", sqs_client=sqs_client
    )
    # Delete the source queue.
    delete_queue(queue_name=queue_name, sqs_client=sqs_client)

    # Get the Amazon Resource Name (ARN) of the dead-letter queue.
    dead_letter_queue_name = queue_name + "_deadletter"
    dead_letter_queue_url = get_queue_url(queue_name=dead_letter_queue_name, sqs_client=sqs_client)
    dead_letter_queue_arn = get_queue_attribute(
        queue_name=dead_letter_queue_name, attribute_name="QueueArn", sqs_client=sqs_client
    )
    # Check if the deadletter queue is empty or not.
    # if empty, delete it.
    response = sqs_client.receive_message(
        QueueUrl=dead_letter_queue_url, AttributeNames=["All"], MaxNumberOfMessages=1
    )

    messages = response["Messages"]

    if len(messages) == 0:
        delete_queue(queue_name=dead_letter_queue_name, sqs_client=sqs_client)
        arn = ",".join([source_queue_arn, dead_letter_queue_arn])

    return arn
