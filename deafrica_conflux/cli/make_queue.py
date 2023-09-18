import click

import deafrica_conflux.queues


@click.command("make-sqs-queue", no_args_is_help=True)
@click.option("queue-name", help="Name of the SQS queue to create.")
@click.option("--timeout", type=int, help="Visibility timeout in seconds", default=18 * 60)
@click.option(
    "--retention-period",
    type=int,
    help="The length of time, in seconds, for which the queue S retains a message.",
    default=7 * 24 * 3600,
)
@click.option("--retries", type=int, help="Number of retries", default=5)
def make_sqs_queue(queue_name, timeout, retention_period, retries):
    """
    Make an SQS queue.
    """

    # Verify queue name.
    deafrica_conflux.queues.verify_queue_name(queue_name)

    # Verify dead-letter queue name.
    dead_letter_queue_name = queue_name + "_deadletter"
    deafrica_conflux.queues.verify_queue_name(dead_letter_queue_name)

    deafrica_conflux.queues.make_source_queue(
        queue_name=queue_name,
        dead_letter_queue_name=dead_letter_queue_name,
        timeout=timeout,
        retries=retries,
        retention_period=retention_period,
    )
