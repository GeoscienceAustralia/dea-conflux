import click
import logging
from .common import main, logging_setup

import deafrica_conflux.queues


@main.command("push-to-queue", no_args_is_help=True)
@click.option(
    "--txt",
    type=click.Path(),
    required=True,
    help="REQUIRED. Path to TXT file to push to queue.",
)
@click.option("--queue", required=True, help="REQUIRED. Queue name to push to.")
@click.option("-v", "--verbose", count=True)
def push_to_queue(txt, queue, verbose):
    """
    Push lines of a text file to a SQS queue.
    """
    # Cribbed from datacube-alchemist
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    alive_queue = deafrica_conflux.queues.get_queue(queue)

    def post_messages(messages, count):
        alive_queue.send_messages(Entries=messages)
        _log.info(f"Added {count} messages...")
        return []

    count = 0
    messages = []
    _log.info("Adding messages...")
    with open(txt) as file:
        ids = [line.strip() for line in file]
    _log.debug(f"Adding IDs {ids}")
    for id_ in ids:
        message = {
            "Id": str(count),
            "MessageBody": str(id_),
        }
        messages.append(message)

        count += 1
        if count % 10 == 0:
            messages = post_messages(messages, count)

    # Post the last messages if there are any
    if len(messages) > 0:
        post_messages(messages, count)
