import click
import boto3
import datacube
import logging
from rasterio.errors import RasterioIOError

from ._cli_common import main, logging_setup
from ._vector_file_utils import get_crs, guess_id_field, load_and_reproject_shapefile
from .plugins.utils import run_plugin, validate_plugin

import deafrica_conflux.db
import deafrica_conflux.io
import deafrica_conflux.stack
import deafrica_conflux.drill

import deafrica_conflux.queues


@main.command("run-from-queue", no_args_is_help=True)
@click.option(
    "--plugin",
    "-p",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to Conflux plugin (.py).",
)
@click.option("--queue", "-q", help="Queue to read IDs from.")
@click.option(
    "--shapefile",
    "-s",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the polygon " "shapefile to run polygon drill on.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in shapefile.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the output directory.",
)
@click.option(
    "--partial/--no-partial",
    default=True,
    help="Include polygons that only partially intersect the scene.",
)
@click.option(
    "--overedge/--no-overedge",
    default=True,
    help="Include data from over the scene boundary.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Rerun scenes that have already been processed.",
)
@click.option("-v", "--verbose", count=True)
@click.option(
    "--timeout", default=18 * 60, help="The seconds of a received SQS msg is invisible."
)
@click.option("--db/--no-db", default=True, help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_queue(
    plugin,
    queue,
    shapefile,
    use_id,
    output,
    partial,
    overwrite,
    overedge,
    verbose,
    timeout,
    db,
    dump_empty_dataframe,
):
    """
    Run deafrica-conflux on a scene from a queue.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # TODO(MatthewJA): Refactor this to combine with run-one.
    # TODO(MatthewJA): Generalise the database to not just Waterbodies.
    # Maybe this is really easy? It's all done by env vars,
    # so perhaps a documentation/naming change is all we need.

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin)
    _log.info(f"Using plugin {plugin.__file__}")
    validate_plugin(plugin)

    # Get the CRS from the shapefile if one isn't specified.
    if hasattr(plugin, "output_crs"):
        crs = plugin.output_crs
    else:
        crs = get_crs(shapefile)
    _log.debug(f"Found CRS: {crs}")

    # Get the output resolution from the plugin.
    # TODO(MatthewJA): Make this optional by guessing
    # the resolution, if at all possible.
    # I think this is doable provided that everything
    # is in native CRS.
    resolution = plugin.resolution

    # Guess the ID field.
    id_field = guess_id_field(shapefile, use_id)
    _log.debug(f"Guessed ID field: {id_field}")

    # Load and reproject the shapefile.
    shapefile = load_and_reproject_shapefile(
        shapefile,
        id_field,
        crs,
    )

    dl_queue_name = queue + "_deadletter"

    # Read ID/s from the queue.
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue)
    queue_url = queue.url

    if db:
        engine = deafrica_conflux.db.get_engine_waterbodies()

    dc = datacube.Datacube(app="deafrica-conflux-drill")
    message_retries = 10
    while message_retries > 0:
        response = queue.receive_messages(
            AttributeNames=["All"],
            MaxNumberOfMessages=1,
            VisibilityTimeout=timeout,
        )

        messages = response

        if len(messages) == 0:
            _log.info("No messages received from queue")
            message_retries -= 1
            continue

        message_retries = 10

        entries = [
            {"Id": msg.message_id, "ReceiptHandle": msg.receipt_handle}
            for msg in messages
        ]

        # Process each ID.
        ids = [e.body for e in messages]
        _log.info(f"Read {ids} from queue")

        # Loop through the scenes to produce parquet files.
        for i, (entry, id_) in enumerate(zip(entries, ids)):

            success_flag = True

            _log.info(f"Processing {id_} ({i + 1}/{len(ids)})")

            centre_date = dc.index.datasets.get(id_).center_time

            if not overwrite:
                _log.info(f"Checking existence of {id_}")
                exists = deafrica_conflux.io.table_exists(
                    plugin.product_name, id_, centre_date, output
                )

            # NameError should be impossible thanks to short-circuiting
            if overwrite or not exists:
                try:
                    table = deafrica_conflux.drill.drill(
                        plugin,
                        shapefile,
                        id_,
                        crs,
                        resolution,
                        partial=partial,
                        overedge=overedge,
                        dc=dc,
                    )

                    # if always dump drill result, or drill result is not empty,
                    # dump that dataframe as PQ file
                    if (dump_empty_dataframe) or (not table.empty):
                        pq_filename = deafrica_conflux.io.write_table(
                            plugin.product_name, id_, centre_date, table, output
                        )
                        if db:
                            _log.debug(f"Writing {pq_filename} to DB")
                            deafrica_conflux.stack.stack_waterbodies_db(
                                paths=[pq_filename],
                                verbose=verbose,
                                engine=engine,
                                drop=False,
                            )
                except KeyError as keyerr:
                    _log.error(f"Found {id_} has KeyError: {str(keyerr)}")
                    deafrica_conflux.queues.move_to_deadletter_queue(dl_queue_name, id_)
                    success_flag = False
                except TypeError as typeerr:
                    _log.error(f"Found {id_} has TypeError: {str(typeerr)}")
                    deafrica_conflux.queues.move_to_deadletter_queue(dl_queue_name, id_)
                    success_flag = False
                except RasterioIOError as ioerror:
                    _log.error(f"Found {id_} has RasterioIOError: {str(ioerror)}")
                    deafrica_conflux.queues.move_to_deadletter_queue(dl_queue_name, id_)
                    success_flag = False
            else:
                _log.info(f"{id_} already exists, skipping")

            # Delete from queue.
            if success_flag:
                _log.info(f"Successful, deleting {id_}")
            else:
                _log.info(f"Not successful, moved {id_} to DLQ")

            resp = queue.delete_messages(
                QueueUrl=queue_url,
                Entries=[entry],
            )

            if len(resp["Successful"]) != 1:
                raise RuntimeError(f"Failed to delete message: {entry}")

    return 0
