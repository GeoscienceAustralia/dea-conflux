import logging

import boto3
import click
import datacube
import geopandas as gpd
from rasterio.errors import RasterioIOError

import deafrica_conflux.db
import deafrica_conflux.drill
import deafrica_conflux.id_field
import deafrica_conflux.io
import deafrica_conflux.queues
import deafrica_conflux.stack
from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.plugins.utils import run_plugin, validate_plugin


@click.command(
    "run-from-sqs-queue",
    no_args_is_help=True,
    help="Run deafrica-conflux on dataset ids from an SQS queue.",
)
@click.option(
    "--plugin",
    "-p",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to Conflux plugin (.py).",
)
@click.option(
    "-dataset-ids-queue",
    type=str,
    help="SQS Queue to read dataset IDs from to run deafrica-conflux on.",
)
@click.option(
    "--polygons-vector-file",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="Path to the vector file defining the polygon(s) to run polygon drill on.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in polygons vector file.",
)
@click.option(
    "--output-directory",
    "-o",
    type=click.Path(),
    default=None,
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. File URI or S3 URI to the output directory.",
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
    "--timeout",
    default=18 * 60,
    help="The duration in seconds that a received SQS msg is invisible.",
)
@click.option("--db/--no-db", default=True, help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_sqs_queue(
    plugin_file,
    dataset_ids_queue,
    polygons_vector_file,
    use_id,
    output_directory,
    partial,
    overwrite,
    overedge,
    verbose,
    timeout,
    db,
    dump_empty_dataframe,
):
    """
    Run deafrica-conflux on dataset ids from an SQS queue.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin_file)
    _log.info(f"Using plugin {plugin.__file__}")
    validate_plugin(plugin)

    # Get the product name from the plugin.
    product_name = plugin.product_name

    # Read the polygons vector file.
    try:
        polygons_gdf = gpd.read_file(polygons_vector_file)
    except Exception as error:
        _log.exception(f"Could not read file {polygons_vector_file}")
        raise error

    # Guess the ID field.
    id_field = deafrica_conflux.id_field.guess_id_field(polygons_gdf, use_id)
    _log.info(f"Guessed ID field: {id_field}")

    # Set the ID field as the index.
    polygons_gdf.set_index(id_field, inplace=True)

    dead_letter_queue_name = dataset_ids_queue + "_deadletter"

    # Create the service client.
    sqs_client = boto3.client("sqs")
    # Read ID/s from the queue.
    dataset_ids_queue_url = deafrica_conflux.queues.get_queue_url(
        queue_name=dataset_ids_queue, sqs_client=sqs_client
    )

    if db:
        engine = deafrica_conflux.db.get_engine_waterbodies()

    dc = datacube.Datacube(app="deafrica-conflux-drill")

    message_retries = 10
    while message_retries > 0:
        response = sqs_client.receive_message(
            QueueUrl=dataset_ids_queue_url,
            AttributeNames=["All"],
            MaxNumberOfMessages=1,
            VisibilityTimeout=timeout,
        )
        messages = response["Messages"]

        if len(messages) == 0:
            _log.info("No messages received from queue")
            message_retries -= 1
            continue
        else:
            message_retries = 10

        # Process each ID.
        dataset_ids = [msg["Body"] for msg in messages]
        _log.info(f"Read {dataset_ids} from queue")

        entries = [
            {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]} for msg in messages
        ]

        # Loop through the scenes to produce parquet files.
        for i, (entry, id_) in enumerate(zip(entries, dataset_ids)):
            success_flag = True

            _log.info(f"Processing {id_} ({i + 1}/{len(dataset_ids)})")

            centre_date = dc.index.datasets.get(id_).center_time

            if not overwrite:
                _log.info(f"Checking existence of {id_}")
                exists = deafrica_conflux.io.table_exists(
                    product_name, id_, centre_date, output_directory
                )

            if overwrite or not exists:
                try:
                    table = deafrica_conflux.drill.drill(
                        plugin,
                        polygons_gdf,
                        id_,
                        partial=partial,
                        overedge=overedge,
                        dc=dc,
                    )

                    # if always dump drill result, or drill result is not empty,
                    # dump that dataframe as PQ file
                    if (dump_empty_dataframe) or (not table.empty):
                        pq_filename = deafrica_conflux.io.write_table_to_parquet(
                            product_name, id_, centre_date, table, output_directory
                        )
                        if db:
                            _log.debug(f"Writing {pq_filename} to DB")
                            deafrica_conflux.stack.stack_waterbodies_parquet_to_db(
                                parquet_file_paths=[pq_filename],
                                verbose=verbose,
                                engine=engine,
                                drop=False,
                            )
                except KeyError as keyerr:
                    _log.exception(f"Found {id_} has KeyError: {str(keyerr)}")
                    deafrica_conflux.queues.move_to_deadletter_queue(
                        dead_letter_queue_name, id_, sqs_client
                    )
                    success_flag = False
                except TypeError as typeerr:
                    _log.exception(f"Found {id_} has TypeError: {str(typeerr)}")
                    deafrica_conflux.queues.move_to_deadletter_queue(
                        dead_letter_queue_name, id_, sqs_client
                    )
                    success_flag = False
                except RasterioIOError as ioerror:
                    _log.exception(f"Found {id_} has RasterioIOError: {str(ioerror)}")
                    deafrica_conflux.queues.move_to_deadletter_queue(
                        dead_letter_queue_name, id_, sqs_client
                    )
                    success_flag = False
            else:
                _log.info(f"{id_} already exists, skipping")

            # Delete from queue.
            if success_flag:
                _log.info(f"Successful, deleting {id_}")
                response = sqs_client.delete_message_batch(
                    QueueUrl=dataset_ids_queue_url, Entries=[entry]
                )
                if len(response["Successful"]) != 1:
                    _log.error(f"Failed to delete message: {entry}")
                    raise RuntimeError(f"Failed to delete message: {entry}")
            else:
                _log.info(
                    f"Not successful, moved {id_} to dead letter queue {dead_letter_queue_name}"
                )
