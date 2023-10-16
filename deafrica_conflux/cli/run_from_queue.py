import logging
from importlib import import_module

import boto3
import click
import datacube
import geopandas as gpd
from rasterio.errors import RasterioIOError

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.db import get_engine_waterbodies
from deafrica_conflux.drill import drill
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.io import table_exists, write_table_to_parquet
from deafrica_conflux.plugins.utils import run_plugin, validate_plugin
from deafrica_conflux.queues import (
    delete_batch_with_retry,
    get_queue_url,
    move_to_dead_letter_queue,
    receive_a_message,
)
from deafrica_conflux.stack import stack_waterbodies_parquet_to_db


@click.command(
    "run-from-sqs-queue",
    no_args_is_help=True,
    help="Run deafrica-conflux on dataset ids from an SQS queue.",
)
@click.option(
    "--plugin-name",
    type=str,
    help="Name of the plugin. Plugin file must be in the deafrica_conflux/plugins/ directory.",
)
@click.option(
    "--dataset-ids-queue",
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
    "--visibility-timeout",
    default=18 * 60,
    help="The duration in seconds that a received SQS msg is invisible.",
)
@click.option(
    "--max-retries",
    default=10,
    help="Maximum number of times to retry sending/receiving messages to/from a SQS queue.",
)
@click.option("--db/--no-db", default=True, help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_sqs_queue(
    plugin_name,
    dataset_ids_queue,
    polygons_vector_file,
    use_id,
    output_directory,
    partial,
    overwrite,
    overedge,
    verbose,
    visibility_timeout,
    max_retries,
    db,
    dump_empty_dataframe,
):
    """
    Run deafrica-conflux on dataset ids from an SQS queue.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Read the plugin as a Python module.
    module = import_module(f"deafrica_conflux.plugins.{plugin_name}")
    plugin_file = module.__file__
    plugin = run_plugin(plugin_file)
    _log.info(f"Using plugin {plugin_file}")
    validate_plugin(plugin)

    # Get the product name from the plugin.
    product_name = plugin.product_name

    # Read the polygons vector file.
    try:
        polygons_gdf = gpd.read_file(polygons_vector_file)
    except Exception as error:
        _log.exception(f"Could not read file {polygons_vector_file}")
        raise error
    else:
        # Guess the ID field.
        id_field = guess_id_field(polygons_gdf, use_id)
        _log.info(f"Guessed ID field: {id_field}")

        # Set the ID field as the index.
        polygons_gdf.set_index(id_field, inplace=True)

    # Create the service client.
    sqs_client = boto3.client("sqs")

    dataset_ids_queue_url = get_queue_url(queue_name=dataset_ids_queue, sqs_client=sqs_client)
    # Get the dead-letter queue.
    dead_letter_queue_name = f"{dataset_ids_queue}-deadletter"
    dead_letter_queue_url = get_queue_url(queue_name=dead_letter_queue_name, sqs_client=sqs_client)

    if db:
        engine = get_engine_waterbodies()

    dc = datacube.Datacube(app="deafrica-conflux-drill")

    retries = 0
    while retries <= max_retries:
        # Retrieve a single message from the dataset_ids_queue.
        message = receive_a_message(
            queue_url=dataset_ids_queue_url,
            max_retries=max_retries,
            visibility_timeout=visibility_timeout,
            sqs_client=sqs_client,
        )
        if message is None:
            retries += 1
        else:
            retries = 0  # reset the count

            # Process the ID.
            dataset_id = message["Body"]
            _log.info(f"Read dataset id {dataset_id} from queue {dataset_ids_queue_url}")

            entry_to_delete = [
                {"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
            ]

            # Produce the parquet file.
            success_flag = True
            centre_date = dc.index.datasets.get(dataset_id).center_time

            if not overwrite:
                _log.info(f"Checking existence of {dataset_id}")
                exists = table_exists(
                    drill_name=product_name,
                    uuid=dataset_id,
                    centre_date=centre_date,
                    output_directory=output_directory,
                )

            if overwrite or not exists:
                try:
                    table = drill(
                        plugin=plugin,
                        polygons_gdf=polygons_gdf,
                        scene_uuid=dataset_id,
                        partial=partial,
                        overedge=overedge,
                        dc=dc,
                    )

                    # if always dump drill result, or drill result is not empty,
                    # dump that dataframe as PQ file
                    if (dump_empty_dataframe) or (not table.empty):
                        pq_filename = write_table_to_parquet(
                            drill_name=product_name,
                            uuid=dataset_id,
                            centre_date=centre_date,
                            table=table,
                            output_directory=output_directory,
                        )
                        if db:
                            _log.info(f"Writing {pq_filename} to DB")
                            stack_waterbodies_parquet_to_db(
                                parquet_file_paths=[pq_filename],
                                verbose=verbose,
                                engine=engine,
                                drop=False,
                            )
                except KeyError as keyerr:
                    _log.exception(f"Found {dataset_id} has KeyError: {str(keyerr)}")
                    _log.error(f"Moving {dataset_id} to deadletter queue {dead_letter_queue_url}")
                    move_to_dead_letter_queue(
                        dead_letter_queue_url=dead_letter_queue_url,
                        message_body=dataset_id,
                        sqs_client=sqs_client,
                    )
                    success_flag = False
                except TypeError as typeerr:
                    _log.exception(f"Found {dataset_id} has TypeError: {str(typeerr)}")
                    _log.error(f"Moving {dataset_id} to deadletter queue {dead_letter_queue_url}")
                    move_to_dead_letter_queue(
                        dead_letter_queue_url=dead_letter_queue_url,
                        message_body=dataset_id,
                        sqs_client=sqs_client,
                    )
                    success_flag = False
                except RasterioIOError as ioerror:
                    _log.exception(f"Found {dataset_id} has RasterioIOError: {str(ioerror)}")
                    _log.error(f"Moving {dataset_id} to deadletter queue {dead_letter_queue_url}")
                    move_to_dead_letter_queue(
                        dead_letter_queue_url=dead_letter_queue_url,
                        message_body=dataset_id,
                        sqs_client=sqs_client,
                    )
                    success_flag = False
            else:
                _log.info(f"{dataset_id} already exists, skipping")

            # Delete datased id from queue.
            if success_flag:
                _log.info(f"Successful, deleting {dataset_id}")
                (
                    successfully_deleted,
                    failed_to_delete,
                ) = delete_batch_with_retry(
                    queue_url=dataset_ids_queue_url,
                    entries=entry_to_delete,
                    max_retries=max_retries,
                    sqs_client=sqs_client,
                )
                if failed_to_delete:
                    _log.error(
                        f"Failed to delete dataset id {dataset_id} from queue {dataset_ids_queue_url}"
                    )
                    raise RuntimeError(f"Failed to delete dataset id: {dataset_id}")
                else:
                    _log.info(f"Deleted dataset id {dataset_id} from queue")

            else:
                _log.info(
                    f"Not successful, moved {dataset_id} to dead letter queue {dead_letter_queue_url}"
                )
