import logging
import os
from importlib import import_module

import click
import datacube
import fsspec
import geopandas as gpd
from rasterio.errors import RasterioIOError

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.db import get_engine_waterbodies
from deafrica_conflux.drill import drill
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.io import (
    check_file_exists,
    check_if_s3_uri,
    table_exists,
    write_table_to_parquet,
)
from deafrica_conflux.plugins.utils import run_plugin, validate_plugin
from deafrica_conflux.stack import stack_waterbodies_parquet_to_db


@click.command(
    "run-from-txt",
    no_args_is_help=True,
    help="Run deafrica-conflux on dataset ids from a text file.",
)
@click.option("-v", "--verbose", count=True)
@click.option(
    "--dataset-ids-file",
    type=click.Path(),
    help="Text file to read dataset IDs from to run deafrica-conflux on.",
)
@click.option(
    "--plugin-name",
    type=str,
    help="Name of the plugin. Plugin file must be in the \
        deafrica_conflux/plugins/ directory.",
)
@click.option(
    "--polygons-directory",
    type=str,
    # Don't mandate existence since this might be s3://.
    help="Directory containing the parquet files for the polygons to perform the drill on \
    split by product regions.",
)
@click.option(
    "--use-id",
    type=str,
    default="",
    help="Optional. Unique key id in polygons vector file.",
)
@click.option(
    "--output-directory",
    type=str,
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
@click.option("--db/--no-db", default=False, help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=False,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_txt(
    verbose,
    dataset_ids_file,
    plugin_name,
    polygons_directory,
    use_id,
    output_directory,
    partial,
    overedge,
    overwrite,
    db,
    dump_empty_dataframe,
):
    """
    Run deafrica-conflux on dataset ids from a text file.
    """
    # "Support" pathlib Paths.
    dataset_ids_file = str(dataset_ids_file)
    polygons_directory = str(polygons_directory)
    output_directory = str(output_directory)

    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Read the plugin as a Python module.
    module = import_module(f"deafrica_conflux.plugins.{plugin_name}")
    plugin_file = module.__file__
    plugin = run_plugin(plugin_file)
    _log.info(f"Using plugin {plugin_file}")
    validate_plugin(plugin)

    # Check if the text file containing the dataset ids exists.
    if not check_file_exists(dataset_ids_file):
        _log.error(f"Could not find text file {dataset_ids_file}!")
        raise FileNotFoundError(f"Could not find text file {dataset_ids_file}!")
    else:
        # Read ID/s from the S3 URI or File URI.
        if check_if_s3_uri(dataset_ids_file):
            fs = fsspec.filesystem("s3")
        else:
            fs = fsspec.filesystem("file")

        with fs.open(dataset_ids_file, "r") as file:
            dataset_ids = [line.strip() for line in file]
        _log.info(f"Read {dataset_ids} from file.")

    if db:
        engine = get_engine_waterbodies()

    # Connect to the datacube.
    dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Get the product name from the plugin.
    product_name = plugin.product_name

    # Process each ID.
    # Loop through the scenes to produce parquet files.
    failed_dataset_ids = []
    for i, dataset_id in enumerate(dataset_ids):
        _log.info(f"Processing {dataset_id} ({i + 1}/{len(dataset_ids)})")

        # Load the dataset using the dataset id.
        reference_dataset = dc.index.datasets.get(dataset_id)

        # Get the region code for the dataset.
        region_code = reference_dataset.metadata.region_code

        # Get the center time for the dataset
        centre_time = reference_dataset.center_time

        # Use the center time to check if a parquet file for the dataset already
        # exists in the output directory.
        if not overwrite:
            _log.info(f"Checking existence of {dataset_id}")
            exists = table_exists(product_name, dataset_id, centre_time, output_directory)

        if overwrite or not exists:
            try:
                # Load the water body polygons for the region
                polygons_vector_file = os.path.join(polygons_directory, f"{region_code}.parquet")
                try:
                    polygons_gdf = gpd.read_parquet(polygons_vector_file)
                except Exception as error:
                    _log.exception(f"Could not read file {polygons_vector_file}")
                    raise error
                else:
                    # Guess the ID field.
                    id_field = guess_id_field(polygons_gdf, use_id)
                    _log.debug(f"Guessed ID field: {id_field}")

                    # Set the ID field as the index.
                    polygons_gdf.set_index(id_field, inplace=True)

                # Perform the polygon drill on the dataset
                table = drill(
                    plugin=plugin,
                    polygons_gdf=polygons_gdf,
                    reference_dataset=reference_dataset,
                    partial=partial,
                    overedge=overedge,
                    dc=dc,
                )

                # Write the table to a parquet file.
                if (dump_empty_dataframe) or (not table.empty):
                    pq_filename = write_table_to_parquet(
                        drill_name=product_name,
                        uuid=dataset_id,
                        centre_date=centre_time,
                        table=table,
                        output_directory=output_directory,
                    )
                    if db:
                        _log.info(f"Writing {pq_filename} to database")
                        stack_waterbodies_parquet_to_db(
                            parquet_file_paths=[pq_filename],
                            verbose=verbose,
                            engine=engine,
                            drop=False,
                        )
            except KeyError as keyerr:
                _log.exception(f"Found {dataset_id} has KeyError: {str(keyerr)}")
                failed_dataset_ids.append(dataset_id)
            except TypeError as typeerr:
                _log.exception(f"Found {dataset_id} has TypeError: {str(typeerr)}")
                failed_dataset_ids.append(dataset_id)
            except RasterioIOError as ioerror:
                _log.exception(f"Found {dataset_id} has RasterioIOError: {str(ioerror)}")
                failed_dataset_ids.append(dataset_id)
            else:
                _log.info(f"{dataset_id} successful")
        else:
            _log.info(f"{dataset_id} already exists, skipping")

        if failed_dataset_ids:
            # Write the failed dataset ids to a text file.
            parent_folder, file_name = os.path.split(dataset_ids_file)
            file, file_extension = os.path.splitext(file_name)
            failed_datasets_text_file = os.path.join(
                parent_folder, file + "_failed_dataset_ids" + file_extension
            )

            with fs.open(failed_datasets_text_file, "a") as file:
                for dataset_id in failed_dataset_ids:
                    file.write(f"{dataset_id}\n")

            _log.info(
                f"Failed dataset IDs {failed_dataset_ids} written to: {failed_datasets_text_file}."
            )
