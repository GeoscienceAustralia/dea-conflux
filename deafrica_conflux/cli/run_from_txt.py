import os
import logging

import click
import fsspec
import datacube
import geopandas as gpd
from rasterio.errors import RasterioIOError

import deafrica_conflux.db
import deafrica_conflux.io
import deafrica_conflux.stack
import deafrica_conflux.drill
import deafrica_conflux.id_field
from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.plugins.utils import run_plugin, validate_plugin


@click.command("run-from-txt",
               no_args_is_help=True,
               help="Run deafrica-conflux on dataset ids from a text file.")
@click.option(
    "--plugin-file",
    "-p",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to Conflux plugin (.py).",
)
@click.option(
    "--dataset-ids-file",
    type=click.Path(),
    help="Text file to read dataset IDs from to run deafrica-conflux on."
)
@click.option(
    "--polygons-vector-file",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="Path to the vector file defining the polygon(s) to run polygon drill on."
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
@click.option("--db/--no-db",
              default=False,
              help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_txt(
    plugin_file,
    dataset_ids_file,
    polygons_vector_file,
    use_id,
    output_directory,
    partial,
    overwrite,
    overedge,
    verbose,
    db,
    dump_empty_dataframe,
):
    """
    Run deafrica-conflux on dataset ids from a text file.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin_file)
    _log.info(f"Using plugin {plugin.__file__}")
    validate_plugin(plugin)
    
    # Get the product name from the plugin.
    product_name = plugin.product_name

    try:
        polygons_gdf = gpd.read_file(polygons_vector_file)
    except Exception as error:
        _log.exception(f"Could not read file {polygons_vector_file}")
        raise error

    # Guess the ID field.
    id_field = deafrica_conflux.id_field.guess_id_field(polygons_gdf, use_id)
    _log.debug(f"Guessed ID field: {id_field}")

    # Set the ID field as the index.
    polygons_gdf.set_index(id_field, inplace=True)

    # Read dataset ids.
    # Check if the file exists.
    # Check if file is an s3 file.
    is_s3_file = deafrica_conflux.io.check_if_s3_uri(dataset_ids_file)

    if is_s3_file:
        deafrica_conflux.io.check_s3_object_exists(dataset_ids_file, error_if_exists=False)
    else:
        deafrica_conflux.io.check_local_file_exists(dataset_ids_file, error_if_exists=False)

    # Read ID/s from the S3 URI or File URI.
    with fsspec.open(dataset_ids_file, "rb") as file:
        dataset_ids = [line.decode().strip() for line in file]
    _log.info(f"Read {dataset_ids} from file.")
   
    if db:
        engine = deafrica_conflux.db.get_engine_waterbodies()

    dc = datacube.Datacube(app="deafrica-conflux-drill")
    
    # Process each ID.
    # Loop through the scenes to produce parquet files.
    failed_dataset_ids = []
    for i, id_ in enumerate(dataset_ids):
        success_flag = True

        _log.info(f"Processing {id_} ({i + 1}/{len(dataset_ids)})")

        centre_date = dc.index.datasets.get(id_).center_time

        if not overwrite:
            _log.info(f"Checking existence of {id_}")
            exists = deafrica_conflux.io.table_exists(product_name,
                                                      id_,
                                                      centre_date,
                                                      output_directory)

        if overwrite or not exists:
            try:
                table = deafrica_conflux.drill.drill(plugin,
                                                     polygons_gdf,
                                                     id_,
                                                     partial=partial,
                                                     overedge=overedge,
                                                     dc=dc,)

                # if always dump drill result, or drill result is not empty,
                # dump that dataframe as PQ file
                if (dump_empty_dataframe) or (not table.empty):
                    pq_filename = deafrica_conflux.io.write_table(
                        product_name,
                        id_,
                        centre_date,
                        table,
                        output_directory,)
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
                failed_dataset_ids.append(id_)
                success_flag = False
            except TypeError as typeerr:
                _log.exception(f"Found {id_} has TypeError: {str(typeerr)}")
                failed_dataset_ids.append(id_)
                success_flag = False
            except RasterioIOError as ioerror:
                _log.exception(f"Found {id_} has RasterioIOError: {str(ioerror)}")
                failed_dataset_ids.append(id_)
                success_flag = False
        else:
            _log.info(f"{id_} already exists, skipping")

        if success_flag:
            _log.info(f"{id_} successful")
        else:
            _log.error(f"{id_} not successful")

    # Write the failed dataset ids to a text file.
    parent_folder, file_name = os.path.split(dataset_ids_file)
    file, file_extension = os.path.splitext(file_name)
    failed_datasets_text_file = os.path.join(parent_folder, file + "_failed_dataset_ids" + file_extension)

    with fsspec.open(failed_datasets_text_file, "a") as file:
        for dataset_id in failed_dataset_ids:
            file.write("%s\n" % dataset_id)

    _log.info(f"Failed dataset IDs {failed_dataset_ids} written to: {failed_datasets_text_file}.")
    
    return 0
