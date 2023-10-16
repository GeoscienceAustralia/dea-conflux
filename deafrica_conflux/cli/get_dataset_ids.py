import logging
import os
import sys
import urllib

import click
import fsspec
import geopandas as gpd
from datacube.ui import click as ui

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.drill import filter_datasets
from deafrica_conflux.hopper import find_datasets
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.io import check_file_exists, check_if_s3_uri


@click.command(
    "get-dataset-ids",
    no_args_is_help=True,
)
@click.argument("product", type=str)
@ui.parsed_search_expressions
@click.option("-v", "--verbose", count=True)
@click.option(
    "--polygons-vector-file",
    type=str,
    help="Path to the vector file defining the polygon(s) to run polygon drill on to filter datasets.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in polygons vector file.",
)
@click.option(
    "--output-file-path",
    type=click.Path(),
    help="File URI or S3 URI of the text file to write the dataset ids to.",
)
@click.option(
    "--num-worker",
    type=int,
    help="The number of processes to filter datasets.",
    default=4,
)
def get_dataset_ids(
    product,
    expressions,
    verbose,
    polygons_vector_file,
    use_id,
    output_file_path,
    num_worker,
):
    """
    Get dataset IDs based on an expression.
    """
    # Support pathlib paths.
    output_file_path = str(output_file_path)

    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dss = find_datasets(expressions, [product])

    if polygons_vector_file is not None:
        # Read the vector file.
        try:
            polygons_gdf = gpd.read_file(polygons_vector_file)
        except Exception as error:
            _log.exception(f"Could not read the file {polygons_vector_file}")
            raise error
        else:
            # Guess the ID field.
            id_field = guess_id_field(polygons_gdf, use_id)
            _log.info(f"Guessed ID field: {id_field}")

            # Set the ID field as the index.
            polygons_gdf.set_index(id_field, inplace=True)

            _log.info(f"Polygons vector file RAM usage: {sys.getsizeof(polygons_gdf)} bytes.")

            # Reprojection is done to avoid UserWarning: Geometry is in a geographic CRS.
            # when using filter_datasets when polygons are in "EPSG:4326" crs.
            polygons_gdf = polygons_gdf.to_crs("EPSG:6933")

            _log.info(
                f"Filtering out datasets that are not near the polygons in {polygons_vector_file}"
            )
            dataset_ids = filter_datasets(dss, polygons_gdf, worker_num=num_worker)
    else:
        dataset_ids = [str(ds.id) for ds in dss]

    _log.info(f"Found {len(dataset_ids)} datasets.")

    # Check if the output file exists.
    if check_file_exists(output_file_path):
        _log.error(f"{output_file_path} exists!")
        raise FileExistsError(f"{output_file_path} exists!")
    else:
        if check_if_s3_uri(output_file_path):
            fs = fsspec.filesystem("s3")
            _log.info("Dataset ids will be saved to a s3 text file")
        else:
            fs = fsspec.filesystem("file")
            _log.info("Dataset ids will be saved to a local text file")
            parsed_output_fp = urllib.parse.urlparse(output_file_path).path
            absolute_output_fp = os.path.abspath(parsed_output_fp)
            path_head, path_tail = os.path.split(absolute_output_fp)
            if path_head:
                if not fs.exists(path_head):
                    fs.mkdirs(path_head, exist_ok=True)
                    _log.info(f"Local folder {path_head} created.")

        with fs.open(output_file_path, "w") as file:
            for dataset_id in dataset_ids:
                file.write(f"{dataset_id}\n")

        _log.info(f"Dataset IDs written to: {output_file_path}.")
