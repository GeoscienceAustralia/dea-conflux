import os
import sys
import click
import logging
import fsspec
import json
import geopandas as gpd
from datacube.ui import click as ui

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.cli.group_options import MutuallyExclusiveOption

from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.hopper import find_datasets
from deafrica_conflux.drill import filter_datasets


@click.command("get-ids",
               no_args_is_help=True,
               )
@click.argument("product", type=str)
@ui.parsed_search_expressions
@click.option("-v", "--verbose", count=True)
@click.option(
    "--polygons-vector-file",
    type=click.Path(),
    help="Path to the vector file defining the polygon(s) to run polygon drill on to filter datasets.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in shapefile.",
)
@click.option("--s3",
              "storage_location",
              flag_value="s3",
              help="Save the output to an s3 bucket.")
@click.option("--local",
              "storage_location",
              flag_value="local",
              default=True,
              help="Save the output to a local folder.")
@click.option("--output-bucket-name",
              type=str,
              show_default=True,
              cls=MutuallyExclusiveOption,
              mutually_exclusive=["output_local_folder"],
              help="The s3 bucket to write the output to.",)
@click.option("--output-local-folder",
              type=click.Path(),
              cls=MutuallyExclusiveOption,
              mutually_exclusive=["output_bucket_name"],
              help="Local directory to write the waterbody polygons to.",)
@click.option(
    "--num-worker",
    type=int,
    help="The number of processes to filter datasets.",
    default=4,
)
@click.option("--product-version",
              type=str,
              default="0.0.1",
              show_default=True,
              help="Product version for the DE Africa Waterbodies product.")
def get_ids(product,
            expressions,
            verbose,
            polygons_vector_file,
            use_id,
            storage_location,
            output_bucket_name,
            output_local_folder,
            num_worker,
            product_version):
    
    """
    Get IDs based on an expression.
    """

    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dss = find_datasets(expressions, [product])

    if polygons_vector_file:

        # Read the vector file.
        try:
            polygons_gdf = gpd.read_file(polygons_vector_file)
        except Exception as error:
            _log.error(error)
            raise
        
        # Guess the ID field.
        id_field = guess_id_field(polygons_gdf, use_id)
        _log.debug(f"Guessed ID field: {id_field}")

        # Set the ID field as the index.
        polygons_gdf.set_index(id_field)

        _log.info(f"Polygons vector file RAM usage: {sys.getsizeof(polygons_gdf)} bytes.")

        # Reprojection is done to avoid UserWarning: Geometry is in a geographic CRS.
        # when using filter_datasets when polygons are in "EPSG:4326" crs.
        polygons_gdf = polygons_gdf.to_crs("EPSG:6933")

        ids = filter_datasets(dss, polygons_gdf, worker_num=num_worker)
    else:
        ids = [str(ds.id) for ds in dss]

    object_prefix = f'{product_version.replace(".", "-")}/timeseries/conflux/'

    if storage_location == "local":
        output_directory = os.path.join(os.path.abspath(output_local_folder), object_prefix)
        
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            _log.info(f"{output_directory} folder created.")
    
        out_path = f"file://{output_directory}conflux_ids.txt"
    else:
        out_path = f"s3://{output_bucket_name}/{object_prefix}conflux_ids.txt"

    _log.info(f"Writing IDs to: {out_path}.")
    
    with fsspec.open(out_path, "w") as f:
        f.write("\n".join(ids))
    
    print(json.dumps({"ids_path": out_path}), end="")

    return 0
