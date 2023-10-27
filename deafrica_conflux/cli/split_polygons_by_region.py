import logging

import click
import geopandas as gpd

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.group_polygons import split_polygons_by_region
from deafrica_conflux.id_field import guess_id_field


@click.command(
    "split-polygons",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", count=True)
@click.option("--product", type=str, help="DE Africa product to get regions from.")
@click.option(
    "--polygons-vector-file",
    type=str,
    # Don't mandate existence since this might be s3://.
    help="Path to the vector file defining the polygon(s) to split by region.",
)
@click.option(
    "--use-id",
    type=str,
    default=None,
    help="Optional. Unique key id in polygons vector file.",
)
@click.option(
    "--output-directory",
    type=str,
    help="Path to the directory to write the parquet files to.",
)
def split_polygons(
    verbose,
    product,
    polygons_vector_file,
    use_id,
    output_directory,
):
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

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

    split_polygons_fps = split_polygons_by_region(  # noqa F841
        product=product, polygons_gdf=polygons_gdf, output_directory=output_directory
    )
