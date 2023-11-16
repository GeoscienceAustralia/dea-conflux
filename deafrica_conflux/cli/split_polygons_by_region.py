import logging

import click
import geopandas as gpd

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.group_polygons import get_polygon_length, split_polygons_by_region
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
    """Split polygons using a DE Africa product's regions."""
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

    # Get the original crs of the polygons
    original_crs = polygons_gdf.crs
    # Get the orginal count of the polygons.
    original_count = len(polygons_gdf)

    # Reproject to a projected CRS.
    polygons_gdf = polygons_gdf.to_crs("EPSG:6933")
    assert polygons_gdf.crs.is_projected

    # Get the length of each polygon.
    polygons_gdf["polygon_length_m"] = polygons_gdf["geometry"].apply(get_polygon_length)

    # Filter out polygons whose length is larger than a single Landsat scene
    ls_scene_length = 185 * 1000
    filtered_polygons_gdf = polygons_gdf[polygons_gdf["polygon_length_m"] <= ls_scene_length]
    _log.info(
        f"Filtered out {original_count - len(filtered_polygons_gdf)} polygons out of {original_count} polygons"
    )

    # Reproject back to the original crs.
    filtered_polygons_gdf = filtered_polygons_gdf.to_crs(original_crs)

    # Split the filtered polygons by region.
    split_polygons_fps = split_polygons_by_region(  # noqa F841
        product=product, polygons_gdf=polygons_gdf, output_directory=output_directory
    )
