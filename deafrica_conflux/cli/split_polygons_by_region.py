import logging
import os

import click
import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.io import check_dir_exists, check_if_s3_uri


@click.command(
    "split-polygons-by-region",
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
def split_polygons_by_region(
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

    # Load the regions file.
    product_regions_fp = f"https://explorer.digitalearth.africa/api/regions/{product}"
    product_regions = gpd.read_file(product_regions_fp).to_crs(polygons_gdf.crs)
    product_regions.set_index("region_code", inplace=True)

    # Split each row in the product_regions into a GeoDataFrame of its own.
    regions = np.array_split(product_regions, len(product_regions))
    assert len(regions) == len(product_regions)

    # For each region get the IDs for the polygons that intersect with the region.
    def get_intersecting_polygons_ids(region, polygons_gdf):
        intersecting_polygons_ids = gpd.sjoin(
            polygons_gdf, region, how="inner", predicate="intersects"
        ).index.to_list()
        region["intersecting_polygons_ids"] = ",".join(intersecting_polygons_ids)
        return region

    regions_ = [get_intersecting_polygons_ids(region, polygons_gdf) for region in regions]

    # Filter to remove regions with no intersecting polygons.
    filtered_regions = [region for region in regions_ if region.iloc[0].intersecting_polygons_ids]

    filtered_regions_gdf = pd.concat(filtered_regions, ignore_index=True)

    if not check_dir_exists(output_directory):
        if check_if_s3_uri(output_directory):
            fs = fsspec.filesystem("s3")
        else:
            fs = fsspec.filesystem("file")

        fs.mkdirs(output_directory, exist_ok=True)
        _log.info(f"Created directory {output_directory}")

    def export_polygons(row, polygons_gdf, output_directory):
        region_id = row.name
        polygon_ids = row.intersecting_polygons_ids.split(",")

        output_fp = os.path.join(output_directory, f"{region_id}.parquet")
        polygons_gdf.loc[polygon_ids].reset_index().to_parquet(output_fp)
        _log.info(f"Polygons for region {region_id} written to {output_fp}")
        return output_fp

    filtered_regions_gdf["polygon_file_paths"] = filtered_regions_gdf.apply(
        lambda row: export_polygons(row, polygons_gdf, output_directory), axis=1
    )
