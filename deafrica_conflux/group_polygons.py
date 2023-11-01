import logging
import os

import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd

from deafrica_conflux.io import check_dir_exists, check_if_s3_uri

_log = logging.getLogger(__name__)


def get_intersecting_polygons_ids(
    region: gpd.GeoDataFrame, polygons_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Get the IDs of the polygons that intersect with a region.

    Parameters
    ----------
    region : gpd.GeoDataFrame
        A single row GeoDataFrame of the product region of interest.
    polygons_gdf : gpd.GeoDataFrame
        A set of polygons to filter by intersection with the region.

    Returns
    -------
    gpd.GeoDataFrame
        The single row GeoDataFrame of the product region of interest with a
        column containing the ids of the polygons that intersect with the region.
    """
    assert len(region) == 1

    intersecting_polygons_ids = gpd.sjoin(
        polygons_gdf, region, how="inner", predicate="intersects"
    ).index.to_list()
    region["intersecting_polygons_ids"] = ",".join(intersecting_polygons_ids)

    return region


def export_polygons(
    region: pd.Series, polygons_gdf: gpd.GeoDataFrame, output_directory: str
) -> str:
    """
    Export the set of polygons for a region as a parquet file.


    Parameters
    ----------
    region : pd.Series
        The row in a DataFrame representing a region.
    polygons_gdf : gpd.GeoDataFrame
        The set of polygons to select from.
    output_directory : str
        The output directory to write the output parquet file to.

    Returns
    -------
    str
        The file path of the output parquet file.
    """
    region_id = region.name
    polygon_ids = region.intersecting_polygons_ids.split(",")

    output_fp = os.path.join(output_directory, f"{region_id}.parquet")

    polygons_gdf.loc[polygon_ids].reset_index().to_parquet(output_fp)

    _log.info(f"Polygons for region {region_id} written to {output_fp}")

    return output_fp


def split_polygons_by_region(
    product: str,
    polygons_gdf: gpd.GeoDataFrame,
    output_directory: str,
) -> dict:
    """
    Split a set of polygons by the regions in a DE Africa's product regions
    GeoJSON file.

    Parameters
    ----------
    product : str
        The DE Africa product to use to get the regions and region codes.
    polygons_gdf : gpd.GeoDataFrame
        The set of polygons to split by region, with the polygon IDs column set
        as the index.
    output_directory : str
        The directory to write the parquet files for the GeoDataFrames
        from the split by regions.

    Returns
    -------
    dict
        A dictionary of the region codes and the file path to the polygons that
        intersect with the region.
    """
    # Load the regions file.
    product_regions_fp = f"https://explorer.digitalearth.africa/api/regions/{product}"
    product_regions = gpd.read_file(product_regions_fp).to_crs(polygons_gdf.crs)
    product_regions.set_index("region_code", inplace=True)

    # Split each row in the product_regions into a GeoDataFrame of its own.
    regions = np.array_split(product_regions, len(product_regions))
    assert len(regions) == len(product_regions)

    # For each region get the IDs for the polygons that intersect with the region.
    regions_ = [get_intersecting_polygons_ids(region, polygons_gdf) for region in regions]

    # Filter to remove regions with no intersecting polygons.
    filtered_regions = [region for region in regions_ if region.iloc[0].intersecting_polygons_ids]

    filtered_regions_gdf = pd.concat(filtered_regions, ignore_index=False)

    if not check_dir_exists(output_directory):
        if check_if_s3_uri(output_directory):
            fs = fsspec.filesystem("s3")
        else:
            fs = fsspec.filesystem("file")

        fs.mkdirs(output_directory, exist_ok=True)
        _log.info(f"Created directory {output_directory}")

    # Export each regions' polygons as a parquet file.
    filtered_regions_gdf["polygon_file_paths"] = filtered_regions_gdf.apply(
        lambda row: export_polygons(row, polygons_gdf, output_directory), axis=1
    )

    return filtered_regions_gdf["polygon_file_paths"].to_dict()
