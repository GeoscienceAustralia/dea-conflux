"""
Run a polygon drill step on a scene.

Matthew Alger, Vanessa Newey
Geoscience Australia
2021
"""

import datetime
import logging
import warnings
from types import ModuleType

import datacube
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry
from datacube.model import Dataset
from datacube.utils.geometry import Geometry
from deafrica_tools.spatial import xr_rasterize
from skimage.measure import regionprops

_log = logging.getLogger(__name__)


def _get_directions(og_geom: shapely.geometry.Polygon, int_geom: shapely.geometry.Polygon) -> set:
    """
    Helper to get direction of intersection between geometry, intersection.

    Arguments
    ---------
    og_geom : shapely.geometry.Polygon
        Original polygon.

    int_geom : shapely.geometry.Polygon
        Polygon after intersecting with extent.

    Returns
    -------
        set of directions in which the polygon overflows the extent.
    """
    boundary_intersections = int_geom.boundary.difference(og_geom.boundary)
    try:
        # Is a MultiLineString.
        boundary_intersection_lines = list(boundary_intersections.geoms)
    except AttributeError:
        # Is not a MultiLineString.
        boundary_intersection_lines = [boundary_intersections]
    # Split up multilines.
    boundary_intersection_lines_ = []
    for line_ in boundary_intersection_lines:
        coords = list(line_.coords)
        for a, b in zip(coords[:-1], coords[1:]):
            boundary_intersection_lines_.append(shapely.geometry.LineString((a, b)))
    boundary_intersection_lines = boundary_intersection_lines_

    boundary_directions = set()
    for line in boundary_intersection_lines:
        angle = np.arctan2(
            line.coords[1][1] - line.coords[0][1], line.coords[1][0] - line.coords[0][0]
        )
        horizontal = abs(angle) <= np.pi / 4 or abs(angle) >= 3 * np.pi / 4

        if horizontal:
            ys_line = [c[1] for c in line.coords]
            southern_coord_line = min(ys_line)
            northern_coord_line = max(ys_line)

            # Find corresponding southernmost/northernmost point
            # in intersection
            try:
                ys_poly = [c[1] for g in list(int_geom.boundary.geoms) for c in g.coords]
            except AttributeError:
                ys_poly = [c[1] for c in int_geom.boundary.coords]
            southern_coord_poly = min(ys_poly)
            northern_coord_poly = max(ys_poly)

            # If the south/north match the south/north, we have the
            # south/north boundary
            if southern_coord_poly == southern_coord_line:
                # We are south!
                boundary_directions.add("South")
            elif northern_coord_poly == northern_coord_line:
                boundary_directions.add("North")
        else:
            xs_line = [c[0] for c in line.coords]
            western_coord_line = min(xs_line)
            eastern_coord_line = max(xs_line)

            # Find corresponding southernmost/northernmost point
            # in intersection
            try:
                xs_poly = [c[0] for g in list(int_geom.boundary.geoms) for c in g.coords]
            except AttributeError:
                xs_poly = [c[0] for c in int_geom.boundary.coords]
            western_coord_poly = min(xs_poly)
            eastern_coord_poly = max(xs_poly)

            # If the south/north match the south/north, we have the
            # south/north boundary
            if western_coord_poly == western_coord_line:
                # We are west!
                boundary_directions.add("West")
            elif eastern_coord_poly == eastern_coord_line:
                boundary_directions.add("East")
    return boundary_directions


def get_intersections(polygons_gdf: gpd.GeoDataFrame, ds_extent: Geometry) -> pd.DataFrame:
    """
    Find which polygons intersect with a Dataset or DataArray extent
    and in what direction.

    Arguments
    ---------
    polygons_gdf : gpd.GeoDataFrame
        Set of polygons.

    ds_extent : Geometry
        Valid extent of a dataset to check intersection against.

    Returns
    -------
    pd.DataFrame
        Table of intersections.
    """
    # Check if the set of polygons and the dataset extent have the same
    # CRS.
    assert polygons_gdf.crs == ds_extent.crs

    # Get the geometry of the dataset extent.
    ds_extent_geom = ds_extent.geom

    all_intersection = polygons_gdf.geometry.intersection(ds_extent_geom)
    # Which ones have decreased in area thanks to our intersection?
    intersects_mask = ~(all_intersection.area == 0)
    ratios = all_intersection.area / polygons_gdf.area
    directions = []
    dir_names = ["North", "South", "East", "West"]
    for ratio, intersects, idx in zip(ratios, intersects_mask, ratios.index):
        # idx is index into gdf
        if not intersects or ratio == 1:
            directions.append({d: False for d in dir_names})
            continue
        og_geom = polygons_gdf.loc[idx].geometry
        # Buffer to dodge some bad geometry behaviour
        int_geom = all_intersection.loc[idx].buffer(0)
        dirs = _get_directions(og_geom, int_geom)
        directions.append({d: d in dirs for d in dir_names})
        assert any(directions[-1].values())
    return pd.DataFrame(directions, index=ratios.index)


def get_polygons_within_ds_extent(polygons_gdf: gpd.GeoDataFrame, ds: Dataset) -> gpd.GeoDataFrame:
    """
    Filter a set of polygons to include only polygons within (contained in)
    the extent of a dataset.
    """
    # Get the extent of the dataset.
    ds_extent = ds.extent
    ds_extent_crs = ds_extent.crs
    ds_extent_geom = ds_extent.geom
    ds_extent_gdf = gpd.GeoDataFrame(geometry=[ds_extent_geom], crs=ds_extent_crs).to_crs(
        polygons_gdf.crs
    )

    # Get all polygons that are contained withn the extent of the dataset.
    polygon_ids_within_ds_extent = ds_extent_gdf.sjoin(
        polygons_gdf, how="inner", predicate="contains"
    )["index_right"].to_list()
    polygons_within_ds_extent = polygons_gdf.loc[polygon_ids_within_ds_extent]

    return polygons_within_ds_extent


def get_polygons_intersecting_ds_extent(
    polygons_gdf: gpd.GeoDataFrame, ds: Dataset
) -> gpd.GeoDataFrame:
    """
    Filter a set of polygons to only include polygons that intersect with
    the extent of a dataset.

    Parameters
    ---------
    polygons_gdf : gpd.GeoDataFrame
    ds : Dataset

    Returns
    -------
    gpd.GeoDataFrame
    """
    # Get the extent of the dataset.
    ds_extent = ds.extent
    ds_extent_crs = ds_extent.crs
    ds_extent_geom = ds_extent.geom
    ds_extent_gdf = gpd.GeoDataFrame(geometry=[ds_extent_geom], crs=ds_extent_crs).to_crs(
        polygons_gdf.crs
    )

    # Get all polygons that intersect with the extent of the dataset.
    polygon_ids_intersecting_ds_extent = ds_extent_gdf.sjoin(
        polygons_gdf, how="inner", predicate="intersects"
    )["index_right"].to_list()
    polygons_intersecting_ds_extent = polygons_gdf.loc[polygon_ids_intersecting_ds_extent]

    return polygons_intersecting_ds_extent


def filter_large_polygons(polygons_gdf: gpd.GeoDataFrame, ds: Dataset) -> gpd.GeoDataFrame:
    """
    Filter out large polygons from the set of polygons.
    Large polygons are defined as polygons which are large than 3 scenes
    in width and in height.

    Arguments
    ---------
    polygons_gdf : gpd.GeoDataFrame
    ds : datacube.model.Dataset

    Returns
    -------
    gpd.GeoDataFrame
    """
    # Get the extent of the dataset.
    ds_extent = ds.extent

    # Reproject the extent of the dataset to match the set of polygons.
    ds_extent = ds_extent.to_crs(polygons_gdf.crs)

    # Get the bounding box of the extent of the dataset.
    bbox = ds_extent.boundingbox
    left, bottom, right, top = bbox

    # Create a polygon 3 dataset extents in width and height.
    width = right - left
    height = top - bottom

    testbox = shapely.geometry.Polygon(
        [
            (left - width, bottom - height),
            (left - width, top + height),
            (right + width, top + height),
            (right + width, bottom - height),
        ]
    )

    filtered_polygons_gdf = polygons_gdf[~polygons_gdf.geometry.intersects(testbox.boundary)]

    return filtered_polygons_gdf


def remove_duplicate_datasets(required_datasets: list[Dataset]) -> list[Dataset]:
    """
    Remove duplicate datasets based on region code and creation date.
    Picks the most recently created dataset.

    Parameters
    ----------
    required_datasets : list
        List of datasets to filter.

    Returns
    -------
    list
        List of filtered datasets.
    """
    filtered_req_datasets = []

    ds_region_codes = list(set([ds.metadata.region_code for ds in required_datasets]))
    for regioncode in ds_region_codes:
        matching_ds = [ds for ds in required_datasets if ds.metadata.region_code == regioncode]
        matching_ds_sorted = sorted(matching_ds, key=lambda x: x.metadata.creation_dt, reverse=True)
        keep = matching_ds_sorted[0]
        filtered_req_datasets.append(keep)

    return filtered_req_datasets


def drill(
    plugin: ModuleType,
    polygons_gdf: gpd.GeoDataFrame,
    reference_dataset: Dataset,
    partial=True,
    overedge=True,
    dc: datacube.Datacube | None = None,
) -> pd.DataFrame:
    """
    Perform a polygon drill.

    Arguments
    ---------
    plugin : module
        A validated plugin to drill with.

    polygons_gdf : GeoDataFrame
        A GeoDataFrame with the ID (column containing the polygons ids) as the index.

    reference_dataset : Dataset
        Refernce dataset to process.

    partial : bool
        Optional (defaults to True). Whether to include polygons that partially
        overlap with the scene. If partial is True, polygons that partially
        overlap with the scene are included. If partial is False, polygons that
        partially overlap with the scene are excluded from the drill, and going
        off the edge of the scene will exclude the entire polygon. Describes
        what happens to the polygon, not what happens to the data. Interacts
        with overedge, which describes what happens to the overedge data.

    overedge : bool
        Optional (defaults to False). Whether to include data from other scenes
        in partially overedge polygons. If overedge is False, data from other
        scenes is not included in results. If overedge is True, data from other
        scenes is included in results. Interacts with partial.

    dc : datacube.Datacube
        Optional existing Datacube.

    time_buffer : datetime.timedelta
        Optional (default 1 hour). Only consider datasets within
        this time range for overedge.

    Returns
    -------
    Drill table : pd.DataFrame
        Index = polygon ID
        Columns = output bands
    """

    # Validate partial and overedge parameters.
    if not partial:
        if overedge:
            _log.error("overedge=True expects partial=True")
            raise ValueError("overedge=True expects partial=True")

    # TODO: Generalize to work with multiple products and
    # products with multiple measurements.

    # Check the plugin does not have multiple products to load.
    # Using multiple products is not is not implemented.
    if len(plugin.input_products.items()) > 1:
        raise NotImplementedError("Expected one product in plugin")
    else:
        reference_product = reference_dataset.type.name
        assert reference_product == list(plugin.input_products.keys())[0]
        measurements = plugin.input_products[reference_product]
        if len(measurements) > 1:
            raise NotImplementedError("Expected 1 measurement in plugin")
        else:
            measurement = measurements[0]

    # Get a datacube if we don't have one already.
    if dc is None:
        dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Get the output crs and resolution from the plugin.
    output_crs = plugin.output_crs
    resolution = plugin.resolution
    if hasattr(plugin, "resampling"):
        resampling = plugin.resampling
    else:
        resampling = "nearest"

    # Reproject the polygons to the required CRS.
    polygons_gdf = polygons_gdf.to_crs(output_crs)

    # Filter the polygons based on the partial and overedge parameter.
    if partial:
        # Include polygons that partially overlap with the scene.
        # i.e. only  include polygons that intersect with the dataset extent.
        filtered_polygons_gdf = get_polygons_intersecting_ds_extent(polygons_gdf, reference_dataset)
        _log.info(
            f"Filtered out {len(polygons_gdf) - len(filtered_polygons_gdf)} polygons out of {len(polygons_gdf)} polygons."
        )
        if overedge:
            # If overedge, remove anything which intersects with a 3-scene
            # width box.
            n = len(filtered_polygons_gdf)
            filtered_polygons_gdf = filter_large_polygons(filtered_polygons_gdf, reference_dataset)
            _log.info(
                f"Overedge filter removed {n - len(filtered_polygons_gdf)} polygons larger than 3 scenes in width and height out of {n} polygons."
            )
    else:
        # Do not include polygons that partially overlap with the scene.
        # i.e. only include polygons within the dataset extent.
        filtered_polygons_gdf = get_polygons_within_ds_extent(polygons_gdf, reference_dataset)
        _log.info(
            f"Filtered out {len(polygons_gdf)- len(filtered_polygons_gdf)} polygons out of {len(polygons_gdf)} polygons."
        )

    if len(filtered_polygons_gdf) == 0:
        scene_uuid = str(reference_dataset.id)
        _log.warning(f"No polygons found in scene {scene_uuid}")
        return pd.DataFrame({})

    # Load the reference dataset.
    if overedge:
        # Search for all the datasets neighbouring our reference dataset that we need to cover
        # the area of the polygons.

        # Get the bounding box for the polygons.
        _log.debug("Getting bounding box for the polygons...")
        geopolygon = Geometry(
            geom=shapely.geometry.box(*filtered_polygons_gdf.total_bounds),
            crs=filtered_polygons_gdf.crs,
        )

        # Get the time range to use for searching for datasets neighbouring our reference dataset.

        # The 1 hour buffer ensures we are only finding neighbouring datasets on the same path.
        time_buffer = datetime.timedelta(hours=1)

        time_span = (
            reference_dataset.center_time - time_buffer,
            reference_dataset.center_time + time_buffer,
        )
        _log.debug(
            f"Time range to use for searching for datasets neighbouring our reference dataset {time_span}."
        )

        req_datasets_ = dc.find_datasets(
            product=reference_product, geopolygon=geopolygon, time=time_span, ensure_location=True
        )
        _log.debug(
            f"Found the {len(req_datasets_)} required datasets to cover all the polygons: {', '.join([str(dataset.id) for dataset in req_datasets_])} ."
        )

        req_datasets = remove_duplicate_datasets(req_datasets_)
        _log.debug(f"Removed {len(req_datasets_) - len(req_datasets)} duplicate datasets.")

        reference_scene = dc.load(
            datasets=req_datasets,
            measurements=measurements,
            geopolygon=geopolygon,
            time=time_span,
            output_crs=output_crs,
            resolution=resolution,
            group_by="solar_day",
            resampling=resampling,
        )

        _log.info(
            f"Loaded the {len(req_datasets)} required datasets to cover all the polygons: {', '.join([str(dataset.id) for dataset in req_datasets])} ."
        )

    else:
        # Load the reference scene.
        reference_scene = dc.load(
            datasets=[reference_dataset],
            measurements=measurements,
            output_crs=output_crs,
            resolution=resolution,
            resampling=resampling,
        )

        _log.info(f"Loaded the reference dataset {str(reference_dataset.id)} .")

    _log.info(f"Reference scene is {reference_scene.sizes}")

    # Transform the loaded data.
    # Force warnings to raise exceptions.
    # This means users have to explicitly ignore warnings.
    ds = reference_scene.isel(time=0)
    with warnings.catch_warnings():
        warnings.filterwarnings("error")
        ds_transformed = plugin.transform(ds)[measurement]

    # Assign a one-indexed numeric column for the polygons.
    # This will allow us to build a polygon enumerated raster.
    attr_col = "_conflux_one_index"
    # This mutates the (in-memory) polygons_gdf, but that's OK.
    filtered_polygons_gdf[attr_col] = range(1, len(filtered_polygons_gdf.index) + 1)
    conflux_one_index_to_id = {v: k for k, v in filtered_polygons_gdf[attr_col].to_dict().items()}

    # Build the enumerated polygon raster.
    polygon_raster = xr_rasterize(filtered_polygons_gdf, reference_scene, attr_col)

    # For each polygon, perform the summary.
    props = regionprops(
        label_image=polygon_raster.values,
        intensity_image=ds_transformed.values,
        extra_properties=(plugin.summarise,),
    )

    summary_df_list = []
    for region_prop in props:
        polygon_summary_df = region_prop.summarise
        polygon_index = conflux_one_index_to_id[region_prop.label]
        polygon_summary_df.index = [polygon_index]
        summary_df_list.append(polygon_summary_df)

    summary_df = pd.concat(summary_df_list, ignore_index=False)

    # Detect intersections.
    # We only have to do this if partial and not overedge.
    # If not partial, then there can't be any intersections.
    # If overedge, then there are no partly observed polygons.
    if partial and not overedge:
        intersection_features = get_intersections(filtered_polygons_gdf, reference_scene.extent)
        intersection_features.rename(
            inplace=True,
            columns={
                "North": "conflux_n",
                "South": "conflux_s",
                "East": "conflux_e",
                "West": "conflux_w",
            },
        )
        # Merge in the edge information.
        summary_df = summary_df.join(
            # left join only includes objects with some
            # representation in the scene
            intersection_features,
            how="left",
        )

    return summary_df
