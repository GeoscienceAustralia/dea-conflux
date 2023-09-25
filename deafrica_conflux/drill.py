"""
Run a polygon drill step on a scene.

Matthew Alger, Vanessa Newey
Geoscience Australia
2021
"""

import collections
import datetime
import logging
import multiprocessing
import warnings
from functools import partial
from types import ModuleType

import datacube
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry
import tqdm
import xarray as xr
from datacube.model import Dataset
from datacube.utils.geometry import Geometry
from deafrica_tools.spatial import xr_rasterize

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


def find_datasets_for_plugin(
    dc: datacube.Datacube, plugin: ModuleType, scene_uuid: str, strict: bool = False
) -> dict[str, Dataset]:
    """
    Find the datasets that a plugin requires given a related scene UUID.

    Arguments
    ---------
    dc : Datacube
        A Datacube to search.
    plugin : module
        Plugin defining input products.
    scene_uuid : str
        UUID of scene to look up.
    strict : bool
        Default False. Error on duplicate scenes (otherwise warn).

    Returns
    -------
    dict[str, Dataset]
        Dataset for each of the input products in the plugin.
    """
    # Load the metadata of the specified scene.
    metadata = dc.index.datasets.get(scene_uuid)
    # Find the datasets that have the same centre time and
    # fall within this extent.
    datasets = {}
    for input_product in plugin.input_products:
        datasets_ = dc.find_datasets(
            product=input_product, geopolygon=metadata.extent, time=metadata.center_time
        )
        if len(datasets_) > 1:
            if strict:
                raise ValueError(f"Found multiple datasets at same time for {scene_uuid}")
            else:
                warnings.warn(
                    f"Found multiple datasets at same time for {scene_uuid}, "
                    "choosing one arbitrarily",
                    RuntimeWarning,
                )
        elif len(datasets_) == 0:
            raise ValueError("Found no datasets associated with given scene")
        datasets[input_product] = datasets_[0]
    return datasets


def dataset_to_dict(ds: xr.Dataset) -> dict:
    """
    Convert a 0d dataset into a dictionary.

    Arguments
    ---------
    ds : xr.Dataset

    Returns
    -------
    dict
    """
    ds_dict = ds.to_dict(data="list")
    data_vars_dict = ds_dict["data_vars"]

    data_dict = {}
    for key, val in data_vars_dict.items():
        data_dict[key] = val["data"]

    return data_dict


def polygons_in_ds_extent(polygons_gdf: gpd.GeoDataFrame, ds: Dataset) -> gpd.GeoDataFrame:
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

    # Reproject the extent of the dataset to match the set of polygons.
    ds_extent = ds_extent.to_crs(polygons_gdf.crs)

    # Get the shapely geometry of the reprojected extent of the dataset.
    ds_extent_geom = ds_extent.geom

    # Filter the polygons.
    filtered_polygons_gdf = polygons_gdf[polygons_gdf.geometry.intersects(ds_extent_geom)]

    return filtered_polygons_gdf


def polygons_centroids_in_ds_extent_bbox(
    polygons_gdf: gpd.GeoDataFrame, ds: Dataset, buffer=True
) -> gpd.GeoDataFrame:
    """
    Filter a set of polygons to only include polygons whose centroids intersect
    with the (buffered) bounding box of the extent of a dataset.

    Arguments
    ---------
    polygons_gdf : gpd.GeoDataFrame
    ds : Dataset
    buffer : bool
        Optional (True).
        Extend the bounding box of the extent of a dataset by the
        width of the extent of the dataset.

    Returns
    -------
    gpd.GeoDataFrame
    """
    # Get the extent of the dataset.
    ds_extent = ds.extent

    # Reproject the extent of the dataset to match the set of polygons.
    ds_extent = ds_extent.to_crs(polygons_gdf.crs)

    # Get the buffered bounding box of the extent of the dataset.
    bbox = ds_extent.boundingbox
    left, bottom, right, top = bbox

    width = height = 0
    if buffer:
        width = right - left
        height = top - bottom

    buffered_bbox_geom = shapely.geometry.box(
        left - width, bottom - height, right + width, top + height
    )

    # Get the centroids of the polygons.
    centroids = polygons_gdf.centroid

    # Filter the polygons.
    filtered_polygons_gdf = polygons_gdf[centroids.intersects(buffered_bbox_geom)]

    return filtered_polygons_gdf


def check_ds_near_polygons(
    polygons_gdf: gpd.GeoDataFrame,
    ds: Dataset,
):
    """
    Use the 'polygons_in_ds_extent' and 'polygons_centroids_in_ds_extent_bbox'
    functions to check if a dataset is near a set of polygons.
    Returns the dataset's id if the dataset is near a set of polygons else
    returns an empty string.

    Parameters
    ----------
    polygons_gdf : gpd.GeoDataFrame
    ds : Dataset

    Returns
    -------
    str
    """
    if len(polygons_centroids_in_ds_extent_bbox(polygons_gdf, ds)) > 0:
        if len(polygons_in_ds_extent(polygons_gdf, ds)) > 0:
            return str(ds.id)
        else:
            return ""
    else:
        return ""


def filter_datasets(
    dss: list[Dataset], polygons_gdf: gpd.GeoDataFrame, worker_num: int = 1
) -> list[str]:
    """
    Filter out datasets that are not near a set of polygons, using a
    multi-process approach to run the check_ds_near_polygons function.

    Arguments
    ---------
    dss : list[Dataset]
    polygons_gdf : gpd.GeoDataFrame
    worker_num : int

    Returns
    -------
    list[str]
        List of dataset ids for datasets near the set of polygons.
    """
    with multiprocessing.Pool(processes=worker_num) as pool:
        filtered_datasets_ = list(
            tqdm.tqdm(pool.imap(partial(check_ds_near_polygons, polygons_gdf), dss))
        )

    # Remove empty strings.
    filtered_datasets = [item for item in filtered_datasets_ if item]

    return filtered_datasets


def polygons_in_tripled_ds_extent_bbox_boundary(
    polygons_gdf: gpd.GeoDataFrame, ds: Dataset
) -> gpd.GeoDataFrame:
    """
    Filter a set of polygons to remove polygons that intersect with the
    boundary of a polygon three times the size of the bounding box of the extent
    of a dataset.

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

    # Create a polygon 3 times the size of the bounding box.
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


def drill(
    plugin: ModuleType,
    polygons_gdf: gpd.GeoDataFrame,
    scene_uuid: str,
    partial=True,
    overedge=False,
    dc: datacube.Datacube | None = None,
    time_buffer=datetime.timedelta(hours=1),
) -> pd.DataFrame:
    """
    Perform a polygon drill.

    Arguments
    ---------
    plugin : module
        A validated plugin to drill with.

    polygons_gdf : GeoDataFrame
        A GeoDataFrame in the same CRS as the output_crs,
        with the ID (column containing the polygons ids) as the index.

    scene_uuid : str
        ID of scene to process.

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

    # partial and overedge interact.
    # we have two loading options (dc.load and scene-based),
    # two polygon screening options (cautious and centroid),
    # and two reporting options (report or not).

    # loading:
    #              | partial | not partial
    # overedge     | dc.load | warn; scene
    # not overedge | scene   | scene

    # screening:
    #              | partial  | not partial
    # overedge     | cautious | centroid
    # not overedge | cautious | centroid

    # reporting:
    #              | partial | not partial
    # overedge     | not     | not
    # not overedge | report  | not

    if not partial:
        if overedge:
            # warnings.warn("overedge=True expects partial=True")
            raise ValueError("overedge=True expects partial=True")
        else:
            raise NotImplementedError()

    # Get a datacube if we don't have one already.
    if dc is not None:
        dc = datacube.Datacube(app="deafrica-conflux-drill")

    # Get the output crs and resolution from the plugin.
    output_crs = plugin.output_crs
    resolution = plugin.resolution

    # Reproject the polygons to the required CRS.
    polygons_gdf = polygons_gdf.to_crs(output_crs)
    assert str(polygons_gdf.crs).lower() == str(output_crs).lower()

    # Assign a one-indexed numeric column for the polygons.
    # This will allow us to build a polygon enumerated raster.
    attr_col = "_conflux_one_index"
    # This mutates the (in-memory) polygons_gdf, but that's OK.
    polygons_gdf[attr_col] = range(1, len(polygons_gdf.index) + 1)
    conflux_one_index_to_id = {v: k for k, v in polygons_gdf[attr_col].to_dict().items()}

    # Get the dataset we asked for.
    reference_dataset = dc.index.datasets.get(scene_uuid)

    # Filter out polygons that aren't anywhere near this scene.
    _n_initial = len(polygons_gdf)
    filtered_polygons_gdf = polygons_centroids_in_ds_extent_bbox(
        polygons_gdf,
        reference_dataset,
        # ...and limit it to centroids in this scene if not partial.  # noqa: E128
        buffer=partial,
    )
    _n_filtered_quick = len(filtered_polygons_gdf)
    _log.debug(f"Quick filter removed {_n_initial - _n_filtered_quick} polygons")

    # Remove things outside the box.
    # We do this after the quick filter so the intersection is way faster.
    filtered_polygons_gdf = polygons_in_ds_extent(filtered_polygons_gdf, reference_dataset)
    _n_filtered_full = len(filtered_polygons_gdf)
    _log.debug(f"Full filter removed {_n_filtered_quick - _n_filtered_full} polygons")

    # If overedge, remove anything which intersects with a 3-scene
    # width box.
    if overedge:
        filtered_polygons_gdf = polygons_in_tripled_ds_extent_bbox_boundary(
            filtered_polygons_gdf, reference_dataset
        )
        _log.debug(
            f"Overedge filter removed {_n_filtered_full - len(filtered_polygons_gdf)} polygons"
        )

    if len(filtered_polygons_gdf) == 0:
        _log.warning(f"No polygons found in scene {scene_uuid}")
        return pd.DataFrame({})

    # Load the image of the input scene so we can build the raster.
    # TODO(MatthewJA): If this is also a dataset required for drilling,
    # we will load it twice - and even worse, we'll load it with
    # more bands than we need the first time! Ignore for MVP.
    if not overedge:
        # just load the scene we asked for
        _log.debug("Loading datasets:")
        _log.debug(f"\t{reference_dataset.id}")
        reference_scene = dc.load(
            datasets=[reference_dataset], output_crs=output_crs, resolution=resolution
        )
        # and grab the datasets we want too
        datasets = find_datasets_for_plugin(dc, plugin, scene_uuid)
    else:
        # search for all the datasets we need to cover the area
        # of the polygons.
        reference_product = reference_dataset.type.name

        geopolygon = Geometry(
            geom=shapely.geometry.box(*filtered_polygons_gdf.total_bounds),
            crs=filtered_polygons_gdf.crs,
        )

        time_span = (
            reference_dataset.center_time - time_buffer,
            reference_dataset.center_time + time_buffer,
        )

        req_datasets = dc.find_datasets(
            product=reference_product, geopolygon=geopolygon, time=time_span
        )
        _log.debug("Loading datasets:")
        for ds_ in req_datasets:
            _log.debug(f"\t{ds_.id}")

        _log.debug(f"Going to load {len(req_datasets)} datasets")
        # There really shouldn't be more than nine of these.
        # But, they sometimes split into two scenes per tile in
        # collection 2. So we'll insist there's <= 18.
        assert len(req_datasets) <= 18
        reference_scene = dc.load(
            product=reference_product,
            geopolygon=geopolygon,
            time=time_span,
            output_crs=output_crs,
            resolution=resolution,
        )

    _log.info(f"Reference scene is {reference_scene.sizes}")

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

    # Build the enumerated polygon raster.
    polygon_raster = xr_rasterize(filtered_polygons_gdf, reference_scene, attr_col)

    # Load the images.
    if hasattr(plugin, "resampling"):
        resampling = plugin.resampling
    else:
        resampling = "nearest"

    bands = {}
    for product, measurements in plugin.input_products.items():
        for band in measurements:
            assert band not in bands, f"Duplicate band: {product}{band}"
        query = dict(
            measurements=measurements,
            output_crs=output_crs,
            resolution=resolution,
            resampling=resampling,
        )
        if not overedge:
            query["datasets"] = [datasets[product]]
        else:
            query["product"] = product
            query["geopolygon"] = geopolygon
            query["time"] = time_span
            query["group_by"] = "solar_day"
        _log.debug(f"Query: {repr(query)}")
        da = dc.load(**query)
        for band in measurements:
            bands[band] = da[band]
    ds = xr.Dataset(bands).isel(time=0)

    # Transform the data.
    # Force warnings to raise exceptions.
    # This means users have to explicitly ignore warnings.
    with warnings.catch_warnings():
        warnings.filterwarnings("error")
        ds_transformed = plugin.transform(ds)
    transformed_bands = list(ds_transformed.keys())

    # For each polygon, perform the summary.
    summaries = {}  # ID -> summary

    # Instead of masking for each polygon,
    # find _all_ polygon indices at once.
    flat_bands = xr.Dataset(
        data_vars={
            band: xr.DataArray(ds_transformed[band].values.ravel(), dims=["idx"])
            for band in transformed_bands
        }
    )
    flat_ids = polygon_raster.values.ravel()

    id_to_indexes = collections.defaultdict(list)  # id -> [idx]
    for i, v in enumerate(flat_ids):
        if v > 0:
            id_to_indexes[v].append(i)

    for oid in id_to_indexes:
        if oid == 0:
            continue

        values = flat_bands.isel(idx=id_to_indexes[oid])
        # Force warnings to raise exceptions.
        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            summary = plugin.summarise(values, resolution)
        # Convert that summary (a 0d dataset) into a dict.
        summary = dataset_to_dict(summary)
        summaries[oid] = summary

    def map_with_err(k):
        # KeyError if the index is missing
        return conflux_one_index_to_id[k]

    summary_df = pd.DataFrame({conflux_one_index_to_id[int(k)]: summaries[k] for k in summaries}).T

    # Merge in the edge information.
    if partial and not overedge:
        summary_df = summary_df.join(
            # left join only includes objects with some
            # representation in the scene
            intersection_features,
            how="left",
        )

    return summary_df
