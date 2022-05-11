"""Run a polygon drill step on a scene.

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
from typing import Union

import datacube
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio.features
import shapely.geometry
import tqdm
import xarray as xr
from datacube.utils.geometry import assign_crs

from dea_conflux.types import CRS

logger = logging.getLogger(__name__)


def xr_rasterise(
    gdf: gpd.GeoDataFrame, da: Union[xr.DataArray, xr.Dataset], attribute_col: str
) -> xr.DataArray:
    """
    Rasterizes a geopandas.GeoDataFrame into an xarray.DataArray.

    Cribbed from dea-tools:
    Krause, C., Dunn, B., Bishop-Taylor, R., Adams, C., Burton, C.,
    Alger, M., Chua, S., Phillips, C., Newey, V., Kouzoubov, K.,
    Leith, A., Ayers, D., Hicks, A., DEA Notebooks contributors 2021.
    Digital Earth Australia notebooks and tools repository.
    Geoscience Australia, Canberra. https://doi.org/10.26186/145234

    Arguments
    ----------
    gdf : geopandas.GeoDataFrame
        Vectors to rasterise in the same CRS as the da.
    da : xarray.DataArray / xarray.Dataset
        Template for raster.
    attribute_col : string
        Name of the attribute column that the pixels
        in the raster will contain.

    Returns
    -------
    xarray.DataArray
    """
    # Get the CRS
    crs = da.geobox.crs
    if crs is None:
        raise ValueError("da must have a CRS")
    assert crs == gdf.crs

    # Same for transform
    transform = da.geobox.transform
    if transform is None:
        raise TypeError("da must have a transform")

    # Grab the 2D dims (not time)
    dims = da.geobox.dims

    # Coords
    xy_coords = [da[dims[0]], da[dims[1]]]

    # Shape
    try:
        y, x = da.geobox.shape
    except ValueError:
        y, x = len(xy_coords[0]), len(xy_coords[1])

    logger.debug(f"Rasterizing to match xarray.DataArray dimensions ({y}, {x})")

    # Use the geometry and attributes from `gdf` to create an iterable
    shapes = zip(gdf.geometry, gdf[attribute_col])

    # Rasterise shapes into an array
    arr = rasterio.features.rasterize(
        shapes=shapes, out_shape=(y, x), transform=transform
    )

    # Convert result to a xarray.DataArray
    xarr = xr.DataArray(
        arr, coords=xy_coords, dims=dims, attrs=da.attrs, name="polygons"
    )

    # Add back crs if xarr.attrs doesn't have it
    if xarr.geobox is None:
        xarr = assign_crs(xarr, str(crs))

    return xarr


def _get_directions(og_geom, int_geom):
    """Helper to get direction of intersection between geometry, intersection.

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
        boundary_intersection_lines = list(boundary_intersections)
    except TypeError:
        # Not a multiline
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
                ys_poly = [c[1] for g in int_geom.boundary for c in g.coords]
            except TypeError:
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
                xs_poly = [c[0] for g in int_geom.boundary for c in g.coords]
            except TypeError:
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


def get_intersections(
    gdf: gpd.GeoDataFrame, extent: shapely.geometry.Polygon
) -> gpd.GeoDataFrame:
    """Find which polygons intersect with an extent and in what direction.

    Arguments
    ---------
    gdf : gpd.GeoDataFrame
        Set of polygons.

    extent : shapely.geometry.Polygon
        Extent polygon to check intersection against.

    Returns
    -------
    gpd.GeoDataFrame
        Table of intersections.
    """
    all_intersection = gdf.geometry.intersection(extent)
    # Which ones have decreased in area thanks to our intersection?
    intersects_mask = ~(all_intersection.area == 0)
    ratios = all_intersection.area / gdf.area
    directions = []
    dir_names = ["North", "South", "East", "West"]
    for ratio, intersects, idx in zip(ratios, intersects_mask, ratios.index):
        # idx is index into gdf
        if not intersects or ratio == 1:
            directions.append({d: False for d in dir_names})
            continue
        og_geom = gdf.loc[idx].geometry
        # Buffer to dodge some bad geometry behaviour
        int_geom = all_intersection.loc[idx].buffer(0)
        dirs = _get_directions(og_geom, int_geom)
        directions.append({d: d in dirs for d in dir_names})
        assert any(directions[-1].values())
    return pd.DataFrame(directions, index=ratios.index)


def find_datasets(
    dc: datacube.Datacube, plugin: ModuleType, uuid: str, strict: bool = False
) -> [datacube.model.Dataset]:
    """Find the datasets that a plugin requires given a related scene UUID.

    Arguments
    ---------
    dc : Datacube
        A Datacube to search.
    plugin : module
        Plugin defining input products.
    uuid : str
        UUID of scene to look up.
    strict : bool
        Default False. Error on duplicate scenes (otherwise warn).

    Returns
    -------
    [Dataset]
        List of datasets.
    """
    # Load the metadata of the specified scene.
    metadata = dc.index.datasets.get(uuid)
    # Find the datasets that have the same centre time and
    # fall within this extent.
    datasets = {}
    for input_product in plugin.input_products:
        datasets_ = dc.find_datasets(
            product=input_product, geopolygon=metadata.extent, time=metadata.center_time
        )
        if len(datasets_) > 1:
            if strict:
                raise ValueError(f"Found multiple datasets at same time for {uuid}")
            else:
                warnings.warn(
                    f"Found multiple datasets at same time for {uuid}, "
                    "choosing one arbitrarily",
                    RuntimeWarning,
                )
        elif len(datasets_) == 0:
            raise ValueError("Found no datasets associated with given scene")
        datasets[input_product] = datasets_[0]
    return datasets


def dataset_to_dict(ds: xr.Dataset) -> dict:
    """Convert a 0d dataset into a dict.

    Arguments
    ---------
    ds : xr.Dataset

    Returns
    -------
    dict
    """
    return {key: val["data"] for key, val in ds.to_dict()["data_vars"].items()}


def filter_shapefile_full(
    gdf: gpd.GeoDataFrame, ds: datacube.model.Dataset
) -> gpd.GeoDataFrame:
    """Filter a shapefile to only include objects that are in a scene.

    Arguments
    ---------
    gdf : gpd.GeoDataFrame
    ds : datacube.model.Dataset

    Returns
    -------
    gpd.GeoDataFrame
    """
    # reproject the ds extent into gdf crs
    ext = gpd.GeoDataFrame(geometry=[ds.extent], crs=ds.crs).to_crs(gdf.crs).geometry[0]

    return gdf[gdf.geometry.intersects(ext)]


def filter_shapefile_quick(
    gdf: gpd.GeoDataFrame, ds: datacube.model.Dataset, buffer=True
) -> gpd.GeoDataFrame:
    """Filter a shapefile to only include nearby objects.

    Checks if centroids are in a (buffered) bounding box.

    Arguments
    ---------
    gdf : gpd.GeoDataFrame
    ds : datacube.model.Dataset
    buffer : bool
        Optional (True). Extend the bounding box by the
        width of a scene.

    Returns
    -------
    gpd.GeoDataFrame
    """
    # reproject the ds extent into gdf crs
    ext = gpd.GeoDataFrame(geometry=[ds.extent], crs=ds.crs).to_crs(gdf.crs).geometry[0]
    # e.g. (1494917.6079637874, -4008086.2291621473,
    #       1749149.241417757, -3774896.017328557)
    bbox = ext.bounds
    left, bottom, right, top = bbox
    centroids = gdf.centroid
    width = height = 0
    if buffer:
        width = right - left
        height = top - bottom
    included = (
        (centroids.x > (left - width))
        & (centroids.x < (right + width))
        & (centroids.y < (top + height))
        & (centroids.y > (bottom - height))
    )

    gdf = gdf[included]
    return gdf


def filter_shapefile_intersections(
    gdf: gpd.GeoDataFrame, ds: datacube.model.Dataset
) -> gpd.GeoDataFrame:
    """Filter a shapefile to remove objects more than 1 scene away.

    Arguments
    ---------
    gdf : gpd.GeoDataFrame
    ds : datacube.model.Dataset

    Returns
    -------
    gpd.GeoDataFrame
    """
    # reproject the ds extent into gdf crs
    ext = gpd.GeoDataFrame(geometry=[ds.extent], crs=ds.crs).to_crs(gdf.crs).geometry[0]
    # e.g. (1494917.6079637874, -4008086.2291621473,
    #       1749149.241417757, -3774896.017328557)
    bbox = ext.bounds
    left, bottom, right, top = bbox

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
    return gdf[~gdf.geometry.intersects(testbox.boundary)]


def filter_dataset(dss, shapefile, worker_num=1):
    """Use multi-process approach to run polygon_in_dataset method.
    Only keep the dataset id which can pass polygon_in_dataset check.

    Arguments
    ---------
    dss : [datacube.model.Dataset]
    shapefile : gpd.GeoDataFrame

    Returns
    -------
    filtered_datasets: [str]
    """
    with multiprocessing.Pool(processes=worker_num) as pool:
        filtered_datasets = list(
            tqdm.tqdm(pool.imap(partial(polygon_in_dataset, shapefile=shapefile), dss))
        )

    return [e for e in filtered_datasets if e]


def polygon_in_dataset(ds, shapefile):
    """Use method filter_shapefile_quick to filter out dataset which no
    polygon near it.

    Arguments
    ---------
    ds : datacube.model.Dataset
    shapefile : gpd.GeoDataFrame

    Returns
    -------
    ds.id: str
    """
    if len(filter_shapefile_quick(shapefile, ds)) > 0:
        if len(filter_shapefile_full(shapefile, ds)) > 0:
            return str(ds.id)
        else:
            return ""
    else:
        return ""


def drill(
    plugin: ModuleType,
    shapefile: gpd.GeoDataFrame,
    uuid: str,
    crs: CRS,
    resolution: (int, int),
    partial=True,
    overedge=False,
    dc: datacube.Datacube = None,
    time_buffer=datetime.timedelta(hours=1),
) -> pd.DataFrame:
    """Perform a polygon drill.

    Arguments
    ---------
    plugin : module
        Plugin to drill with.

    shapefile : GeoDataFrame
        A shapefile loaded into GeoPandas, in the correct CRS,
        with the ID as the index.

    uuid : str
        ID of scene to process.

    crs : datacube.utils.geometry.CRS
        CRS to output to.

    resolution : (int, int)
        Raster resolution in (-metres, metres).

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

    if overedge and not partial:
        warnings.warn("overedge=True expects partial=True")

    if not partial:
        raise NotImplementedError()

    assert str(shapefile.crs).lower() == str(crs).lower()

    # Get a datacube if we don't have one already.
    if not dc:
        dc = datacube.Datacube(app="dea-conflux-drill")

    # Assign a one-indexed numeric column for the polygons.
    # This will allow us to build a polygon enumerated raster.
    attr_col = "_conflux_one_index"
    # This mutates the (in-memory) shapefile, but that's OK.
    shapefile[attr_col] = range(1, len(shapefile.index) + 1)
    one_index_to_id = {v: k for k, v in shapefile[attr_col].to_dict().items()}

    # Get the dataset we asked for.
    reference_dataset = dc.index.datasets.get(uuid)

    # Filter out polygons that aren't anywhere near this scene.
    _n_initial = len(shapefile)
    shapefile = filter_shapefile_quick(
        shapefile,
        reference_dataset,
        # ...and limit it to centroids in this scene if not partial.  # noqa: E128
        buffer=partial,
    )
    _n_filtered_quick = len(shapefile)
    logger.debug(f"Quick filter removed {_n_initial - _n_filtered_quick} polygons")

    # Remove things outside the box.
    # We do this after the quick filter so the intersection is way faster.
    shapefile = filter_shapefile_full(shapefile, reference_dataset)
    _n_filtered_full = len(shapefile)
    logger.debug(f"Full filter removed {_n_filtered_quick - _n_filtered_full} polygons")

    # If overedge, remove anything which intersects with a 3-scene
    # width box.
    if overedge:
        shapefile = filter_shapefile_intersections(shapefile, reference_dataset)
        logger.debug(
            "Overedge filter removed {} polygons".format(
                _n_filtered_full - len(shapefile)
            )
        )

    if len(shapefile) == 0:
        logger.warning(f"No polygons found in scene {uuid}")
        return pd.DataFrame({})

    # Load the image of the input scene so we can build the raster.
    # TODO(MatthewJA): If this is also a dataset required for drilling,
    # we will load it twice - and even worse, we'll load it with
    # more bands than we need the first time! Ignore for MVP.
    if not overedge:
        # just load the scene we asked for
        reference_scene = dc.load(
            datasets=[reference_dataset], output_crs=crs, resolution=resolution
        )
        # and grab the datasets we want too
        datasets = find_datasets(dc, plugin, uuid)
    else:
        # search for all the datasets we need to cover the area
        # of the polygons.
        reference_product = reference_dataset.type.name
        geopolygon = datacube.utils.geometry.Geometry(
            shapely.geometry.box(*shapefile.total_bounds), crs=shapefile.crs
        )
        time_span = (
            reference_dataset.center_time - time_buffer,
            reference_dataset.center_time + time_buffer,
        )
        req_datasets = dc.find_datasets(
            product=reference_product, geopolygon=geopolygon, time=time_span
        )
        logger.debug("Loading datasets:")
        for ds_ in req_datasets:
            logger.debug(f"\t{ds_.id}")

        logger.debug(f"Going to load {len(req_datasets)} datasets")
        # There really shouldn't be more than nine of these.
        # But, they sometimes split into two scenes per tile in
        # collection 2. So we'll insist there's <= 18.
        assert len(req_datasets) <= 18
        reference_scene = dc.load(
            product=reference_product,
            geopolygon=geopolygon,
            time=time_span,
            output_crs=crs,
            resolution=resolution,
        )

    logger.info(f"Reference scene is {reference_scene.sizes}")

    # Detect intersections.
    # We only have to do this if partial and not overedge.
    # If not partial, then there can't be any intersections.
    # If overedge, then there are no partly observed polygons.
    if partial and not overedge:
        intersection_features = get_intersections(
            shapefile, reference_scene.extent.geom
        )
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
    polygon_raster = xr_rasterise(shapefile, reference_scene, attr_col)

    # Load the images.
    resampling = "nearest"
    if hasattr(plugin, "resampling"):
        resampling = plugin.resampling

    bands = {}
    for product, measurements in plugin.input_products.items():
        for band in measurements:
            assert band not in bands, f"Duplicate band: {product}{band}"
        query = dict(
            measurements=measurements,
            output_crs=crs,
            resolution=getattr(plugin, "resolution", None),
            resampling=resampling,
        )
        if not overedge:
            query["datasets"] = [datasets[product]]
        else:
            query["product"] = product
            query["geopolygon"] = geopolygon
            query["time"] = time_span
            query["group_by"] = "solar_day"
        logger.debug(f"Query: {repr(query)}")
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
            summary = plugin.summarise(values)
        # Convert that summary (a 0d dataset) into a dict.
        summary = dataset_to_dict(summary)
        summaries[oid] = summary

    def map_with_err(k):
        # KeyError if the index is missing
        return one_index_to_id[k]

    summary_df = pd.DataFrame(
        {one_index_to_id[int(k)]: summaries[k] for k in summaries}
    ).T

    # Merge in the edge information.
    if partial and not overedge:
        summary_df = summary_df.join(
            # left join only includes objects with some
            # representation in the scene
            intersection_features,
            how="left",
        )

    return summary_df
