"""Run a polygon drill step on a scene.

Matthew Alger, Vanessa Newey
Geoscience Australia
2021
"""

from types import ModuleType
from typing import Union
import logging
import warnings

import datacube
from datacube.utils.geometry import assign_crs
import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio.features
import xarray as xr

from dea_conflux.types import CRS

logger = logging.getLogger(__name__)


def xr_rasterise(gdf: gpd.GeoDataFrame,
                 da: Union[xr.DataArray, xr.Dataset],
                 attribute_col: str) -> xr.DataArray:    
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
        Vectors to rasterise.
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

    # Reproject shapefile to match CRS of raster
    logger.debug(
        f'Rasterizing to match xarray.DataArray dimensions ({y}, {x})')

    try:
        gdf_reproj = gdf.to_crs(crs=crs)
    except TypeError:
        # Sometimes the crs can be a datacube utils CRS object
        # so convert to string before reprojecting
        gdf_reproj = gdf.to_crs(crs={'init': str(crs)})

    # Use the geometry and attributes from `gdf` to create an iterable
    shapes = zip(gdf_reproj.geometry, gdf_reproj[attribute_col])

    # Rasterise shapes into an array
    arr = rasterio.features.rasterize(shapes=shapes,
                                      out_shape=(y, x),
                                      transform=transform)

    # Convert result to a xarray.DataArray
    xarr = xr.DataArray(arr,
                        coords=xy_coords,
                        dims=dims,
                        attrs=da.attrs,
                        name='polygons')

    # Add back crs if xarr.attrs doesn't have it
    if xarr.geobox is None:
        xarr = assign_crs(xarr, str(crs))

    return xarr


def find_datasets(
        dc: datacube.Datacube,
        plugin: ModuleType,
        uuid: str) -> [datacube.model.Dataset]:
    """Find the datasets that a plugin requires given a related scene UUID.

    Arguments
    ---------
    dc : Datacube
        A Datacube to search.
    plugin : module
        Plugin defining input products.

    uuid : str
        UUID of scene to look up.

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
                product=input_product,
                geopolygon=metadata.extent,
                time=metadata.center_time)
        assert len(datasets_) == 1, "Found multiple datasets at same time"
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
    return {key: val['data']
            for key, val in ds.to_dict()['data_vars'].items()}


def drill(
        plugin: ModuleType,
        shapefile: Union[str, gpd.GeoDataFrame],
        uuid: str,
        id_field: str,
        crs: CRS,
        partial=True,
        dc: datacube.Datacube = None) -> pd.DataFrame:
    """Perform a polygon drill.

    Arguments
    ---------
    plugin : module
        Plugin to drill with.

    shapefile : str / GeoDataFrame
        Either a path to a shapefile or a shapefile loaded into GeoPandas.

    uuid : str
        ID of scene to process.

    id_field : str
        The name of the ID field in the shapefile.

    crs : datacube.utils.geometry.CRS
        CRS to output to.

    partial : bool
        Optional (True). Whether to include polygons that partially
        overlap with the scene.

    dc : datacube.Datacube
        Optional existing Datacube.

    Returns
    -------
    Drill table : pd.DataFrame
        Index = polygon ID
        Columns = output bands
    """

    if not partial:
        raise NotImplementedError()

    # Get a datacube if we don't have one already.
    if not dc:
        dc = datacube.Datacube(app='dea-conflux-drill')

    # Open the shapefile if it's not already open.
    try:
        shapefile = gpd.read_file(shapefile)
    except AttributeError:
        # Must have already been open.
        pass

    shapefile = shapefile.set_index(id_field)

    # Assign a one-indexed numeric column for the polygons.
    # This will allow us to build a polygon enumerated raster.
    attr_col = 'one_index'
    shapefile[attr_col] = range(1, len(shapefile.index) + 1)
    one_index_to_id = {v: k for k, v in shapefile[attr_col].to_dict().items()}

    # Get required datasets.
    datasets = find_datasets(dc, plugin, uuid)

    # Load the image of the input scene so we can build the raster.
    # TODO(MatthewJA): If this is also a dataset required for drilling,
    # we will load it twice - and even worse, we'll load it with
    # more bands than we need the first time! Ignore for MVP.
    reference_dataset = dc.index.datasets.get(uuid)
    reference_scene = dc.load(datasets=[reference_dataset])

    # Build the enumerated polygon raster.
    polygon_raster = xr_rasterise(shapefile, reference_scene, attr_col)

    # Load the images.
    resampling = 'nearest'
    if hasattr(plugin, 'resampling'):
        resampling = plugin.resampling

    bands = {}
    for product, measurements in plugin.input_products.items():
        for band in measurements:
            assert band not in bands, "Duplicate band: {}{}".format(
                product, band
            )
        da = dc.load(datasets=[datasets[product]],
                     measurements=measurements,
                     crs=crs, resampling=resampling)
        for band in measurements:
            bands[band] = da[band]
    ds = xr.Dataset(bands).isel(time=0)

    # Transform the data.
    # Force warnings to raise exceptions.
    # This means users have to explicitly ignore warnings.
    with warnings.catch_warnings():
        warnings.filterwarnings('error')
        ds_transformed = plugin.transform(ds)

    # For each polygon, perform the summary.
    summaries = {}  # ID -> summary
    ids_in_range = np.unique(polygon_raster)
    for oid in ids_in_range:
        if oid == 0:
            continue

        mask = polygon_raster == oid
        values = {band: ds_transformed[band].values[mask]
                  for band in bands}
        values = xr.Dataset(
            data_vars={k: xr.DataArray(v) for k, v in values.items()})
        # Force warnings to raise exceptions.
        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            summary = plugin.summarise(values)
        # Convert that summary (a 0d dataset) into a dict.
        summary = dataset_to_dict(summary)
        summaries[oid] = summary

    summary_df = pd.DataFrame(summaries).T
    summary_df.index = summary_df.index.map(one_index_to_id)

    return summary_df
