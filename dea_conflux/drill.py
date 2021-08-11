"""Run a polygon drill step on a scene.

Matthew Alger, Vanessa Newey
Geoscience Australia
2021
"""

from types import ModuleType
from typing import Union
import logging

import datacube
from datacube.utils.geometry import assign_crs
import pandas as pd
import geopandas as gpd
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
    datasets = []
    for input_product in plugin.input_products:
        datasets.extend(
            dc.find_datasets(
                product=input_product,
                geopolygon=metadata.extent,
                time=metadata.center_time))
    return datasets


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
    shapefile['one_index'] = range(1, len(shapefile.index) + 1)

    # Get required datasets.
    datasets = find_datasets(plugin, uuid)

    # Build the enumerated raster.
