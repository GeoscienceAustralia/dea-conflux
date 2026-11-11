"""Polygon-based, tiled multi-scene WIT composites."""

import datetime
import logging
import uuid
import warnings
from collections import defaultdict

import datacube
import geohash
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio.features
import xarray as xr
from odc.geo import resyx_
from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry

from dea_conflux import io

logger = logging.getLogger(__name__)
WIT_BANDS = ("water", "wet", "bs", "pv", "npv")


def validate_plugin(plugin):
    """Limit this workflow to the WIT plugin input and output contract."""
    ard = [name for name in plugin.input_products if "_ard_" in name]
    required = {"ga_ls_wo_3", "ga_ls_fc_3"}
    if (
        not plugin.product_name.startswith("wit_")
        or len(ard) != 1
        or set(plugin.input_products) != required | set(ard)
        or "water" not in plugin.input_products["ga_ls_wo_3"]
        or not {"bs", "pv", "npv"}.issubset(plugin.input_products["ga_ls_fc_3"])
        or not {
            "nbart_blue", "nbart_green", "nbart_red", "nbart_nir",
            "nbart_swir_1", "nbart_swir_2",
        }.issubset(plugin.input_products[ard[0]])
    ):
        raise ValueError("Multi-scene mode requires a compatible wit_* Landsat plugin")
    return ard[0]


def observations(dc, plugin, polygon, start, end):
    """Join derived products by exact acquisition time within one polygon."""
    reference = validate_plugin(plugin)
    grouped = {}
    for product in plugin.input_products:
        by_time = defaultdict(list)
        for dataset in dc.find_datasets(
            product=product, geopolygon=polygon, time=(start, end)
        ):
            by_time[dataset.center_time].append(dataset)
        grouped[product] = by_time

    result = []
    for timestamp in sorted(grouped[reference]):
        missing = [product for product in grouped if timestamp not in grouped[product]]
        if missing:
            logger.warning(
                "Skipping observation %s: missing products %s",
                timestamp, ", ".join(missing),
            )
            continue
        datasets = {
            product: sorted(groups[timestamp], key=lambda ds: str(ds.id))
            for product, groups in grouped.items()
        }
        result.append((timestamp, datasets))
    return result


def time_windows(items, days):
    """Partition ordered observations into non-overlapping, start-anchored windows."""
    if days < 1:
        raise ValueError("Composite window must be at least one day")
    index = 0
    while index < len(items):
        end = index + 1
        limit = items[index][0] + datetime.timedelta(days=days)
        while end < len(items) and items[end][0] < limit:
            end += 1
        yield items[index:end]
        index = end


def _load_observation(dc, plugin, datasets, tile):
    bands = {}
    for product, measurements in plugin.input_products.items():
        loaded = dc.load(
            datasets=datasets[product],
            measurements=measurements,
            like=tile,
            resampling=getattr(plugin, "resampling", "nearest"),
            skip_broken_datasets=False,
        )
        if loaded.sizes.get("time") != 1:
            raise ValueError(
                f"Expected one acquisition time for {product}, got {loaded.sizes.get('time', 0)}"
            )
        for name in measurements:
            if name in bands:
                raise ValueError(f"Duplicate measurement: {name}")
            bands[name] = loaded[name].isel(time=0, drop=True)
    return xr.Dataset(bands)


def _valid_pixels(inputs):
    water = inputs["water"]
    valid_water = (water & 0b01100011) == 0
    water_nodata = water.attrs.get("nodata")
    if water_nodata is not None:
        valid_water = valid_water & (water != water_nodata)
    open_water = (water & 128) != 0
    valid_land = xr.ones_like(valid_water, dtype=bool)
    for name in inputs.data_vars:
        if name == "water":
            continue
        band = inputs[name]
        valid_land = valid_land & np.isfinite(band)
        nodata = band.attrs.get("nodata")
        if nodata is not None and not np.isnan(nodata):
            valid_land = valid_land & (band != nodata)
    return valid_water & (open_water | valid_land)


def composite_summary(dc, plugin, geometry, crs, window, tile_size=1024):
    """Composite first valid pixels and accumulate WIT means without a full raster."""
    if tile_size < 1:
        raise ValueError("Tile size must be positive")
    geobox = GeoBox.from_geopolygon(
        Geometry(geometry, crs), resolution=resyx_(*plugin.resolution)
    )
    sums = {band: 0.0 for band in WIT_BANDS}
    counts = {band: 0 for band in WIT_BANDS}
    total = valid_count = 0

    for y in range(0, geobox.shape.y, tile_size):
        for x in range(0, geobox.shape.x, tile_size):
            tile = geobox[y:y + tile_size, x:x + tile_size]
            inside = rasterio.features.rasterize(
                [(geometry, 1)],
                out_shape=tile.shape,
                transform=tile.transform,
                all_touched=True,
                dtype="uint8",
            ).astype(bool)
            if not inside.any():
                continue
            total += int(inside.sum())
            filled = np.zeros(inside.shape, dtype=bool)
            tile_values = {
                band: np.full(inside.shape, np.nan, dtype="float64")
                for band in WIT_BANDS
            }
            for _, datasets in window:
                inputs = _load_observation(dc, plugin, datasets, tile)
                valid = _valid_pixels(inputs).values & inside & ~filled
                if not valid.any():
                    continue
                with warnings.catch_warnings():
                    warnings.filterwarnings("error")
                    transformed = plugin.transform(inputs)
                missing = set(WIT_BANDS) - set(transformed.data_vars)
                if missing:
                    raise ValueError(f"WIT transform missing output bands: {sorted(missing)}")
                for band in WIT_BANDS:
                    tile_values[band][valid] = transformed[band].values[valid]
                filled |= valid
                if np.all(filled | ~inside):
                    break
            valid_count += int(filled.sum())
            for band, values in tile_values.items():
                finite = np.isfinite(values)
                sums[band] += float(values[finite].sum())
                counts[band] += int(finite.sum())

    if total == 0:
        raise ValueError("Polygon contains no raster pixels")
    summary = {
        band: sums[band] / counts[band] if counts[band] else float("nan")
        for band in WIT_BANDS
    }
    summary["pc_missing"] = 1 - valid_count / total
    return summary


def run(plugin, polygons, start, end, output, days=15, tile_size=1024, dc=None):
    """Write one compatible Parquet table per polygon and observation window."""
    validate_plugin(plugin)
    if start > end:
        raise ValueError("Start date must not be after end date")
    if days < 1 or tile_size < 1:
        raise ValueError("Window and tile size must be positive")
    if polygons.empty:
        raise ValueError("Shapefile contains no polygons")
    if not polygons.index.is_unique or polygons.index.hasnans:
        raise ValueError("Polygon IDs must be unique and non-null")
    if len({str(value) for value in polygons.index}) != len(polygons):
        raise ValueError("Polygon IDs must have unique string representations")
    dc = dc if dc is not None else datacube.Datacube(app="dea-conflux-wit-multiscene")
    paths = []
    # Datacube time intervals are inclusive. Include the whole final calendar day.
    end_time = end + datetime.timedelta(days=1) - datetime.timedelta(microseconds=1)
    for feature_id, row in polygons.iterrows():
        geometry = row.geometry
        if geometry is None or geometry.is_empty or geometry.geom_type not in ("Polygon", "MultiPolygon"):
            raise ValueError(f"Feature {feature_id!r} is not a non-empty polygon")
        polygon = Geometry(geometry, str(polygons.crs))
        items = observations(dc, plugin, polygon, start, end_time)
        if not items:
            logger.warning("No complete WIT observations for polygon %s", feature_id)
            continue
        centroid = gpd.GeoSeries([geometry.centroid], crs=polygons.crs).to_crs(4326).iloc[0]
        wetland_id = geohash.encode(centroid.y, centroid.x, precision=12)
        for window in time_windows(items, days):
            timestamp = window[0][0]
            summary = composite_summary(dc, plugin, geometry, str(polygons.crs), window, tile_size)
            summary["geohash_wetland_id"] = wetland_id
            table = pd.DataFrame([summary], index=pd.Index([feature_id], name=polygons.index.name))
            sources = ",".join(
                str(dataset.id)
                for _, datasets in window
                for product in sorted(datasets)
                for dataset in datasets[product]
            )
            key = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"dea-conflux-wit-multiscene:{plugin.product_name}:{plugin.version}:"
                f"{feature_id}:{geometry.wkb_hex}:{days}:{timestamp.isoformat()}:{sources}",
            )
            paths.append(io.write_table(plugin.product_name, str(key), timestamp, table, output))
    return paths
