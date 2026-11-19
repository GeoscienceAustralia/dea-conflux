"""STAC-based, resumable multi-scene Wetlands Insight Tool processing.

This is independent of the Datacube-index based Conflux drill. Scene products
are matched before loading; each window is composited on a common tiled grid.
"""

import datetime as dt
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import odc.stac
import pandas as pd
import rasterio.features
from odc.geo import resyx_
from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry
from shapely.geometry import box, shape

from dea_conflux.wit_multiscene import time_windows

logger = logging.getLogger(__name__)

ARD_PRODUCTS = ("ga_ls5t_ard_3", "ga_ls7e_ard_3", "ga_ls8c_ard_3", "ga_ls9c_ard_3")
FC_PRODUCT = "ga_ls_fc_3"
WO_PRODUCT = "ga_ls_wo_3"
REFLECTANCE = tuple(
    f"nbart_{band}" for band in ("blue", "green", "red", "nir", "swir_1", "swir_2")
)
RESULT_BANDS = ("water", "wet", "pv", "npv", "bs")
# Change this whenever the algorithm or checkpoint schema changes.
ALGORITHM_VERSION = "wit-stac-1"


@dataclass(frozen=True)
class Scene:
    time: dt.datetime
    ard: object
    fc: object
    water: object


def _scene_key(item):
    properties = item.properties
    platform = properties.get("platform")
    region = properties.get("odc:region_code")
    if not platform or not region or item.datetime is None:
        raise ValueError(f"STAC item {item.id} lacks platform, region or acquisition time")
    return platform, region, item.datetime


def _allowed_satellite(item):
    """Apply wit_tooling's historical Landsat 5/7 selection; include Landsat 9."""
    date = item.datetime.date()
    platform = item.properties["platform"]
    if platform == "landsat-5":
        return date <= dt.date(1999, 12, 31) or dt.date(2003, 1, 1) <= date <= dt.date(2011, 12, 31)
    if platform == "landsat-7":
        return date <= dt.date(2003, 5, 31) or dt.date(2010, 1, 1) <= date <= dt.date(2013, 5, 31)
    if platform == "landsat-8":
        return date >= dt.date(2013, 1, 1)
    return platform == "landsat-9"


def search_scenes(catalog, polygon, start, end):
    """Find complete ARD/FC/WOfS scene triplets. End date is exclusive."""
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    if start.tzinfo is not None or end.tzinfo is not None:
        raise ValueError("Use timezone-naive calendar dates")
    if start >= end:
        raise ValueError("The exclusive end date must be after the start date")
    footprint = polygon.to_crs("EPSG:4326")
    bbox = list(footprint.boundingbox)
    interval = f"{start.isoformat()}Z/{(end - pd.Timedelta(microseconds=1)).isoformat()}Z"
    products = [*ARD_PRODUCTS, FC_PRODUCT, WO_PRODUCT]
    grouped = {name: {} for name in products}
    for item in catalog.search(
        collections=products, bbox=bbox, datetime=interval, method="GET", limit=500
    ).items():
        product = item.collection_id
        if product not in grouped:
            raise ValueError(f"Unexpected STAC collection {product}")
        if not shape(item.geometry).intersects(footprint.geom):
            continue
        key = _scene_key(item)
        if not _allowed_satellite(item):
            continue
        if key in grouped[product]:
            raise ValueError(f"Multiple {product} STAC items for scene {key}")
        grouped[product][key] = item

    scenes = []
    skipped = 0
    for product in ARD_PRODUCTS:
        for key, ard in grouped[product].items():
            if key not in grouped[FC_PRODUCT] or key not in grouped[WO_PRODUCT]:
                skipped += 1
                continue
            scenes.append(Scene(ard.datetime, ard, grouped[FC_PRODUCT][key], grouped[WO_PRODUCT][key]))
    if skipped:
        logger.warning("Skipped %d ARD scenes without matching FC and WOfS", skipped)
    scenes.sort(key=lambda scene: (scene.time, scene.ard.id))
    return scenes


def _load(item, bands, tile, resampling):
    data = odc.stac.load(
        [item], bands=bands, geobox=tile, resampling=resampling,
        fail_on_error=True,
    )
    if data.sizes.get("time") != 1:
        raise ValueError(f"Expected one time step for STAC item {item.id}")
    return data.isel(time=0, drop=True)


def _not_nodata(array):
    values = array.values
    valid = np.isfinite(values)
    nodata = array.attrs.get("nodata")
    if nodata is not None and not np.isnan(nodata):
        valid &= values != nodata
    return valid


def _scene_pixels(scene, tile, available):
    water = _load(scene.water, ["water"], tile, "nearest")["water"]
    water_values = water.values
    quality = _not_nodata(water) & ((water_values & 0b01100011) == 0) & available
    open_water = quality & ((water_values & 128) != 0)
    land_candidate = quality & ~open_water
    if not land_candidate.any():
        return open_water, np.zeros(available.shape, bool), None, None

    ard = _load(scene.ard, [*REFLECTANCE, "oa_fmask"], tile, {
        "oa_fmask": "nearest", "*": "bilinear",
    })
    valid_land = land_candidate & np.isin(ard.oa_fmask.values, (1, 5))
    for band in REFLECTANCE:
        valid_land &= _not_nodata(ard[band])
    if not valid_land.any():
        return open_water, valid_land, None, None

    tcw = (
        0.0315 * ard.nbart_blue.values + 0.2021 * ard.nbart_green.values
        + 0.3102 * ard.nbart_red.values + 0.1594 * ard.nbart_nir.values
        - 0.6806 * ard.nbart_swir_1.values - 0.6109 * ard.nbart_swir_2.values
    )
    wet = valid_land & (tcw >= -350)
    dry = valid_land & ~wet
    fc = _load(scene.fc, ["pv", "npv", "bs"], tile, "nearest") if dry.any() else None
    return open_water, wet, dry, fc


def composite_window(scenes, geometry, crs="EPSG:3577", resolution=30, tile_size=1024):
    """Return polygon means for a window, using the first valid class per pixel."""
    if not scenes or resolution <= 0 or tile_size <= 0:
        raise ValueError("A non-empty window, positive resolution and tile size are required")
    polygon = Geometry(geometry, crs)
    grid = GeoBox.from_geopolygon(polygon, resolution=resyx_(-resolution, resolution))
    footprints = [
        Geometry(shape(scene.ard.geometry), "EPSG:4326").to_crs(crs).geom
        for scene in scenes
    ]
    totals = dict.fromkeys(RESULT_BANDS, 0.0)
    total = valid_count = 0
    processed_tiles = 0
    for y in range(0, grid.shape.y, tile_size):
        for x in range(0, grid.shape.x, tile_size):
            tile = grid[y:y + tile_size, x:x + tile_size]
            inside = rasterio.features.rasterize(
                [(geometry, 1)], out_shape=tile.shape, transform=tile.transform,
                all_touched=True, dtype="uint8",
            ).astype(bool)
            if not inside.any():
                continue
            processed_tiles += 1
            if processed_tiles % 20 == 0:
                logger.info("Compositing tile %d", processed_tiles)
            total += int(inside.sum())
            filled = np.zeros(inside.shape, dtype=bool)
            tile_extent = box(*tile.extent.boundingbox)
            for scene, footprint in zip(scenes, footprints):
                if not footprint.intersects(tile_extent):
                    continue
                open_water, wet, dry, fc = _scene_pixels(scene, tile, inside & ~filled)
                totals["water"] += int(open_water.sum())
                totals["wet"] += int(wet.sum())
                if dry is not None and dry.any():
                    for band in ("pv", "npv", "bs"):
                        values = fc[band].values
                        valid_fc = dry & _not_nodata(fc[band])
                        totals[band] += float(values[valid_fc].sum(dtype="float64")) / 100
                    filled |= dry
                filled |= open_water | wet
                if np.all(filled | ~inside):
                    break
            valid_count += int(filled.sum())
    if total == 0:
        raise ValueError("Polygon contains no raster pixels")
    result = {
        band: totals[band] / valid_count if valid_count else float("nan")
        for band in RESULT_BANDS
    }
    result["pc_missing"] = 1 - valid_count / total
    return result


def _checkpoint_path(cache_dir, geometry, crs, resolution, scenes):
    payload = {
        "algorithm": ALGORITHM_VERSION,
        "geometry": geometry.wkb_hex,
        "crs": str(crs),
        "resolution": resolution,
        "scenes": [[scene.ard.id, scene.fc.id, scene.water.id] for scene in scenes],
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return Path(cache_dir) / f"{digest}.json"


def run_windows(scenes, geometry, cache_dir, crs="EPSG:3577", resolution=30,
                tile_size=1024, window_days=15):
    """Compute or resume one independently checkpointed result per time window."""
    if not scenes:
        raise ValueError("No matching Landsat, FC and WOfS scenes found")
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    items = [(scene.time, scene) for scene in scenes]
    rows = []
    for number, window in enumerate(time_windows(items, window_days), start=1):
        window_scenes = [scene for _, scene in window]
        path = _checkpoint_path(cache_dir, geometry, crs, resolution, window_scenes)
        if path.exists():
            with path.open() as stream:
                row = json.load(stream)
            status = "cached"
        else:
            print(f"Computing window {number}: {window[0][0].date()}, "
                  f"{len(window)} scenes", flush=True)
            row = composite_window(window_scenes, geometry, crs, resolution, tile_size)
            row["date"] = window[0][0].isoformat()
            row["scene_count"] = len(window)
            tmp = path.with_suffix(f".{os.getpid()}.tmp")
            try:
                with tmp.open("w") as stream:
                    json.dump(row, stream)
                os.replace(tmp, path)
            finally:
                if tmp.exists():
                    tmp.unlink()
            status = "computed"
        rows.append(row)
        print(f"Window {number}: {row['date'][:10]}, {len(window)} scenes, "
              f"{row['pc_missing']:.1%} missing ({status})", flush=True)
    result = pd.DataFrame(rows)
    result["date"] = pd.to_datetime(result["date"], utc=True).dt.tz_localize(None)
    return result


def normalise_results(frame):
    """Normalise FC means to the portion not classified as water or wet."""
    result = frame.copy()
    vegetation_area = 1 - result.water - result.wet
    vegetation_sum = result.pv + result.npv + result.bs
    for band in ("pv", "npv", "bs"):
        result[f"norm_{band}"] = result[band]
        mask = vegetation_sum > 0
        result.loc[mask, f"norm_{band}"] = (
            result.loc[mask, band] / vegetation_sum[mask] * vegetation_area[mask]
        )
    undefined = (vegetation_sum == 0) & (vegetation_area > 0)
    if undefined.any():
        logger.warning("Cannot normalise %d observations with zero vegetation total", undefined.sum())
        result.loc[undefined, ["norm_pv", "norm_npv", "norm_bs"]] = np.nan
    return result


def plot_results(frame, polygon_name, png_name):
    """Display and save a normalised WIT graph, excluding >=10% missing."""
    from dea_conflux.stack import display_wit_stack_with_df

    graph = frame.loc[frame.pc_missing < 0.1].copy()
    if graph.empty:
        logger.warning("No observations with less than 10%% missing for %s", polygon_name)
        graph = frame.copy()
        graph.loc[:, ["water", "wet", "norm_pv", "norm_npv", "norm_bs"]] = np.nan
        polygon_name = f"{polygon_name} (no observations with less than 10% missing)"
    elif len(graph) == 1:
        polygon_name = f"{polygon_name} (single observation)"
    if len(graph) == 1:
        graph = pd.concat([graph, graph], ignore_index=True)
        graph.loc[0, "date"] -= pd.Timedelta(hours=12)
        graph.loc[1, "date"] += pd.Timedelta(hours=12)
    labels = "months" if graph.date.max() - graph.date.min() < pd.Timedelta(days=365) else "years"
    display_wit_stack_with_df(graph, polygon_name, str(png_name), x_axis_labels=labels)
