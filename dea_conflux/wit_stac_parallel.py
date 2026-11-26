"""Experimental parallel scheduling for independent STAC WIT windows."""

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from dea_conflux.wit_multiscene import time_windows
from dea_conflux.wit_stac import run_windows


def run_windows_parallel(scenes, geometry, cache_dir, crs="EPSG:3577", resolution=30,
                         tile_size=1024, window_days=15, max_workers=2):
    """Run independent chronological windows concurrently, preserving their order."""
    if not scenes:
        raise ValueError("No matching Landsat, FC and WOfS scenes found")
    if not isinstance(max_workers, int) or isinstance(max_workers, bool) or max_workers < 1:
        raise ValueError("max_workers must be a positive integer")
    items = [(scene.time, scene) for scene in scenes]
    windows = [[scene for _, scene in window] for window in time_windows(items, window_days)]

    def run(window):
        return run_windows(window, geometry, cache_dir, crs=crs, resolution=resolution,
                           tile_size=tile_size, window_days=window_days)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        frames = list(executor.map(run, windows))
    return pd.concat(frames, ignore_index=True)
