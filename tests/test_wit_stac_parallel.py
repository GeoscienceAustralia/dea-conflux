import datetime as dt
import threading
from types import SimpleNamespace

import pandas as pd
import pytest
from shapely.geometry import box

from dea_conflux import wit_stac
from dea_conflux.wit_stac_parallel import run_windows_parallel


def make_scenes():
    time = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)
    scenes = []
    for day in (0, 3, 15, 32):
        ard = SimpleNamespace(id=f"ard-{day}")
        fc = SimpleNamespace(id=f"fc-{day}")
        water = SimpleNamespace(id=f"water-{day}")
        scenes.append(wit_stac.Scene(time + dt.timedelta(days=day), ard, fc, water))
    return scenes


def test_parallel_matches_serial_and_reuses_checkpoints(tmp_path, monkeypatch):
    scenes = make_scenes()
    geometry = box(0, 0, 60, 30)
    calls = []

    def composite(window, geometry, crs, resolution, tile_size):
        calls.append(window[0].time)
        return {"water": 0.1, "wet": 0.2, "pv": 0.3, "npv": 0.2, "bs": 0.2,
                "pc_missing": 0.0}

    monkeypatch.setattr(wit_stac, "composite_window", composite)
    serial = wit_stac.run_windows(scenes, geometry, tmp_path / "serial")
    parallel = run_windows_parallel(scenes, geometry, tmp_path / "parallel")
    pd.testing.assert_frame_equal(serial, parallel)
    assert len(calls) == 6
    pd.testing.assert_frame_equal(
        parallel, run_windows_parallel(scenes, geometry, tmp_path / "parallel")
    )
    assert len(calls) == 6


def test_parallel_starts_independent_windows_together(tmp_path, monkeypatch):
    barrier = threading.Barrier(2)

    def composite(window, geometry, crs, resolution, tile_size):
        barrier.wait(timeout=10)
        return {"water": 1.0, "wet": 0.0, "pv": 0.0, "npv": 0.0, "bs": 0.0,
                "pc_missing": 0.0}

    monkeypatch.setattr(wit_stac, "composite_window", composite)
    result = run_windows_parallel(make_scenes()[:3], box(0, 0, 60, 30), tmp_path)
    assert result.scene_count.tolist() == [2, 1]


def test_parallel_rejects_bad_inputs_and_propagates_errors(tmp_path, monkeypatch):
    geometry = box(0, 0, 60, 30)
    with pytest.raises(ValueError, match="No matching"):
        run_windows_parallel([], geometry, tmp_path)
    with pytest.raises(ValueError, match="max_workers"):
        run_windows_parallel(make_scenes(), geometry, tmp_path, max_workers=0)

    def fail(*args):
        raise RuntimeError("composite failed")

    monkeypatch.setattr(wit_stac, "composite_window", fail)
    with pytest.raises(RuntimeError, match="composite failed"):
        run_windows_parallel(make_scenes(), geometry, tmp_path)
