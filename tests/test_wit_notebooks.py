import builtins
import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace

import matplotlib
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from odc.geo.geom import Geometry
from shapely.geometry import box, mapping


matplotlib.use("Agg")
NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"


@pytest.mark.parametrize(
    "filename,parallel",
    [
        ("DEA_Wetlands_Insight_Tool_Multiscene.ipynb", False),
        ("WIT_multiscene_parallelised.ipynb", True),
    ],
)
def test_standalone_notebook_processing(filename, parallel, tmp_path, monkeypatch):
    """Run the notebook's helpers without allowing any Conflux imports."""
    notebook = json.loads((NOTEBOOKS / filename).read_text())
    original_import = builtins.__import__

    def independent_import(name, *args, **kwargs):
        if name == "dea_conflux" or name.startswith("dea_conflux."):
            raise ImportError("The notebook must not import dea_conflux")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", independent_import)
    namespace = {"__name__": "__main__"}
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code" and "wit-helper" in cell["metadata"].get("tags", []):
            exec(compile("".join(cell["source"]), filename, "exec"), namespace)

    footprint = mapping(
        Geometry(box(-1000, -1000, 1000, 1000), "EPSG:3577").to_crs("EPSG:4326").geom
    )
    time = dt.datetime(2022, 1, 1, tzinfo=dt.timezone.utc)
    scenes = []
    for day in (0, 1, 16):
        ard = SimpleNamespace(id=f"ard-{day}", geometry=footprint)
        fc = SimpleNamespace(id=f"fc-{day}")
        water = SimpleNamespace(id=f"water-{day}")
        scenes.append(namespace["Scene"](time + dt.timedelta(days=day), ard, fc, water))

    def load(item, bands, tile, resampling):
        shape = tile.shape
        if bands == ["water"]:
            values = np.array([[128, 2]]) if item.id == "water-0" else np.zeros(shape, dtype="uint8")
            return xr.Dataset({"water": (("y", "x"), values, {"nodata": 1})})
        if "oa_fmask" in bands:
            return xr.Dataset({
                **{band: (("y", "x"), np.full(shape, 1000), {"nodata": -999})
                   for band in namespace["REFLECTANCE"]},
                "oa_fmask": (("y", "x"), np.ones(shape, dtype="uint8")),
            })
        return xr.Dataset({
            band: (("y", "x"), np.full(shape, value), {"nodata": 255})
            for band, value in (("pv", 20), ("npv", 30), ("bs", 50))
        })

    namespace["_load"] = load
    run = namespace["run_windows_parallel" if parallel else "run_windows"]
    result = run(scenes, box(0, 0, 60, 30), tmp_path / "checkpoints", tile_size=2)
    assert result.scene_count.tolist() == [2, 1]
    assert result.pc_missing.tolist() == [0, 0]
    assert result.loc[0, ["water", "wet", "pv", "npv", "bs"]].tolist() == pytest.approx(
        [0.5, 0, 0.1, 0.15, 0.25]
    )

    def fail_load(*args):
        raise AssertionError("A completed window should be loaded from its checkpoint")

    namespace["_load"] = fail_load
    cached = run(scenes, box(0, 0, 60, 30), tmp_path / "checkpoints", tile_size=2)
    pd.testing.assert_frame_equal(result, cached)
    normalised = namespace["normalise_results"](result)
    assert (normalised.water + normalised.wet + normalised.norm_pv
            + normalised.norm_npv + normalised.norm_bs).tolist() == pytest.approx([1, 1])
    namespace["plot_results"](normalised, "Test wetland", tmp_path / "test")
    assert (tmp_path / "test.png").is_file()
