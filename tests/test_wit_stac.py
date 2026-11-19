import datetime as dt
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from odc.geo.geom import Geometry
from shapely.geometry import box, mapping

from dea_conflux import wit_stac


UTC = dt.timezone.utc
TIME = dt.datetime(2022, 1, 1, tzinfo=UTC)


def item(product, name, time=TIME, platform="landsat-8"):
    return SimpleNamespace(
        collection_id=product, id=name, datetime=time,
        geometry=mapping(box(130, -40, 150, -20)),
        properties={"platform": platform, "odc:region_code": "098079"},
    )


def test_stac_scene_matching_and_historical_selection(caplog):
    a = item("ga_ls8c_ard_3", "ard")
    f = item("ga_ls_fc_3", "fc")
    w = item("ga_ls_wo_3", "wo")
    # Same time, wrong satellite: it must never be paired with the Landsat 8 ARD.
    wrong = item("ga_ls_wo_3", "wrong", platform="landsat-9")
    missing = item("ga_ls8c_ard_3", "missing", TIME + dt.timedelta(days=1))
    old_ls7 = item("ga_ls7e_ard_3", "slc-off", TIME, "landsat-7")
    catalog = SimpleNamespace(search=lambda **kwargs: SimpleNamespace(
        items=lambda: iter([a, f, w, wrong, missing, old_ls7])
    ))
    scenes = wit_stac.search_scenes(
        catalog, Geometry(box(138, -30, 140, -28), "EPSG:4326"),
        "2022-01-01", "2022-02-01",
    )
    assert len(scenes) == 1
    assert (scenes[0].ard.id, scenes[0].fc.id, scenes[0].water.id) == ("ard", "fc", "wo")
    assert "Skipped 1 ARD scenes" in caplog.text


def test_stac_duplicate_items_raise():
    a = item("ga_ls8c_ard_3", "ard")
    b = item("ga_ls8c_ard_3", "other-ard")
    catalog = SimpleNamespace(search=lambda **kwargs: SimpleNamespace(items=lambda: iter([a, b])))
    with pytest.raises(ValueError, match="Multiple"):
        wit_stac.search_scenes(
            catalog, Geometry(box(138, -30, 140, -28), "EPSG:4326"),
            "2022-01-01", "2022-02-01",
        )


@pytest.mark.parametrize(
    "platform,date,expected",
    [
        ("landsat-5", dt.datetime(2001, 1, 1, tzinfo=UTC), False),
        ("landsat-5", dt.datetime(2003, 1, 1, tzinfo=UTC), True),
        ("landsat-7", dt.datetime(2004, 1, 1, tzinfo=UTC), False),
        ("landsat-7", dt.datetime(2011, 1, 1, tzinfo=UTC), True),
        ("landsat-7", dt.datetime(2022, 1, 1, tzinfo=UTC), False),
        ("landsat-9", TIME, True),
    ],
)
def test_historical_satellite_selection(platform, date, expected):
    assert wit_stac._allowed_satellite(item("ard", "x", date, platform)) is expected


def test_scene_pixels_water_fmask_and_reflectance_nodata(monkeypatch):
    water = xr.Dataset({"water": (("y", "x"), [[128, 0, 0, 0, 2]], {"nodata": 1})})
    values = np.array([[1000, 1000, 0, -999, 1000]])
    ard = xr.Dataset({
        **{
            band: (("y", "x"), values, {"nodata": -999})
            for band in wit_stac.REFLECTANCE
        },
        "oa_fmask": (("y", "x"), [[0, 1, 1, 1, 1]]),
    })
    fc = xr.Dataset({
        name: (("y", "x"), [[30, 30, 30, 30, 30]], {"nodata": 255})
        for name in ("pv", "npv", "bs")
    })
    lookup = {"water": water, "ard": ard, "fc": fc}
    monkeypatch.setattr(wit_stac, "_load", lambda scene, bands, tile, resampling: lookup[scene])
    scene = SimpleNamespace(water="water", ard="ard", fc="fc")
    open_water, wet, dry, loaded_fc = wit_stac._scene_pixels(
        scene, None, np.ones((1, 5), dtype=bool)
    )
    assert open_water.tolist() == [[True, False, False, False, False]]
    assert wet.tolist() == [[False, False, True, False, False]]
    assert dry.tolist() == [[False, True, False, False, False]]
    assert loaded_fc is fc


def test_composite_keeps_first_water_and_fills_other_pixel(monkeypatch):
    ard = item("ga_ls8c_ard_3", "ard")
    ard.geometry = mapping(Geometry(box(-1000, -1000, 1000, 1000), "EPSG:3577").to_crs("EPSG:4326").geom)
    scene = wit_stac.Scene(TIME, ard, ard, ard)
    later = wit_stac.Scene(TIME + dt.timedelta(days=1), ard, ard, ard)
    calls = iter([0, 1])

    def pixels(scene, tile, available):
        if next(calls) == 0:
            return available & [[True, False]], np.zeros((1, 2), bool), None, None
        return np.zeros((1, 2), bool), available.copy(), None, None

    monkeypatch.setattr(wit_stac, "_scene_pixels", pixels)
    result = wit_stac.composite_window([scene, later], box(0, 0, 60, 30), tile_size=2)
    assert result == pytest.approx({
        "water": 0.5, "wet": 0.5, "pv": 0, "npv": 0, "bs": 0, "pc_missing": 0,
    })


def test_checkpoints_and_normalisation(tmp_path, monkeypatch):
    scenes = []
    for day in (0, 14, 15):
        a = item("ga_ls8c_ard_3", f"ard-{day}", TIME + dt.timedelta(days=day))
        scenes.append(wit_stac.Scene(a.datetime, a, a, a))
    calls = []

    def composite(window, geometry, crs, resolution, tile_size):
        calls.append(len(window))
        return {"water": 0.2, "wet": 0.1, "pv": 0.4, "npv": 0.3, "bs": 0.1, "pc_missing": 0}

    monkeypatch.setattr(wit_stac, "composite_window", composite)
    geometry = box(0, 0, 60, 30)
    first = wit_stac.run_windows(scenes, geometry, tmp_path)
    second = wit_stac.run_windows(scenes, geometry, tmp_path)
    assert calls == [2, 1]
    pd.testing.assert_frame_equal(first, second)
    assert first.scene_count.tolist() == [2, 1]
    result = wit_stac.normalise_results(first)
    assert (result.water + result.wet + result.norm_pv + result.norm_npv + result.norm_bs).tolist() \
        == pytest.approx([1.0, 1.0])
    wit_stac.run_windows(scenes, geometry, tmp_path, resolution=60)
    assert calls == [2, 1, 2, 1]


def test_zero_vegetation_cannot_be_normalised(caplog):
    frame = pd.DataFrame([{"water": 0.2, "wet": 0.1, "pv": 0., "npv": 0., "bs": 0.}])
    result = wit_stac.normalise_results(frame)
    assert result[["norm_pv", "norm_npv", "norm_bs"]].isna().all().all()
    assert "Cannot normalise" in caplog.text


@pytest.mark.parametrize("missing", [[0.0, 0.1], [0.1, 1.0]])
def test_plot_excludes_ten_percent_or_more_missing(missing, tmp_path, monkeypatch):
    from dea_conflux import stack

    frame = pd.DataFrame({
        "date": pd.to_datetime(["2022-01-01", "2022-02-01"]),
        "pc_missing": missing,
        "water": [0.1, 0.2], "wet": [0.2, 0.3],
        "norm_pv": [0.3, 0.2], "norm_npv": [0.2, 0.1], "norm_bs": [0.2, 0.2],
    })
    calls = []
    monkeypatch.setattr(
        stack, "display_wit_stack_with_df",
        lambda graph, name, path, **kwargs: calls.append((graph.copy(), name, kwargs)),
    )
    wit_stac.plot_results(frame, "wetland", tmp_path / "wetland")
    graph, title, kwargs = calls[0]
    assert kwargs["x_axis_labels"] == "months"
    if missing[0] == 0:
        assert graph.pc_missing.tolist() == [0, 0]
        assert "single observation" in title
    else:
        assert graph.water.isna().all()
        assert "no observations" in title
