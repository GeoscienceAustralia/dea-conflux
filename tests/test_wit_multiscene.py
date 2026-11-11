import datetime as dt
import uuid
from pathlib import Path
from types import SimpleNamespace

import datacube
import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
import xarray as xr
from click.testing import CliRunner
from datacube.index.eo3 import prep_eo3
from PIL import Image
from rasterio.transform import from_origin
from shapely.geometry import box, mapping

from dea_conflux import io, wit_multiscene, wit_multiscene_stack
from dea_conflux.__main__ import main, run_plugin


ROOT = Path(__file__).resolve().parents[1]
START = dt.datetime(2020, 1, 1)


class FakeDatacube:
    def __init__(self, plugin):
        self.plugin = plugin
        self.scenes = {
            product: [
                SimpleNamespace(id=f"{product}-{day}", center_time=START + dt.timedelta(days=day))
                for day in (0, 9, 15)
            ]
            for product in plugin.input_products
        }
        self.queries = []

    def find_datasets(self, product, geopolygon, time):
        self.queries.append((product, time))
        return [
            scene for scene in self.scenes[product]
            if time[0] <= scene.center_time <= time[1]
        ]

    def load(self, datasets, measurements, like, resampling, skip_broken_datasets):
        assert not skip_broken_datasets
        day = (datasets[0].center_time - START).days
        coords = like.coordinates
        x = coords["x"].values
        shape = (like.shape.y, like.shape.x)
        land = np.broadcast_to(x < 60 if day in (0, 15) else x >= 60, shape)
        variables = {}
        for band in measurements:
            if band == "water":
                values = np.where(land, 0, 1).astype("uint8")
                if day == 0:
                    values[:, x < 30] = 128
                nodata = 1
            elif band in ("bs", "pv", "npv"):
                values = np.full(shape, {"bs": 20, "pv": 30, "npv": 50}[band], dtype="int16")
                nodata = -999
            else:
                values = np.full(shape, 1000, dtype="int16")
                nodata = -999
            variables[band] = (("time", "y", "x"), values[np.newaxis], {"nodata": nodata})
        return xr.Dataset(
            variables,
            coords={"time": [np.datetime64(datasets[0].center_time)], "y": coords["y"].values, "x": x},
        )


@pytest.fixture
def plugin():
    return run_plugin(ROOT / "examples" / "wit_ls8.conflux.py")


@pytest.fixture
def polygons():
    return gpd.GeoDataFrame(
        {"uid": ["one", "overlap"], "geometry": [box(0, 0, 120, 60), box(60, 0, 120, 60)]},
        crs="EPSG:3577",
    ).set_index("uid")


@pytest.mark.parametrize("satellite", [5, 7, 8, 9])
def test_supported_plugins(satellite):
    plugin = run_plugin(ROOT / "examples" / f"wit_ls{satellite}.conflux.py")
    assert "_ard_" in wit_multiscene.validate_plugin(plugin)


def test_windows_are_anchored_and_end_exclusive():
    items = [(START + dt.timedelta(days=day), day) for day in (0, 9, 15, 29, 30)]
    assert [[v for _, v in group] for group in wit_multiscene.time_windows(items, 15)] == [
        [0, 9], [15, 29], [30]
    ]


@pytest.mark.parametrize("satellite", [5, 7, 8, 9])
def test_composite_tiling_first_valid_and_missing(satellite):
    plugin = run_plugin(ROOT / "examples" / f"wit_ls{satellite}.conflux.py")
    dc = FakeDatacube(plugin)
    window = wit_multiscene.observations(
        dc, plugin, wit_multiscene.Geometry(box(0, 0, 120, 60), "EPSG:3577"),
        START, START + dt.timedelta(days=30),
    )
    expected = {"water": 0.25, "wet": 0, "bs": 0.15, "pv": 0.225, "npv": 0.375, "pc_missing": 0}
    for tile_size in (1, 2, 20):
        summary = wit_multiscene.composite_summary(
            dc, plugin, box(0, 0, 120, 60), "EPSG:3577", window[:2], tile_size
        )
        assert summary == pytest.approx(expected)
    summary = wit_multiscene.composite_summary(
        dc, plugin, box(0, 0, 120, 60), "EPSG:3577", window[2:], 2
    )
    assert summary["pc_missing"] == pytest.approx(0.5)


def test_validity_respects_cloud_and_nodata_but_keeps_open_water():
    inputs = xr.Dataset({
        "water": ("pixel", np.array([0, 0, 128, 2, 1], dtype="uint8"), {"nodata": 1}),
        "bs": ("pixel", np.array([20, -999, -999, 20, 20]), {"nodata": -999}),
        "nbart_blue": ("pixel", np.array([1000, 1000, -999, 1000, 1000]), {"nodata": -999}),
    })
    assert wit_multiscene._valid_pixels(inputs).values.tolist() == [
        True, False, True, False, False
    ]


def test_incomplete_observation_is_skipped_with_warning(plugin, caplog):
    dc = FakeDatacube(plugin)
    dc.scenes["ga_ls_fc_3"].pop(1)
    items = wit_multiscene.observations(
        dc, plugin, wit_multiscene.Geometry(box(0, 0, 120, 60), "EPSG:3577"),
        START, START + dt.timedelta(days=30),
    )
    assert [t for t, _ in items] == [START, START + dt.timedelta(days=15)]
    assert "missing products ga_ls_fc_3" in caplog.text


def test_output_parquet_csv_and_independent_overlapping_polygons(
    plugin, polygons, tmp_path, monkeypatch
):
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    dc = FakeDatacube(plugin)
    paths = wit_multiscene.run(
        plugin, polygons, START, START + dt.timedelta(days=15),
        str(tmp_path / "parquet"), tile_size=2, dc=dc,
    )
    assert len(paths) == 4
    assert all(query[1][1] == START + dt.timedelta(days=16, microseconds=-1) for query in dc.queries)
    frames = [io.read_table(path) for path in paths]
    first = next(
        frame for frame in frames
        if frame.index[0] == "one" and frame.attrs["date"].startswith("20200101")
    )
    assert first.iloc[0]["water"] == pytest.approx(0.25)
    assert first.iloc[0]["pc_missing"] == 0
    overlap = next(
        frame for frame in frames
        if frame.index[0] == "overlap" and frame.attrs["date"].startswith("20200101")
    )
    assert overlap.iloc[0]["pc_missing"] == 0
    assert overlap.iloc[0]["bs"] == pytest.approx(0.2)
    outputs = wit_multiscene_stack.write_csvs(paths, str(tmp_path / "csv"))
    assert len(outputs) == 2
    csv = pd.read_csv(tmp_path / "csv" / "one.csv")
    assert len(csv) == 2
    assert csv["nwi_id"].tolist() == ["one", "one"]
    assert csv["pc_missing"].tolist() == pytest.approx([0, 0.5])
    assert csv["norm_bs"].iloc[0] == pytest.approx(0.15)
    assert "geohash_wetland_id" in csv.columns
    assert "composite_id" in csv.columns
    assert "ard_scene_id" not in csv.columns
    for name in ("one", "overlap"):
        with Image.open(tmp_path / "csv" / f"{name}.png") as image:
            assert image.format == "PNG"
            image.verify()


def test_cli_runs_multiscene(plugin, polygons, tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    dc = FakeDatacube(plugin)
    monkeypatch.setattr(wit_multiscene.datacube, "Datacube", lambda **kwargs: dc)
    shapefile = tmp_path / "features.shp"
    polygons.reset_index().to_file(shapefile)
    result = CliRunner().invoke(main, [
        "run", "--multi-scene", "-p", str(ROOT / "examples" / "wit_ls8.conflux.py"),
        "-s", str(shapefile), "--use-id", "uid", "--start-date", "2020-01-01",
        "--end-date", "2020-01-16", "-o", str(tmp_path / "pq"),
        "--csv-output", str(tmp_path / "csv"), "--tile-size", "2",
    ])
    assert result.exit_code == 0, (result.output, result.exception)
    assert (tmp_path / "csv" / "one.csv").exists()
    assert (tmp_path / "csv" / "overlap.csv").exists()
    assert (tmp_path / "csv" / "one.png").exists()
    assert (tmp_path / "csv" / "overlap.png").exists()


@pytest.mark.parametrize("missing", [[0.1, 0.1001, 0], [0.2, 0.5, 1]])
def test_plot_quality_filter_and_remote_output(missing, monkeypatch, caplog):
    frame = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
        "pc_missing": missing,
        "water": [0.1] * 3, "wet": [0.2] * 3,
        "norm_pv": [0.3] * 3, "norm_npv": [0.2] * 3, "norm_bs": [0.2] * 3,
    })
    original = wit_multiscene_stack.display_wit_stack_with_df
    calls = []

    def capture(graph, title, path, **kwargs):
        calls.append((graph.copy(), title))
        original(graph, title, path, **kwargs)

    monkeypatch.setattr(wit_multiscene_stack, "display_wit_stack_with_df", capture)
    target = "memory://multiscene-plot/feature.png"
    wit_multiscene_stack._write_plot(frame, "feature", target)
    graph, title = calls[0]
    if missing[0] == 0.1:
        assert graph["pc_missing"].tolist() == [0.1, 0]
        assert title == "feature"
    else:
        assert graph[["water", "wet", "norm_pv", "norm_npv", "norm_bs"]].isna().all().all()
        assert "no observations" in title
        assert "No observations with at most 10% missing" in caplog.text
    with fsspec.open(target, "rb") as stream:
        with Image.open(stream) as image:
            assert image.format == "PNG"
            image.verify()


def test_single_observation_plot_has_visible_span(tmp_path, monkeypatch):
    frame = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01"]),
        "pc_missing": [0.0], "water": [0.1], "wet": [0.2],
        "norm_pv": [0.3], "norm_npv": [0.2], "norm_bs": [0.2],
    })
    original = wit_multiscene_stack.display_wit_stack_with_df
    calls = []

    def capture(graph, title, path, **kwargs):
        calls.append((graph.copy(), title))
        original(graph, title, path, **kwargs)

    monkeypatch.setattr(wit_multiscene_stack, "display_wit_stack_with_df", capture)
    target = tmp_path / "single.png"
    wit_multiscene_stack._write_plot(frame, "single", str(target))
    graph, title = calls[0]
    assert "single observation" in title
    assert graph["date"].tolist() == [
        pd.Timestamp("2019-12-31 12:00"), pd.Timestamp("2020-01-01 12:00")
    ]
    assert graph["water"].tolist() == [0.1, 0.1]
    assert frame["date"].tolist() == [pd.Timestamp("2020-01-01")]
    with Image.open(target) as image:
        image.verify()


def test_cli_rejects_bad_dates():
    result = CliRunner().invoke(main, [
        "run", "--multi-scene", "-p", str(ROOT / "examples" / "wit_ls8.conflux.py"),
        "-s", "unused.shp", "--start-date", "2020-02-01", "--end-date", "2020-01-01",
        "-o", "out", "--csv-output", "csv",
    ])
    assert result.exit_code != 0
    assert "must not be after" in result.output


def test_real_datacube_local_rasters(plugin, tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    dc = datacube.Datacube(raw_config={"default": {"index_driver": "memory"}})
    indexed = {}
    for product_name, measurements in plugin.input_products.items():
        indexed[product_name] = []
        definitions = [
            {
                "name": band,
                "dtype": "uint8" if band == "water" else "int16",
                "nodata": 1 if band == "water" else -999,
                "units": "1",
            }
            for band in measurements
        ]
        product = dc.index.products.from_doc({
            "name": product_name, "description": "Test product", "metadata_type": "eo3",
            "metadata": {"product": {"name": product_name}},
            "measurements": definitions,
        })
        dc.index.products.add(product)
        for day in (0, 9):
            folder = tmp_path / product_name / str(day)
            folder.mkdir(parents=True)
            for definition in definitions:
                band = definition["name"]
                if band == "water":
                    values = np.array([[0, 0, 1, 1], [0, 0, 1, 1]], dtype="uint8")
                    if day == 9:
                        values = 1 - values
                else:
                    value = {"bs": 20, "pv": 30, "npv": 50}.get(band, 1000)
                    values = np.full((2, 4), value, dtype="int16")
                with rasterio.open(
                    folder / f"{band}.tif", "w", driver="GTiff", width=4, height=2,
                    count=1, dtype=definition["dtype"], crs="EPSG:3577",
                    transform=from_origin(0, 60, 30, 30), nodata=definition["nodata"],
                ) as dst:
                    dst.write(values, 1)
            timestamp = START + dt.timedelta(days=day)
            doc = {
                "$schema": "https://schemas.opendatacube.org/dataset",
                "id": str(uuid.uuid4()),
                "product": {"name": product_name},
                "crs": "epsg:3577",
                "geometry": mapping(box(0, 0, 120, 60)),
                "grids": {"default": {
                    "shape": [2, 4], "transform": [30, 0, 0, 0, -30, 60, 0, 0, 1],
                }},
                "properties": {"datetime": timestamp.isoformat() + "Z"},
                "measurements": {band: {"path": f"{band}.tif"} for band in measurements},
                "lineage": {},
            }
            dataset = datacube.model.Dataset(
                product, prep_eo3(doc), uri=(folder / "metadata.yaml").as_uri()
            )
            dc.index.datasets.add(dataset)
            indexed[product_name].append(dataset)
    # The in-memory index does not implement spatial/temporal search.
    monkeypatch.setattr(
        dc, "find_datasets", lambda product, geopolygon, time: indexed[product]
    )
    polygons = gpd.GeoDataFrame(
        {"uid": ["one"], "geometry": [box(0, 0, 120, 60)]}, crs="EPSG:3577"
    ).set_index("uid")
    paths = wit_multiscene.run(
        plugin, polygons, START, START + dt.timedelta(days=10),
        str(tmp_path / "output"), tile_size=2, dc=dc,
    )
    assert len(paths) == 1
    result = io.read_table(paths[0]).iloc[0]
    assert result["pc_missing"] == 0
    assert result["bs"] == pytest.approx(0.2)
    assert result["pv"] == pytest.approx(0.3)
