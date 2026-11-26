import builtins
import datetime as dt
import json
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import datacube
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from datacube.index.eo3 import prep_eo3
from odc.geo.geom import Geometry
from rasterio.transform import from_origin
from shapely.geometry import box, mapping


NOTEBOOK = (
    Path(__file__).resolve().parents[1]
    / "notebooks/DEA_Wetlands_Insight_Tool_Multiscene_ODC.ipynb"
)
UTC = dt.timezone.utc
START = dt.datetime(2022, 1, 1, tzinfo=UTC)
PRODUCTS = ("ga_ls8c_ard_3", "ga_ls_fc_3", "ga_ls_wo_3")
REFLECTANCE = tuple(
    f"nbart_{band}" for band in ("blue", "green", "red", "nir", "swir_1", "swir_2")
)


@pytest.fixture
def indexed_datacube(tmp_path, monkeypatch):
    """Create real ODC datasets and raster assets with a controlled search."""
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    dc = datacube.Datacube(raw_config={"default": {"index_driver": "memory"}})
    measurements = {
        PRODUCTS[0]: [*REFLECTANCE, "oa_fmask"],
        PRODUCTS[1]: ["pv", "npv", "bs"],
        PRODUCTS[2]: ["water"],
    }
    indexed = {}
    for product_name, bands in measurements.items():
        definitions = [
            {
                "name": band,
                "dtype": "uint8" if band in ("water", "oa_fmask") else "int16",
                "nodata": 1 if band == "water" else (0 if band == "oa_fmask" else -999),
                "units": "1",
            }
            for band in bands
        ]
        product = dc.index.products.from_doc({
            "name": product_name, "description": "Test product", "metadata_type": "eo3",
            "metadata": {"product": {"name": product_name}}, "measurements": definitions,
        })
        dc.index.products.add(product)
        indexed[product_name] = []
        for day in (0, 9, 15):
            folder = tmp_path / product_name / str(day)
            folder.mkdir(parents=True)
            for definition in definitions:
                band = definition["name"]
                if band == "water":
                    values = np.array([[128, 0, 1, 1], [128, 0, 1, 1]], dtype="uint8")
                    if day == 9:
                        values = np.array([[1, 1, 0, 0], [1, 1, 0, 0]], dtype="uint8")
                else:
                    value = {"pv": 20, "npv": 30, "bs": 50, "oa_fmask": 1}.get(band, 1000)
                    values = np.full((2, 4), value, dtype=definition["dtype"])
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
                "properties": {
                    "datetime": timestamp.isoformat(),
                    "eo:platform": "landsat-8",
                    "odc:region_code": "098079",
                },
                "measurements": {band: {"path": f"{band}.tif"} for band in bands},
                "lineage": {},
            }
            dataset = datacube.model.Dataset(
                product, prep_eo3(doc), uri=(folder / "metadata.yaml").as_uri()
            )
            dc.index.datasets.add(dataset)
            indexed[product_name].append(dataset)

    queries = []

    def find_datasets(product, geopolygon, time):
        queries.append((product, time))
        return indexed.get(product, [])

    monkeypatch.setattr(dc, "find_datasets", find_datasets)
    return dc, indexed, queries


def load_helpers(monkeypatch, dc):
    notebook = json.loads(NOTEBOOK.read_text())
    original_import = builtins.__import__

    def independent_import(name, *args, **kwargs):
        if name == "dea_conflux" or name.startswith("dea_conflux."):
            raise ImportError("The notebook must not import dea_conflux")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", independent_import)
    namespace = {"__name__": "__main__"}
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code" and "wit-helper" in cell["metadata"].get("tags", []):
            code = "".join(cell["source"])
            assert "odc.stac" not in code
            exec(compile(code, str(NOTEBOOK), "exec"), namespace)
    namespace["dc"] = dc
    return namespace


def test_odc_notebook_real_indexed_rasters(tmp_path, monkeypatch, indexed_datacube):
    dc, indexed, queries = indexed_datacube
    ns = load_helpers(monkeypatch, dc)
    polygon = box(0, 0, 120, 60)
    scenes = ns["search_scenes"](
        dc, Geometry(polygon, "EPSG:3577"), "2022-01-01", "2022-01-16"
    )
    assert [scene.ard.id for scene in scenes] == [ds.id for ds in indexed[PRODUCTS[0]][:2]]
    assert all(query[1][1] == START + dt.timedelta(days=15, microseconds=-1)
               for query in queries)

    result = ns["run_windows"](scenes, polygon, tmp_path / "cache", tile_size=2)
    assert result.scene_count.tolist() == [2]
    assert result.loc[0, ["water", "wet", "pv", "npv", "bs", "pc_missing"]].tolist() == \
        pytest.approx([0.25, 0, 0.15, 0.225, 0.375, 0])
    normalised = ns["normalise_results"](result)
    assert (normalised.water + normalised.wet + normalised.norm_pv
            + normalised.norm_npv + normalised.norm_bs).iloc[0] == pytest.approx(1)
    ns["plot_results"](normalised, "Test wetland", tmp_path / "wetland")
    assert (tmp_path / "wetland.png").is_file()
    monkeypatch.setattr(dc, "load", lambda **kwargs: pytest.fail("Checkpoint not reused"))
    cached = ns["run_windows"](scenes, polygon, tmp_path / "cache", tile_size=2)
    pd.testing.assert_frame_equal(result, cached)


def test_parallel_windows_preserve_results_and_checkpoints(tmp_path, monkeypatch, indexed_datacube):
    dc, _, _ = indexed_datacube
    ns = load_helpers(monkeypatch, dc)
    polygon = box(0, 0, 120, 60)
    scenes = ns["search_scenes"](
        dc, Geometry(polygon, "EPSG:3577"), "2022-01-01", "2022-01-17"
    )
    serial = ns["run_windows"](scenes, polygon, tmp_path / "serial", tile_size=2, max_workers=1)
    parallel = ns["run_windows"](scenes, polygon, tmp_path / "parallel", tile_size=2, max_workers=2)
    pd.testing.assert_frame_equal(serial, parallel)
    assert parallel.scene_count.tolist() == [2, 1]
    monkeypatch.setattr(dc, "load", lambda **kwargs: pytest.fail("Checkpoint not reused"))
    cached = ns["run_windows"](scenes, polygon, tmp_path / "serial", tile_size=2, max_workers=2)
    pd.testing.assert_frame_equal(serial, cached)


def test_parallel_windows_are_concurrent_and_errors_propagate(tmp_path, monkeypatch, indexed_datacube):
    dc, _, _ = indexed_datacube
    ns = load_helpers(monkeypatch, dc)
    polygon = box(0, 0, 120, 60)
    scenes = [
        SimpleNamespace(
            time=START + dt.timedelta(days=15 * i),
            ard=SimpleNamespace(id=f"ard-{i}"),
            fc=SimpleNamespace(id=f"fc-{i}"),
            water=SimpleNamespace(id=f"water-{i}"),
        )
        for i in range(4)
    ]
    barrier = threading.Barrier(4)

    def composite(window, geometry, crs, resolution, tile_size):
        barrier.wait(timeout=10)
        return {"water": 1.0, "wet": 0.0, "pv": 0.0, "npv": 0.0, "bs": 0.0, "pc_missing": 0.0}

    ns["composite_window"] = composite
    result = ns["run_windows"](scenes, polygon, tmp_path / "parallel", max_workers=4)
    assert result.date.tolist() == [
        pd.Timestamp(START + dt.timedelta(days=15 * i)).tz_localize(None)
        for i in range(4)
    ]
    with pytest.raises(ValueError, match="max_workers"):
        ns["run_windows"](scenes, polygon, tmp_path / "invalid", max_workers=0)

    failure_barrier = threading.Barrier(4)
    finished = []

    def fail(window, *args):
        failure_barrier.wait(timeout=10)
        if window[0].ard.id == "ard-0":
            raise RuntimeError("window failed")
        time.sleep(0.1)
        finished.append(window[0].ard.id)
        return {"water": 1.0, "wet": 0.0, "pv": 0.0, "npv": 0.0, "bs": 0.0, "pc_missing": 0.0}

    ns["composite_window"] = fail
    with pytest.raises(RuntimeError, match="window failed"):
        ns["run_windows"](scenes, polygon, tmp_path / "failure", max_workers=4)
    assert len(finished) == 3


def test_odc_scene_matching_detects_missing_and_duplicates(monkeypatch, indexed_datacube, caplog):
    dc, indexed, _ = indexed_datacube
    ns = load_helpers(monkeypatch, dc)
    polygon = Geometry(box(0, 0, 120, 60), "EPSG:3577")
    indexed[PRODUCTS[2]].pop(1)
    scenes = ns["search_scenes"](dc, polygon, "2022-01-01", "2022-01-16")
    assert len(scenes) == 1
    assert "Skipped 1 ARD scenes" in caplog.text
    duplicate = indexed[PRODUCTS[1]][0]
    indexed[PRODUCTS[1]].append(SimpleNamespace(
        id=uuid.uuid4(), metadata_doc=duplicate.metadata_doc
    ))
    with pytest.raises(ValueError, match="Multiple ga_ls_fc_3 datasets"):
        ns["search_scenes"](dc, polygon, "2022-01-01", "2022-01-16")


@pytest.mark.parametrize("final_first", [True, False])
def test_odc_selects_final_over_nrt_and_checks_lineage(monkeypatch, indexed_datacube, caplog, final_first):
    dc, indexed, _ = indexed_datacube
    ns = load_helpers(monkeypatch, dc)
    polygon = Geometry(box(0, 0, 120, 60), "EPSG:3577")
    final = indexed[PRODUCTS[0]][0]
    final.metadata_doc["properties"]["dea:dataset_maturity"] = "final"
    nrt = SimpleNamespace(
        id=uuid.uuid4(),
        metadata_doc={"properties": {**final.metadata_doc["properties"],
                                     "dea:dataset_maturity": "nrt"}},
    )
    indexed[PRODUCTS[0]] = [final, nrt] if final_first else [nrt, final]
    for product in PRODUCTS[1:]:
        indexed[product] = indexed[product][:1]
        indexed[product][0].metadata_doc["lineage"] = {
            "source_datasets": {"ard": {"id": str(final.id)}}
        }
    scenes = ns["search_scenes"](dc, polygon, "2022-01-01", "2022-01-16")
    assert len(scenes) == 1 and scenes[0].ard.id == final.id
    assert "using final" in caplog.text
    assert str(nrt.id) in caplog.text
    indexed[PRODUCTS[2]][0].metadata_doc["lineage"]["source_datasets"]["ard"]["id"] = str(nrt.id)
    with pytest.raises(ValueError, match="does not derive from selected ARD"):
        ns["search_scenes"](dc, polygon, "2022-01-01", "2022-01-16")


def test_odc_duplicate_maturity_remains_ambiguous(monkeypatch, indexed_datacube):
    dc, indexed, _ = indexed_datacube
    ns = load_helpers(monkeypatch, dc)
    source = indexed[PRODUCTS[0]][0]
    source.metadata_doc["properties"]["dea:dataset_maturity"] = "final"
    duplicate = SimpleNamespace(id=uuid.uuid4(), metadata_doc=source.metadata_doc)
    indexed[PRODUCTS[0]].append(duplicate)
    with pytest.raises(ValueError, match="Multiple ga_ls8c_ard_3 datasets"):
        ns["search_scenes"](
            dc, Geometry(box(0, 0, 120, 60), "EPSG:3577"), "2022-01-01", "2022-01-16"
        )


def test_odc_matching_uses_platform_region_and_acquisition_time(monkeypatch, indexed_datacube, caplog):
    dc, indexed, _ = indexed_datacube
    ns = load_helpers(monkeypatch, dc)
    polygon = Geometry(box(0, 0, 120, 60), "EPSG:3577")

    def dataset(name, platform, region, time):
        return SimpleNamespace(
            id=name,
            metadata_doc={"properties": {
                "eo:platform": platform, "odc:region_code": region, "datetime": time,
            }},
            center_time=START + dt.timedelta(hours=3),
        )

    time = "2022-01-01T00:00:00Z"
    indexed[PRODUCTS[0]] = [
        dataset("matched", "landsat-8", "098079", time),
        dataset("wrong-region", "landsat-8", "098080", time),
        dataset("wrong-platform", "landsat-9", "098079", time),
        dataset("wrong-time", "landsat-8", "098079", "2022-01-01T00:00:01Z"),
    ]
    indexed[PRODUCTS[1]] = [dataset("fc", "landsat-8", "098079", time)]
    indexed[PRODUCTS[2]] = [dataset("water", "landsat-8", "098079", time)]
    scenes = ns["search_scenes"](dc, polygon, "2022-01-01", "2022-01-16")
    assert [scene.ard.id for scene in scenes] == ["matched"]
    assert scenes[0].time == START
    assert "Skipped 3 ARD scenes" in caplog.text
    del indexed[PRODUCTS[1]][0].metadata_doc["properties"]["odc:region_code"]
    with pytest.raises(ValueError, match="lacks eo:platform"):
        ns["search_scenes"](dc, polygon, "2022-01-01", "2022-01-16")


def test_odc_notebook_end_to_end_with_multiple_features(
    tmp_path, monkeypatch, indexed_datacube
):
    dc, _, _ = indexed_datacube
    monkeypatch.setattr(datacube, "Datacube", lambda **kwargs: dc)
    polygons = gpd.GeoDataFrame(
        {"fid": [1, 2], "geometry": [box(0, 0, 120, 60), box(60, 0, 120, 60)]},
        crs="EPSG:3577",
    )
    shape_path = tmp_path / "wetlands.shp"
    polygons.to_file(shape_path)
    output = tmp_path / "output"
    notebook = json.loads(NOTEBOOK.read_text())
    namespace = {"__name__": "__main__"}
    for cell in notebook["cells"]:
        if cell["cell_type"] != "code":
            continue
        code = "".join(cell["source"])
        if code.startswith("shapefile ="):
            code = code.replace(
                'Path.home() / "ramsarbig/ramsarbig.shp"', f"Path({str(shape_path)!r})"
            ).replace('"2024-01-01"', '"2022-01-16"')
            code = code.replace('Path("wit_multiscene_odc_output")', f"Path({str(output)!r})")
            code = code.replace("tile_size = 1024", "tile_size = 2")
        exec(compile(code, str(NOTEBOOK), "exec"), namespace)

    for feature_id in (1, 2):
        frame = pd.read_csv(output / f"{feature_id}.csv")
        assert len(frame) == 1
        assert frame.loc[0, "pc_missing"] == 0
        assert (output / f"{feature_id}.png").is_file()
    assert pd.read_csv(output / "1.csv").loc[0, "water"] == pytest.approx(0.25)
    assert pd.read_csv(output / "2.csv").loc[0, "water"] == 0
