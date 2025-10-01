import logging
import sys
from pathlib import Path

import datacube
import geopandas as gpd
import pytest

from dea_conflux.__main__ import load_and_reproject_shapefile, run_plugin
from dea_conflux.drill import _get_directions, drill, find_datasets

logging.basicConfig(level=logging.INFO)

# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile
TEST_SHP = HERE / "data" / "waterbodies_canberra.shp"
TEST_ID_FIELD = "uid"


# Path to a polygon overlapping the test C3 WOfL north boundary
TEST_SOUTH_OVERLAP = HERE / "data" / "small_south_overlay.shp"


TEST_PLUGIN_OK = HERE / "data" / "sum_wet.conflux.py"
TEST_PLUGIN_OK_C3 = HERE / "data" / "sum_wet_c3.conflux.py"
TEST_PLUGIN_COMBINED = HERE / "data" / "sum_pv_wet.conflux.py"
TEST_PLUGIN_MISSING_TRANSFORM = HERE / "data" / "sum_wet_missing_transform.conflux.py"

TEST_C3_WO_ID = "4c116812-58e5-52fb-ac71-4cdf12bf6943"
TEST_C3_FC_ID = "e842fd8a-0cc1-5b74-986c-6cafb1c9862a"
TEST_OVERLAY_ID = "ce5fee76-f699-5dc8-b869-ff17b3d6da42"
TEST_C3_WO_STH_ID = "4c116812-58e5-52fb-ac71-4cdf12bf6943"
TEST_C3_WO_NTH_ID = "e043bffd-05c5-55c3-8740-a973842f7a05"


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.ERROR, force=True)


@pytest.fixture(scope="module")
def dc():
    return datacube.Datacube()


def test_find_datasets_gives_self(dc):
    plugin = run_plugin(TEST_PLUGIN_OK)
    uuid = TEST_C3_WO_ID
    datasets = find_datasets(dc, plugin, uuid)
    assert len(datasets) == 1
    assert str(datasets["ga_ls_wo_3"].id) == uuid


def test_find_datasets_gives_other(dc):
    plugin = run_plugin(TEST_PLUGIN_COMBINED)
    uuid = TEST_C3_WO_ID
    datasets = find_datasets(dc, plugin, uuid)
    assert len(datasets) == 2
    assert str(datasets["ga_ls_wo_3"].id) == uuid
    assert str(datasets["ga_ls_fc_3"].id) == TEST_C3_FC_ID


def test_drill_integration(dc):
    plugin = run_plugin(TEST_PLUGIN_OK)
    shp = load_and_reproject_shapefile(TEST_SHP, TEST_ID_FIELD, "EPSG:3577")
    drill_result = drill(
        plugin, shp, TEST_C3_WO_ID, "EPSG:3577", (-30, 30), partial=True, dc=dc
    )
    assert len(drill_result) == pytest.approx(86, 1)
    # 5 columns, one output and 4 directions
    assert len(drill_result.columns) == 5
    assert "conflux_n" in drill_result.columns


def test_get_directions(dc):
    gdf = gpd.read_file(TEST_SOUTH_OVERLAP)
    extent = dc.index.datasets.get(TEST_OVERLAY_ID).extent.geom
    intersection = gdf.geometry.intersection(extent)
    dirs = _get_directions(gdf.geometry[0], intersection.geometry[0])
    assert dirs == {"South"}


def test_south_overedge(dc):
    test_sth_polygon_id = "r39zjddbt"
    plugin = run_plugin(TEST_PLUGIN_OK_C3)
    shp = load_and_reproject_shapefile(TEST_SHP, TEST_ID_FIELD, "EPSG:3577")
    shp = shp.loc[[test_sth_polygon_id]]
    drill_result = drill(
        plugin,
        shp,
        TEST_C3_WO_STH_ID,
        "EPSG:3577",
        (-30, 30),
        partial=True,
        overedge=True,
        dc=dc,
    )
    assert len(drill_result) == 1
    assert drill_result.water[0] == 41  # check this


def test_north_overedge(dc):
    test_nth_polygon_id = "r3cbj7d6s"
    plugin = run_plugin(TEST_PLUGIN_OK_C3)
    shp = load_and_reproject_shapefile(TEST_SHP, TEST_ID_FIELD, "EPSG:3577")
    shp = shp.loc[[test_nth_polygon_id]]
    drill_result = drill(
        plugin,
        shp,
        TEST_C3_WO_NTH_ID,
        "EPSG:3577",
        (-30, 30),
        partial=True,
        overedge=True,
        dc=dc,
    )
    assert len(drill_result) == 1
    assert drill_result.water[0] == 44
