import logging
import os
import sys
from pathlib import Path

import datacube
import geopandas as gpd
import pytest

import deafrica_conflux.drill
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.plugins import waterbodies_timeseries
from deafrica_conflux.plugins.utils import run_plugin

logging.basicConfig(level=logging.INFO)

# Plugin file path.
TEST_PLUGIN = waterbodies_timeseries.__file__

# Test directory.
HERE = Path(__file__).parent.resolve()
TEST_WATERBODY = os.path.join(HERE, "data", "edumesbb2.geojson")


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []


@pytest.fixture(scope="module")
def dc():
    return datacube.Datacube()


def test_drill_integration(dc):
    uuid = "d15407ff-3fe5-55ec-a713-d4cc9399e6b3"
    reference_dataset = dc.index.datasets.get(uuid)

    plugin = run_plugin(TEST_PLUGIN)

    polygons_gdf = gpd.read_file(TEST_WATERBODY)

    id_field = guess_id_field(polygons_gdf, "UID")
    polygons_gdf.set_index(id_field, inplace=True)

    drill_result = deafrica_conflux.drill.drill(
        plugin, polygons_gdf, reference_dataset, partial=True, overedge=False, dc=dc
    )
    assert len(drill_result) == pytest.approx(86, 1)
    # 13 columns, 9 output and 4 directions
    assert len(drill_result.columns) == 13
    assert "conflux_n" in drill_result.columns


def test_get_directions(dc):
    uuid = "effd8637-1cd0-585b-84b4-b739e8626544"
    polygons_gdf = gpd.read_file(TEST_WATERBODY)

    ds_extent = dc.index.datasets.get(uuid).extent.to_crs(polygons_gdf.crs)
    ds_extent_geom = ds_extent.geom

    intersection = polygons_gdf.geometry.intersection(ds_extent_geom)
    dirs = deafrica_conflux.drill._get_directions(
        polygons_gdf.geometry[0], intersection.geometry[0]
    )
    assert dirs == {"South"}


def test_south_overedge(dc):
    uuid = "effd8637-1cd0-585b-84b4-b739e8626544"
    reference_dataset = dc.index.datasets.get(uuid)

    plugin = run_plugin(TEST_PLUGIN)

    polygons_gdf = gpd.read_file(TEST_WATERBODY)

    id_field = guess_id_field(polygons_gdf, "UID")
    polygons_gdf.set_index(id_field, inplace=True)

    drill_result = deafrica_conflux.drill.drill(
        plugin, polygons_gdf, reference_dataset, partial=True, overedge=True, dc=dc
    )
    assert len(drill_result) == 1
    assert drill_result["pc_wet"].iloc[0] == 42.39275304214028


# TODO: Find waterbody polygon with nothern overlap and test.
# df test_north_overedge(dc):
