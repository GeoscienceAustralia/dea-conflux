import logging
import sys

import datacube
import geopandas as gpd
import pytest

import deafrica_conflux.drill
from deafrica_conflux.id_field import guess_id_field
from deafrica_conflux.plugins.utils import run_plugin

logging.basicConfig(level=logging.INFO)


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []


@pytest.fixture(scope="module")
def dc():
    return datacube.Datacube()


def test_find_datasets_for_plugin(dc):
    plugin = run_plugin("plugins/sum_wet.py")
    uuid = "6d1d62de-5edd-5892-9dcc-9e1616251411"
    datasets = deafrica_conflux.drill.find_datasets_for_plugin(dc, plugin, uuid)
    assert len(datasets) == 1
    assert str(datasets["wofs_ls"].id) == uuid


def test_drill_integration(dc):
    plugin = run_plugin("plugins/timeseries.py")
    uuid = "d15407ff-3fe5-55ec-a713-d4cc9399e6b3"
    polygons_gdf = gpd.read_file("data/edumesbb2.geojson")

    id_field = guess_id_field(polygons_gdf, "UID")
    polygons_gdf.set_index(id_field, inplace=True)

    drill_result = deafrica_conflux.drill.drill(plugin,
                                                polygons_gdf,
                                                uuid,
                                                partial=True,
                                                dc=dc)
    assert len(drill_result) == pytest.approx(86, 1)
    # 7 columns, 3 output and 4 directions
    assert len(drill_result.columns) == 7
    assert "conflux_n" in drill_result.columns


def test_get_directions(dc):
    uuid = "effd8637-1cd0-585b-84b4-b739e8626544"
    polygons_gdf = gpd.read_file("data/edumesbb2.geojson")
    
    ds_extent = dc.index.datasets.get(uuid).extent.to_crs(polygons_gdf.crs)
    ds_extent_geom = ds_extent.geom

    intersection = polygons_gdf.geometry.intersection(ds_extent_geom)
    dirs = deafrica_conflux.drill._get_directions(polygons_gdf.geometry[0], intersection.geometry[0])
    assert dirs == {"South"}


def test_south_overedge(dc):
    plugin = run_plugin("plugins/timeseries.py")
    uuid = "effd8637-1cd0-585b-84b4-b739e8626544"
    polygons_gdf = gpd.read_file("data/edumesbb2.geojson")

    id_field = guess_id_field(polygons_gdf, "UID")
    polygons_gdf.set_index(id_field, inplace=True)

    drill_result = deafrica_conflux.drill.drill(plugin,
                                                polygons_gdf,
                                                uuid,
                                                partial=True,
                                                overedge=True,
                                                dc=dc)
    assert len(drill_result) == 1
    assert drill_result["wet_percentage"].iloc[0] == 42.39275304214028


# TODO: Find waterbody polygon with nothern overlap and test.
# df test_north_overedge(dc):
