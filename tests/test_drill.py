import logging
from pathlib import Path
import re
import sys

from click.testing import CliRunner
import datacube
import pytest

from dea_conflux.__main__ import run_plugin
from dea_conflux.drill import find_datasets

# Test directory.
HERE = Path(__file__).parent.resolve()
logging.basicConfig(level=logging.INFO)

# Path to Canberra test shapefile.
TEST_SHP = HERE / 'data' / 'waterbodies_canberra.shp'

TEST_PLUGIN_OK = HERE / 'data' / 'sum_wet.conflux.py'
TEST_PLUGIN_COMBINED = HERE / 'data' / 'sum_pv_wet.conflux.py'
TEST_PLUGIN_MISSING_TRANSFORM = HERE / 'data' / 'sum_wet_missing_transform.conflux.py'

TEST_WOFL_ID = '234fec8f-1de7-488a-a115-818ebd4bfec4'
TEST_FC_ID = '4d243358-152e-404c-bb65-7ea64b21ca38'


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []


@pytest.fixture(scope="module")
def dc():
    return datacube.Datacube()


def test_find_datasets_gives_self(dc):
    plugin = run_plugin(TEST_PLUGIN_OK)
    uuid = TEST_WOFL_ID
    datasets = find_datasets(dc, plugin, uuid)
    assert len(datasets) == 1
    assert str(datasets['wofs_albers'].id) == uuid


def test_find_datasets_gives_other(dc):
    plugin = run_plugin(TEST_PLUGIN_COMBINED)
    uuid = TEST_WOFL_ID
    datasets = find_datasets(dc, plugin, uuid)
    assert len(datasets) == 2
    assert str(datasets['wofs_albers'].id) == uuid
    assert str(datasets['ls7_fc_albers'].id) == TEST_FC_ID
