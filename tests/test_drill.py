import logging
from pathlib import Path
import re
import sys

from click.testing import CliRunner
import datacube
import pytest

from dea_conflux.__main__ import run_plugin
from dea_conflux.drill import find_datasets, drill
from .constants import *

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


def test_drill_integration(dc):
    plugin = run_plugin(TEST_PLUGIN_OK)
    drill_result = drill(
        plugin,
        TEST_SHP,
        TEST_WOFL_ID,
        'UID',
        'EPSG:3577',
        partial=True,
        dc=dc)
    assert len(drill_result) == 86
    assert len(drill_result.columns) == 1
