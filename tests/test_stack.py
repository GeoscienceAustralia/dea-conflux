import datetime
import logging
from pathlib import Path
import re
import sys

from click.testing import CliRunner
import datacube
import pandas as pd
import pytest

import dea_conflux.stack

logging.basicConfig(level=logging.INFO)

# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile.
TEST_SHP = HERE / 'data' / 'waterbodies_canberra.shp'
# UID of Lake Ginninderra.
LAKE_GINNINDERRA_ID = 'r3dp84s8n'

TEST_PLUGIN_OK = HERE / 'data' / 'sum_wet.conflux.py'
TEST_PLUGIN_COMBINED = HERE / 'data' / 'sum_pv_wet.conflux.py'
TEST_PLUGIN_MISSING_TRANSFORM = HERE / 'data' / 'sum_wet_missing_transform.conflux.py'

TEST_PQ_DATA = HERE / 'data' / 'canberra_waterbodies_pq'

TEST_WOFL_ID = '234fec8f-1de7-488a-a115-818ebd4bfec4'
TEST_FC_ID = '4d243358-152e-404c-bb65-7ea64b21ca38'


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []


@pytest.fixture()
def conflux_table():
    return pd.DataFrame({
        'band1': [0, 1, 2],
        'band2': [5, 4, 3],
    }, index=['uid1', 'uid2', 'uid3'])


def test_waterbodies_stacking(tmp_path):
    dea_conflux.stack.stack(
        TEST_PQ_DATA, tmp_path / 'testout',
        mode=dea_conflux.stack.StackMode.WATERBODIES)
    uid = LAKE_GINNINDERRA_ID
    outpath = tmp_path / 'testout' / uid[:4] / f'{uid}.csv'
    assert outpath.exists()
    csv = pd.read_csv(outpath)
    assert len(csv) == 2
    assert len(csv.columns) == 3
    assert int(csv.iloc[1].px_wet) == 1205
