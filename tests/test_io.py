import logging
from pathlib import Path
import re
import sys

from click.testing import CliRunner
import datacube
import pytest

import dea_conflux.io as io
from .constants import *

logging.basicConfig(level=logging.INFO)


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []


@pytest.fixture()
def conflux_table():
    return pd.DataFrame({
        'band1': [0, 1, 2],
        'band2': [5, 4, 3],
    }, index=['uid1', 'uid2', 'uid3'])


def test_write_table(conflux_table, tmp_path):
    test_date = datetime.datetime(2018, 1, 1)
    io.write_table('name', 'uuid', test_date,
                   conflux_table, tmp_path / 'outdir')
    outpath = tmp_path / 'outdir' / 'name_uuid_20180101-000000-000000.pq'
    assert outpath.exists()
