import datetime
import logging
import random
import sys
from pathlib import Path

import pandas as pd
import pytest

import deafrica_conflux.io

logging.basicConfig(level=logging.INFO)


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []


@pytest.fixture()
def conflux_table():
    return pd.DataFrame(
        {
            "band1": [0, 1, 2],
            "band2": [5, 4, 3],
        },
        index=["uid1", "uid2", "uid3"],
    )


def test_write_table(conflux_table, tmp_path):
    test_date = datetime.datetime(2018, 1, 1)
    deafrica_conflux.io.write_table_to_parquet("name", "uuid", test_date, conflux_table, tmp_path / "outdir")
    outpath = tmp_path / "outdir" / "20180101" / "name_uuid_20180101-000000-000000.pq"
    assert outpath.exists()


def test_read_write_table(conflux_table, tmp_path):
    test_date = datetime.datetime(2018, 1, 1)
    deafrica_conflux.io.write_table_to_parquet("name", "uuid", test_date, conflux_table, tmp_path / "outdir")
    outpath = tmp_path / "outdir" / "20180101" / "name_uuid_20180101-000000-000000.pq"
    table = deafrica_conflux.io.read_table_from_parquet(outpath)
    assert len(table) == 3
    assert len(table.columns) == 2
    assert table.attrs["date"] == "20180101-000000-000000"
    assert table.attrs["drill"] == "name"


def test_string_date():
    random.seed(0)
    for _ in range(100):
        d = random.randrange(1, 1628000000)  # from the beginning of time...
        d = datetime.datetime.fromtimestamp(d)
        assert deafrica_conflux.io.string_to_date(deafrica_conflux.io.date_to_string(d)) == d
