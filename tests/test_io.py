import datetime
import logging
import os
import random
import sys
from pathlib import Path

import boto3
import fsspec
import pandas as pd
import pytest
from moto import mock_s3

import deafrica_conflux.io

logging.basicConfig(level=logging.INFO)

# Test directory.
HERE = Path(__file__).parent.resolve()
TEST_LOCAL_DIR = HERE / "outdir"
TEST_LOCAL_FILE = TEST_LOCAL_DIR / "test_txt.txt"
TEST_BUCKET = "test-bucket"
TEST_S3_DIR = f"s3://{TEST_BUCKET}/test-folder"
TEST_S3_FILE = f"s3://{TEST_BUCKET}/{TEST_S3_DIR}/test_txt.txt"


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


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3_client(aws_credentials):
    return boto3.client("s3", region_name="af-south-1")


def test_write_table(conflux_table):
    test_date = datetime.datetime(2018, 1, 1)
    deafrica_conflux.io.write_table_to_parquet(
        drill_name="name",
        uuid="uuid",
        centre_date=test_date,
        table=conflux_table,
        output_directory=TEST_LOCAL_DIR,
    )
    outpath = os.path.join(TEST_LOCAL_DIR, "20180101", "name_uuid_20180101-000000-000000.pq")

    fs = fsspec.filesystem("file")
    if fs.exists(outpath):
        assert True
        # Tear down.
        fs.rm(TEST_LOCAL_DIR, recursive=True)
    else:
        assert False
