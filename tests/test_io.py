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

HERE = Path(__file__).parent.resolve()
TEST_LOCAL_DIR = HERE / "data" / "test_dir"
TEST_LOCAL_FILE = os.path.join(TEST_LOCAL_DIR, "test_txt.txt")

TEST_BUCKET = "test-bucket"
TEST_S3_DIR = f"s3://{TEST_BUCKET}/test_dir"
TEST_S3_FILE = os.path.join(TEST_S3_DIR, "test_txt.txt")


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
    os.environ["AWS_DEFAULT_REGION"] = "af-south-1"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="af-south-1")


def test_write_table_local(conflux_table):
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


def test_write_table_s3(conflux_table, s3_client):
    # Create bucket.
    s3_client.create_bucket(
        ACL="public-read-write",
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "af-south-1"},
    )

    test_date = datetime.datetime(2018, 1, 1)
    deafrica_conflux.io.write_table_to_parquet(
        drill_name="name",
        uuid="uuid",
        centre_date=test_date,
        table=conflux_table,
        output_directory=TEST_S3_DIR,
    )
    outpath = os.path.join(TEST_S3_DIR, "20180101", "name_uuid_20180101-000000-000000.pq")

    fs = fsspec.filesystem("s3")
    if fs.exists(outpath):
        assert True
        # Tear down.
        fs.rm(TEST_S3_DIR, recursive=True)
    else:
        assert False


def test_read_write_table(conflux_table):
    test_date = datetime.datetime(2018, 1, 1)
    deafrica_conflux.io.write_table_to_parquet(
        drill_name="name",
        uuid="uuid",
        centre_date=test_date,
        table=conflux_table,
        output_directory=TEST_LOCAL_DIR,
    )
    outpath = os.path.join(TEST_LOCAL_DIR, "20180101", "name_uuid_20180101-000000-000000.pq")
    table = deafrica_conflux.io.read_table_from_parquet(outpath)
    assert len(table) == 3
    assert len(table.columns) == 2
    assert table.attrs["date"] == "20180101-000000-000000"
    assert table.attrs["drill"] == "name"

    # Tear down.
    fs = fsspec.filesystem("file")
    fs.rm(TEST_LOCAL_DIR, recursive=True)


def test_string_date():
    random.seed(0)
    for _ in range(100):
        d = random.randrange(1, 1628000000)  # from the beginning of time...
        d = datetime.datetime.fromtimestamp(d)
        assert deafrica_conflux.io.string_to_date(deafrica_conflux.io.date_to_string(d)) == d


def test_check_local_dir_exists_true():
    # Create dummy directory.
    fs = fsspec.filesystem("file")
    fs.mkdirs(TEST_LOCAL_DIR, exist_ok=True)

    # Check if dir exists.
    assert deafrica_conflux.io.check_dir_exists(TEST_LOCAL_DIR)

    # Tear down.
    fs.rm(TEST_LOCAL_DIR, recursive=True)


def test_check_local_dir_exists_false():
    # Check if dir exists.
    assert not deafrica_conflux.io.check_dir_exists(TEST_LOCAL_DIR)


@mock_s3
def test_check_s3_dir_exists_true(s3_client):
    # Create the test bucket.
    response = s3_client.create_bucket(  # noqa F841
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "af-south-1"},
    )
    # Create the s3 directory.
    fs = fsspec.filesystem("s3")
    fs.mkdirs(TEST_S3_DIR)

    assert deafrica_conflux.io.check_dir_exists(TEST_S3_DIR)

    # Tear down.
    fs.rm(TEST_S3_DIR, recursive=True)
