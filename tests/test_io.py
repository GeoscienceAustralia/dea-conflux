import datetime
import logging
import os
import random
import sys

import boto3
import pandas as pd
import pytest
from moto import mock_s3

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


def test_write_table(conflux_table, tmp_path):
    test_date = datetime.datetime(2018, 1, 1)
    deafrica_conflux.io.write_table_to_parquet(
        "name", "uuid", test_date, conflux_table, tmp_path / "outdir"
    )
    outpath = tmp_path / "outdir" / "20180101" / "name_uuid_20180101-000000-000000.pq"
    assert outpath.exists()


def test_read_write_table(conflux_table, tmp_path):
    test_date = datetime.datetime(2018, 1, 1)
    deafrica_conflux.io.write_table_to_parquet(
        "name", "uuid", test_date, conflux_table, tmp_path / "outdir"
    )
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


def test_check_local_dir_exists_true():
    # Create dummy directory.
    dir_path = "dummy_dir"
    os.makedirs(dir_path)
    # Check if dir exists.
    assert deafrica_conflux.io.check_local_dir_exists(dir_path)
    # Remove dummy dir.
    os.rmdir(dir_path)


def test_check_local_dir_exists_false():
    dir_path = "dummy_dir"
    # Check if dir exists.
    assert not deafrica_conflux.io.check_local_dir_exists(dir_path)


def test_check_local_file_exists_true():
    # Create dummy text file.
    file_fp = "dummy.txt"
    with open(file_fp, "w") as f:  # noqa F841
        pass

    assert deafrica_conflux.io.check_local_file_exists(file_fp)
    # Remove dummy file.
    os.remove(file_fp)


def test_check_local_file_exists_false():
    file_fp = "dummy.txt"
    assert not deafrica_conflux.io.check_local_file_exists(file_fp)


def test_check_if_s3_uri_using_local_file():
    # Create a dummy text file.
    file_fp = "dummy.txt"
    with open(file_fp, "w") as f:  # noqa F841
        pass

    if deafrica_conflux.io.check_if_s3_uri(file_fp) is False:
        assert True
    else:
        assert False

    # Delete dummy text file.
    os.remove(file_fp)


@mock_s3
def test_check_s3_bucket_exists_true(s3_client):
    bucket_name = "my-mock-bucket"
    # Create the test bucket.
    response = s3_client.create_bucket(
        Bucket=bucket_name,  # noqa F841
        CreateBucketConfiguration={"LocationConstraint": "af-south-1"},
    )

    assert deafrica_conflux.io.check_s3_bucket_exists(bucket_name=bucket_name, s3_client=s3_client)


@mock_s3
def test_check_s3_bucket_exists_false(s3_client):
    bucket_name = "my-mock-bucket"
    assert not deafrica_conflux.io.check_s3_bucket_exists(
        bucket_name=bucket_name, s3_client=s3_client
    )


@mock_s3
def test_check_s3_objects_exists_true(s3_client):
    bucket_name = "my-mock-bucket"
    file_fp = "dummy.txt"

    # Create bucket.
    response = s3_client.create_bucket(
        Bucket=bucket_name,  # noqa F841
        CreateBucketConfiguration={"LocationConstraint": "af-south-1"},
    )

    # Create dummy text file.
    with open(file_fp, "w") as f:  # noqa F841
        pass

    object_name = os.path.basename(file_fp)

    # Upload file to bucket.
    s3_client.upload_file(file_fp, bucket_name, object_name)

    os.remove(file_fp)

    # Get object url
    object_url = f"s3://{bucket_name}/{object_name}"

    assert deafrica_conflux.io.check_s3_object_exists(object_url, s3_client=s3_client)


@mock_s3
def test_check_s3_objects_exists_false(s3_client):
    bucket_name = "my-mock-bucket"
    object_name = "dummy.txt"
    object_url = f"s3://{bucket_name}/{object_name}"

    # Create bucket.
    response = s3_client.create_bucket(
        Bucket=bucket_name,  # noqa F841
        CreateBucketConfiguration={"LocationConstraint": "af-south-1"},
    )

    assert not deafrica_conflux.io.check_s3_object_exists(object_url, s3_client=s3_client)
