import datetime
import logging
import sys
from pathlib import Path

import boto3
import botocore
import moto
import pandas as pd
import pytest
from moto import mock_s3

import dea_conflux.db
import dea_conflux.stack

logging.basicConfig(level=logging.INFO)

# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile.
TEST_SHP = HERE / "data" / "waterbodies_canberra.shp"
# UID of Lake Ginninderra.
LAKE_GINNINDERRA_ID = "r3dp84s8n"

WIT_POLYGON_ID = "r4ucrn3y1_v2"

TEST_PLUGIN_OK = HERE / "data" / "sum_wet.conflux.py"
TEST_PLUGIN_COMBINED = HERE / "data" / "sum_pv_wet.conflux.py"
TEST_PLUGIN_MISSING_TRANSFORM = HERE / "data" / \
    "sum_wet_missing_transform.conflux.py"

TEST_WB_PQ_DATA = HERE / "data" / "canberra_waterbodies_pq"
TEST_WB_PQ_DATA_FILE = (
    TEST_WB_PQ_DATA
    / "waterbodies_234fec8f-1de7-488a-a115-818ebd4bfec4_20000202-234328-500000.pq"
)

TEST_WIT_PQ_DATA = HERE / "data" / "qld_waterbodies_pq"
TEST_WIT_PQ_DATA_FILE = (
    TEST_WIT_PQ_DATA
    / "wit_ls5_aa7116e4-b27d-466b-b987-7c99f7f29b63_19870523-234949-486906.pq"
)

TEST_WIT_DUPLICATES_PQ_DATA = HERE / "data" / "wit_r3bz75m73_pq"
TEST_WIT_DUPLICATES_POLYGON_ID = "r3bz75m73"
TEST_WIT_CSV_DATA = HERE / "data" / "qld_waterbodies_csv"
TEST_WIT_CSV_DATA_FILE = TEST_WIT_CSV_DATA / "r4e3jw0v8_v2.csv"

TEST_WOFL_ID = "234fec8f-1de7-488a-a115-818ebd4bfec4"
TEST_FC_ID = "4d243358-152e-404c-bb65-7ea64b21ca38"


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


# https://github.com/aio-libs/aiobotocore/issues/755
@pytest.fixture()
def mock_aws_response() -> None:
    class MockedAWSResponse(botocore.awsrequest.AWSResponse):
        raw_headers = {}  # type: ignore

        async def read(self):  # type: ignore
            return self.text

    botocore.awsrequest.AWSResponse = MockedAWSResponse
    moto.core.models.AWSResponse = MockedAWSResponse


def test_waterbodies_stacking(tmp_path):
    dea_conflux.stack.stack(
        TEST_WB_PQ_DATA,
        mode=dea_conflux.stack.StackMode.WATERBODIES,
        output_dir=tmp_path / "testout",
    )
    uid = LAKE_GINNINDERRA_ID
    outpath = tmp_path / "testout" / uid[:4] / f"{uid}.csv"
    assert outpath.exists()
    csv = pd.read_csv(outpath)
    assert len(csv) == 2
    assert len(csv.columns) == 4  # 3 bands + date


def test_wit_stacking(tmp_path):
    dea_conflux.stack.stack(
        TEST_WIT_PQ_DATA,
        mode=dea_conflux.stack.StackMode.WITTOOLING,
        output_dir=f"{tmp_path}/testout",
    )
    outpath = tmp_path / "testout" / f"{WIT_POLYGON_ID}.csv"
    assert outpath.exists()
    csv = pd.read_csv(outpath)
    assert len(csv) == 1
    assert (
        len(csv.columns) == 11
    )  # bs, npv, pc_missing, pv, water, wet, date, feature_id, norm_pv, norm_npv, norm_bs


def test_wit_duplicate_stacking(tmp_path):
    dea_conflux.stack.stack(
        TEST_WIT_DUPLICATES_PQ_DATA,
        mode=dea_conflux.stack.StackMode.WITTOOLING,
        output_dir=f"{tmp_path}/testout",
    )
    outpath = tmp_path / "testout" / f"{TEST_WIT_DUPLICATES_POLYGON_ID}.csv"
    assert outpath.exists()
    csv = pd.read_csv(outpath)
    assert len(csv) == 4
    assert (
        len(csv.columns) == 11
    )  # bs, npv, pc_missing, pv, water, wet, date, feature_id, norm_pv, norm_npv, norm_bs


def test_wit_single_file_stacking(tmp_path):
    dea_conflux.stack.stack(
        path=TEST_WIT_CSV_DATA,
        mode=dea_conflux.stack.StackMode.WITTOOLING_SINGLE_FILE_DELIVERY,
        output_dir=f"{tmp_path}/testout",
        precision=2,
    )
    out_csv_path = tmp_path / "testout" / "overall.csv"
    out_pq_path = tmp_path / "testout" / "overall.pq"
    assert out_csv_path.exists()
    assert out_pq_path.exists()


@mock_s3
def test_find_parquet_files_s3(mock_aws_response):
    # Set up some Parquet files to find.
    s3 = boto3.resource("s3", region_name="ap-southeast-2")
    bucket_name = "testbucket"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": "ap-southeast-2",
        },
    )
    parquet_keys = ["hello.pq", "hello/world.pq",
                    "hello/world/this/is.parquet"]
    not_parquet_keys = ["not_parquet", "hello/alsonotparquet"]
    parquet_keys_constrained = [
        "hello/world/missme.pq",
    ]
    for key in parquet_keys + not_parquet_keys + parquet_keys_constrained:
        s3.Object(bucket_name, key).put(Body=b"")

    res = dea_conflux.stack.find_parquet_files(f"s3://{bucket_name}")
    for key in parquet_keys + parquet_keys_constrained:
        assert f"s3://{bucket_name}/{key}" in res
    for key in not_parquet_keys:
        assert f"s3://{bucket_name}/{key}" not in res

    # Repeat that test with a constraint.
    res = dea_conflux.stack.find_parquet_files(
        f"s3://{bucket_name}", pattern="[^m]*$")
    for key in parquet_keys:
        assert f"s3://{bucket_name}/{key}" in res
    for key in not_parquet_keys + parquet_keys_constrained:
        assert f"s3://{bucket_name}/{key}" not in res


def test_waterbodies_db_stacking():
    engine = dea_conflux.db.get_engine_inmem()
    Session = dea_conflux.stack.sessionmaker(bind=engine)
    session = Session()
    dea_conflux.stack.stack_waterbodies_db(
        paths=[TEST_WB_PQ_DATA_FILE], verbose=True, engine=engine, uids=None
    )
    all_obs = list(session.query(dea_conflux.db.WaterbodyObservation).all())
    # Check all observations exist
    assert len(all_obs) == 445
    # Check time is correct
    correct_time = datetime.datetime(2000, 2, 2, 23, 43, 28, 500000)
    assert all(obs.date == correct_time for obs in all_obs)


def test_db_to_csv_stacking(tmp_path):
    engine = dea_conflux.db.get_engine_inmem()
    Session = dea_conflux.stack.sessionmaker(bind=engine)
    _ = Session()
    dea_conflux.stack.stack_waterbodies_db(
        paths=[TEST_WB_PQ_DATA_FILE], verbose=True, engine=engine, uids=None
    )
    dea_conflux.stack.stack_waterbodies_db_to_csv(
        out_path=tmp_path / "testout",
        verbose=True,
        engine=engine,
        uids=None,
        n_workers=1,
    )
