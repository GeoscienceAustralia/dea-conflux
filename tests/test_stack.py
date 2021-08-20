import datetime
import logging
from pathlib import Path
import re
import sys

import boto3
import botocore
from click.testing import CliRunner
import datacube
import moto
from moto import mock_s3
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


# https://github.com/aio-libs/aiobotocore/issues/755
@pytest.fixture()
def mock_AWSResponse() -> None:
    class MockedAWSResponse(botocore.awsrequest.AWSResponse):
        raw_headers = {}  # type: ignore

        async def read(self):  # type: ignore
            return self.text

    botocore.awsrequest.AWSResponse = MockedAWSResponse
    moto.core.models.AWSResponse = MockedAWSResponse


def test_waterbodies_stacking(tmp_path):
    dea_conflux.stack.stack(
        TEST_PQ_DATA, tmp_path / 'testout',
        mode=dea_conflux.stack.StackMode.WATERBODIES)
    uid = LAKE_GINNINDERRA_ID
    outpath = tmp_path / 'testout' / uid[:4] / f'{uid}.csv'
    assert outpath.exists()
    csv = pd.read_csv(outpath)
    assert len(csv) == 2
    assert len(csv.columns) == 4  # 3 bands + date

@mock_s3
def test_find_parquet_files_s3(mock_AWSResponse):
    # Set up some Parquet files to find.
    s3 = boto3.resource('s3', region_name='ap-southeast-2')
    bucket_name = 'testbucket'
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'ap-southeast-2',
    })
    parquet_keys = [
        'hello.pq',
        'hello/world.pq',
        'hello/world/this/is.parquet']
    not_parquet_keys = [
        'not_parquet',
        'hello/alsonotparquet']
    for key in parquet_keys + not_parquet_keys:
        s3.Object(bucket_name, key).put(Body=b'')
    
    res = dea_conflux.stack.find_parquet_files(f's3://{bucket_name}')
    for key in parquet_keys:
        assert f's3://{bucket_name}/{key}' in res
    for key in not_parquet_keys:
        assert f's3://{bucket_name}/{key}' not in res

