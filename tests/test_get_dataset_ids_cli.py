import os
from pathlib import Path

import boto3
import fsspec
import pytest
from click.testing import CliRunner
from moto import mock_s3

from deafrica_conflux.cli.get_dataset_ids import get_dataset_ids

# Test directory.
HERE = Path(__file__).parent.resolve()
TEST_WATERBODY = HERE / "data" / "edumesbb2.geojson"
TEST_TEXT_FILE_LOCAL = HERE / "data" / "edumesbb2_conflux_ids.txt"
TEST_S3_BUCKET = "test-bucket"
TEST_TEXT_FILE_S3 = f"s3://{TEST_S3_BUCKET}/edumesbb2_conflux_ids.txt"


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


def test_get_dataset_ids_write_to_local_file():
    runner = CliRunner(echo_stdin=True)
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-15]"
    polygons_vector_file = TEST_WATERBODY
    use_id = "UID"
    output_file_path = TEST_TEXT_FILE_LOCAL
    num_worker = 8
    cmd = f"{product} {expressions} --verbose --polygons-vector-file={polygons_vector_file} --use-id={use_id} --output-file-path={output_file_path} --num-worker={num_worker}"

    fs = fsspec.filesystem("file")

    if fs.exists(output_file_path):
        fs.rm(output_file_path)

    result = runner.invoke(get_dataset_ids, cmd)
    assert result.exit_code == 0

    with fs.open(output_file_path, "rb") as f:
        dataset_ids = f.readlines()
        dataset_ids = [idx.strip() for idx in dataset_ids]

    assert len(dataset_ids) == 5


def test_get_dataset_ids_with_existing_ids_local_file():
    runner = CliRunner(echo_stdin=True)
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-30]"
    polygons_vector_file = TEST_WATERBODY
    use_id = "UID"
    output_file_path = TEST_TEXT_FILE_LOCAL
    num_worker = 8
    cmd = f"{product} {expressions} --verbose --polygons-vector-file={polygons_vector_file} --use-id={use_id} --output-file-path={output_file_path} --num-worker={num_worker}"
    result = runner.invoke(get_dataset_ids, cmd)

    fs = fsspec.filesystem("file")

    if fs.exists(output_file_path):
        fs.rm(output_file_path)

    assert type(result.exception) is FileExistsError


@mock_s3
def test_get_dataset_ids_write_to_s3_file(s3_client):
    # Create the test bucket.
    response = s3_client.create_bucket(  # noqa F841
        Bucket=TEST_S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "af-south-1"},
    )

    runner = CliRunner(echo_stdin=True)
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-15]"
    polygons_vector_file = TEST_WATERBODY
    use_id = "UID"
    output_file_path = TEST_TEXT_FILE_S3
    num_worker = 8
    cmd = f"{product} {expressions} --verbose --polygons-vector-file={polygons_vector_file} --use-id={use_id} --output-file-path={output_file_path} --num-worker={num_worker}"

    result = runner.invoke(get_dataset_ids, cmd)
    assert result.exit_code == 0

    fs = fsspec.filesystem("s3")
    with fs.open(output_file_path, "rb") as f:
        dataset_ids = f.readlines()
        dataset_ids = [idx.strip() for idx in dataset_ids]

    assert len(dataset_ids) == 5
