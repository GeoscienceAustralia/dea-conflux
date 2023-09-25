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
TEST_WATERBODY = os.path.join(HERE, "data", "edumesbb2.geojson")
TEST_TEXT_FILE_LOCAL = HERE / "data" / "conflux_dataset_ids_edumesbb2.txt"
TEST_S3_BUCKET = "test-bucket"
TEST_TEXT_FILE_S3 = f"s3://{TEST_S3_BUCKET}/conflux_dataset_ids_edumesbb2.txt"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3_resource(aws_credentials):
    with mock_s3():
        return boto3.client("s3", region_name="af-south-1")


@pytest.fixture
def runner():
    return CliRunner(echo_stdin=True)


def test_get_dataset_ids_write_to_local_file(runner):
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-30]"
    polygons_vector_file = str(TEST_WATERBODY)
    use_id = "UID"
    output_file_path = str(TEST_TEXT_FILE_LOCAL)
    num_worker = "8"
    args = ["--verbose",
            f"--polygons-vector-file={polygons_vector_file}",
            f"--use-id={use_id}",
            f"--output-file-path={output_file_path}",
            f"--num-worker={num_worker}",
            product,
            expressions]
    
    # If file exists remove it.
    fs = fsspec.filesystem("file")
    if fs.exists(output_file_path):
        fs.rm(output_file_path)

    result = runner.invoke(get_dataset_ids, args=args, catch_exceptions=True)

    assert result.exit_code == 0

    with fs.open(output_file_path, "r") as f:
        dataset_ids = f.readlines()
        dataset_ids = [idx.strip() for idx in dataset_ids]

    assert len(dataset_ids) == 10

    # Clean up
    fs.rm(output_file_path)


def test_get_dataset_ids_write_to_existing_local_file(runner):
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-30]"
    polygons_vector_file = str(TEST_WATERBODY)
    use_id = "UID"
    output_file_path = str(TEST_TEXT_FILE_LOCAL)
    num_worker = "8"
    args = ["--verbose",
            f"--polygons-vector-file={polygons_vector_file}",
            f"--use-id={use_id}",
            f"--output-file-path={output_file_path}",
            f"--num-worker={num_worker}",
            product,
            expressions]
    
    # If file exists remove it.
    fs = fsspec.filesystem("file")
    if fs.exists(output_file_path):
        fs.rm(output_file_path)

    # Create an empty file.
    fs.touch(output_file_path)

    result = runner.invoke(get_dataset_ids, args=args, catch_exceptions=True)

    assert result.exit_code == 1
    assert FileExistsError in result.exc_info

    # Clean up
    fs.rm(output_file_path)


@pytest.mark.skip(reason="This test fails due to this error from aiobotocore issue AttributeError: 'MockRawResponse' object has no attribute 'raw_headers'")
@mock_s3
def test_get_dataset_ids_write_to_s3_file(runner, s3_client):
    # This test fails due to this error from aiobotocore issue
    # AttributeError: 'MockRawResponse' object has no attribute 'raw_headers'
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-30]"
    polygons_vector_file = str(TEST_WATERBODY)
    use_id = "UID"
    output_file_path = str(TEST_TEXT_FILE_S3)
    num_worker = "8"
    args = ["--verbose",
            f"--polygons-vector-file={polygons_vector_file}",
            f"--use-id={use_id}",
            f"--output-file-path={output_file_path}",
            f"--num-worker={num_worker}",
            product,
            expressions]
    
    # If file exists remove it.
    fs = fsspec.filesystem("s3")
    if fs.exists(output_file_path):
        fs.rm(output_file_path)

    result = runner.invoke(get_dataset_ids, args=args, catch_exceptions=True)

    assert result.exit_code == 0
