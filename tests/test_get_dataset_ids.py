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


def test_get_dataset_ids_write_to_local_file(runner, capsys: pytest.CaptureFixture):
    product = "wofs_ls"
    temporal_range = "2023-01-01--P1M"
    polygons_split_by_region_directory = os.path.join(HERE, "data", "polygons_split_by_region")
    output_directory = os.path.join(HERE, "data")
    args = [
        "--verbose",
        f"--product={product}",
        f"--temporal-range={temporal_range}",
        f"--polygons-split-by-region-directory={polygons_split_by_region_directory}",
        f"--output-directory={output_directory}"
    ]

    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(get_dataset_ids, args=args, catch_exceptions=True)

    assert result.exit_code == 0

    output_file_path = os.path.join(output_directory, "conflux_dataset_ids", f"{product}_{temporal_range}_batch1.txt")
        
    fs = fsspec.filesystem("file")
    with fs.open(output_file_path, "r") as f:
        dataset_ids = f.readlines()
        dataset_ids = [idx.strip() for idx in dataset_ids]

    assert len(dataset_ids) == 4

    # Clean up
    fs.rm(os.path.join(output_directory, "conflux_dataset_ids"), recursive=True)
