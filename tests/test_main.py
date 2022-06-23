import logging
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner
from moto import mock_s3, mock_sqs

import dea_conflux.__main__ as main_module
from dea_conflux.__main__ import main

# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile.
TEST_SHP = HERE / "data" / "waterbodies_canberra.shp"
SAME_ID_SHAPE = HERE / "data" / "same_user_id_value.shp"
TEST_ID_FIELD = "uid"

TEST_PLUGIN_OK = HERE / "data" / "sum_wet.conflux.py"
TEST_PLUGIN_COMBINED = HERE / "data" / "sum_pv_wet.conflux.py"
TEST_PLUGIN_MISSING_TRANSFORM = HERE / "data" / "sum_wet_missing_transform.conflux.py"

# Under: s3://dea-public-data/baseline/ga_ls7e_ard_3/090/084/2000/02/02/*.json
ARD_UUID = "b17ad657-00fa-4abe-91a6-07fd24895e5d"

TEST_WOFL_ID = "234fec8f-1de7-488a-a115-818ebd4bfec4"
TEST_FC_ID = "4d243358-152e-404c-bb65-7ea64b21ca38"


def setup_module(module):
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger("").handlers = []


@pytest.fixture
def run_main():
    def _run_cli(
        opts,
        catch_exceptions=False,
        expect_success=True,
        cli_method=main,
        input=None,
    ):
        exe_opts = []
        exe_opts.extend(opts)

        runner = CliRunner()
        result = runner.invoke(
            cli_method, exe_opts, catch_exceptions=catch_exceptions, input=input
        )
        if expect_success:
            assert 0 == result.exit_code, "Error for {!r}. output: {!r}".format(
                opts,
                result.output,
            )
        return result

    return _run_cli


def test_main(run_main):
    result = run_main([], expect_success=False)
    # TODO(MatthewJA): Make this assert that the output makes sense.
    assert result


def test_get_crs():
    crs = main_module.get_crs(TEST_SHP)
    assert crs.epsg == 3577


def test_guess_id_field():
    id_field = main_module.guess_id_field(TEST_SHP)
    assert id_field == TEST_ID_FIELD


def test_guess_id_field_with_same_value():
    with pytest.raises(ValueError) as e_info:
        main_module.guess_id_field(SAME_ID_SHAPE, "same_id")
    assert "values are not unique" in str(e_info)


def test_guess_id_field_with_not_exist_user_id():
    with pytest.raises(ValueError) as e_info:
        main_module.guess_id_field(TEST_SHP, "not_exist_id")
        # Ideally, we should put these text to a config file
    assert "Couldn't find any ID field" in str(e_info)


def test_guess_id_field_with_user_id():
    id_field = main_module.guess_id_field(TEST_SHP, TEST_ID_FIELD)
    assert id_field == TEST_ID_FIELD


def test_validate_plugin():
    plugin = main_module.run_plugin(TEST_PLUGIN_OK)
    main_module.validate_plugin(plugin)


def test_validate_plugin_no_transform():
    plugin = main_module.run_plugin(TEST_PLUGIN_MISSING_TRANSFORM)
    with pytest.raises(ValueError):
        main_module.validate_plugin(plugin)


def test_run_one(run_main):
    run_one_result = run_main(
        ["run-one", "-p", TEST_PLUGIN_OK, "-s", TEST_SHP, "-o", "test_output", "-vv"],
        expect_success=True,
    )
    print(run_one_result)


@mock_sqs
def test_run_from_queue(run_main):
    queue_name = "waterbodies_queue_name"
    import boto3

    sqs = boto3.resource("sqs")
    waterbodies_queue = sqs.create_queue(QueueName=queue_name)
    _ = sqs.create_queue(QueueName=queue_name + "_deadletter")

    waterbodies_queue.send_message(MessageBody=ARD_UUID)

    not_overwrite = run_main(
        [
            "run-from-queue",
            "-p",
            TEST_PLUGIN_OK,
            "-q",
            queue_name,
            "-s",
            TEST_SHP,
            "-o",
            "test_output",
            "--no-db",
            "-vv",
        ],
        expect_success=True,
    )
    print(not_overwrite)

    waterbodies_queue.send_message(MessageBody=ARD_UUID)

    overwrite = run_main(
        [
            "run-from-queue",
            "-p",
            TEST_PLUGIN_OK,
            "-q",
            queue_name,
            "-s",
            TEST_SHP,
            "-o",
            "test_output",
            "--no-db",
            "--overwrite",
            "-vv",
        ],
        expect_success=True,
    )
    print(overwrite)


@mock_s3
def test_get_ids(run_main):

    import boto3

    get_ids_result = run_main(
        [
            "get-ids",
            "ga_ls_wo_3",
            "-vv",
        ],
        expect_success=True,
    )
    print(get_ids_result)

    s3 = boto3.resource("s3", region_name="ap-southeast-2")
    bucket_name = "testbucket"
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": "ap-southeast-2",
        },
    )

    get_ids_result = run_main(
        ["get-ids", "ga_ls_wo_3", "-vv", "--s3", "--bucket-name", bucket_name],
        expect_success=True,
    )
    print(get_ids_result)

    get_ids_result = run_main(
        [
            "get-ids",
            "ga_ls_wo_3",
            "-vv",
            "-s",
            TEST_SHP,
        ],
        expect_success=True,
    )
    print(get_ids_result)


@mock_sqs
def test_filter_from_queue(run_main):
    queue_name = "waterbodies_queue_name"
    raw_queue_name = queue_name + "_raw"
    import boto3

    sqs = boto3.resource("sqs")
    _ = sqs.create_queue(QueueName=queue_name)
    raw_queue = sqs.create_queue(QueueName=raw_queue_name)

    # uuid is: s3://dea-public-data/baseline/ga_ls7e_ard_3/090/084/2000/02/02/*.json
    raw_queue.send_message(MessageBody=ARD_UUID)

    filter_result = run_main(
        [
            "filter-from-queue",
            "-iq",
            raw_queue_name,
            "-oq",
            queue_name,
            "-s",
            TEST_SHP,
            "-vv",
        ],
        expect_success=True,
    )
    print(filter_result)


@mock_sqs
def test_make_s3_queue(run_main):
    make_queue_result = run_main(
        ["make", "waterbodies_queue_name"], expect_success=True
    )
    print(make_queue_result)


@mock_sqs
def test_push_to_s3_queue(run_main):

    import os

    import boto3

    queue_name = "waterbodies_queue_name"
    file_name = "test_id.txt"

    sqs = boto3.resource("sqs")
    _ = sqs.create_queue(QueueName=queue_name)

    with open(file_name, "w") as f:
        f.write(ARD_UUID)

    make_queue_result = run_main(
        ["push-to-queue", "--txt", file_name, "--queue", queue_name],
        expect_success=True,
    )
    print(make_queue_result)
    os.remove(file_name)


@mock_sqs
def test_delete_s3_queue(run_main):
    queue_name = "waterbodies_queue_name"
    import boto3

    sqs = boto3.resource("sqs")
    _ = sqs.create_queue(QueueName=queue_name)
    _ = sqs.create_queue(QueueName=queue_name + "_deadletter")

    del_queue_result = run_main(
        ["delete", queue_name],
        expect_success=True,
    )
    print(del_queue_result)


# TODO(MatthewJA): Add a test on scene 234fec8f-1de7-488a-a115-818ebd4bfec4.
# This is a WOfL that we are already loading for testing.
