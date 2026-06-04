import pytest
from click import ClickException
from moto import mock_aws

from dea_conflux.queues import get_queue, move_to_deadletter_queue, verify_name

# Under: s3://dea-public-data/baseline/ga_ls7e_ard_3/090/084/2000/02/02/*.json
ARD_UUID = "b17ad657-00fa-4abe-91a6-07fd24895e5d"


@mock_aws
def test_get_queue():
    import boto3

    sqs = boto3.resource("sqs")
    _ = sqs.create_queue(QueueName="waterbodies_queue_name")

    _ = get_queue("waterbodies_queue_name")


def test_verify_name():
    with pytest.raises(ClickException) as e_info:
        verify_name("another_queue_name")
    assert "DEA conflux queues must start with waterbodies_ or wit_" in str(e_info)


@mock_aws
def test_move_to_deadletter_queue():
    import boto3

    sqs = boto3.resource("sqs")
    _ = sqs.create_queue(QueueName="waterbodies_queue_dl")
    _ = move_to_deadletter_queue("waterbodies_queue_dl", ARD_UUID)
