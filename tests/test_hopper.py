import datacube
import pytest
from datacube.ui.click import parse_expressions

from deafrica_conflux.hopper import find_datasets


@pytest.fixture(scope="module")
def dc():
    return datacube.Datacube()


def test_find_datasets(dc):
    query = parse_expressions("lon in [-10.002, -7.929] lat in [13.408, 15.496] time in 2023-06-12")
    datasets = find_datasets(query=query, products=["wofs_ls"], dc=dc)
    assert len(list(datasets)) == 3


def test_find_no_datasets(dc):
    datasets = find_datasets(query={}, products=["not_exist_product"], dc=dc)
    assert len(list(datasets)) == 0
