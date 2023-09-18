import datacube
import pytest

from deafrica_conflux.hopper import find_datasets


@pytest.fixture(scope="module")
def dc():
    return datacube.Datacube()


def test_find_datasets(dc):
    datasets = find_datasets(query={}, products=["wofs_ls"], dc=dc)
    dss = list(datasets)
    assert len(dss) == 24


def test_find_no_datasets(dc):
    datasets = find_datasets(query={}, products=["not_exist_product"], dc=dc)
    dss = list(datasets)
    assert len(dss) == 0
