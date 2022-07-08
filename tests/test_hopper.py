import inspect
import os
import sys

import datacube
import pytest

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from dea_conflux.hopper import find_datasets


@pytest.fixture(scope="module")
def dc():
    return datacube.Datacube()


def test_find_datasets(dc):
    datasets = find_datasets(query={}, products=["ga_ls_wo_3"], dc=dc)
    assert len(list(datasets)) == 4


def test_find_no_datasets(dc):
    datasets = find_datasets(query={}, products=["not_exist_product"], dc=dc)
    assert len(list(datasets)) == 0
