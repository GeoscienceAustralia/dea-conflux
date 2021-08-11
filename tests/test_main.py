import logging
from pathlib import Path
import re
import sys

from click.testing import CliRunner
import pytest

from dea_conflux.__main__ import main
import dea_conflux.__main__ as main_module


# Test directory.
HERE = Path(__file__).parent.resolve()
logging.basicConfig(level=logging.INFO)

# Path to Canberra test shapefile.
TEST_SHP = HERE / 'data' / 'waterbodies_canberra.shp'

TEST_PLUGIN_OK = HERE / 'data' / 'sum_wet.conflux.py'
TEST_PLUGIN_MISSING_TRANSFORM = HERE / 'data' / 'sum_wet_missing_transform.conflux.py'


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
            cli_method, exe_opts,
            catch_exceptions=catch_exceptions, input=input)
        if expect_success:
            assert 0 == result.exit_code, "Error for %r. output: %r" % (
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
    assert id_field == 'UID'


def test_validate_plugin():
    plugin = main_module.run_plugin(TEST_PLUGIN_OK)
    main_module.validate_plugin(plugin)
    assert True  # haven't thrown an exception


def test_validate_plugin_no_transform():
    plugin = main_module.run_plugin(TEST_PLUGIN_MISSING_TRANSFORM)
    with pytest.raises(ValueError):
        main_module.validate_plugin(plugin)
