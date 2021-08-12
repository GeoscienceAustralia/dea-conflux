"""Test constants."""

from pathlib import Path

# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile.
TEST_SHP = HERE / 'data' / 'waterbodies_canberra.shp'

TEST_PLUGIN_OK = HERE / 'data' / 'sum_wet.conflux.py'
TEST_PLUGIN_COMBINED = HERE / 'data' / 'sum_pv_wet.conflux.py'
TEST_PLUGIN_MISSING_TRANSFORM = HERE / 'data' / 'sum_wet_missing_transform.conflux.py'

TEST_WOFL_ID = '234fec8f-1de7-488a-a115-818ebd4bfec4'
TEST_FC_ID = '4d243358-152e-404c-bb65-7ea64b21ca38'
