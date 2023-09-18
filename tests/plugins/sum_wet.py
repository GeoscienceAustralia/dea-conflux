import numpy as np
import xarray as xr

product_name = "sum_wet"
version = "0.0.1"
resolution = (-30, 30)
output_crs = "EPSG:6933"

input_products = {
    "wofs_ls": ["water"],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    wofl = inputs.water

    clear_and_wet = wofl == 128
    clear_and_dry = wofl == 0

    clear = clear_and_wet | clear_and_dry

    # Set the invalid (not clear) pixels to np.nan.
    wofl_clear = wofl.where(clear, np.nan)
    return xr.Dataset({"water": wofl_clear})


def summarise(inputs: xr.Dataset) -> xr.Dataset:
    pixel_count = inputs.water.size

    valid_count = np.count_nonzero(~np.isnan(inputs.water))
    invalid_count = np.count_nonzero(np.isnan(inputs.water))

    assert valid_count + invalid_count == pixel_count

    valid_and_dry_count = np.count_nonzero(inputs.water == 0)
    valid_and_wet_count = np.count_nonzero(inputs.water == 128)

    valid_and_wet_percentage = (valid_and_wet_count / pixel_count) * 100
    valid_and_dry_percentage = (valid_and_dry_count / pixel_count) * 100  # noqa F841
    invalid_percentage = (invalid_count / pixel_count) * 100

    return xr.Dataset(
        {
            "wet_percentage": valid_and_wet_percentage,
            "wet_pixel_count": valid_and_wet_count,
            "invalid_percentage": invalid_percentage,
        }
    )
