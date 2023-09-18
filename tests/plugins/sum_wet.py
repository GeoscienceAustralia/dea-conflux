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
    valid_and_wet_count = np.count_nonzero(inputs.water == 128)

    return xr.Dataset(
        {
            "wet_pixel_count": valid_and_wet_count,
        }
    )
