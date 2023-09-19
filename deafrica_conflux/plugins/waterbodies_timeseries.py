import numpy as np
import xarray as xr

product_name = "waterbodies"
version = "0.0.1"
resampling = "nearest"
output_crs = "EPSG:6933"
resolution = (-30, 30)

input_products = {
    "wofs_ls": ["water"],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    wofl = inputs.water

    clear_and_wet = wofl == 128
    clear_and_dry = wofl == 0

    clear = clear_and_wet | clear_and_dry

    # Set the invalid (not clear) pixels to np.nan
    # Remaining values will be 1 if water, 0 if dry
    wofl_masked = clear_and_wet.where(clear)
    return xr.Dataset({"water": wofl_masked})


def summarise(inputs: xr.Dataset, resolution: tuple) -> xr.Dataset:
    """
    Input to this function is dataset, where the .water array contains
    pixel values for a single polygon
    Values are as follows
        1 = water
        0 = dry
        null = invalid (not wet or dry)
    """
    # Area of one pixel in metres squared
    # Use absolute value to remove any negative sign from resolution tuple
    px_area = abs(resolution[0] * resolution[1])

    # Start with pixel based calculations, then get areas and percentage
    px_total = float(len(inputs.water))
    px_invalid = inputs.water.isnull().sum()
    pc_invalid = (px_invalid / px_total) * 100.0
    ar_invalid = px_invalid * px_area

    # Set wet and dry values to nan, which will be used if insufficient pixels are observed
    px_wet = float("nan")
    pc_wet = float("nan")
    ar_wet = float("nan")
    px_dry = float("nan")
    pc_dry = float("nan")
    ar_dry = float("nan")

    # If the proportion of the waterbody missing is less than 10%, calculate values for wet and dry
    if pc_invalid <= 10.0:
        px_wet = inputs.water.sum()
        pc_wet = (px_wet / px_total) * 100.0
        ar_wet = px_wet * px_area

        px_dry = px_total - px_invalid - px_wet
        pc_dry = 100.0 - pc_invalid - pc_wet
        ar_dry = px_dry * px_area

    # Return all calculated values
    return xr.Dataset(
        {
            "pc_wet": pc_wet,
            "px_wet": px_wet,
            "area_wet_m2": ar_wet,
            "pc_dry": pc_dry,
            "px_dry": px_dry,
            "area_dry_m2": ar_dry,
            "pc_invalid": pc_invalid,
            "px_invalid": px_invalid,
            "area_invalid_m2": ar_invalid,
        }
    )
