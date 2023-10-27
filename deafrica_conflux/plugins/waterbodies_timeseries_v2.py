import numpy as np
import pandas as pd
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
    """
    Input to this function is a WOfS Feature Layers dataset where the .water
    array contains bit flags to flag pixels as "wet" or otherwise. The output is
    the masked dataset with the values:
        1 = water
        0 = dry
        np.nan = invalid (not wet or dry)

    Parameters
    ----------
    inputs : xr.Dataset
        WOfS Feature Layers dataset to transform.

    Returns
    -------
    xr.Dataset
        Transformed WOfS Feature Layers dataset.
    """
    wofl = inputs.water

    clear_and_wet = wofl == 128
    clear_and_dry = wofl == 0

    clear = clear_and_wet | clear_and_dry

    # Set the invalid (not clear) pixels to np.nan
    # Remaining values will be 1 if water, 0 if dry
    wofl_masked = clear_and_wet.where(clear)
    return xr.Dataset({"water": wofl_masked})


def summarise(region_mask, intensity_image):
    masked_intensity_image = intensity_image[region_mask]

    # Area of one pixel in metres squared
    # Use absolute value to remove any negative sign from resolution tuple
    px_area = abs(resolution[0] * resolution[1])

    dry_pixel_value = 0
    wet_pixel_value = 1
    invalid_pixel_value = -9999

    unique_values, unique_value_counts = np.unique(masked_intensity_image, return_counts=True)
    unique_values = np.where(np.isnan(unique_values), invalid_pixel_value, unique_values)
    unique_values_and_counts = dict(zip(unique_values, unique_value_counts))

    # Start with pixel based calculations.
    px_total = np.sum(unique_value_counts)
    px_invalid = unique_values_and_counts.get(invalid_pixel_value, np.nan)
    px_dry = unique_values_and_counts.get(dry_pixel_value, np.nan)
    px_wet = unique_values_and_counts.get(wet_pixel_value, np.nan)

    # Calculate areas.
    area_invalid = px_invalid * px_area
    area_dry = px_dry * px_area
    area_wet = px_wet * px_area

    # Calculate percentages.
    pc_wet = (px_wet / px_total) * 100.0
    pc_dry = (px_dry / px_total) * 100.0
    pc_invalid = (px_invalid / px_total) * 100.0

    # If the proportion of the waterbody missing is greater than 10%,
    # set the values for pc_wet and pc_dry to nan.
    if pc_invalid > 10.0:
        pc_wet = np.nan
        pc_dry = np.nan

    # Return all calculated values as a DataFrame.
    results = {
        "pc_wet": [pc_wet],
        "px_wet": [px_wet],
        "area_wet_m2": [area_wet],
        "pc_dry": [pc_dry],
        "px_dry": [px_dry],
        "area_dry_m2": [area_dry],
        "pc_invalid": [pc_invalid],
        "px_invalid": [px_invalid],
        "area_invalid_m2": [area_invalid],
    }
    results_df = pd.DataFrame(results)
    return results_df
