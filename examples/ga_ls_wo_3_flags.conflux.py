import xarray as xr

product_name = 'ga_ls_wo_3_flags'
version = '0.0.1'
resampling = 'nearest'
output_crs = 'EPSG:3577'
resolution = (-30, 30)

input_products = {
    'ga_ls_wo_3': ['water'],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    return inputs


def summarise(inputs: xr.Dataset) -> xr.Dataset:
    flags = {
        'nodata': 0,
        'noncontiguous': 1,
        'low_solar_angle': 2,
        'terrain_shadow': 3,
        'high_slope': 4,
        'cloud_shadow': 5,
        'cloud': 6,
        'water_observed': 7,
    }
    nandata = inputs.water.isnull().sum()
    flag_values = {
        'nan': nandata,
    }
    for flag, bit in flags.items():
        flag_values[flag] = ((inputs.water & (1 << bit)) > 0).sum()
    return xr.Dataset(flag_values)
