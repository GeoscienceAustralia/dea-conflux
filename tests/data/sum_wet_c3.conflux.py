import xarray as xr

product_name = 'sum_wet_c3'
version = '0.0.1'
resolution = (-30, 30)
output_crs = 'EPSG:3577'
resampling = 'nearest'

input_products = {
    'ga_ls_wo_3': ['water'],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    return inputs == 128


def summarise(inputs: xr.Dataset) -> xr.Dataset:
    return inputs.sum()
