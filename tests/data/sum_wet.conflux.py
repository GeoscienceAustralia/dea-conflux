import xarray as xr

product_name = 'sum_wet'
version = '0.0.1'
resolution = (-25, 25)
output_crs = 'EPSG:3577'

input_products = {
    'wofs_albers': ['water'],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    return inputs == 128


def summarise(inputs: xr.Dataset) -> xr.Dataset:
    return inputs.sum()
