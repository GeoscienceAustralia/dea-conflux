import xarray as xr

product_name = 'sum_wet'
version = '0.0.1'

input_products = {
    'ga_ls_wo_3': ['water'],
}

def transform(inputs: xr.Dataset) -> xr.Dataset:
    return inputs == 128

def summarise(inputs: xr.Dataset) -> xr.Dataset:
    return inputs.water.sum()
