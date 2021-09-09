import xarray as xr

product_name = 'sum_pv_wet'
version = '0.0.1'

input_products = {
    'wofs_albers': ['water'],
    'ls7_fc_albers': ['PV'],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    return xr.Dataset({
        'water': inputs.water == 128,
        'pv': inputs.PV > 0.5,
    })


def summarise(inputs: xr.Dataset) -> xr.Dataset:
    return inputs.sum()
