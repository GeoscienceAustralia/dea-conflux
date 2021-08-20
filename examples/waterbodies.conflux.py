import xarray as xr

product_name = 'waterbodies'
version = '0.0.1'
resampling = 'nearest'
output_crs = 'EPSG:3577'
resolution = (-25, 25)

input_products = {
    'wofs_albers': ['water'],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    is_wet = inputs.water == 128
    is_ok = is_wet | (inputs.water == 0)
    masked_wet = is_wet.where(is_ok)
    return xr.Dataset({'water': masked_wet})


def summarise(inputs: xr.Dataset) -> xr.Dataset:
    pc_missing = inputs.water.isnull().mean()
    px_wet = pc_wet = float('nan')
    if pc_missing <= 0.1:
        px_wet = inputs.water.sum()
        pc_wet = px_wet / inputs.water.size
    return xr.Dataset({
        'px_wet': px_wet,
        'pc_wet': pc_wet,
        'pc_missing': pc_missing,
    })
