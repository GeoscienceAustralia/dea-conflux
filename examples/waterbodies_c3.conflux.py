import xarray as xr

product_name = 'waterbodies_c3'
version = '0.0.1'
resampling = 'nearest'
output_crs = 'EPSG:3577'
resolution = (-30, 30)

input_products = {
    'ga_ls_wo_3': ['water'],
}


def transform(inputs: xr.Dataset) -> xr.Dataset:
    # ignore sea, terrain/low solar angle
    # by disabling those flags
    wofl = inputs.water & 0b11110011
    # then check for wet, dry
    is_wet = wofl == 128
    is_ok = is_wet | (wofl == 0)
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