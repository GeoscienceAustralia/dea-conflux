import numpy as np
import xarray as xr

product_name = "wit_ls9"
version = "0.0.1"
resampling = {
    "water": "nearest",
    "bs": "nearest",
    "pv": "nearest",
    "npv": "nearest",
    "fmask": "nearest",
    "*": "bilinear",
}
output_crs = "EPSG:3577"
resolution = (-30, 30)

# load Water Observations, Landsat 9 and Fractional Cover data
input_products = {
    "ga_ls_wo_3": ["water"],
    "ga_ls9c_ard_3": [
        "nbart_blue",
        "nbart_green",
        "nbart_red",
        "nbart_nir",
        "nbart_swir_1",
        "nbart_swir_2",
    ],
    "ga_ls_fc_3": ["bs", "pv", "npv"],
}


def _tcw(ds: xr.Dataset) -> xr.DataArray:
    # Tasseled Cap Wetness, Crist 1985
    ds = ds  # don't normalise!
    return (
        0.0315 * ds.nbart_blue
        + 0.2021 * ds.nbart_green
        + 0.3102 * ds.nbart_red
        + 0.1594 * ds.nbart_nir
        + -0.6806 * ds.nbart_swir_1
        + -0.6109 * ds.nbart_swir_2
    )


def transform(inputs: xr.Dataset) -> xr.Dataset:
    # organize the datas structure
    # to apply WIT Notebook processing
    # approach

    ard_ds = xr.merge([inputs[e] for e in input_products["ga_ls9c_ard_3"]])
    wo_ds = xr.merge([inputs[e] for e in input_products["ga_ls_wo_3"]])
    fc_ds = xr.merge([inputs[e] for e in input_products["ga_ls_fc_3"]])

    tcw = _tcw(ard_ds)

    # divide FC values by 100 to keep them in [0, 1]
    bs = fc_ds.bs / 100
    pv = fc_ds.pv / 100
    npv = fc_ds.npv / 100

    # generate the WIT raster bands
    # create an empty dataset called 'output_rast' and populate with values from input datasets
    rast_names = ["pv", "npv", "bs", "wet", "water"]
    output_rast = {n: xr.zeros_like(ard_ds) for n in rast_names}

    output_rast["bs"] = bs
    output_rast["pv"] = pv
    output_rast["npv"] = npv

    # Mask noncontiguous data, low solar incidence angle, cloud, and water out of the wet category
    # by disabling those flags
    mask = (wo_ds.water & 0b01100011) == 0
    # not apply poly_raster cause we will do it before summarise

    open_water = wo_ds.water & (1 << 7) > 0

    # Thresholding
    # set wet pixels where not masked and above threshold of -350
    wet = tcw.where(mask) > -350

    # TCW
    output_rast["wet"] = wet.astype(float)
    for name in rast_names[:3]:
        output_rast[name].values[wet.values] = 0

    # WO
    output_rast["water"] = open_water.astype(float)

    for name in rast_names[0:4]:
        output_rast[name].values[open_water.values] = 0

    # save this mask then can do 90% check in summarise
    output_rast["mask"] = (mask).astype(int)

    # masking
    ds_wit = xr.Dataset(output_rast).where(mask)

    return ds_wit


def summarise(inputs: xr.Dataset) -> xr.Dataset:

    # calculate percentage missing
    pc_missing = 1 - (np.nansum(inputs.mask.values) / len(inputs.mask.values))
    # inputs = inputs.where(pc_missing < 0.1)

    output = {}  # band -> value
    output["water"] = inputs.water.mean()
    output["wet"] = inputs.wet.mean()
    output["bs"] = inputs.bs.mean()
    output["pv"] = inputs.pv.mean()
    output["npv"] = inputs.npv.mean()
    output["pc_missing"] = pc_missing

    return xr.Dataset(output)
