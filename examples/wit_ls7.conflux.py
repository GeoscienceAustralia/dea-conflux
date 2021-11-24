import xarray as xr

product_name = "wit_ls7"
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

input_products = {
    "ga_ls_wo_3": ["water"],
    "ga_ls7e_ard_3": [
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

    ard_ds = xr.merge([inputs[e] for e in input_products["ga_ls7e_ard_3"]])
    wo_ds = xr.merge([inputs[e] for e in input_products["ga_ls_wo_3"]])
    fc_ds = xr.merge([inputs[e] for e in input_products["ga_ls_fc_3"]])

    tcw = _tcw(ard_ds)

    bs = fc_ds.bs / 100
    pv = fc_ds.pv / 100
    npv = fc_ds.npv / 100

    rast_names = ["pv", "npv", "bs", "wet", "water"]
    output_rast = {n: xr.zeros_like(ard_ds) for n in rast_names}

    output_rast["bs"] = bs
    output_rast["pv"] = pv
    output_rast["npv"] = npv

    mask = (wo_ds.water & 0b0110011) == 0
    # not apply poly_raster cause we did it before

    open_water = wo_ds.water & (1 << 7) > 0
    wet = tcw.where(~mask) > -350

    # TCW
    output_rast["wet"] = wet.astype(float)
    for name in rast_names[:3]:
        output_rast[name].values[wet.values] = 0

    output_rast["water"] = open_water.astype(float)

    for name in rast_names[0:4]:
        output_rast[name].values[open_water.values] = 0

    # save this unmask then can do 90% check in summarise
    output_rast["unmask"] = ~mask

    ds_wit = xr.Dataset(output_rast).where(mask)

    return ds_wit


def summarise(inputs: xr.Dataset) -> xr.Dataset:

    inputs = inputs.where(inputs.unmask.mean() < 0.1)

    output = {}  # band -> value
    output["water"] = inputs.water.mean()
    output["wet"] = inputs.wet.mean()
    output["bs"] = inputs.bs.mean()
    output["pv"] = inputs.pv.mean()
    output["npv"] = inputs.npv.mean()

    return xr.Dataset(output)
