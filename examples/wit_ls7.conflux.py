import xarray as xr

product_name = "wit_ls7"
version = "0.0.1"
resampling = {"water": "nearest", "*": "bilinear"}
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
    # Masking
    cloud = (inputs.water & (1 << 6)) > 0
    shadow = (inputs.water & (1 << 5)) > 0
    mask = ~cloud & ~shadow
    # TCW
    tcw = (_tcw(inputs) > -350).where(mask)
    # WO
    is_wet = inputs.water == 128
    is_ok = is_wet | (inputs.water == 0)
    masked_wet = is_wet.where(is_ok)
    # FC
    bs = inputs.bs.clip(0, 100) / 100
    pv = inputs.pv.clip(0, 100) / 100
    npv = inputs.npv.clip(0, 100) / 100
    sum_fc = bs + pv + npv
    masked_bs = (bs / sum_fc).where(mask)
    masked_pv = (pv / sum_fc).where(mask)
    masked_npv = (npv / sum_fc).where(mask)
    return xr.Dataset(
        {
            "water": masked_wet,
            "tcw": tcw,
            "bs": masked_bs,
            "pv": masked_pv,
            "npv": masked_npv,
        }
    )


def summarise(inputs: xr.Dataset) -> xr.Dataset:
    output = {}  # band -> value
    # water takes priority
    output["water"] = inputs.water.sum()
    # TCW comes in where there is no water
    output["wet"] = inputs.tcw.where(~inputs.water.astype(bool)).sum()
    # FC everywhere else
    fc_mask = ~inputs.water.astype(bool) & ~inputs.tcw.astype(bool)
    output["pv"] = inputs.pv.where(fc_mask).sum()
    output["npv"] = inputs.npv.where(fc_mask).sum()
    output["bs"] = inputs.bs.where(fc_mask).sum()
    output["px_missing"] = inputs.bs.isnull().sum()
    return xr.Dataset(output)
