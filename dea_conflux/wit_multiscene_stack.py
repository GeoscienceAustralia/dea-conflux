"""Export multi-scene WIT Parquet results as polygon CSVs and plots."""

import logging
import shutil
import tempfile
from pathlib import Path

import fsspec
import pandas as pd

from dea_conflux.stack import (
    WIT_CSV_COLUMN_ORDER,
    WIT_CSV_COLUMN_RENAMES,
    display_wit_stack_with_df,
    generate_low_quality_data_periods,
    load_pq_file,
)

logger = logging.getLogger(__name__)


def _write_plot(frame, feature_id, output):
    graph = frame.loc[frame["pc_missing"] <= 0.1].copy()
    title = str(feature_id)
    if graph.empty:
        logger.warning("No observations with at most 10%% missing for polygon %s", feature_id)
        graph = frame.copy()
        graph.loc[:, ["water", "wet", "norm_pv", "norm_npv", "norm_bs"]] = float("nan")
        title += " (no observations with at most 10% missing)"
    elif len(graph) == 1:
        title += " (single observation)"

    if len(graph) == 1:
        graph = pd.concat([graph, graph], ignore_index=True)
        graph.loc[0, "date"] -= pd.Timedelta(hours=12)
        graph.loc[1, "date"] += pd.Timedelta(hours=12)

    with tempfile.TemporaryDirectory(prefix="conflux-wit-plot-") as directory:
        plot_path = Path(directory) / "plot"
        display_wit_stack_with_df(graph, title, str(plot_path), x_axis_labels="years")
        with open(f"{plot_path}.png", "rb") as source, fsspec.open(output, "wb") as target:
            shutil.copyfileobj(source, target)


def write_csvs(paths, output):
    if not paths:
        return []
    result = pd.concat([load_pq_file(path) for path in paths]).rename(
        columns={"ard_scene_id": "composite_id"}
    )
    written = []
    for feature_id, group in result.groupby(level=0, sort=False):
        name = str(feature_id)
        if name in ("", ".", "..") or "/" in name or "\\" in name:
            raise ValueError(f"Polygon ID cannot be used as a CSV filename: {name!r}")
        frame = group.copy().sort_values("date")
        frame["nwi_id"] = frame.index
        frame = frame.reset_index(drop=True)
        frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
        frame["veg_areas"] = 1 - frame["water"] - frame["wet"]
        frame["overall_veg_num"] = frame["pv"] + frame["npv"] + frame["bs"]
        nonzero = frame["overall_veg_num"] != 0
        for band in ("pv", "npv", "bs"):
            frame["norm_" + band] = frame[band]
            frame.loc[nonzero, "norm_" + band] = (
                frame.loc[nonzero, band] / frame.loc[nonzero, "overall_veg_num"]
                * frame.loc[nonzero, "veg_areas"]
            )
        frame = generate_low_quality_data_periods(frame)
        if "://" not in output:
            Path(output).mkdir(parents=True, exist_ok=True)
        _write_plot(frame, feature_id, f"{output.rstrip('/')}/{name}.png")
        frame = frame.rename(columns=WIT_CSV_COLUMN_RENAMES)
        ordered = [column for column in WIT_CSV_COLUMN_ORDER if column in frame.columns]
        frame = frame[ordered + [column for column in frame.columns if column not in ordered]]
        filename = f"{output.rstrip('/')}/{name}.csv"
        with fsspec.open(filename, "w") as stream:
            frame.to_csv(stream, index=False)
        written.append(filename)
    return written
