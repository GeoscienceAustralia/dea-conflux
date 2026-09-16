#!/usr/bin/env python3
"""
Split polygon layers from a GeoPackage (or shapefile) into N non-overlapping
processing sets using greedy graph colouring.

Memory-efficient algorithm (fits 2 M complex MultiPolygons in 7.6 GB RAM):
  1. Load only geometry BOUNDS (xmin, ymin, xmax, ymax) per feature.
     Uses geometry-only reads + immediate deletion — ~64 MB for 2 M polygons.
  2. Build Shapely 2 box geometries from bounds and bulk-query all
     bbox-overlapping pairs with STRtree. O(n log n).
  3. Greedy graph-colour in descending-bbox-area order. Auto-escalates
     from 3 → 4 layers if any polygon needs a fallback assignment.
  4. Re-read each source layer's geometry and write the subset assigned to
      each colour into the output GeoPackage.

Bounding-box intersection is a conservative proxy for actual polygon overlap:
every true overlap is captured; a small number of bbox-only overlaps may put
neighbouring (but non-overlapping) polygons into separate layers.

Usage:
  python split_layers.py INPUT.gpkg OUTPUT.gpkg [--max-layers 3]
  python split_layers.py INPUT.shp  OUTPUT.gpkg [--max-layers 3]
"""

import gc
import logging
import pickle
import sys
import time
import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pyogrio
import shapely
from shapely.strtree import STRtree

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 1: discover layers and load bounds only
# ---------------------------------------------------------------------------

def get_polygon_layers(input_path: Path):
    """Return non-empty polygon layer names (None = single shapefile)."""
    if input_path.suffix.lower() in (".gpkg", ".gdb"):
        layers = []
        for lname, geometry_type in pyogrio.list_layers(str(input_path)):
            info = pyogrio.read_info(str(input_path), layer=lname)
            if info["features"] > 0 and "Polygon" in str(geometry_type):
                layers.append(lname)
        return layers
    return [None]


def load_all_bounds(input_path: Path, polygon_layers):
    """
    Load only geometry bounds for every feature — geometry column only,
    then extract (N,4) float64 bounds array and free geometry objects.

    Returns
    -------
    bounds_arr : float64 ndarray (N, 4)  — xmin, ymin, xmax, ymax
    layer_map  : [(layer_name_or_None, start_idx, end_idx), …]
    total      : int
    """
    chunks = []
    layer_map = []
    cursor = 0

    for lname in polygon_layers:
        t0 = time.time()
        try:
            # columns=[] → geometry column only (no attribute data loaded)
            if lname is not None:
                gdf = gpd.read_file(str(input_path), layer=lname, columns=[])
            else:
                gdf = gpd.read_file(str(input_path), columns=[])
        except Exception as exc:
            log.warning("  Could not read layer %s: %s — skipping", lname, exc)
            continue

        valid = ~gdf.geometry.is_empty & gdf.geometry.notna()
        geoms = gdf.geometry.values[valid]
        del gdf
        gc.collect()

        n = len(geoms)
        if n == 0:
            del geoms
            continue

        # shapely.bounds(array) → (N,4) float64: minx, miny, maxx, maxy
        bounds = shapely.bounds(geoms)
        del geoms
        gc.collect()

        chunks.append(bounds)
        layer_map.append((lname, cursor, cursor + n))
        cursor += n
        log.info(
            "  %-62s  %8d  (total %d)  %.1fs",
            str(lname), n, cursor, time.time() - t0,
        )

    bounds_arr = np.vstack(chunks) if chunks else np.empty((0, 4), dtype=np.float64)
    log.info("Total bounds loaded: %d  (%.1f MB)", len(bounds_arr),
             bounds_arr.nbytes / 1e6)
    return bounds_arr, layer_map, cursor


# ---------------------------------------------------------------------------
# Phase 2: conflict pairs via bbox STRtree
# ---------------------------------------------------------------------------

def find_conflict_pairs(bounds_arr: np.ndarray):
    """
    Build rectangle geometries from bounds, find all bbox-overlapping pairs.
    Returns deduplicated (left_idx, right_idx) with left < right.
    """
    n = len(bounds_arr)
    log.info("Creating %d box geometries from bounds …", n)
    t0 = time.time()
    boxes = shapely.box(
        bounds_arr[:, 0], bounds_arr[:, 1],
        bounds_arr[:, 2], bounds_arr[:, 3],
    )
    log.info("  Done in %.1fs", time.time() - t0)

    log.info("Building STRtree …")
    t0 = time.time()
    tree = STRtree(boxes)
    log.info("  STRtree built in %.1fs", time.time() - t0)

    log.info("Bulk bbox-intersects query …")
    t0 = time.time()
    left_raw, right_raw = tree.query(boxes, predicate="intersects")
    log.info(
        "  %d raw pairs in %.1fs (incl. self & symmetric)",
        len(left_raw), time.time() - t0,
    )

    # Keep only i < j (removes self-pairs and symmetric duplicates)
    mask = left_raw < right_raw
    left_idx = left_raw[mask]
    right_idx = right_raw[mask]
    del left_raw, right_raw, boxes, tree
    gc.collect()

    log.info("Unique conflict pairs (bbox overlap): %d", len(left_idx))
    return left_idx, right_idx


# ---------------------------------------------------------------------------
# Phase 3: greedy colouring
# ---------------------------------------------------------------------------

def greedy_color(n: int, bounds_arr: np.ndarray, left_idx, right_idx, max_colors: int):
    """
    Greedy graph colouring in descending bbox-area order via CSR adjacency.

    Returns
    -------
    color          : int8 ndarray, shape (N,)
    fallback_count : int
    """
    log.info("Building CSR adjacency for %d nodes …", n)
    all_l = np.concatenate([left_idx, right_idx])
    all_r = np.concatenate([right_idx, left_idx])
    sort_order = np.argsort(all_l, kind="stable")
    sorted_l = all_l[sort_order]
    sorted_r = all_r[sort_order]
    del all_l, all_r, sort_order
    gc.collect()

    offsets = np.zeros(n + 1, dtype=np.int64)
    unique_l, cnts = np.unique(sorted_l, return_counts=True)
    offsets[unique_l + 1] = cnts
    np.cumsum(offsets, out=offsets)

    # Process largest bounding boxes first → fewest downstream conflicts
    areas = (bounds_arr[:, 2] - bounds_arr[:, 0]) * (bounds_arr[:, 3] - bounds_arr[:, 1])
    order = np.argsort(-areas)

    log.info("Greedy colouring %d polygons with %d colours …", n, max_colors)
    t0 = time.time()
    color = np.full(n, -1, dtype=np.int8)
    color_load = np.zeros(max_colors, dtype=np.int64)
    fallback_count = 0

    for i, idx in enumerate(order):
        idx = int(idx)
        s, e = int(offsets[idx]), int(offsets[idx + 1])
        nbr_colors = color[sorted_r[s:e]]
        forbidden = set(int(c) for c in nbr_colors if c >= 0)

        available = [c for c in range(max_colors) if c not in forbidden]

        if available:
            # Retain the non-overlap constraint while keeping set sizes even.
            chosen = min(available, key=lambda c: (color_load[c], c))
        else:
            # Fallback: minimise conflicts first, then minimise set size.
            cnt = np.zeros(max_colors, dtype=np.int32)
            for c in nbr_colors:
                if 0 <= c < max_colors:
                    cnt[c] += 1
            chosen = min(range(max_colors), key=lambda c: (cnt[c], color_load[c], c))
            fallback_count += 1

        color[idx] = chosen
        color_load[chosen] += 1

        if (i + 1) % 250_000 == 0:
            log.info(
                "  Coloured %d / %d  (%.0f%%)  fallbacks so far: %d",
                i + 1, n, 100.0 * (i + 1) / n, fallback_count,
            )

    log.info(
        "Colouring done in %.1fs. Fallbacks: %d. Set sizes: %s",
        time.time() - t0, fallback_count, color_load.tolist(),
    )
    return color, fallback_count


# ---------------------------------------------------------------------------
# Phase 4: write output GeoPackage (incremental — one source layer at a time)
# ---------------------------------------------------------------------------

def write_output(input_path, layer_map, color, n_colors, output_path):
    """
    Write processing sets incrementally to cap peak memory at ONE source layer.

    Output contains geometry only.  Source attributes are deliberately omitted:
    they are not needed for processing and their incompatible schemas caused
    field-write errors when source layers were combined.
    """
    if output_path.exists():
        output_path.unlink()

    layer_first_written: set = set()
    counts = [0] * n_colors

    for lname, start, end in layer_map:
        layer_color = color[start:end]
        if not any((layer_color == c).any() for c in range(n_colors)):
            continue

        t0 = time.time()
        gdf = gpd.read_file(
            str(input_path), **({"layer": lname} if lname else {}), columns=[]
        )
        valid = ~gdf.geometry.is_empty & gdf.geometry.notna()
        gdf = gdf[valid].reset_index(drop=True)

        for c in range(n_colors):
            local_mask = layer_color == c
            if not local_mask.any():
                continue

            subset = gpd.GeoDataFrame(
                geometry=gdf.geometry[local_mask].reset_index(drop=True),
                crs=gdf.crs,
            )

            layer_name = f"PROCESSING_SET_{c + 1}"
            mode = "a" if layer_name in layer_first_written else "w"
            subset.to_file(
                str(output_path), layer=layer_name, driver="GPKG", mode=mode,
            )
            layer_first_written.add(layer_name)

            counts[c] += int(local_mask.sum())

        del gdf, layer_color
        gc.collect()
        per_set = "  ".join(
            f"set{c + 1}={int((color[start:end] == c).sum())}" for c in range(n_colors)
        )
        log.info("  %-60s  %s  (%.1fs)", str(lname), per_set, time.time() - t0)

    for i, cnt in enumerate(counts):
        log.info("  → PROCESSING_SET_%d: %d polygons written", i + 1, cnt)

    return sum(counts), counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Split GeoPackage/shapefile polygon layers into non-overlapping "
            "processing sets using greedy graph colouring."
        )
    )
    parser.add_argument("input", help="Input GeoPackage (.gpkg) or shapefile (.shp)")
    parser.add_argument("output", help="Output GeoPackage path")
    parser.add_argument(
        "--max-layers", type=int, default=3,
        help="Starting number of layers; auto-escalates to 4 if needed (default: 3)",
    )
    args = parser.parse_args()

    input_path = Path(args.input).expanduser()
    output_path = Path(args.output).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    t_start = time.time()
    log.info("Input : %s", input_path)
    log.info("Output: %s", output_path)

    n_colors = args.max_layers
    checkpoint_path = output_path.with_suffix(".checkpoint.pkl")

    if checkpoint_path.exists():
        log.info("Loading colouring checkpoint from %s …", checkpoint_path)
        with open(checkpoint_path, "rb") as fh:
            ckpt = pickle.load(fh)
        color = ckpt["color"]
        layer_map = ckpt["layer_map"]
        n_colors = ckpt["n_colors"]
        total_input = ckpt["total_input"]
        log.info("Checkpoint loaded: %d polygons, %d colours", total_input, n_colors)
    else:
        log.info("=== Phase 1: Discovering layers ===")
        polygon_layers = get_polygon_layers(input_path)
        log.info("Found %d polygon layer(s)", len(polygon_layers))

        log.info("=== Phase 2: Loading bounds (geometry-only, memory-efficient) ===")
        bounds_arr, layer_map, total_input = load_all_bounds(input_path, polygon_layers)

        log.info("=== Phase 3: Finding bbox conflict pairs ===")
        left_idx, right_idx = find_conflict_pairs(bounds_arr)

        log.info("=== Phase 4: Greedy colouring (max %d colours) ===", n_colors)
        color, fallback_count = greedy_color(
            total_input, bounds_arr, left_idx, right_idx, n_colors
        )

        if fallback_count > 0 and n_colors < 4:
            log.warning(
                "%d fallbacks with %d layers — retrying with 4 layers …",
                fallback_count, n_colors,
            )
            n_colors = 4
            color, fallback_count = greedy_color(
                total_input, bounds_arr, left_idx, right_idx, n_colors
            )

        if fallback_count > 0:
            log.warning(
                "%d fallback assignments even with %d layers — "
                "a small number of bbox pairs may share a layer.",
                fallback_count, n_colors,
            )

        del bounds_arr, left_idx, right_idx
        gc.collect()

        # Save checkpoint — if write phase is killed, we can resume here
        with open(checkpoint_path, "wb") as fh:
            pickle.dump(
                {"color": color, "layer_map": layer_map,
                 "n_colors": n_colors, "total_input": total_input},
                fh,
            )
        log.info("Colouring checkpoint saved → %s", checkpoint_path)

    log.info("=== Phase 5: Writing output (%d layers) ===", n_colors)
    total_written, counts = write_output(
        input_path, layer_map, color, n_colors, output_path
    )

    elapsed = time.time() - t_start
    log.info("=== Summary ===")
    log.info("Elapsed         : %dm %ds", int(elapsed // 60), int(elapsed % 60))
    log.info("Input polygons  : %d", total_input)
    log.info("Output polygons : %d", total_written)
    log.info("Count match     : %s", "YES" if total_input == total_written else "NO — MISMATCH")
    for i, cnt in enumerate(counts):
        log.info("  PROCESSING_SET_%d : %d", i + 1, cnt)

    if total_input != total_written:
        log.error("POLYGON COUNT MISMATCH — input %d vs output %d", total_input, total_written)
        sys.exit(1)

    log.info("Done → %s", output_path)


if __name__ == "__main__":
    main()
