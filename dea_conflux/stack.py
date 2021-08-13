"""Stack Parquet scene outputs into other formats.

Matthew Alger
Geoscience Australia
2021
"""

import collections
import enum
import logging
import os
from pathlib import Path
import re

import pandas as pd

import dea_conflux.io
from dea_conflux.io import PARQUET_EXTENSIONS

logger = logging.getLogger(__name__)


class StackMode(enum.Enum):
    WATERBODIES = 'waterbodies'


def find_parquet_files(path: str, pattern: str = '.*') -> [str]:
    """Find Parquet files matching a pattern.

    Arguments
    ---------
    path : str
        Path to search for Parquet files.
    
    pattern : str
        Regex to match filenames against.
    
    Returns
    -------
    [str]
        List of paths.
    """
    pattern = re.compile(pattern)
    all_paths = []
    for root, dir_, files in os.walk(path):
        paths = [Path(root) / file for file in files]
        for path in paths:
            if path.suffix not in PARQUET_EXTENSIONS:
                continue

            if not pattern.match(path.name):
                continue

            all_paths.append(path)

    return all_paths


def stack_waterbodies(paths: [str], output_dir: str):
    """Stack Parquet files into CSVs like DEA Waterbodies does.
    
    Arguments
    ---------
    paths : [str]
        List of paths to Parquet files to stack.
    
    output_dir : str
        Path to output directory.
    """
    # TODO(MatthewJA): Support S3.
    if output_dir.startswith('s3'):
        raise NotImplementedError('S3 not yet supported')

    # id -> [series of date x bands]
    id_to_series = collections.defaultdict(list)
    for path in paths:
        df = dea_conflux.io.read_table(path)
        date = df.attrs['date']
        # df is ids x bands
        # for each ID...
        for uid, series in df.iterrows():
            series.name = date
            id_to_series[uid].append(series)
    outpath = Path(output_dir)
    for uid, seriess in id_to_series.items():
        df = pd.DataFrame(seriess)
        filename = outpath / uid[:4] / f'{uid}.csv'
        logger.info(f'Writing {filename}')
        os.makedirs(filename.parent, exist_ok=True)
        df.to_csv(filename)


def stack(
        path: str,
        output_dir: str,
        pattern: str = '.*',
        mode: StackMode = StackMode.WATERBODIES):
    """Stack Parquet files.

    Arguments
    ---------
    path : str
        Path to search for Parquet files.

    output_dir : str
        Path to write to.
    
    pattern : str
        Regex to match filenames against.
    
    mode : StackMode
        Method of stacking. Default is like DEA Waterbodies v1,
        a collection of polygon CSVs.
    """
    # TODO(MatthewJA): Support S3.
    if path.startswith('s3'):
        raise NotImplementedError('S3 not yet supported')

    path = Path(path)
    
    paths = find_parquet_files(path, pattern)

    if mode != StackMode.WATERBODIES:
        raise NotImplementedError('Only waterbodies stacking is implemented')

    return stack_waterbodies(paths, output_dir)
