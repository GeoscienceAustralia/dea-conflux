"""Stack Parquet scene outputs into other formats.

Matthew Alger
Geoscience Australia
2021
"""

import collections
import datetime
import enum
import logging
import os
from pathlib import Path
import re

import fsspec
import s3fs
import pandas as pd
from tqdm.auto import tqdm

import dea_conflux.io
from dea_conflux.io import PARQUET_EXTENSIONS

logger = logging.getLogger(__name__)


class StackMode(enum.Enum):
    WATERBODIES = 'waterbodies'


def waterbodies_format_date(date: datetime.datetime) -> str:
    """Format a date to match DEA Waterbodies.

    Arguments
    ---------
    date : datetime

    Returns
    -------
    str
    """
    # e.g. 1987-05-24T01:30:18Z
    return date.strftime('%Y-%m-%dT%H:%M:%SZ')


def find_parquet_files(path: str, pattern: str = '.*') -> [str]:
    """Find Parquet files matching a pattern.

    Arguments
    ---------
    path : str
        Path (s3 or local) to search for Parquet files.
    
    pattern : str
        Regex to match filenames against.
    
    Returns
    -------
    [str]
        List of paths.
    """
    pattern = re.compile(pattern)
    all_paths = []

    # "Support" pathlib Paths
    try:
        path.startswith
    except AttributeError:
        path = str(path)

    if path.startswith('s3://'):
        # Find Parquet files on S3.
        fs = s3fs.S3FileSystem(anon=True)
        files = fs.find(path)
        for file in files:
            _, ext = os.path.splitext(file)
            if ext not in PARQUET_EXTENSIONS:
                continue

            _, filename = os.path.split(file)
            if not pattern.match(filename):
                continue

            all_paths.append(f's3://{file}')
    else:
        # Find Parquet files locally.
        for root, dir_, files in os.walk(path):
            paths = [Path(root) / file for file in files]
            for path_ in paths:
                if path_.suffix not in PARQUET_EXTENSIONS:
                    continue

                if not pattern.match(path_.name):
                    continue

                all_paths.append(path_)

    return all_paths


def stack_waterbodies(
        paths: [str],
        output_dir: str,
        verbose: bool = False):
    """Stack Parquet files into CSVs like DEA Waterbodies does.
    
    Arguments
    ---------
    paths : [str]
        List of paths to Parquet files to stack.
    
    output_dir : str
        Path to output directory.

    verbose : bool
    """
    # id -> [series of date x bands]
    id_to_series = collections.defaultdict(list)
    logger.info('Reading...')
    if verbose:
        paths = tqdm(paths)
    for path in paths:
        df = dea_conflux.io.read_table(path)
        date = dea_conflux.io.string_to_date(df.attrs['date'])
        date = waterbodies_format_date(date)
        # df is ids x bands
        # for each ID...
        for uid, series in df.iterrows():
            series.name = date
            id_to_series[uid].append(series)
    outpath = output_dir
    outpath = str(outpath)  # handle Path type
    logger.info('Writing...')
    for uid, seriess in id_to_series.items():
        df = pd.DataFrame(seriess)
        df.sort_index(inplace=True)
        filename = f'{outpath}/{uid[:4]}/{uid}.csv'
        logger.info(f'Writing {filename}')
        if not outpath.startswith('s3://'):
            os.makedirs(Path(filename).parent, exist_ok=True)
        with fsspec.open(filename, 'w') as f:
            df.to_csv(f, index_label='date')


def stack(
        path: str,
        output_dir: str,
        pattern: str = '.*',
        mode: StackMode = StackMode.WATERBODIES,
        verbose: bool = False):
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

    verbose : bool
    """
    path = str(path)
    
    paths = find_parquet_files(path, pattern)

    if mode != StackMode.WATERBODIES:
        raise NotImplementedError('Only waterbodies stacking is implemented')

    return stack_waterbodies(paths, output_dir, verbose=verbose)
