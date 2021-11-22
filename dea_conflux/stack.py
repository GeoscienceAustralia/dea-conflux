"""Stack Parquet scene outputs into other formats.

Lots of this code is domain-specific and not intended to be fully general.

Matthew Alger
Geoscience Australia
2021
"""

import collections
import datetime
import enum
import logging
import os
import re
from pathlib import Path

import fsspec
import geohash
import pandas as pd
import s3fs
from tqdm.auto import tqdm

import dea_conflux.db
import dea_conflux.io
from dea_conflux.io import PARQUET_EXTENSIONS
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


class StackMode(enum.Enum):
    WATERBODIES = "waterbodies"
    WATERBODIES_DB = "waterbodies_db"


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
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


def find_parquet_files(path: str, pattern: str = ".*") -> [str]:
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

    if path.startswith("s3://"):
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

            all_paths.append(f"s3://{file}")
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


def stack_waterbodies(paths: [str], output_dir: str, verbose: bool = False):
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
    logger.info("Reading...")
    if verbose:
        paths = tqdm(paths)
    for path in paths:
        df = dea_conflux.io.read_table(path)
        date = dea_conflux.io.string_to_date(df.attrs["date"])
        date = waterbodies_format_date(date)
        # df is ids x bands
        # for each ID...
        for uid, series in df.iterrows():
            series.name = date
            id_to_series[uid].append(series)
    outpath = output_dir
    outpath = str(outpath)  # handle Path type
    logger.info("Writing...")
    for uid, seriess in id_to_series.items():
        df = pd.DataFrame(seriess)
        df.sort_index(inplace=True)
        filename = f"{outpath}/{uid[:4]}/{uid}.csv"
        logger.info(f"Writing {filename}")
        if not outpath.startswith("s3://"):
            os.makedirs(Path(filename).parent, exist_ok=True)
        with fsspec.open(filename, "w") as f:
            df.to_csv(f, index_label="date")


def get_waterbody_key(uid, session):
    """Create or get a unique key from the database."""
    # decode into a coordinate
    # uid format is gh_version
    gh = uid.split('_')[0]
    lat, lon = geohash.decode(gh)
    defaults = {
        'geofabric_name': '',
        'centroid_lat': lat,
        'centroid_lon': lon,
    }
    inst, _ = dea_conflux.db.get_or_create(
        session,
        dea_conflux.db.Waterbody,
        wb_name=uid,
        defaults=defaults)
    return inst.wb_id

            
            
def stack_waterbodies_db(
        paths: [str],
        verbose: bool = False,
        engine=None,
        uids: {str}=None):
    """Stack Parquet files into the waterbodies interstitial DB.

    Arguments
    ---------
    paths : [str]
        List of paths to Parquet files to stack.

    verbose : bool

    engine: sqlalchemy.engine.Engine
        Database engine. Default postgres, which is
        connected to if engine=None.
    
    uids : {uids}
        Set of waterbody IDs. If not specified, guessed from
        parquet files, but that's slower.
    """
    if verbose:
        paths = tqdm(paths)
    
    # connect to the db
    if not engine:
        engine = dea_conflux.db.get_engine_waterbodies()

    Session = sessionmaker(bind=engine)
    session = Session()

    # ensure tables exist
    dea_conflux.db.create_waterbody_tables(engine)
    
    if not uids:
        uids = set()

    # confirm all the UIDs exist in the db
    uid_to_key = {}
    uids_ = uids
    if verbose:
        uids_ = tqdm(uids)
    for uid in uids_:
        key = get_waterbody_key(uid, session)
        uid_to_key[uid] = key

    for path in paths:
        # read the table in...
        df = dea_conflux.io.read_table(path)
        # parse the date...
        date = dea_conflux.io.string_to_date(df.attrs["date"])
        # df is ids x bands
        # for each ID...
        obss = []
        for uid, series in df.iterrows():
            if uid not in uid_to_key:
                # add this uid
                key = get_waterbody_key(uid, session)
                uid_to_key[uid] = key

            key = uid_to_key[uid]
            obs = dea_conflux.db.WaterbodyObservation(
                wb_id=key,
                px_wet=series.px_wet,
                pc_wet=series.pc_wet,
                pc_missing=series.pc_missing,
                platform='UNK',
                date=date)
            obss.append(obs)
        # basically just hoping that these don't exist already
        # TODO: Insert or update
        session.bulk_save_objects(obss)
        session.commit()


def stack(
    path: str,
    pattern: str = ".*",
    mode: StackMode = StackMode.WATERBODIES,
    verbose: bool = False,
    **kwargs,
):
    """Stack Parquet files.

    Arguments
    ---------
    path : str
        Path to search for Parquet files.

    pattern : str
        Regex to match filenames against.

    mode : StackMode
        Method of stacking. Default is like DEA Waterbodies v1,
        a collection of polygon CSVs.

    verbose : bool
    
    **kwargs
        Passed to underlying stack method.
    """
    path = str(path)

    paths = find_parquet_files(path, pattern)

    if mode == StackMode.WATERBODIES:
        return stack_waterbodies(paths, verbose=verbose, **kwargs)
    if mode == StackMode.WATERBODIES_DB:
        return stack_waterbodies_db(paths, verbose=verbose, **kwargs)
