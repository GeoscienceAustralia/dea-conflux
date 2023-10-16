"""Stack Parquet scene outputs into other formats.

Lots of this code is domain-specific and not intended to be fully general.

Matthew Alger
Geoscience Australia
2021
"""

import collections
import concurrent.futures
import datetime
import enum
import logging
import os
import re
from pathlib import Path

import fsspec
import geohash
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from tqdm.auto import tqdm

from deafrica_conflux.db import (
    Engine,
    Waterbody,
    WaterbodyObservation,
    create_waterbody_tables,
    drop_waterbody_tables,
    get_engine_waterbodies,
    get_or_create,
)
from deafrica_conflux.io import (
    PARQUET_EXTENSIONS,
    check_if_s3_uri,
    read_table_from_parquet,
    string_to_date,
)

_log = logging.getLogger(__name__)


class StackMode(enum.Enum):
    WATERBODIES = "waterbodies"
    WATERBODIES_DB = "waterbodies_db"


def stack_format_date(date: datetime.datetime) -> str:
    """
    Format a date to match DE Africa conflux products datetime.

    Arguments
    ---------
    date : datetime

    Returns
    -------
    str
    """
    # e.g. 1987-05-24T01:30:18Z
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


def find_parquet_files(path: str | Path, pattern: str = ".*") -> [str]:
    """
    Find Parquet files matching a pattern.

    Arguments
    ---------
    path : str | Path
        Path (s3 or local) to search for Parquet files.

    pattern : str
        Regex to match file names against.

    Returns
    -------
    [str]
        List of paths.
    """
    pattern = re.compile(pattern)

    # "Support" pathlib Paths.
    path = str(path)

    if check_if_s3_uri(path):
        # Find Parquet files on S3.
        file_system = fsspec.filesystem("s3")
    else:
        # Find Parquet files localy.
        file_system = fsspec.filesystem("file")

    pq_file_paths = []

    files = file_system.find(path)
    for file in files:
        _, file_extension = os.path.splitext(file)
        if file_extension not in PARQUET_EXTENSIONS:
            continue
        else:
            _, file_name = os.path.split(file)
            if not pattern.match(file_name):
                continue
            else:
                pq_file_paths.append(file)

    if check_if_s3_uri(path):
        pq_file_paths = [f"s3://{file}" for file in pq_file_paths]

    _log.info(f"Found {len(pq_file_paths)} parquet files.")
    return pq_file_paths


def remove_timeseries_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicated (same day) data in the timeseries DataFrame.

    Arguments
    ---------
    df : pd.DataFrame
        The polygon base timeseries result.

    Returns
    -------
    pd.DataFrame
        The polygon base timeseries result without duplicated data.
    """

    if "date" not in df.columns:
        # In the WaterBody PQ to CSV use case, the index is date
        df = df.assign(DAY=[e.split("T")[0] for e in df.index])
    else:
        df = df.assign(DAY=[e.split("T")[0] for e in df["date"]])

    df.sort_values(["DAY", "pc_invalid"], ascending=True, inplace=True)
    # For the invalid_percentage, the less the better.
    # So we only keep the first one.
    df.drop_duplicates("DAY", keep="first", inplace=True)

    # Remember to remove the temp column DAY.
    df.drop(columns=["DAY"], inplace=True)

    return df


def load_parquet_file(path: str | Path) -> pd.DataFrame:
    """
    Load Parquet file from given path.

    Arguments
    ---------
    path : str | Path
        Path (s3 or local) to search for Parquet files.
    Returns
    -------
    pandas.DataFrame
        pandas DataFrame
    """
    # "Support" pathlib Paths.
    path = str(path)

    df = read_table_from_parquet(path)
    # the pq file will be empty if no polygon belongs to that scene
    if df.empty is not True:
        date = string_to_date(df.attrs["date"])
        date = stack_format_date(date)
        df.loc[:, "date"] = date
    return df


def stack_waterbodies_parquet_to_csv(
    parquet_file_paths: [str | Path],
    output_directory: str | Path,
    remove_duplicated_data: bool = True,
    verbose: bool = False,
):
    """
    Stack Parquet files into CSVs like DE Africa Waterbodies does.

    Arguments
    ---------
    parquet_file_paths : [str | Path]
        List of paths to Parquet files to stack.

    output_directory : str | Path
        Path to output directory.

    remove_duplicated_data: bool
        Remove timeseries duplicated data or not

    verbose : bool
    """
    # "Support" pathlib Paths.
    parquet_file_paths = [str(pq_file_path) for pq_file_path in parquet_file_paths]
    output_directory = str(output_directory)

    # id -> [series of date x bands]
    id_to_series = collections.defaultdict(list)
    _log.info("Reading...")
    if verbose:
        parquet_file_paths = tqdm(parquet_file_paths)
    for pq_file_path in parquet_file_paths:
        df = read_table_from_parquet(pq_file_path)
        date = string_to_date(df.attrs["date"])
        date = stack_format_date(date)
        # df is ids x bands
        # for each ID...
        for uid, series in df.iterrows():
            series.name = date
            id_to_series[uid].append(series)

    _log.info("Writing...")
    for uid, seriess in id_to_series.items():
        df = pd.DataFrame(seriess)
        if remove_duplicated_data:
            df = remove_timeseries_duplicates(df)
        df.sort_index(inplace=True)

        output_file_name = os.path.join(output_directory, f"{uid[:4]}", f"{uid}.csv")
        _log.info(f"Writing {output_file_name}")
        if check_if_s3_uri(output_directory):
            fs = fsspec.filesystem("s3")
        else:
            fs = fsspec.filesystem("file")
            fs.mkdirs(os.path.join(output_directory, f"{uid[:4]}"), exist_ok=True)

        with fs.open(output_file_name, "w") as f:
            df.to_csv(f, index_label="date")


def get_waterbody_key(uid: str, session: Session):
    """
    Create or get a unique key from the database.
    """
    # decode into a coordinate
    # uid format is gh_version
    gh = uid.split("_")[0]
    lat, lon = geohash.decode(gh)
    defaults = {
        "geofabric_name": "",
        "centroid_lat": lat,
        "centroid_lon": lon,
    }
    inst, _ = get_or_create(session, Waterbody, wb_name=uid, defaults=defaults)
    return inst.wb_id


def stack_waterbodies_parquet_to_db(
    parquet_file_paths: [str | Path],
    verbose: bool = False,
    engine: Engine = None,
    uids: {str} = None,
    drop: bool = False,
):
    """
    Stack Parquet files into the waterbodies interstitial DB.

    Arguments
    ---------
    parquet_file_paths : [str | Path]
        List of paths to Parquet files to stack.

    verbose : bool

    engine: sqlalchemy.engine.Engine
        Database engine. Default postgres, which is
        connected to if engine=None.

    uids : {uids}
        Set of waterbody IDs. If not specified, guessed from
        parquet files, but that's slower.

    drop : bool
        Whether to drop the database. Default False.
    """
    parquet_file_paths = [str(pq_file_path) for pq_file_path in parquet_file_paths]

    if verbose:
        parquet_file_paths = tqdm(parquet_file_paths)

    # connect to the db
    if not engine:
        engine = get_engine_waterbodies()

    Session = sessionmaker(bind=engine)
    session = Session()

    # drop tables if requested
    if drop:
        drop_waterbody_tables(engine)

    # ensure tables exist
    create_waterbody_tables(engine)

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

    for pq_file_path in parquet_file_paths:
        # read the table in...
        df = read_table_from_parquet(pq_file_path)
        # parse the date...
        date = string_to_date(df.attrs["date"])
        # df is ids x bands
        # for each ID...
        obss = []
        for uid, series in df.iterrows():
            if uid not in uid_to_key:
                # add this uid
                key = get_waterbody_key(uid, session)
                uid_to_key[uid] = key

            key = uid_to_key[uid]
            obs = WaterbodyObservation(
                wb_id=key,
                wet_pixel_count=series.wet_pixel_count,
                wet_percentage=series.wet_percentage,
                invalid_percentage=series.invalid_percentage,
                platform="UNK",
                date=date,
            )
            obss.append(obs)
        # basically just hoping that these don't exist already
        # TODO: Insert or update
        session.bulk_save_objects(obss)
        session.commit()


def stack_waterbodies_db_to_csv(
    output_directory: str | Path,
    verbose: bool = False,
    uids: {str} = None,
    remove_duplicated_data: bool = True,
    engine=None,
    n_workers: int = 8,
    index_num: int = 0,
    split_num: int = 1,
):
    """
    Write waterbodies CSVs out from the interstitial DB.

    Arguments
    ---------
    output_directory : str | Path
        Path to write CSVs to.

    verbose : bool

    engine: sqlalchemy.engine.Engine
        Database engine. Default postgres, which is
        connected to if engine=None.

    uids : {uids}
        Set of waterbody IDs. If not specified, use all.

    remove_duplicated_data: bool
        Remove timeseries duplicated data or not

    engine : Engine
        Database engine. If not specified, use the
        Waterbodies engine.

    n_workers : int
        Number of threads to connect to the database with.

    index_num: int
        Index number of waterbodies ID list. Use to create the subset of
        waterbodies, then generate relative CSV files.

    split_num: int
        Number of chunks after split overall waterbodies ID list

    """
    # "Support" pathlib Paths.
    output_directory = str(output_directory)

    # connect to the db
    if not engine:
        engine = get_engine_waterbodies()

    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)

    # Iterate over waterbodies.

    def thread_run(wb: Waterbody):
        session = Session()

        # get all observations
        _log.debug(f"Processing {wb.wb_name}")
        obs = (
            session.query(WaterbodyObservation)
            .filter(WaterbodyObservation.wb_id == wb.wb_id)
            .order_by(WaterbodyObservation.date.asc())
            .all()
        )

        rows = [
            {
                "date": stack_format_date(ob.date),
                "wet_percentage": ob.wet_percentage,
                "wet_pixel_count": ob.wet_pixel_count,
                "invalid_percentage": ob.invalid_percentage,
            }
            for ob in obs
        ]

        df = pd.DataFrame(
            rows, columns=["date", "wet_percentage", "wet_pixel_count", "invalid_percentage"]
        )
        if remove_duplicated_data:
            df = remove_duplicated_data(df)

        # The pc_missing should not in final WaterBodies result
        df.drop(columns=["pc_missing"], inplace=True)

        output_file_name = os.path.join(output_directory, wb.wb_name[:4], wb.wb_name + ".csv")

        if check_if_s3_uri(output_file_name):
            fs = fsspec.filesystem("s3")
        else:
            fs = fsspec.filesystem("file")

        with fs.open(output_file_name, "w") as f:
            df.to_csv(f, header=True, index=False)

        Session.remove()

    session = Session()
    if not uids:
        # query all
        waterbodies = session.query(Waterbody).all()
    else:
        # query some
        waterbodies = session.query(Waterbody).filter(Waterbody.wb_name.in_(uids)).all()

    # generate the waterbodies list
    waterbodies = np.array_split(waterbodies, split_num)[index_num]

    # Write all CSVs with a thread pool.
    with tqdm(total=len(waterbodies)) as bar:
        # https://stackoverflow.com/a/63834834/1105803
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = {executor.submit(thread_run, wb): wb for wb in waterbodies}
            for future in concurrent.futures.as_completed(futures):
                # _ = futures[future]
                bar.update(1)

    Session.remove()


def stack_parquet(
    path: str | Path,
    pattern: str = ".*",
    mode: StackMode = StackMode.WATERBODIES,
    verbose: bool = False,
    **kwargs,
):
    """
    Stack Parquet files.

    Arguments
    ---------
    path : str | Path
        Path (s3 or local) to search for parquet files.

    pattern : str
        Regex to match file names against.

    mode : StackMode
        Method of stacking. Default is like DE Africa Waterbodies v1,
        a collection of polygon CSVs.

    verbose : bool

    **kwargs
        Passed to underlying stack method.
    """
    # "Support" pathlib Paths.
    path = str(path)

    _log.info(f"Begin to query {path} with pattern {pattern}")

    paths = find_parquet_files(path, pattern)

    if mode == StackMode.WATERBODIES:
        return stack_waterbodies_parquet_to_csv(parquet_file_paths=paths, verbose=verbose, **kwargs)
    if mode == StackMode.WATERBODIES_DB:
        return stack_waterbodies_parquet_to_db(parquet_file_paths=paths, verbose=verbose, **kwargs)
