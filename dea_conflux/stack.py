"""Stack Parquet scene outputs into other formats.

Lots of this code is domain-specific and not intended to be fully general.

Matthew Alger
Geoscience Australia
2021
"""

import collections
import concurrent.futures
import datetime
from datetime import timedelta
import enum
import logging
import multiprocessing
import os
import re
from io import StringIO
from pathlib import Path

import boto3
import fsspec
import geohash
import numpy as np
import pandas as pd
import s3fs
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from tqdm.auto import tqdm

import dea_conflux.db
import dea_conflux.io
from dea_conflux.db import Engine
from dea_conflux.io import CSV_EXTENSIONS, PARQUET_EXTENSIONS

import dea_tools.bandindices
import dea_tools.datahandling
import dea_tools.wetlands

logger = logging.getLogger(__name__)


class StackMode(enum.Enum):
    WATERBODIES = "waterbodies"
    WATERBODIES_DB = "waterbodies_db"
    WITTOOLING = "wit_tooling"
    WITTOOLING_SINGLE_FILE_DELIVERY = "wit_tooling_single_file_delivery"


def stack_format_date(date: datetime.datetime) -> str:
    """Format a date to match DEA conflux products datetime.

    Arguments
    ---------
    date : datetime

    Returns
    -------
    str
    """
    # e.g. 1987-05-24T01:30:18Z
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


def find_csv_files(path: str, pattern: str = ".*") -> [str]:
    """Find CSV files matching a pattern.

    Arguments
    ---------
    path : str
        Path (s3 or local) to search for CSV files.

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

    # Frankly, load CSV and load PQ should be a same method
    # but no plan to touch the existing waterbodies pipeline code
    # before I finish the waterbodies run test.
    if path.startswith("s3://"):
        # Find CSV files on S3.
        fs = s3fs.S3FileSystem(anon=True)
        files = fs.find(path)
        for file in files:
            _, ext = os.path.splitext(file)
            if ext not in CSV_EXTENSIONS:
                continue

            _, filename = os.path.split(file)
            if not pattern.match(filename):
                continue

            all_paths.append(f"s3://{file}")
    else:
        # Find CSV files locally.
        for root, dir_, files in os.walk(path):
            paths = [Path(root) / file for file in files]
            for path_ in paths:
                if path_.suffix not in CSV_EXTENSIONS:
                    continue

                if not pattern.match(path_.name):
                    continue

                all_paths.append(path_)

    return all_paths


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


def remove_timeseries_with_duplicated(df: pd.DataFrame) -> pd.DataFrame:
    """Removed the timeseries duplicated (same day) data in DataFrame.

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
    df = df.sort_values(["DAY", "pc_missing"], ascending=True)
    # The pc_missing the less the better, so we only keep the first one
    df = df.drop_duplicates("DAY", keep="first")

    # Remove entries within 60s, deals with edge cases where duplicates wrap across midnight UTC

    if "date" in df.columns and len(df) > 1:
        df['TIMEDIFF'] = pd.to_datetime(df['date'].shift(-1)) - pd.to_datetime(df['date'])
        df = df[~(df['TIMEDIFF'] < timedelta(seconds=60))]
        df = df.drop(columns=["TIMEDIFF"])

    # Remember to remove the temp column day in result_df
    return df.drop(columns=["DAY"])


def load_pq_file(path):
    """Load Parquet file from given path.

    Arguments
    ---------
    path : str
        Path (s3 or local) to search for Parquet files.
    Returns
    -------
    [pandas.DataFrame]
        pandas.DataFrame
    """
    df = dea_conflux.io.read_table(path)
    # the pq file will be empty if no polygon belongs to that scene
    if df.empty is not True:
        date = dea_conflux.io.string_to_date(df.attrs["date"])
        date = stack_format_date(date)
        df.loc[:, "date"] = date
        df.loc[:, "ard_product"] = str(path).split("/")[-1].split("_")[-3]
    return df


def save_df_as_csv(single_polygon_df, feature_id, outpath, remove_duplicated_data):
    """Save polygon base pandas.DataFrame as
    CSV file in output folder.

    Arguments
    ---------
    single_polygon_df: pandas.Dataframe
        The polygon base dea-conflux drill result.
    feature_id: str
        Polygon unique ID.
    outpath : str
        Path (s3 or local) to save the CSV files.
    remove_duplicated_data: bool
        Remove timeseries duplicated data or not
    """
    # feature_id, single_polygon_df = item
    filename = f"{outpath}/{feature_id}.csv"

    if remove_duplicated_data:
        # Remove the timeseries duplicated data
        single_polygon_df = remove_timeseries_with_duplicated(single_polygon_df)

    single_polygon_df["feature_id"] = single_polygon_df.index
    single_polygon_df.reset_index(inplace=True)

    # WIT Normalise Step

    # 1. compute the expected vegetation area total size: 1 - water (%) - wet (%)
    single_polygon_df["veg_areas"] = (
        1 - single_polygon_df["water"] - single_polygon_df["wet"]
    )

    # 2. normalse the vegetation values based on vegetation size (to handle FC values more than 100 issue)
    # WARNNING: Not touch the water and wet, cause they are pixel classification result
    single_polygon_df["overall_veg_num"] = (
        single_polygon_df["pv"] + single_polygon_df["npv"] + single_polygon_df["bs"]
    )

    # 3. if the overall_veg_num is 0, no need to normalize veg area
    norm_veg_index = single_polygon_df["overall_veg_num"] != 0

    # the normlized values will be saved as norm_bs/norm_pv/norm_npv
    for band in ["pv", "npv", "bs"]:

        # assign pv/npv/bs values to norm_pv/norm_npv/norm_bs firstly
        single_polygon_df.loc[:, "norm_" + band] = single_polygon_df.loc[:, band]

        # only modify the norm_pv/npv/bs which overall_ver_num is not 0
        single_polygon_df.loc[norm_veg_index, "norm_" + band] = (
            single_polygon_df.loc[norm_veg_index, band]
            / single_polygon_df.loc[norm_veg_index, "overall_veg_num"]
            * single_polygon_df.loc[norm_veg_index, "veg_areas"]
        )
    single_polygon_df = single_polygon_df[~(single_polygon_df['pc_missing'] > 0.1)]
    single_polygon_df = single_polygon_df.reset_index()
    single_polygon_df['date'] = pd.to_datetime(single_polygon_df['date']).dt.tz_localize(None)
    print(single_polygon_df)
    dea_tools.wetlands.display_wit_stack_with_df(single_polygon_df, feature_id, feature_id, x_axis_labels="years")

    # remove the temp column
    single_polygon_df.drop(
        ["overall_veg_num", "veg_areas", "index"], axis=1, inplace=True
    )
    if not outpath.startswith("s3://"):
        os.makedirs(Path(filename).parent, exist_ok=True)
    with fsspec.open(filename, "w") as f:
        single_polygon_df.to_csv(f, index=False)
    return filename


def stack_wit_tooling_to_single_file(
    paths: [str], output_dir: str, precision: int, verbose: bool = False
):
    """This method aims to handle the request from QLD
    team. It will loading all polygon base result (CSV files),
    then concat the pandas.DataFrame, and create a single
    CSV and parquet file.

    Arguments
    ---------
    paths : [str]
        List of paths to Parquet files to stack.
    output_dir : str
        Path to output directory.

    verbose : bool
    """
    
    polygon_df_list = []
    logger.info("Reading...")

    # Note: the stack_wit_tooling_to_single_file() input files are CSV file, which generate by save_df_as_csv()
    # then we assume they already had the norm_pv, norm_npv, norm_bs there.
    with tqdm(total=len(paths)) as bar:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=multiprocessing.cpu_count() * 16
        ) as executor:
            polygon_df_list = []
            futures = {executor.submit(pd.read_csv, path): path for path in paths}
            for future in concurrent.futures.as_completed(futures):
                polygon_df_list.append(future.result())
                bar.update(1)

    if len(polygon_df_list) == 0:
        logger.warning("Cannot find any available WIT result.")
        return 0
    else:
        logger.info("Concat WIT result...")
        overall_result = pd.concat(polygon_df_list)

    logger.info("Writing overall result...")
    overall_pq_filename = f"{output_dir}/overall.pq"
    overall_csv_filename = f"{output_dir}/overall.csv"
    if not output_dir.startswith("s3://"):
        os.makedirs(Path(overall_pq_filename).parent, exist_ok=True)

    column_names = [
        "bs",
        "npv",
        "pc_missing",
        "pv",
        "water",
        "wet",
        "norm_pv",
        "norm_npv",
        "norm_bs",
    ]

    logger.info(f"Begin to reduce the precision of the data to {str(precision)}")

    for column_name in column_names:
        overall_result[column_name] = overall_result[column_name].round(
            decimals=precision
        )

    # Add normalise method section
    # 1) compute vegetation_area_size (1 - water - wet)
    # 2) normlise pv/npv/bs by vegetation_area_size

    overall_result.to_parquet(overall_pq_filename, index=False)
    overall_result.to_csv(overall_csv_filename, index=False)


def stack_wit_tooling(
    paths: [str],
    output_dir: str,
    remove_duplicated_data: bool = True,
):
    """Stack wit tooling parquet result files into CSVs.

    Arguments
    ---------
    paths : [str]
        List of paths to Parquet files to stack.
    output_dir : str
        Path to output directory.
    remove_duplicated_data: bool
        Remove timeseries duplicated data

    verbose : bool
    """
    wit_df_list = []
    logger.info("Reading...")

    with tqdm(total=len(paths)) as bar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            wit_df_list = []
            futures = {executor.submit(load_pq_file, path): path for path in paths}
            for future in concurrent.futures.as_completed(futures):
                wit_df_list.append(future.result())
                bar.update(1)

    if len(wit_df_list) == 0:
        logger.warning("Cannot find any available WIT result.")
        return 0
    else:
        logger.info("Concat WIT result...")
        wit_result = pd.concat(wit_df_list)

    # delete the temp result to release RAM
    del wit_df_list

    logger.info("Writing overall result...")
    overall_filename = f"{output_dir}/overall.pq"

    if not output_dir.startswith("s3://"):
        os.makedirs(Path(overall_filename).parent, exist_ok=True)
    wit_result.to_parquet(overall_filename)

    logger.info("Writing polygon base result...")

    polygon_groups = wit_result.groupby(wit_result.index)
    feature_ids = wit_result.index.unique()

    # delete the temp result to release RAM
    del wit_result

    with tqdm(total=len(feature_ids)) as bar:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=multiprocessing.cpu_count()
        ) as executor:
            futures = {
                executor.submit(
                    save_df_as_csv,
                    polygon_groups.get_group(feature_id),
                    feature_id,
                    output_dir,
                    remove_duplicated_data,
                ): feature_id
                for feature_id in feature_ids
            }
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()
                bar.update(1)


def stack_waterbodies(
    paths: [str],
    output_dir: str,
    remove_duplicated_data: bool = True,
    verbose: bool = False,
):
    """Stack Parquet files into CSVs like DEA Waterbodies does.

    Arguments
    ---------
    paths : [str]
        List of paths to Parquet files to stack.

    output_dir : str
        Path to output directory.

    remove_duplicated_data: bool
        Remove timeseries duplicated data or not

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
        date = stack_format_date(date)
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
        if remove_duplicated_data:
            df = remove_timeseries_with_duplicated(df)
        df.sort_index(inplace=True)
        filename = f"{outpath}/{uid[:4]}/{uid}.csv"
        logger.info(f"Writing {filename}")
        if not outpath.startswith("s3://"):
            os.makedirs(Path(filename).parent, exist_ok=True)
        with fsspec.open(filename, "w") as f:
            df.to_csv(f, index_label="date")


def get_waterbody_key(uid: str, session: Session):
    """Create or get a unique key from the database."""
    # decode into a coordinate
    # uid format is gh_version
    gh = uid.split("_")[0]
    lat, lon = geohash.decode(gh)
    defaults = {
        "geofabric_name": "",
        "centroid_lat": lat,
        "centroid_lon": lon,
    }
    inst, _ = dea_conflux.db.get_or_create(
        session, dea_conflux.db.Waterbody, wb_name=uid, defaults=defaults
    )
    return inst.wb_id


def stack_waterbodies_db(
    paths: [str],
    verbose: bool = False,
    engine: Engine = None,
    uids: {str} = None,
    drop: bool = False,
):
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

    drop : bool
        Whether to drop the database. Default False.
    """
    if verbose:
        paths = tqdm(paths)

    # connect to the db
    if not engine:
        engine = dea_conflux.db.get_engine_waterbodies()

    Session = sessionmaker(bind=engine)
    session = Session()

    # drop tables if requested
    if drop:
        dea_conflux.db.drop_waterbody_tables(engine)

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
                platform="UNK",
                date=date,
            )
            obss.append(obs)
        # basically just hoping that these don't exist already
        # TODO: Insert or update
        session.bulk_save_objects(obss)
        session.commit()


def stack_waterbodies_db_to_csv(
    out_path: str,
    verbose: bool = False,
    uids: {str} = None,
    remove_duplicated_data: bool = True,
    engine=None,
    n_workers: int = 8,
    index_num: int = 0,
    split_num: int = 1,
):
    """Write waterbodies CSVs out from the interstitial DB.

    Arguments
    ---------
    out_path : str
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
    # connect to the db
    if not engine:
        engine = dea_conflux.db.get_engine_waterbodies()

    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)

    # Iterate over waterbodies.

    def thread_run(wb: dea_conflux.db.Waterbody):
        session = Session()

        # get all observations
        logger.debug(f"Processing {wb.wb_name}")
        obs = (
            session.query(dea_conflux.db.WaterbodyObservation)
            .filter(dea_conflux.db.WaterbodyObservation.wb_id == wb.wb_id)
            .order_by(dea_conflux.db.WaterbodyObservation.date.asc())
            .all()
        )

        rows = [
            {
                "date": stack_format_date(ob.date),
                "pc_wet": round(ob.pc_wet * 100, 2),
                "px_wet": ob.px_wet,
                "pc_missing": ob.pc_missing,
            }
            for ob in obs
        ]

        df = pd.DataFrame(rows, columns=["date", "pc_wet", "px_wet", "pc_missing"])
        if remove_duplicated_data:
            df = remove_timeseries_with_duplicated(df)
            print(out_path + "/" + wb.wb_name[:4] + "/" + wb.wb_name + ".csv")
        # The pc_missing should not in final WaterBodies result
        df.drop(columns=["pc_missing"], inplace=True)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, header=True, index=False)
        csv_data = csv_buffer.getvalue()

        from urllib.parse import urlparse

        # Parse the S3 URI
        parsed_uri = urlparse(
            out_path + "/" + wb.wb_name[:4] + "/" + wb.wb_name + ".csv"
        )

        if parsed_uri.scheme == "s3":
            # Extract the bucket name and object key
            bucket_name = parsed_uri.netloc
            object_key = parsed_uri.path.lstrip("/")

            s3 = boto3.client("s3")

            s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=csv_data,
                ACL="bucket-owner-full-control",  # Set the ACL to bucket-owner-full-control
            )
        else:
            df.to_csv(
                out_path + "/" + wb.wb_name[:4] + "/" + wb.wb_name + ".csv",
                header=True,
                index=False,
            )

        Session.remove()

    session = Session()
    if not uids:
        # query all
        waterbodies = session.query(dea_conflux.db.Waterbody).all()
    else:
        # query some
        waterbodies = (
            session.query(dea_conflux.db.Waterbody)
            .filter(dea_conflux.db.Waterbody.wb_name.in_(uids))
            .all()
        )

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

    logger.info(f"Begin to query {path} with pattern {pattern}")

    if mode == StackMode.WITTOOLING_SINGLE_FILE_DELIVERY:
        paths = find_csv_files(path, pattern)
    else:
        paths = find_parquet_files(path, pattern)

    if mode == StackMode.WATERBODIES:
        return stack_waterbodies(paths, verbose=verbose, **kwargs)
    if mode == StackMode.WATERBODIES_DB:
        return stack_waterbodies_db(paths, verbose=verbose, **kwargs)
    if mode == StackMode.WITTOOLING:
        return stack_wit_tooling(paths, verbose=verbose, **kwargs)
    if mode == StackMode.WITTOOLING_SINGLE_FILE_DELIVERY:
        return stack_wit_tooling_to_single_file(paths, verbose=verbose, **kwargs)
