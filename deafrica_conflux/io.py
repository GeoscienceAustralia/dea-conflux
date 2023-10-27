"""Input/output for Conflux.

Matthew Alger
Geoscience Australia
2021
"""
import datetime
import json
import logging
import os
import re
import urllib
from pathlib import Path

import fsspec
import pandas as pd
import pyarrow
import pyarrow.parquet

_log = logging.getLogger(__name__)

# File extensions to recognise as Parquet files.
PARQUET_EXTENSIONS = {".pq", ".parquet"}

# File extensions to recognise as CSV files.
CSV_EXTENSIONS = {".csv", ".CSV"}

# Metadata key for Parquet files.
PARQUET_META_KEY = b"conflux.metadata"

# Format of string date metadata.
DATE_FORMAT = "%Y%m%d-%H%M%S-%f"
DATE_FORMAT_DAY = "%Y%m%d"


def date_to_string(date: datetime.datetime) -> str:
    """
    Serialise a date.

    Arguments
    ---------
    date : datetime

    Returns
    -------
    str
    """
    return date.strftime(DATE_FORMAT)


def date_to_string_day(date: datetime.datetime) -> str:
    """
    Serialise a date discarding hours/mins/seconds.

    Arguments
    ---------
    date : datetime

    Returns
    -------
    str
    """
    return date.strftime(DATE_FORMAT_DAY)


def string_to_date(date: str) -> datetime.datetime:
    """
    Unserialise a date.

    Arguments
    ---------
    date : str

    Returns
    -------
    datetime
    """
    return datetime.datetime.strptime(date, DATE_FORMAT)


def make_parquet_file_name(drill_name: str, uuid: str, centre_date: datetime.datetime) -> str:
    """
    Make filename for Parquet.

    Arguments
    ---------
    drill_name : str
        Name of the drill.

    uuid : str
        UUID of reference dataset.

    centre_date : datetime
        Centre date of reference dataset.

    Returns
    -------
    str
        Parquet filename.
    """
    datestring = date_to_string(centre_date)

    parquet_file_name = f"{drill_name}_{uuid}_{datestring}.pq"

    return parquet_file_name


def check_if_s3_uri(file_path: str | Path) -> bool:
    """
    Checks if a file path is an S3 URI.

    Parameters
    ----------
    file_path : str | Path
        File path to check

    Returns
    -------
    bool
        True if the file path is an S3 URI.
    """
    # "Support" pathlib Paths.
    file_path = str(file_path)

    file_scheme = urllib.parse.urlparse(file_path).scheme

    valid_s3_schemes = ["s3"]

    if file_scheme in valid_s3_schemes:
        return True
    else:
        return False


def table_exists(
    drill_name: str, uuid: str, centre_date: datetime.datetime, output_directory: str
) -> bool:
    """
    Check whether a table already exists.

    Arguments
    ---------
    drill_name : str
        Name of the drill.

    uuid : str
        UUID of reference dataset.

    centre_date : datetime
        Centre date of reference dataset.

    table : pd.DataFrame
        Dataframe with index polygons and columns bands.

    output_directory : str
        Path to output directory.

    Returns
    -------
    bool
    """
    # "Support" pathlib Paths.
    output_directory = str(output_directory)

    if check_if_s3_uri(output_directory):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    file_name = make_parquet_file_name(drill_name, uuid, centre_date)
    folder_name = date_to_string_day(centre_date)

    path = os.path.join(output_directory, folder_name, file_name)

    if fs.exists(path):
        _log.info(f"{path} exists.")
    else:
        _log.info(f"{path} does not exist.")

    return fs.exists(path)


def write_table_to_parquet(
    drill_name: str,
    uuid: str,
    centre_date: datetime.datetime,
    table: pd.DataFrame,
    output_directory: str | Path,
) -> str:
    """
    Write a table to Parquet.

    Arguments
    ---------
    drill_name : str
        Name of the drill.

    uuid : str
        UUID of reference dataset.

    centre_date : datetime
        Centre date of reference dataset.

    table : pd.DataFrame
        Dataframe with index polygons and columns bands.

    output_directory : str | Path
        Path to output directory.

    Returns
    -------
    str
        Path written to.
    """
    # "Support" pathlib Paths.
    output_directory = str(output_directory)

    # Convert the table to pyarrow.
    table_pa = pyarrow.Table.from_pandas(table)

    # Dump new metadata to JSON.
    meta_json = json.dumps(
        {
            "drill": drill_name,
            "date": date_to_string(centre_date),
        }
    )

    # Dump existing (Pandas) metadata.
    # https://towardsdatascience.com/
    #   saving-metadata-with-dataframes-71f51f558d8e
    existing_meta = table_pa.schema.metadata
    combined_meta = {
        PARQUET_META_KEY: meta_json.encode(),
        **existing_meta,
    }
    # Replace the metadata.
    table_pa = table_pa.replace_schema_metadata(combined_meta)

    # Write the table.
    folder_name = date_to_string_day(centre_date)
    file_name = make_parquet_file_name(drill_name, uuid, centre_date)

    if check_if_s3_uri(output_directory):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    # Check if the parent folder exists.
    parent_folder = os.path.join(output_directory, folder_name)
    if not check_dir_exists(parent_folder):
        fs.makedirs(parent_folder, exist_ok=True)
        _log.info(f"Created directory: {parent_folder}")

    output_file_path = os.path.join(parent_folder, file_name)

    pyarrow.parquet.write_table(table=table_pa, where=output_file_path, compression="GZIP")

    _log.info(f"Table written to {output_file_path}")
    return output_file_path


def read_table_from_parquet(path: str | Path) -> pd.DataFrame:
    """
    Read a Parquet file with Conflux metadata.

    Arguments
    ---------
    path : str | Path
        Path to Parquet file.

    Returns
    -------
    pd.DataFrame
        DataFrame with attrs set.
    """
    # "Support" pathlib Paths.
    path = str(path)

    table = pyarrow.parquet.read_table(path)
    df = table.to_pandas()
    meta_json = table.schema.metadata[PARQUET_META_KEY]
    metadata = json.loads(meta_json)
    for key, val in metadata.items():
        df.attrs[key] = val
    return df


def check_dir_exists(dir_path: str | Path):
    """
    Checks if a specified path is an existing directory.

    Parameters
    ----------
    dir_path : str | Path
        Path to check.

    Returns
    -------
    bool
        True if the path exists and is a directory.
        False if the path does not exists or if the path exists and it is not a directory.
    """
    # "Support" pathlib Paths.
    dir_path = str(dir_path)

    if check_if_s3_uri(dir_path):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    if fs.exists(dir_path):
        if fs.isdir(dir_path):
            return True
        else:
            return False
    else:
        return False


def check_file_exists(file_path: str | Path) -> bool:
    """
    Checks if a specified path is an existing file.

    Parameters
    ----------
    file_path : str | Path
        Path to check.

    Returns
    -------
    bool
        True if the path exists and is a file.
        False if the path does not exists or if the path exists and it is not a file.
    """
    # "Support" pathlib Paths.
    file_path = str(file_path)

    if check_if_s3_uri(file_path):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    if fs.exists(file_path):
        if fs.isfile(file_path):
            return True
        else:
            return False
    else:
        return False


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


def find_csv_files(path: str | Path, pattern: str = ".*") -> [str]:
    """
    Find CSV files matching a pattern.

    Arguments
    ---------
    path : str | Path
        Path (s3 or local) to search for CSV files.

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
        # Find CSV files on S3.
        file_system = fsspec.filesystem("s3")
    else:
        # Find CSV files localy.
        file_system = fsspec.filesystem("file")

    csv_file_paths = []

    files = file_system.find(path)
    for file in files:
        _, file_extension = os.path.splitext(file)
        if file_extension not in CSV_EXTENSIONS:
            continue
        else:
            _, file_name = os.path.split(file)
            if not pattern.match(file_name):
                continue
            else:
                csv_file_paths.append(file)

    if check_if_s3_uri(path):
        csv_file_paths = [f"s3://{file}" for file in csv_file_paths]

    _log.info(f"Found {len(csv_file_paths)} csv files.")
    return csv_file_paths
