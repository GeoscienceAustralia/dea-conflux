"""Input/output for Conflux.

Matthew Alger
Geoscience Australia
2021
"""
import datetime
import json
import logging
import os
import urllib

import boto3
import fsspec
import pandas as pd
import pyarrow
import pyarrow.parquet
import s3urls
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

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


def check_if_s3_uri(file_path: str) -> bool:
    """
    Checks if a file path is an S3 URI.

    Parameters
    ----------
    file_path : str
        File path to check

    Returns
    -------
    bool
        True if the file path is an S3 URI or Object URL.
        False if the file path is a local file path or File URI.
    """

    file_scheme = urllib.parse.urlparse(file_path).scheme

    # Assumption here is urls with the scheme https and http
    # are s3 file paths.

    # All others are assumed to be local files.

    valid_s3_schemes = ["s3", "http", "https"]

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
    output_directory = str(output_directory)  # Support pathlib paths.

    if check_if_s3_uri(output_directory):
        file_system = fsspec.filesystem("s3")
    else:
        file_system = fsspec.filesystem("file")

    file_name = make_parquet_file_name(drill_name, uuid, centre_date)
    folder_name = date_to_string_day(centre_date)

    path = os.path.join(output_directory, folder_name, file_name)

    return file_system.exists(path)


def write_table_to_parquet(
    drill_name: str,
    uuid: str,
    centre_date: datetime.datetime,
    table: pd.DataFrame,
    output_directory: str,
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

    output_directory : str
        Path to output directory.

    Returns
    -------
    str
        Path written to.
    """

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
    output_directory = str(output_directory)  # support pathlib paths
    folder_name = date_to_string_day(centre_date)
    file_name = make_parquet_file_name(drill_name, uuid, centre_date)
    output_file_path = os.path.join(output_directory, folder_name, file_name)

    if not check_if_s3_uri(output_directory):
        os.makedirs(os.path.join(output_directory, folder_name), exist_ok=True)

    pyarrow.parquet.write_table(table_pa, output_file_path, compression="GZIP")

    return output_file_path


def read_table_from_parquet(path: str) -> pd.DataFrame:
    """
    Read a Parquet file with Conflux metadata.

    Arguments
    ---------
    path : str
        Path to Parquet file.

    Returns
    -------
    pd.DataFrame
        DataFrame with attrs set.
    """
    table = pyarrow.parquet.read_table(path)
    df = table.to_pandas()
    meta_json = table.schema.metadata[PARQUET_META_KEY]
    metadata = json.loads(meta_json)
    for key, val in metadata.items():
        df.attrs[key] = val
    return df


def check_local_dir_exists(dir_path: str):
    """
    Checks if a specified path is an existing directory.

    Parameters
    ----------
    dir_path : str
        Path to check.

    Returns
    -------
    bool
        True if the path exists and is a directory.
        False if the path does not exists or if the path exists and it is not a directory.
    """
    if os.path.exists(dir_path):
        if os.path.isdir(dir_path):
            return True
        else:
            return False
    else:
        return False


def check_local_file_exists(file_path: str) -> bool:
    """
    Checks if a specified path is an existing file.

    Parameters
    ----------
    file_path : str
        Path to check.
    """
    if os.path.exists(file_path):
        if os.path.isfile(file_path):
            return True
        else:
            return False
    else:
        return False


def check_s3_bucket_exists(bucket_name: str, s3_client: S3Client = None) -> bool:
    """
    Check if a bucket exists and if the user has permission to access it.

    Parameters
    ----------
    bucket_name : str
        Name of s3 bucket to check.
    s3_client : S3Client
        A low-level client representing Amazon Simple Storage Service (S3), by default None.

    bool:
        True if the bucket exists and the user has access to the bucket.
        False if the bucket does not exist or if the bucket exists but the user does not have access to the bucket.
    """
    # Get the service client.
    if s3_client is None:
        s3_client = boto3.client("s3")

    try:
        response = s3_client.head_bucket(Bucket=bucket_name)  # noqa E501
    except ClientError:
        return False
    else:
        return True


def check_s3_object_exists(s3_object_uri: str, s3_client: S3Client = None):
    """
    Check if an object in an S3 bucket exists.

    Parameters
    ----------
    s3_object_uri : str
        S3 URI of the object to check.
    s3_client : S3Client
        A low-level client representing Amazon Simple Storage Service (S3), by default None.
    bool:
        True if the object exists and the user has access to the object.
        False if the object does not exist or if the object exists but the user does not have access to the object.
    """
    # Get the service client.
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket_name = s3urls.parse_url(s3_object_uri)["bucket"]
    object_key = s3urls.parse_url(s3_object_uri)["key"]

    # First check if bucket exists.
    if check_s3_bucket_exists(bucket_name, s3_client) is True:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)  # noqa
        except ClientError:
            return False
        else:
            return True
    else:
        return False
