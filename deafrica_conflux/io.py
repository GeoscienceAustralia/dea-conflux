"""Input/output for Conflux.

Matthew Alger
Geoscience Australia
2021
"""
import os
import json
import urllib
import logging
import datetime

import boto3
import fsspec
import s3urls
import pyarrow
import pyarrow.parquet
import pandas as pd
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


def make_parquet_file_name(
        drill_name: str,
        uuid: str,
        centre_date: datetime.datetime) -> str:
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


def table_exists(
    drill_name: str,
    uuid: str,
    centre_date: datetime.datetime,
    output_directory: str
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
    file_name = make_parquet_file_name(drill_name, uuid, centre_date)
    folder_name = date_to_string_day(centre_date)

    path = os.path.join(output_directory, folder_name, file_name)

    if output_directory.startswith("s3://"):
        file_system = fsspec.filesystem("s3")
    else:
        file_system = fsspec.filesystem("file")
    
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
    output_directory = str(output_directory)
    folder_name = date_to_string_day(centre_date)
    file_name = make_parquet_file_name(drill_name, uuid, centre_date)
    output_file_path = os.path.join(output_directory, folder_name, file_name)
    
    if not output_directory.startswith("s3://"):
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


def check_local_dir_exists(
        dir_path: str,
        error_if_exists=True):
    """
    Checks if a specified path is an existing directory.
    if `error_if_exists == True`, raises an error if the directory exists.
    if `error_if_exists == False`, raises an error if the directory does not exist.

    Parameters
    ----------
    dir_path : str
        Path to check.
    error_if_exists: bool, optional
        If True, raise an error if the directory exists.
        If False, raise an error if the directory does NOT exist.
        By default True.
    """
    if error_if_exists:
        if os.path.exists(dir_path):
            if os.path.isdir(dir_path):
                raise FileExistsError(f"Directory {dir_path} exists!")
            else:
                raise NotADirectoryError(f"Directory {dir_path} is not a directory!")
        else:
            pass

    else:
        if os.path.exists(dir_path):
            if not os.path.isdir(dir_path):
                raise NotADirectoryError(f"{dir_path} is not a directory!")
            else:
                pass
        else:
            raise FileNotFoundError(f"Directory {dir_path} does not exist!")


def check_local_file_exists(
        file_path: str,
        error_if_exists=True):
    """
    Checks if a specified path is an existing file.
    if `error_if_exists == True`, raises an error if the file exists.
    if `error_if_exists == False`, raises an error if the file does not exist.

    Parameters
    ----------
    file_path : str
        Path to check.
    error_if_exists : bool, optional
        If True, raise an error if the file exists.
        If False, raise an error if the file does NOT exist.
        By default True.
    """
    if error_if_exists:
        if os.path.exists(file_path):
            if not os.path.isfile(file_path):
                raise ValueError(f"{file_path} is not a file!")
            else:
                raise FileExistsError(f"File {file_path} exists!")
        else:
            pass

    else:
        if os.path.exists(file_path):
            if not os.path.isfile(file_path):
                raise ValueError(f"{file_path} is not a file!")
            else:
                pass
        else:
            raise FileNotFoundError(f"{file_path} does not exist!")
    return 0


def check_s3_bucket_exists(
        bucket_name: str,
        s3_client: S3Client = None):
    """
    Check if a bucket exists and if the user has permission to access it.

    Parameters
    ----------
    bucket_name : str
        Name of s3 bucket to check.
    s3_client : S3Client
        A low-level client representing Amazon Simple Storage Service (S3), by default None.
    
    """
    # Get the service client.
    if s3_client is None:
        s3_client = boto3.client("s3")
        
    try:
        response = s3_client.head_bucket(Bucket=bucket_name) # noqa E501
    except ClientError as error:
        error_code = int(error.response['Error']['Code'])

        if error_code == 403:
            raise PermissionError(f"{bucket_name} is a private Bucket. Forbidden Access!")
        elif error_code == 404:
            raise FileNotFoundError(f"Bucket {bucket_name} Does Not Exist!")
    except Exception as error:
        _log.exception(error)
        raise error


def check_s3_object_exists(
        s3_object_uri: str,
        error_if_exists=True,
        s3_client: S3Client = None):
    """
    Check if an object in an S3 bucket exists.
    if error_if_exists is True, raises an error if the object exists.
    if error_if_exists is False, raises an error if the object does not exist.

    Parameters
    ----------
    s3_object_uri : str
        S3 URI of the object to check.
    error_if_exists : bool, optional
        If True, raise an error if the object exists.
        If False, raise an error if the object does NOT exist.
        By default True.
    s3_client : S3Client
        A low-level client representing Amazon Simple Storage Service (S3), by default None.
    """
    # Get the service client.
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket_name = s3urls.parse_url(s3_object_uri)["bucket"]
    object_key = s3urls.parse_url(s3_object_uri)["key"]

    # First check if bucket exists.
    check_s3_bucket_exists(bucket_name, s3_client)

    if error_if_exists:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            raise FileExistsError(f"Object {s3_object_uri} already exists!")
        except ClientError as error:
            error_code = int(error.response['Error']['Code'])

            # Object exists but user does not have access.
            if error_code == 403:
                raise FileExistsError(f"Object {s3_object_uri} already exists! Forbidden Access!")
            # Object does not exist.
            elif error_code == 404:
                pass
        except Exception as error:
            _log.exception(error)
            raise error
    else:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)  # noqa E501
        except ClientError as error:
            error_code = int(error.response['Error']['Code'])

            # Object exists but user does not have access.
            if error_code == 403:
                raise PermissionError(f"Object {s3_object_uri} exists, but forbidden access!")
            # File does not exist.
            elif error_code == 404:
                raise FileNotFoundError(f"Object {s3_object_uri} does not exist!")
        except Exception as error:
            _log.exception(error)
            raise error
