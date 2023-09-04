"""Input/output for Conflux.

Matthew Alger
Geoscience Australia
2021
"""

import datetime
import json
import logging
import os
from pathlib import Path
import urllib
import s3urls
import boto3
from botocore.exceptions import ClientError

import pandas as pd
import pyarrow
import pyarrow.parquet
import s3fs

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
    """Serialise a date.

    Arguments
    ---------
    date : datetime

    Returns
    -------
    str
    """
    return date.strftime(DATE_FORMAT)


def date_to_string_day(date: datetime.datetime) -> str:
    """Serialise a date discarding hours/mins/seconds.

    Arguments
    ---------
    date : datetime

    Returns
    -------
    str
    """
    return date.strftime(DATE_FORMAT_DAY)


def string_to_date(date: str) -> datetime.datetime:
    """Unserialise a date.

    Arguments
    ---------
    date : str

    Returns
    -------
    datetime
    """
    return datetime.datetime.strptime(date, DATE_FORMAT)


def make_name(drill_name: str, uuid: str, centre_date: datetime.datetime) -> str:
    """Make filename for Parquet.

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
    return f"{drill_name}_{uuid}_{datestring}.pq"


def table_exists(
    drill_name: str,
    uuid: str,
    centre_date: datetime.datetime,
    output: str
) -> bool:
    """Check whether a table already exists.

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

    output : str
        Path to output directory.

    Returns
    -------
    bool
    """
    name = make_name(drill_name, uuid, centre_date)
    foldername = date_to_string_day(centre_date)

    if not output.endswith("/"):
        output = output + "/"
    if not foldername.endswith("/"):
        foldername = foldername + "/"

    path = output + foldername + name

    if not output.startswith("s3://"):
        # local
        return os.path.exists(path)

    return s3fs.S3FileSystem().exists(path)


def write_table(
    drill_name: str,
    uuid: str,
    centre_date: datetime.datetime,
    table: pd.DataFrame,
    output: str,
) -> str:
    """Write a table to Parquet.

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

    output : str
        Path to output directory.

    Returns
    -------
    Path written to.
    """
    output = str(output)

    is_s3 = output.startswith("s3://")

    foldername = date_to_string_day(centre_date)

    if not is_s3:
        path = Path(output)
        os.makedirs(path / foldername, exist_ok=True)

    filename = make_name(drill_name, uuid, centre_date)

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
    if not output.endswith("/"):
        output = output + "/"

    if not foldername.endswith("/"):
        foldername = foldername + "/"

    output_path = output + foldername + filename
    pyarrow.parquet.write_table(table_pa, output_path, compression="GZIP")
    return output_path


def read_table(path: str) -> pd.DataFrame:
    """Read a Parquet file with Conflux metadata.

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


def check_if_s3_uri(file_path: str):
    """
    Checks if a file path is an S3 URI.

    Parameters
    ----------
    file_path : str
        File path to check
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

    """
    if error_if_exists:
        if os.path.exists(dir_path):
            if os.path.isdir(dir_path):
                _log.error(f"Directory {dir_path} exists!")
                raise FileExistsError(f"Directory {dir_path} exists!")
            else:
                _log.error(f"{dir_path} is not a directory!")
                raise NotADirectoryError(f"Directory {dir_path} is not a directory!")
        else:
            _log.info(f"Directory {dir_path} does not exist.")

    else:
        if os.path.exists(dir_path):
            if not os.path.isdir(dir_path):
                _log.error(f"{dir_path} is not a directory!")
                raise NotADirectoryError(f"{dir_path} is not a directory!")
            else:
                _log.info(f"Directory {dir_path} exists.")
        else:
            _log.error(f"{dir_path} does not exist!")
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

    """
    if error_if_exists:
        if os.path.exists(file_path):
            if not os.path.isfile(file_path):
                _log.error(f"{file_path} is not a file!")
                raise ValueError(f"{file_path} is not a file!")
            else:
                _log.error(f"File {file_path} exists!")
                raise FileExistsError(f"File {file_path} exists!")
        else:
            _log.info(f"{file_path} does not exist.")

    else:
        if os.path.exists(file_path):
            if not os.path.isfile(file_path):
                _log.error(f"{file_path} is not a file!")
                raise ValueError(f"{file_path} exists is not a file!")
            else:
                _log.info(f"File {file_path} exists.")
        else:
            _log.error(f"{file_path} does not exist!")
            raise FileNotFoundError(f"{file_path} does not exist!")


def check_s3_bucket_exists(bucket_name: str):
    """
    Check if a bucket exists and if user has permission to access it.

    Parameters
    ----------
    bucket_name : str
        Name of s3 bucket to check.
    """
    s3_client = boto3.client("s3")
    try:
        response = s3_client.head_bucket(Bucket=bucket_name)
        _log.info("Bucket {bucket_name} exists.")
    except ClientError as error:
        _log.error(error)

        error_code = int(error.response['Error']['Code'])

        if error_code == 403:
            _log.error(f"{bucket_name} is a private Bucket. Forbidden Access!")
            raise PermissionError(f"{bucket_name} is a private Bucket. Forbidden Access!")
        elif error_code == 404:
            _log.error(f"Bucket {bucket_name} Does Not Exist!")
            raise FileNotFoundError(f"Bucket {bucket_name} Does Not Exist!")
    except Exception as error:
        _log.error(error)
        raise


def check_s3_object_exists(
        s3_object_uri: str,
        error_if_exists=True):
    """
    Check if an object in an S3 bucket exists.
    if error_if_exists is True, raises an error if the object exists.
    if error_if_exists is False, raises an error if the object does not exist.

    Parameters
    ----------
    s3_object_uri : str
        S3 URI of the object to check.
    """

    bucket_name = s3urls.parse_url(s3_object_uri)["bucket"]
    object_key = s3urls.parse_url(s3_object_uri)["key"]

    # First check if bucket exists.
    check_s3_bucket_exists(bucket_name)

    s3_client = boto3.client("s3")

    if error_if_exists:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            _log.error(f"Object {s3_object_uri}  already exists!")
            raise FileExistsError(f"Object {s3_object_uri} already exists!")
        except ClientError as error:

            error_code = int(error.response['Error']['Code'])

            # Object exists but user does not have access.
            if error_code == 403:
                _log.error(f"Object {s3_object_uri} already exists!")
                raise FileExistsError(f"Object {s3_object_uri} already exists!")
            # Object does not exist.
            elif error_code == 404:
                _log.info(f"Object {s3_object_uri} does not exist.")
        except Exception as error:
            _log.error(error)
            raise
    else:
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            _log.info(f"Object {s3_object_uri} exists!")
        except ClientError as error:

            error_code = int(error.response['Error']['Code'])

            # Object exists but user does not have access..
            if error_code == 403:
                _log.error(f"Object {s3_object_uri} exists, but forbidden access!")
                raise PermissionError("Object exists, but forbidden access!")
            # File does not exist.
            elif error_code == 404:
                _log.error(f"Object {s3_object_uri} does not exist!")
                raise FileNotFoundError(f"Object {s3_object_uri} does not exist!")
        except Exception as error:
            _log.error(error)
            raise


def check_dir_exists(dir_path: str):
    """
    Checks if a directory path exists.

    Parameters
    ----------
    dir_path : str
        File URI or S3 URI of a directory.
    """

    is_s3_uri = check_if_s3_uri(dir_path)

    if not is_s3_uri:
        check_local_dir_exists(dir_path)
    else:
        check_s3_object_exists(dir_path, error_if_exists=False)
