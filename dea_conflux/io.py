"""Input/output for Conflux.

Matthew Alger
Geoscience Australia
2021
"""

import datetime
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow
import pyarrow.parquet

logger = logging.getLogger(__name__)


# File extensions to recognise as Parquet files.
PARQUET_EXTENSIONS = {'.pq', '.parquet'}

# Metadata key for Parquet files.
PARQUET_META_KEY = 'conflux.metadata'.encode('ascii')


def date_to_string(date: datetime.datetime) -> str:
    """Serialise a date.

    Arguments
    ---------
    date : datetime
    
    Returns
    -------
    str
    """
    return date.strftime('%Y%m%d-%H%M%S-%f')


def make_name(
        drill_name: str,
        uuid: str,
        centre_date: datetime.datetime) -> str:
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
    return f'{drill_name}_{uuid}_{datestring}.pq'


def write_table(
        drill_name: str, uuid: str,
        centre_date: datetime.datetime,
        table: pd.DataFrame, output: str):
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
    """
    output = str(output)

    # TODO(MatthewJA): Support S3 write.
    if output.startswith('s3'):
        raise NotImplementedError()
    
    path = Path(output)
    os.makedirs(path, exist_ok=True)
    
    filename = make_name(drill_name, uuid, centre_date)

    # Convert the table to pyarrow.
    table_pa = pyarrow.Table.from_pandas(table)

    # Dump new metadata to JSON.
    meta_json = json.dumps({
        'drill': drill_name,
        'date': date_to_string(centre_date),
    })

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
    pyarrow.parquet.write_table(
        table_pa,
        path / filename,
        compression='GZIP')


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
