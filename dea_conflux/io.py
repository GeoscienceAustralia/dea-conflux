"""Input/output for Conflux.

Matthew Alger
Geoscience Australia
2021
"""

import pandas as pd


def make_name(drill_name: str, uuid: str, centre_date: str) -> str:
    """Make filename for Parquet.
    
    Arguments
    ---------
    drill_name : str
        Name of the drill.

    uuid : str
        UUID of reference dataset.
    
    centre_date : str
        Centre date of reference dataset.
    
    Returns
    -------
    str
        Parquet filename.
    """
    datestring = centre_date.strftime('%Y%m%d-%H%M%S-%f')
    return f'{drill_name}_{uuid}_{datestring}.pq'


def write_table(
        drill_name: str, uuid: str,
        centre_date: str, table: pd.DataFrame, output: str):
    """Write a table to Parquet.
    
    Arguments
    ---------
    drill_name : str
        Name of the drill.

    uuid : str
        UUID of reference dataset.
    
    centre_date : str
        Centre date of reference dataset.
    
    table : pd.DataFrame
        Dataframe with index polygons and columns bands.
    
    output : str
        Path to output directory.
    """
    # TODO(MatthewJA): Support S3 write.
    if output.startswith('s3'):
        raise NotImplementedError()
    
    filename = make_name(drill_name, uuid, centre_date)
    table.to_parquet(f'{output}/{filename}')
