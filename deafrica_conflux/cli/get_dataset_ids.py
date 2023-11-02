import logging
import os

import click
import datacube
import fsspec
from datacube.model import Range
from odc.stats.model import DateTimeRange

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.hopper import check_ds_region, find_datasets
from deafrica_conflux.io import check_dir_exists, check_if_s3_uri, find_parquet_files
from deafrica_conflux.queues import batch_messages


@click.command(
    "get-dataset-ids",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", count=True)
@click.option("--product", type=str, help="Datacube product to search datasets for.")
@click.option(
    "--temporal-range",
    type=str,
    help=(
        "Only extract datasets for a given time range," "Example '2020-05--P1M' month of May 2020"
    ),
)
@click.option(
    "--polygons-split-by-region-directory",
    type=str,
    help="Path to the directory containing the parquet files which contain polygons grouped by product region.",
)
@click.option(
    "--output-directory",
    type=str,
    help="Path to the directory to write the dataset ids text files to.",
)
def get_dataset_ids(
    verbose,
    product,
    temporal_range,
    polygons_split_by_region_directory,
    output_directory,
):
    """
    Get dataset IDs..
    """
    # Set up logger.
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Support pathlib paths.
    polygons_split_by_region_directory = str(polygons_split_by_region_directory)
    output_directory = str(output_directory)

    # Get the region codes from the polygon files.
    pq_files = find_parquet_files(path=polygons_split_by_region_directory, pattern=".*")
    region_codes = [os.path.splitext(os.path.basename(i))[0] for i in pq_files]
    _log.info(f"Found {len(region_codes)} regions.")
    _log.debug(f"Found regions: {region_codes}")

    # Connect to the datacube.
    dc = datacube.Datacube(app="FindDatasetIDs")

    # Parse the temporal range.
    temporal_range_ = DateTimeRange(temporal_range)
    # Create the query to find the datasets.
    query = {"time": Range(begin=temporal_range_.start, end=temporal_range_.end)}

    # Find the datasets using the product names and time range.
    dss = find_datasets(query=query, products=[product], dc=dc)
    dss_ = list(dss)
    _log.info(
        f"Found {len(dss_)} datasets for the product {product} in the time range {temporal_range_.start.strftime('%Y-%m-%d %X')} to {temporal_range_.end.strftime('%Y-%m-%d %X')}"
    )

    # Filter the found datasets using the region code.
    filtered_dataset_ids_ = [check_ds_region(region_codes, ds) for ds in dss_]
    filtered_dataset_ids = [item for item in filtered_dataset_ids_ if item]
    _log.info(f"Filter by region code removed {len(dss_) - len(filtered_dataset_ids)} datasets")
    _log.info(f"Dataset ids count: {len(filtered_dataset_ids)}")

    sqs_message_limit = 120000
    _log.info(
        f"Grouping dataset ids in batches of {sqs_message_limit} due to sqs in message limit."
    )
    batched_dataset_ids = batch_messages(messages=filtered_dataset_ids, n=sqs_message_limit)

    dataset_ids_directory = os.path.join(output_directory, "conflux_dataset_ids")

    # Get the file system to use.
    if check_if_s3_uri(dataset_ids_directory):
        fs = fsspec.filesystem("s3")
    else:
        fs = fsspec.filesystem("file")

    # Check if the directory exists.
    if not check_dir_exists(dataset_ids_directory):
        fs.mkdirs(dataset_ids_directory, exist_ok=True)
        _log.info(f"Created the output directory {dataset_ids_directory}")

    # Write the dataset ids into text file.
    for idx, batch in enumerate(batched_dataset_ids):
        batch_file_name = os.path.join(
            dataset_ids_directory, f"{product}_{temporal_range}_batch{idx+1}.txt"
        )
        with fs.open(batch_file_name, "w") as file:
            for dataset_id in batch:
                file.write(f"{dataset_id}\n")
        _log.info(f"Dataset IDs written to: {batch_file_name}.")
