import os
import sys
import click
import logging
import fsspec
import json
import uuid as pyuuid
from datacube.ui import click as ui

from ._cli_common import main, command_required_option_from_option, logging_setup
from ._vector_file_utils import get_crs, guess_id_field, load_and_reproject_shapefile

import deafrica_conflux.hopper

required_options = {
    True: 'bucket_name',
    False: 'output_folder',
}


@main.command("get-ids", no_args_is_help=True, cls=command_required_option_from_option('s3', required_options))
@click.argument("product", type=str)
@ui.parsed_search_expressions
@click.option("-v", "--verbose", count=True)
@click.option(
    "--shapefile",
    "-s",
    type=click.Path(),
    help="Path to the polygon " "shapefile to run polygon drill on to filter datasets.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in shapefile.",
)
@click.option("--s3/--local", default=False)
@click.option(
    "--bucket-name",
    type=str,
    help="The default s3 bucket to save the get_ids result.",
    default="deafrica-waterbodies-dev",
    show_default=True,
    required=False,
    is_eager=True
)
@click.option(
    "--output-folder",
    type=str,
    help="The default folder to save the get_ids result. Default is the current directory.",
    default="",
    show_default=True,
    required=False,
    is_eager=True
)
@click.option(
    "--num-worker",
    type=int,
    help="The number of processes to filter datasets.",
    default=4,
)
def get_ids(product,
            expressions,
            verbose,
            shapefile,
            use_id,
            s3,
            bucket_name,
            output_folder,
            num_worker):
    
    """
    Get IDs based on an expression.
    """

    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dss = deafrica_conflux.hopper.find_datasets(expressions, [product])

    if shapefile:
        crs = get_crs(shapefile)

        # Guess the ID field.
        id_field = guess_id_field(shapefile, use_id)
        _log.debug(f"Guessed ID field: {id_field}")

        # Load and reproject the shapefile.
        shapefile = load_and_reproject_shapefile(
            shapefile,
            id_field,
            crs,
        )
        _log.info(f"shapefile RAM usage: {sys.getsizeof(shapefile)}.")

        ids = deafrica_conflux.drill.filter_dataset(dss, shapefile, worker_num=num_worker)
    else:
        ids = [str(ds.id) for ds in dss]

    if not s3:
        output_directory_suffix = "timeseries/conflux/"
        output_directory = os.path.join(os.path.abspath(output_folder), output_directory_suffix)
        
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
    
        out_path = (
            f"{output_directory}"
            + "conflux_ids_"
            + str(pyuuid.uuid4())
            + ".json"
        )
    else:
        out_path = (
            f"s3://{bucket_name}/timeseries/conflux/"
            + "conflux_ids_"
            + str(pyuuid.uuid4())
            + ".json"
        )
        
    _log.info(f"Writing IDs to: {out_path}.")
    
    with fsspec.open(out_path, "w") as f:
        f.write("\n".join(ids))
    
    print(json.dumps({"ids_path": out_path}), end="")

    return 0
