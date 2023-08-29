import click
import boto3
import datacube
import logging
from rasterio.errors import RasterioIOError

from .common import main, logging_setup, get_crs, guess_id_field, load_and_reproject_shapefile
from ..plugins.utils import run_plugin, validate_plugin

import deafrica_conflux.db
import deafrica_conflux.io
import deafrica_conflux.stack
import deafrica_conflux.drill
import deafrica_conflux.queues


@main.command("run-from-txt", no_args_is_help=True)
@click.option(
    "--plugin",
    "-p",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to Conflux plugin (.py).",
)
@click.option("--txt", help="Text file or json file to read IDs from.")
@click.option(
    "--shapefile",
    "-s",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the polygon " "shapefile to run polygon drill on.",
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in shapefile.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the output directory.",
)
@click.option(
    "--partial/--no-partial",
    default=True,
    help="Include polygons that only partially intersect the scene.",
)
@click.option(
    "--overedge/--no-overedge",
    default=True,
    help="Include data from over the scene boundary.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Rerun scenes that have already been processed.",
)
@click.option("-v", "--verbose", count=True)
@click.option("--db/--no-db", default=True, help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_txt(
    plugin,
    txt,
    shapefile,
    use_id,
    output,
    partial,
    overwrite,
    overedge,
    verbose,
    db,
    dump_empty_dataframe,
):
    """
    Run deafrica-conflux on a scene from a text file.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # TODO(MatthewJA): Refactor this to combine with run-one.
    # TODO(MatthewJA): Generalise the database to not just Waterbodies.
    # Maybe this is really easy? It's all done by env vars,
    # so perhaps a documentation/naming change is all we need.

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin)
    _log.info(f"Using plugin {plugin.__file__}")
    validate_plugin(plugin)

    # Get the CRS from the shapefile if one isn't specified.
    if hasattr(plugin, "output_crs"):
        crs = plugin.output_crs
    else:
        crs = get_crs(shapefile)
    _log.debug(f"Found CRS: {crs}")

    # Get the output resolution from the plugin.
    # TODO(MatthewJA): Make this optional by guessing
    # the resolution, if at all possible.
    # I think this is doable provided that everything
    # is in native CRS.
    resolution = plugin.resolution

    # Guess the ID field.
    id_field = guess_id_field(shapefile, use_id)
    _log.debug(f"Guessed ID field: {id_field}")

    # Load and reproject the shapefile.
    shapefile = load_and_reproject_shapefile(
        shapefile,
        id_field,
        crs,
    )

    # Read ID/s from the queue.
    with open(txt) as file:
        ids = [line.strip() for line in file]

    _log.info(f"Read {ids} from file.")

    if db:
        engine = deafrica_conflux.db.get_engine_waterbodies()

    dc = datacube.Datacube(app="deafrica-conflux-drill")
    
    # Process each ID.
    # Loop through the scenes to produce parquet files.
    for i, id_ in enumerate(ids):
        success_flag = True

        _log.info(f"Processing {id_} ({i + 1}/{len(ids)})")

        centre_date = dc.index.datasets.get(id_).center_time

        if not overwrite:
            _log.info(f"Checking existence of {id_}")
            exists = deafrica_conflux.io.table_exists(
                plugin.product_name, id_, centre_date, output
            )

        # NameError should be impossible thanks to short-circuiting
        if overwrite or not exists:
            try:
                table = deafrica_conflux.drill.drill(
                    plugin,
                    shapefile,
                    id_,
                    crs,
                    resolution,
                    partial=partial,
                    overedge=overedge,
                    dc=dc,
                )

                # if always dump drill result, or drill result is not empty,
                # dump that dataframe as PQ file
                if (dump_empty_dataframe) or (not table.empty):
                    pq_filename = deafrica_conflux.io.write_table(
                        plugin.product_name, id_, centre_date, table, output
                    )
                    if db:
                        _log.debug(f"Writing {pq_filename} to DB")
                        deafrica_conflux.stack.stack_waterbodies_db(
                            paths=[pq_filename],
                            verbose=verbose,
                            engine=engine,
                            drop=False,
                        )
            except KeyError as keyerr:
                _log.error(f"Found {id_} has KeyError: {str(keyerr)}")
                success_flag = False
            except TypeError as typeerr:
                _log.error(f"Found {id_} has TypeError: {str(typeerr)}")
                success_flag = False
            except RasterioIOError as ioerror:
                _log.error(f"Found {id_} has RasterioIOError: {str(ioerror)}")
                success_flag = False
        else:
            _log.info(f"{id_} already exists, skipping")

        if success_flag:
            _log.info(f"{id_} Successful")
        else:
            _log.info(f"{id_} Not successful")

    return 0