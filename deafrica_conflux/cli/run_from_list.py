import click
import datacube
import logging
import geopandas as gpd
from rasterio.errors import RasterioIOError

from deafrica_conflux.cli.logs import logging_setup
from deafrica_conflux.plugins.utils import run_plugin, validate_plugin
from deafrica_conflux.id_field import guess_id_field

import deafrica_conflux.db
import deafrica_conflux.io
import deafrica_conflux.stack
import deafrica_conflux.drill


@click.command("run-from-list",
               no_args_is_help=True,
               help="Run deafrica-conflux on a list of dataset ids passed as a string.")
@click.option(
    "--plugin-file",
    "-p",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to Conflux plugin (.py).",
)
@click.option(
    "-dataset-ids-list",
    type=str,
    help="A list of dataset IDs to run deafrica-conflux on.")
@click.option(
    "--polygons-vector-file",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="Path to the vector file defining the polygon(s) to run polygon drill on."
)
@click.option(
    "--use-id",
    "-u",
    type=str,
    default=None,
    help="Optional. Unique key id in polygons vector file.",
)
@click.option(
    "--output-directory",
    "-o",
    type=click.Path(),
    default=None,
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. File URI or S3 URI to the output directory.",
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
@click.option("--db/--no-db",
              default=True,
              help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_list(
    plugin_file,
    dataset_ids_list,
    polygons_vector_file,
    use_id,
    output_directory,
    partial,
    overwrite,
    overedge,
    verbose,
    db,
    dump_empty_dataframe,
):
    """
    Run deafrica-conflux on a list of dataset ids passed as a string.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin_file)
    _log.info(f"Using plugin {plugin.__file__}")
    validate_plugin(plugin)

    # Read the vector file.
    try:
        polygons_gdf = gpd.read_file(polygons_vector_file)
    except Exception as error:
        _log.error(error)
        raise
    
    # Guess the ID field.
    id_field = guess_id_field(polygons_gdf, use_id)
    _log.debug(f"Guessed ID field: {id_field}")

    # Set the ID field as the index.
    polygons_gdf.set_index(id_field)

    # Get the CRS.
    if hasattr(plugin, "output_crs"):
        crs = plugin.output_crs
    else:
        # If a CRS is not specified use the crs "EPSG:6933"
        crs = "EPSG:6933"

    _log.debug(f"Found CRS: {crs}")
    # Reproject the polygons to the required CRS.
    polygons_gdf = polygons_gdf.to_crs(crs)

    # Get the output resolution from the plugin.
    # TODO(MatthewJA): Make this optional by guessing
    # the resolution, if at all possible.
    # I think this is doable provided that everything
    # is in native CRS.
    resolution = plugin.resolution

    # Read dataset ids.
    dataset_ids = dataset_ids_list.split(" ")
    _log.info(f"Read {dataset_ids} from list.")
  
    if db:
        engine = deafrica_conflux.db.get_engine_waterbodies()

    dc = datacube.Datacube(app="deafrica-conflux-drill")
    
    # Process each ID.
    # Loop through the scenes to produce parquet files.
    for i, id_ in enumerate(dataset_ids):
        success_flag = True

        _log.info(f"Processing {id_} ({i + 1}/{len(dataset_ids)})")

        centre_date = dc.index.datasets.get(id_).center_time

        if not overwrite:
            _log.info(f"Checking existence of {id_}")
            exists = deafrica_conflux.io.table_exists(
                plugin.product_name, id_, centre_date, output_directory
            )

        # NameError should be impossible thanks to short-circuiting
        if overwrite or not exists:
            try:
                table = deafrica_conflux.drill.drill(
                    plugin,
                    polygons_gdf,
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
                        plugin.product_name, id_, centre_date, table, output_directory
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
