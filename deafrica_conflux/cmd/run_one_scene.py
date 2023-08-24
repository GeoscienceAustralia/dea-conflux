import click
import datacube
import logging
from rasterio.errors import RasterioIOError

from .common import main, logging_setup, get_crs, guess_id_field, load_and_reproject_shapefile
from ..plugins.utils import run_plugin, validate_plugin

import deafrica_conflux.drill
import deafrica_conflux.io


@main.command("run-one-scene", no_args_is_help=True)
@click.option(
    "--plugin",
    "-p",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to Conflux plugin (.py).",
)
@click.option("--uuid", "-i", type=str, help="ID of scene to process.")
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
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
@click.option("-v", "--verbose", count=True)
def run_one(
    plugin,
    uuid,
    shapefile,
    use_id,
    output,
    partial,
    overedge,
    dump_empty_dataframe,
    verbose,
):
    """
    Run deafrica-conflux on one scene.
    """
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

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
    resolution = plugin.redea_conflux.iosolution

    # Guess the ID field.
    id_field = guess_id_field(shapefile, use_id)
    _log.debug(f"Guessed ID field: {id_field}")

    # Load and reproject the shapefile.
    shapefile = load_and_reproject_shapefile(
        shapefile,
        id_field,
        crs,
    )

    # add try catch to catpure exception:
    # KeyError: missing water key in WIT, should be gone after filter by gqa_mean_x in [-1, 1]
    # TypeError: ufunc 'bitwise_and' not supported for the input types in WIT, no idea on root reason
    # rasterio.errors.RasterioIOError: cannot load tif file from S3

    try:
        # Do the drill!
        dc = datacube.Datacube(app="deafrica-conflux-drill")
        table = deafrica_conflux.drill.drill(
            plugin,
            shapefile,
            uuid,
            crs,
            resolution,
            partial=partial,
            overedge=overedge,
            dc=dc,
        )

        # if always dump drill result, or drill result is not empty,
        # dump that dataframe as PQ file
        if (dump_empty_dataframe) or (not table.empty):
            centre_date = dc.index.datasets.get(uuid).center_time
            deafrica_conflux.io.write_table(
                plugin.product_name, uuid, centre_date, table, output
            )
    except KeyError as keyerr:
        _log.error(f"Found {uuid} has KeyError: {str(keyerr)}")
    except TypeError as typeerr:
        _log.error(f"Found {uuid} has TypeError: {str(typeerr)}")
    except RasterioIOError as ioerror:
        _log.error(f"Found {uuid} has RasterioIOError: {str(ioerror)}")
    finally:
        return 0
