"""CLI: Run a polygon drill step on a scene.

Matthew Alger, Vanessa Newey, Alex Leith
Geoscience Australia
2021
"""

import importlib.util
import json
import logging
import sys
from types import ModuleType
import uuid as pyuuid

import click
import datacube
from datacube.ui import click as ui
import fsspec
import geopandas as gpd

import dea_conflux.__version__
import dea_conflux.drill
import dea_conflux.io
import dea_conflux.hopper
import dea_conflux.queues
import dea_conflux.stack
from dea_conflux.types import CRS

logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def get_crs(shapefile_path: str) -> CRS:
    """Get the CRS of a shapefile.

    Arguments
    ---------
    shapefile_path : str
        Path to shapefile.
    
    Returns
    -------
    CRS
    """
    from datacube.utils import geometry
    import fiona
    with fiona.open(shapefile_path) as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
    return crs


def guess_id_field(shapefile_path: str) -> str:
    """Guess the name of the ID field in a shapefile.

    Arguments
    ---------
    shapefile_path : str
        Path to shapefile.
    
    Returns
    -------
    ID field : str
    """
    import fiona
    with fiona.open(shapefile_path) as shapes:
        row = next(iter(shapes))
        keys = set(row['properties'].keys())
    possible_guesses = [
        # In order of preference.
        'UID', 'WB_ID', 'FID_1', 'FID', 'ID', 'OBJECTID',
        'ORIG_FID',
    ]
    for guess in possible_guesses:
        if guess in keys:
            return guess
        guess = guess.lower()
        if guess in keys:
            return guess
    raise ValueError(
        'Couldn\'t find an ID field in {}'.format(keys))


def load_and_reproject_shapefile(
        shapefile: str,
        id_field: str,
        crs: CRS) -> gpd.GeoDataFrame:
    """Load a shapefile, project into CRS, and set index.
    
    Arguments
    ---------
    shapefile : str
        Path to shapefile.
    
    id_field : str
        Name of ID field. This will become the index.
    
    crs : CRS
        CRS to reproject into.
        
    Returns
    -------
    GeoDataFrame
    """
    # awful little hack to get around a datacube bug...
    has_s3 = 's3' in gpd.io.file._VALID_URLS
    gpd.io.file._VALID_URLS.discard('s3')
    logger.info(f'Attempting to read {shapefile}')
    shapefile = gpd.read_file(shapefile, driver='ESRI Shapefile')
    if has_s3:
        gpd.io.file._VALID_URLS.add('s3')

    shapefile = shapefile.set_index(id_field)

    # Reproject shapefile to match target CRS
    try:
        shapefile = shapefile.to_crs(crs=crs)
    except TypeError:
        # Sometimes the crs can be a datacube utils CRS object
        # so convert to string before reprojecting
        shapefile = shapefile.to_crs(crs={'init': str(crs)})
    
    # zero-buffer to fix some oddities.
    shapefile.geometry = shapefile.geometry.buffer(0)
    return shapefile


def run_plugin(plugin_path: str) -> ModuleType:
    """Run a Python plugin from a path.

    Arguments
    ---------
    plugin_path : str
        Path to Python plugin file.
    
    Returns
    -------
    module
    """
    spec = importlib.util.spec_from_file_location(
        "dea_conflux.plugin", plugin_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_plugin(plugin: ModuleType):
    """Check that a plugin declares required globals."""
    # Check globals.
    required_globals = [
        'product_name', 'version', 'input_products',
        'transform', 'summarise', 'resolution',
        'output_crs']
    for name in required_globals:
        if not hasattr(plugin, name):
            raise ValueError(f'Plugin missing {name}')

    # Check that functions are runnable.
    required_functions = ['transform', 'summarise']
    for name in required_functions:
        assert hasattr(getattr(plugin, name), '__call__')


def logging_setup(verbose: int):
    """Set up logging.
    
    Arguments
    ---------
    verbose : int
        Verbosity level (0, 1, 2).
    """
    loggers = [logging.getLogger(name)
               for name in logging.root.manager.loggerDict
               if not name.startswith('fiona')
               and not name.startswith('sqlalchemy')
               and not name.startswith('boto')]
    # For compatibility with docker+pytest+click stack...
    stdout_hdlr = logging.StreamHandler(sys.stdout)
    for logger in loggers:
        if verbose == 0:
            logging.basicConfig(level=logging.WARNING)
        elif verbose == 1:
            logging.basicConfig(level=logging.INFO)
        elif verbose == 2:
            logging.basicConfig(level=logging.DEBUG)
        else:
            raise click.ClickException('Maximum verbosity is -vv')
        logger.addHandler(stdout_hdlr)


@click.group()
@click.version_option(version=dea_conflux.__version__)
def main():
    """Run dea-conflux."""
    pass


@main.command()
@click.option('--plugin', '-p',
              type=click.Path(exists=True, dir_okay=False),
              help='Path to Conflux plugin (.py).')
@click.option('--uuid', '-i',
              type=str,
              help='ID of scene to process.')
@click.option('--shapefile', '-s', type=click.Path(),
              # Don't mandate existence since this might be s3://.
              help='REQUIRED. Path to the polygon '
              'shapefile to run polygon drill on.')
@click.option('--output', '-o', type=click.Path(), default=None,
              # Don't mandate existence since this might be s3://.
              help='REQUIRED. Path to the output directory.')
@click.option('--partial/--no-partial', default=True,
              help='Include polygons that only partially intersect the scene.')
@click.option('--overedge/--no-overedge', default=True,
              help='Include data from over the scene boundary.')
@click.option('-v', '--verbose', count=True)
def run_one(plugin, uuid, shapefile, output, partial, overedge, verbose):
    """
    Run dea-conflux on one scene.
    """
    logging_setup(verbose)

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin)
    logger.info(f'Using plugin {plugin.__file__}')
    validate_plugin(plugin)

    # Get the CRS from the shapefile if one isn't specified.
    if hasattr(plugin, 'output_crs'):
        crs = plugin.output_crs
    else:
        crs = get_crs(shapefile)
    logger.debug(f'Found CRS: {crs}')

    # Get the output resolution from the plugin.
    # TODO(MatthewJA): Make this optional by guessing
    # the resolution, if at all possible.
    # I think this is doable provided that everything
    # is in native CRS.
    resolution = plugin.resolution

    # Guess the ID field.
    id_field = guess_id_field(shapefile)
    logger.debug(f'Guessed ID field: {id_field}')

    # Load and reproject the shapefile.
    shapefile = load_and_reproject_shapefile(
        shapefile, id_field, crs,
    )

    # Do the drill!
    dc = datacube.Datacube(app='dea-conflux-drill')
    table = dea_conflux.drill.drill(
        plugin, shapefile, uuid, crs, resolution,
        partial=partial, overedge=overedge, dc=dc)
    centre_date = dc.index.datasets.get(uuid).center_time
    dea_conflux.io.write_table(
        plugin.product_name, uuid,
        centre_date, table, output)

    return 0


@main.command()
@click.option('--plugin', '-p',
              type=click.Path(exists=True, dir_okay=False),
              help='Path to Conflux plugin (.py).')
@click.option('--queue', '-q',
              help='Queue to read IDs from.')
@click.option('--shapefile', '-s', type=click.Path(),
              # Don't mandate existence since this might be s3://.
              help='REQUIRED. Path to the polygon '
              'shapefile to run polygon drill on.')
@click.option('--output', '-o', type=click.Path(), default=None,
              # Don't mandate existence since this might be s3://.
              help='REQUIRED. Path to the output directory.')
@click.option('--partial/--no-partial', default=True,
              help='Include polygons that only partially intersect the scene.')
@click.option('--overedge/--no-overedge', default=True,
              help='Include data from over the scene boundary.')
@click.option('--overwrite/--no-overwrite', default=False,
              help='Rerun scenes that have already been processed.')
@click.option('-v', '--verbose', count=True)
@click.option('--timeout', default=15 * 60,
              help='The duration (in seconds) that the received SQS messages are hidden.')
def run_from_queue(plugin, queue, shapefile, output, partial,
                   overwrite, overedge, verbose, timeout):
    """
    Run dea-conflux on a scene from a queue.
    """
    logging_setup(verbose)
    # TODO(MatthewJA): Refactor this to combine with run-one.

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin)
    logger.info(f'Using plugin {plugin.__file__}')
    validate_plugin(plugin)

    # Get the CRS from the shapefile if one isn't specified.
    if hasattr(plugin, 'output_crs'):
        crs = plugin.output_crs
    else:
        crs = get_crs(shapefile)
    logger.debug(f'Found CRS: {crs}')

    # Get the output resolution from the plugin.
    # TODO(MatthewJA): Make this optional by guessing
    # the resolution, if at all possible.
    # I think this is doable provided that everything
    # is in native CRS.
    resolution = plugin.resolution

    # Guess the ID field.
    id_field = guess_id_field(shapefile)
    logger.debug(f'Guessed ID field: {id_field}')

    # Load and reproject the shapefile.
    shapefile = load_and_reproject_shapefile(
        shapefile, id_field, crs,
    )

    # Read ID/s from the queue.
    import boto3

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue)
    queue_url = queue.url

    dc = datacube.Datacube(app='dea-conflux-drill')
    message_retries = 10
    while message_retries > 0:
        response = queue.receive_messages(
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=timeout,
        )

        messages = response

        if len(messages) == 0:
            logger.info('No messages received from queue')
            message_retries -= 1
            continue

        message_retries = 10

        entries = [
            {'Id': msg.message_id,
             'ReceiptHandle': msg.receipt_handle}
            for msg in messages
        ]

        # Process each ID.
        ids = [e.body for e in messages]
        logger.info(f'Read {ids} from queue')

        # Loop through the scenes to produce parquet files.
        for i, (entry, id_) in enumerate(zip(entries, ids)):
            logger.info('Processing {} ({}/{})'.format(
                id_,
                i + 1,
                len(ids)))
            table = dea_conflux.drill.drill(
                plugin, shapefile, id_, crs, resolution,
                partial=partial, overedge=overedge, dc=dc)
            centre_date = dc.index.datasets.get(id_).center_time

            exists = dea_conflux.io.table_exists(
                plugin.product_name, id_,
                centre_date, output)
            
            if overwrite or not exists:
                dea_conflux.io.write_table(
                    plugin.product_name, id_,
                    centre_date, table, output)
            else:
                logger.info(f'{id_} already exists, skipping')

            # Delete from queue.
            logger.info(f'Successful, deleting {id_}')
            resp = queue.delete_messages(
                QueueUrl=queue_url, Entries=[entry],
            )

            if len(resp['Successful']) != 1:
                raise RuntimeError(
                    f"Failed to delete message: {entry}"
                )

    return 0


@main.command()
@click.argument('product', type=str)
@ui.parsed_search_expressions
@click.option('-v', '--verbose', count=True)
@click.option('--s3/--stdout', default=False)
def get_ids(product, expressions, verbose, s3):
    """Get IDs based on an expression."""
    logging_setup(verbose)
    dss = dea_conflux.hopper.find_datasets(
        expressions,
        [product])
    ids = [str(ds.id) for ds in dss]
    if not s3:
        # stdout
        for id_ in ids:
            print(id_)
    else:
        out_path = 's3://dea-public-data-dev/waterbodies/conflux/' + \
            'conflux_ids_' + str(pyuuid.uuid4()) + '.json'
        with fsspec.open(out_path, 'w') as f:
            f.write('\n'.join(ids))
        print(json.dumps({'ids_path': out_path}), end='')

    return 0


@main.command()
@click.option('--txt', type=click.Path(), required=True,
              help='REQUIRED. Path to TXT file to push to queue.')
@click.option('--queue', required=True,
              help='REQUIRED. Queue name to push to.')
@click.option('-v', '--verbose', count=True)
def push_to_queue(txt, queue, verbose):
    """
    Push lines of a text file to a SQS queue.
    """
    # Cribbed from datacube-alchemist
    logging_setup(verbose)
    alive_queue = dea_conflux.queues.get_queue(queue)

    def post_messages(messages, count):
        alive_queue.send_messages(Entries=messages)
        logger.info(f"Added {count} messages...")
        return []

    count = 0
    messages = []
    logger.info("Adding messages...")
    with open(txt) as file:
        ids = [line.strip() for line in file]
    logger.debug(f'Adding IDs {ids}')
    for id_ in ids:
        message = {
            "Id": str(count),
            "MessageBody": str(id_),
        }
        messages.append(message)

        count += 1
        if count % 10 == 0:
            messages = post_messages(messages, count)

    # Post the last messages if there are any
    if len(messages) > 0:
        post_messages(messages, count)


@main.command()
@click.option('--parquet-path', type=click.Path(),
              # Don't mandate existence since this might be s3://.
              help='REQUIRED. Path to the Parquet directory.')
@click.option('--output', type=click.Path())
@click.option('--pattern', required=False, default='.*',
              help='Regular expression for filename matching.')
@click.option('--mode',
              type=click.Choice(['waterbodies']),
              default='waterbodies',
              required=False)
@click.option('-v', '--verbose', count=True)
def stack(parquet_path, output, pattern, mode, verbose):
    """
    Stack outputs of dea-conflux into other formats.
    """
    logging_setup(verbose)

    # Convert mode to StackMode
    mode_map = {
        'waterbodies': dea_conflux.stack.StackMode.WATERBODIES,
    }

    dea_conflux.stack.stack(parquet_path, output, pattern, mode_map[mode],
                            verbose=verbose)

    return 0


if __name__ == "__main__":
    main()
