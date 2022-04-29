"""CLI: Run a polygon drill step on a scene.

Matthew Alger, Vanessa Newey, Alex Leith
Geoscience Australia
2021
"""

import importlib.util
import json
import logging
import sys
import uuid as pyuuid
from types import ModuleType

import click
import datacube
import fsspec
import geopandas as gpd
from datacube.ui import click as ui
from rasterio.errors import RasterioIOError

import dea_conflux.__version__
import dea_conflux.db
import dea_conflux.drill
import dea_conflux.hopper
import dea_conflux.io
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
    import fiona
    from datacube.utils import geometry

    with fiona.open(shapefile_path) as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
    return crs


def check_id_field_values(shapefile_path: str, id_field) -> bool:
    """Check values of id_field are unique or not in shapefile.

    Arguments
    ---------
    shapefile_path : str
        Path to shapefile.
    use_id : str
        Unique key field in shapefile.

    Returns
    -------
    id_field values are unique or not : bool
    """
    gdf = gpd.read_file(shapefile_path)
    return len(set(gdf[id_field])) == len(gdf)


def guess_id_field(shapefile_path: str, use_id: str = "") -> str:
    """Use passed id_field to check ids are unique or not. If not pass
    id_field, guess the name of the ID field in a shapefile.

    Arguments
    ---------
    shapefile_path : str
        Path to shapefile.
    use_id : str
        Unique key field in shapefile.

    Returns
    -------
    ID field : str
    """
    import fiona

    with fiona.open(shapefile_path) as shapes:
        row = next(iter(shapes))
        keys = set(row["properties"].keys())

    # if pass use_id, let check it
    if use_id:
        # if cannot find this one from shapefile
        if use_id not in keys:
            raise ValueError(f"Couldn't find any ID field in {keys}")
        else:
            # if use_id values are not unique
            if check_id_field_values(shapefile_path, use_id):
                return use_id
            else:
                raise ValueError(f"The {use_id} values are not unique in {shapefile_path}.")
    
    # if not pass use_id, just guess it
    else:
        possible_guesses = [
            # In order of preference.
            "UID",
            "WB_ID",
            "FID_1",
            "FID",
            "ID",
            "OBJECTID",
            "ORIG_FID",
            "FeatureID",
        ]

        guess_result = []

        for guess in possible_guesses:
            if guess in keys:
                guess_result.append(guess)
        
        # if not any guess id found, let us
        # try the lower case
        if len(guess_result) == 0:
            for guess in possible_guesses:            
                guess = guess.lower()
                if guess in keys:
                    guess_result.append(guess)

        if len(guess_result) == 0:
            raise ValueError(f"Couldn't find any ID field in {keys}")
        else:
            if len(guess_result) > 1:
                logger.info(f"Possible field ids are {' '.join(guess_result)}.")
            return guess_result[0]


def load_and_reproject_shapefile(
    shapefile: str, id_field: str, crs: CRS
) -> gpd.GeoDataFrame:
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
    has_s3 = "s3" in gpd.io.file._VALID_URLS
    gpd.io.file._VALID_URLS.discard("s3")
    logger.info(f"Attempting to read {shapefile}")
    shapefile = gpd.read_file(shapefile, driver="ESRI Shapefile")
    if has_s3:
        gpd.io.file._VALID_URLS.add("s3")

    shapefile = shapefile.set_index(id_field)

    # Reproject shapefile to match target CRS
    try:
        shapefile = shapefile.to_crs(crs=crs)
    except TypeError:
        # Sometimes the crs can be a datacube utils CRS object
        # so convert to string before reprojecting
        shapefile = shapefile.to_crs(crs={"init": str(crs)})

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
    spec = importlib.util.spec_from_file_location("dea_conflux.plugin", plugin_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_plugin(plugin: ModuleType):
    """Check that a plugin declares required globals."""
    # Check globals.
    required_globals = [
        "product_name",
        "version",
        "input_products",
        "transform",
        "summarise",
        "resolution",
        "output_crs",
    ]
    for name in required_globals:
        if not hasattr(plugin, name):
            raise ValueError(f"Plugin missing {name}")

    # Check that functions are runnable.
    required_functions = ["transform", "summarise"]
    for name in required_functions:
        assert hasattr(getattr(plugin, name), "__call__")


def logging_setup(verbose: int):
    """Set up logging.

    Arguments
    ---------
    verbose : int
        Verbosity level (0, 1, 2).
    """
    loggers = [
        logging.getLogger(name)
        for name in logging.root.manager.loggerDict
        if not name.startswith("fiona")
        and not name.startswith("sqlalchemy")
        and not name.startswith("boto")
    ]
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
            raise click.ClickException("Maximum verbosity is -vv")
        logger.addHandler(stdout_hdlr)
        logger.propagate = False


@click.group()
@click.version_option(version=dea_conflux.__version__)
def main():
    """Run dea-conflux."""


@main.command()
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
    plugin, uuid, shapefile, output, partial, overedge, dump_empty_dataframe, verbose
):
    """
    Run dea-conflux on one scene.
    """
    logging_setup(verbose)

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin)
    logger.info(f"Using plugin {plugin.__file__}")
    validate_plugin(plugin)

    # Get the CRS from the shapefile if one isn't specified.
    if hasattr(plugin, "output_crs"):
        crs = plugin.output_crs
    else:
        crs = get_crs(shapefile)
    logger.debug(f"Found CRS: {crs}")

    # Get the output resolution from the plugin.
    # TODO(MatthewJA): Make this optional by guessing
    # the resolution, if at all possible.
    # I think this is doable provided that everything
    # is in native CRS.
    resolution = plugin.resolution

    # Guess the ID field.
    id_field = guess_id_field(shapefile)
    logger.debug(f"Guessed ID field: {id_field}")

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
        dc = datacube.Datacube(app="dea-conflux-drill")
        table = dea_conflux.drill.drill(
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
            dea_conflux.io.write_table(
                plugin.product_name, uuid, centre_date, table, output
            )
    except KeyError as keyerr:
        logger.error(f"Found {uuid} has KeyError: {str(keyerr)}")
    except TypeError as typeerr:
        logger.error(f"Found {uuid} has TypeError: {str(typeerr)}")
    except RasterioIOError as ioerror:
        logger.error(f"Found {uuid} has RasterioIOError: {str(ioerror)}")
    finally:
        return 0


@main.command()
@click.option(
    "--plugin",
    "-p",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to Conflux plugin (.py).",
)
@click.option("--queue", "-q", help="Queue to read IDs from.")
@click.option(
    "--shapefile",
    "-s",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the polygon " "shapefile to run polygon drill on.",
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
@click.option(
    "--timeout", default=18 * 60, help="The seconds of a received SQS msg is invisible."
)
@click.option("--db/--no-db", default=True, help="Write to the Waterbodies database.")
@click.option(
    "--dump-empty-dataframe/--not-dump-empty-dataframe",
    default=True,
    help="Not matter DataFrame is empty or not, always as it as Parquet file.",
)
def run_from_queue(
    plugin,
    queue,
    shapefile,
    output,
    partial,
    overwrite,
    overedge,
    verbose,
    timeout,
    db,
    dump_empty_dataframe,
):
    """
    Run dea-conflux on a scene from a queue.
    """
    logging_setup(verbose)
    # TODO(MatthewJA): Refactor this to combine with run-one.
    # TODO(MatthewJA): Generalise the database to not just Waterbodies.
    # Maybe this is really easy? It's all done by env vars,
    # so perhaps a documentation/naming change is all we need.

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin)
    logger.info(f"Using plugin {plugin.__file__}")
    validate_plugin(plugin)

    # Get the CRS from the shapefile if one isn't specified.
    if hasattr(plugin, "output_crs"):
        crs = plugin.output_crs
    else:
        crs = get_crs(shapefile)
    logger.debug(f"Found CRS: {crs}")

    # Get the output resolution from the plugin.
    # TODO(MatthewJA): Make this optional by guessing
    # the resolution, if at all possible.
    # I think this is doable provided that everything
    # is in native CRS.
    resolution = plugin.resolution

    # Guess the ID field.
    id_field = guess_id_field(shapefile)
    logger.debug(f"Guessed ID field: {id_field}")

    # Load and reproject the shapefile.
    shapefile = load_and_reproject_shapefile(
        shapefile,
        id_field,
        crs,
    )

    dl_queue_name = queue + "_deadletter"

    # Read ID/s from the queue.
    import boto3

    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue)
    queue_url = queue.url

    if db:
        engine = dea_conflux.db.get_engine_waterbodies()

    dc = datacube.Datacube(app="dea-conflux-drill")
    message_retries = 10
    while message_retries > 0:
        response = queue.receive_messages(
            AttributeNames=["All"],
            MaxNumberOfMessages=1,
            VisibilityTimeout=timeout,
        )

        messages = response

        if len(messages) == 0:
            logger.info("No messages received from queue")
            message_retries -= 1
            continue

        message_retries = 10

        entries = [
            {"Id": msg.message_id, "ReceiptHandle": msg.receipt_handle}
            for msg in messages
        ]

        # Process each ID.
        ids = [e.body for e in messages]
        logger.info(f"Read {ids} from queue")

        # Loop through the scenes to produce parquet files.
        for i, (entry, id_) in enumerate(zip(entries, ids)):

            success_flag = True

            logger.info(f"Processing {id_} ({i + 1}/{len(ids)})")

            centre_date = dc.index.datasets.get(id_).center_time

            if not overwrite:
                logger.info(f"Checking existence of {id_}")
                exists = dea_conflux.io.table_exists(
                    plugin.product_name, id_, centre_date, output
                )

            # NameError should be impossible thanks to short-circuiting
            if overwrite or not exists:
                try:
                    table = dea_conflux.drill.drill(
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
                        pq_filename = dea_conflux.io.write_table(
                            plugin.product_name, id_, centre_date, table, output
                        )
                        if db:
                            logger.debug(f"Writing {pq_filename} to DB")
                            dea_conflux.stack.stack_waterbodies_db(
                                paths=[pq_filename],
                                verbose=verbose,
                                engine=engine,
                                drop=False,
                            )
                except KeyError as keyerr:
                    logger.error(f"Found {id_} has KeyError: {str(keyerr)}")
                    dea_conflux.queues.move_to_deadletter_queue(dl_queue_name, id_)
                    success_flag = False
                except TypeError as typeerr:
                    logger.error(f"Found {id_} has TypeError: {str(typeerr)}")
                    dea_conflux.queues.move_to_deadletter_queue(dl_queue_name, id_)
                    success_flag = False
                except RasterioIOError as ioerror:
                    logger.error(f"Found {id_} has RasterioIOError: {str(ioerror)}")
                    dea_conflux.queues.move_to_deadletter_queue(dl_queue_name, id_)
                    success_flag = False
            else:
                logger.info(f"{id_} already exists, skipping")

            # Delete from queue.
            if success_flag:
                logger.info(f"Successful, deleting {id_}")
            else:
                logger.info(f"Not successful, moved {id_} to DLQ")

            resp = queue.delete_messages(
                QueueUrl=queue_url,
                Entries=[entry],
            )

            if len(resp["Successful"]) != 1:
                raise RuntimeError(f"Failed to delete message: {entry}")

    return 0


@main.command()
@click.argument("product", type=str)
@ui.parsed_search_expressions
@click.option("-v", "--verbose", count=True)
@click.option(
    "--shapefile",
    "-s",
    type=click.Path(),
)
@click.option("--s3/--stdout", default=False)
def get_ids(product, expressions, verbose, shapefile, s3):
    """Get IDs based on an expression."""
    logging_setup(verbose)
    dss = dea_conflux.hopper.find_datasets(expressions, [product])

    if shapefile:
        crs = get_crs(shapefile)

        # Guess the ID field.
        id_field = guess_id_field(shapefile)
        logger.debug(f"Guessed ID field: {id_field}")

        # Load and reproject the shapefile.
        shapefile = load_and_reproject_shapefile(
            shapefile,
            id_field,
            crs,
        )
        ids = dea_conflux.drill.filter_dataset(dss, shapefile)
    else:
        ids = [str(ds.id) for ds in dss]

    if not s3:
        # stdout
        for id_ in ids:
            print(id_)

        print(f"dataset size: {len(ids)}")
    else:
        out_path = (
            "s3://dea-public-data-dev/waterbodies/conflux/"
            + "conflux_ids_"
            + str(pyuuid.uuid4())
            + ".json"
        )
        with fsspec.open(out_path, "w") as f:
            f.write("\n".join(ids))
        print(json.dumps({"ids_path": out_path}), end="")

    return 0


@main.command()
@click.argument("name")
@click.option(
    "--timeout", type=int, help="Visibility timeout in seconds", default=18 * 60
)
@click.option(
    "--retention-period",
    type=int,
    help="The length of time, in seconds before retains a message.",
    default=7 * 24 * 3600,
)
@click.option("--retries", type=int, help="Number of retries", default=5)
def make(name, timeout, retries, retention_period):
    """Make a queue."""
    import boto3
    from botocore.config import Config

    dea_conflux.queues.verify_name(name)

    deadletter = name + "_deadletter"

    sqs_client = boto3.client(
        "sqs",
        config=Config(
            retries={
                "max_attempts": retries,
            }
        ),
    )

    # create deadletter queue
    dl_queue_response = sqs_client.create_queue(QueueName=deadletter)

    # Get ARN from deadletter queue name.
    dl_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dl_queue_response["QueueUrl"], AttributeNames=["All"]
    )

    # create the queue attributes form
    attributes = dict(VisibilityTimeout=str(timeout))
    attributes["RedrivePolicy"] = json.dumps(
        {
            "deadLetterTargetArn": dl_attrs["Attributes"]["QueueArn"],
            "maxReceiveCount": 10,
        }
    )

    attributes["MessageRetentionPeriod"] = str(retention_period)

    queue = sqs_client.create_queue(QueueName=name, Attributes=attributes)

    assert queue
    return 0


@main.command()
@click.argument("name")
def delete(name):
    import boto3

    dea_conflux.queues.verify_name(name)

    sqs = boto3.resource("sqs")

    queue = sqs.get_queue_by_name(QueueName=name)
    arn = queue.attributes["QueueArn"]
    queue.delete()

    deadletter = name + "_deadletter"
    dl_queue = sqs.get_queue_by_name(QueueName=deadletter)
    dl_arn = dl_queue.attributes["QueueArn"]

    # check deadletter is empty or not
    # if empty, delete it
    response = dl_queue.receive_messages(
        AttributeNames=["All"],
        MaxNumberOfMessages=1,
    )

    if len(response) == 0:
        dl_queue.delete()
        arn = ",".join([arn, dl_arn])

    return arn


@main.command()
@click.option(
    "--txt",
    type=click.Path(),
    required=True,
    help="REQUIRED. Path to TXT file to push to queue.",
)
@click.option("--queue", required=True, help="REQUIRED. Queue name to push to.")
@click.option("-v", "--verbose", count=True)
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
    logger.debug(f"Adding IDs {ids}")
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
@click.option(
    "--parquet-path",
    type=click.Path(),
    # Don't mandate existence since this might be s3://.
    help="REQUIRED. Path to the Parquet directory.",
)
@click.option(
    "--output",
    type=click.Path(),
    required=False,
    help="Output directory for waterbodies-style stack",
)
@click.option(
    "--pattern",
    required=False,
    default=".*",
    help="Regular expression for filename matching.",
)
@click.option(
    "--mode",
    type=click.Choice(["waterbodies", "waterbodies_db", "wit_tooling"]),
    default="waterbodies",
    required=False,
)
@click.option("-v", "--verbose", count=True)
@click.option(
    "--drop/--no-drop", default=False, help="Drop database if applicable. Default False"
)
def stack(parquet_path, output, pattern, mode, verbose, drop):
    """
    Stack outputs of dea-conflux into other formats.
    """
    logging_setup(verbose)

    # Convert mode to StackMode
    mode_map = {
        "waterbodies": dea_conflux.stack.StackMode.WATERBODIES,
        "waterbodies_db": dea_conflux.stack.StackMode.WATERBODIES_DB,
        "wit_tooling": dea_conflux.stack.StackMode.WITTOOLING,
    }

    kwargs = {}
    if mode == "waterbodies" or mode == "wit_tooling":
        kwargs["output_dir"] = output
    elif mode == "waterbodies_db":
        kwargs["drop"] = drop

    dea_conflux.stack.stack(
        parquet_path,
        pattern,
        mode_map[mode],
        verbose=verbose,
        **kwargs,
    )

    return 0


@main.command()
@click.option(
    "--output",
    type=click.Path(),
    required=True,
    help="Output directory for Waterbodies-style CSVs",
)
@click.option("-v", "--verbose", count=True)
@click.option(
    "--jobs",
    "-j",
    default=8,
    help="Number of workers",
)
def db_to_csv(output, verbose, jobs):
    """Output Waterbodies-style CSVs from a database."""
    logging_setup(verbose)
    dea_conflux.stack.stack_waterbodies_db_to_csv(
        out_path=output, verbose=verbose > 0, n_workers=jobs
    )


if __name__ == "__main__":
    main()
