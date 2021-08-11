"""Run a polygon drill step on a scene.

2021
Matthew Alger, Vanessa Newey
Geoscience Australia
"""

import importlib.util
import logging
import sys

import click

import dea_conflux.__version__

logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def get_crs(shapefile_path: str) -> "CRS":
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
    ]
    possible_guesses += [k.lower() for k in possible_guesses]
    for guess in possible_guesses:
        if guess in keys:
            return guess
    raise ValueError(
        'Couldn\'t find an ID field in {}'.format(keys))


def run_plugin(plugin_path: str) -> 'module':
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


def validate_plugin(plugin):
    """Check that a plugin declares required globals."""
    # Verify that the plugin has been imported.
    import dea_conflux.plugin
    if dea_conflux.plugin is not plugin:
        raise RuntimeError('Plugin not loaded correctly')

    # Check globals.
    required_globals = [
        'product_name', 'version', 'input_products',
        'transform', 'summarise']
    for name in required_globals:
        if not hasattr(plugin, name):
            raise ValueError(f'Plugin missing {name}')
    
    # Check that functions are runnable.
    required_functions = ['transform', 'summarise']
    for name in required_functions:
        assert hasattr(getattr(plugin, name), '__call__')



@click.group()
@click.version_option(version=dea_conflux.__version__)
def main():
    """Run dea-conflux."""
    pass


@main.command()
@click.option('--plugin', '-p',
              type=click.Path(exists=True, dir_okay=False),
              help='Path to Conflux plugin (.py).')
@click.option('--shapefile', '-s', type=click.Path(),
              # Don't mandate existence since this might be s3://.
              help='REQUIRED. Path to the polygon '
              'shapefile to run polygon drill on.')
@click.option('--output', '-o', type=click.Path(), default=None,
              # Don't mandate existence since this might be s3://.
              help='REQUIRED. Path to the output directory.')
@click.option('-v', '--verbose', count=True)
def run_one(plugin, shapefile, output, verbose):
    """
    Run dea-conflux on one scene.
    """
    # Set up logging.
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

    # Read the plugin as a Python module.
    plugin = run_plugin(plugin)
    logger.info(f'Using plugin {plugin.__file__}')
    import dea_conflux.plugin
    assert plugin == dea_conflux.plugin

    # Get the CRS from the shapefile.
    crs = get_crs(shapefile)
    logger.debug(f'Found CRS: {crs}')

    # Guess the ID field.
    id_field = guess_id_field(shapefile)
    logger.debug(f'Guessed ID field: {id_field}')

    return 0


if __name__ == "__main__":
    main()
