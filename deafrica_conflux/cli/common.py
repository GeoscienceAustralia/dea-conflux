import os
import fiona
import sys
import click
import logging
import geopandas as gpd
from datacube.utils import geometry

from deafrica_conflux.types import CRS
import deafrica_conflux.__version__

_log = logging.getLogger(__name__)


def command_required_option_from_option(require_name, require_map):

    class CommandOptionRequiredClass(click.Command):

        def invoke(self, ctx):
            require = ctx.params[require_name]
            if require not in require_map:
                raise click.ClickException(
                    "Unexpected value for --'{}': {}".format(
                        require_name, require))
            if ctx.params[require_map[require].lower()] is None:
                raise click.ClickException(
                    "With {}={} must specify option --{}".format(
                        require_name, require, require_map[require]))
            super(CommandOptionRequiredClass, self).invoke(ctx)

    return CommandOptionRequiredClass


def get_file_driver(shapefile_path):
    """
    Get the appropriate fiona driver for a
    shapefile or geojson file.
    
    Arguments
    ---------
    shapefile_path : str
        Path to shapefile.
    Returns
    -------
    driver: str
        File Driver
    """
    
    file_extension = os.path.splitext(shapefile_path)[-1]

    if file_extension.lower() == ".geojson":
        file_driver = "GeoJSON"
    elif file_extension.lower() == ".shp":
        file_driver = "ESRI Shapefile"

    return file_driver


def get_crs(shapefile_path: str) -> CRS:
    """
    Get the CRS of a shapefile.

    Arguments
    ---------
    shapefile_path : str
        Path to shapefile.

    Returns
    -------
    CRS
    """
    with fiona.open(shapefile_path) as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
    return crs


def id_field_values_is_unique(shapefile_path: str, id_field) -> bool:
    """
    Check values of id_field are unique or not in shapefile.

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
    has_s3 = "s3" in gpd.io.file._VALID_URLS
    gpd.io.file._VALID_URLS.discard("s3")

    _log.info(f"Attempting to read {shapefile_path} to check id field.")
    gdf = gpd.read_file(shapefile_path, driver=get_file_driver(shapefile_path))
    
    if has_s3:
        gpd.io.file._VALID_URLS.add("s3")
        
    return len(set(gdf[id_field])) == len(gdf)


def guess_id_field(shapefile_path: str, use_id: str = "") -> str:
    """
    Use passed id_field to check ids are unique or not. If not pass
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
            if id_field_values_is_unique(shapefile_path, use_id):
                return use_id
            else:
                raise ValueError(
                    f"The {use_id} values are not unique in {shapefile_path}."
                )

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
                _log.info(f"Possible field ids are {' '.join(guess_result)}.")
            if id_field_values_is_unique(shapefile_path, guess_result[0]):
                return guess_result[0]
            else:
                raise ValueError(
                    f"The {use_id} values are not unique in {shapefile_path}."
                )


def load_and_reproject_shapefile(
    shapefile: str, id_field: str, crs: CRS
) -> gpd.GeoDataFrame:
    """
    Load a shapefile, project into CRS, and set index.

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

    _log.info(f"Attempting to read {shapefile} to load polgyons.")
    shapefile = gpd.read_file(shapefile, driver=get_file_driver(shapefile))
    if has_s3:
        gpd.io.file._VALID_URLS.add("s3")

    shapefile = shapefile.set_index(id_field)

    # Reproject shapefile to match target CRS
    try:
        shapefile = shapefile.to_crs(crs=crs)
    except TypeError:
        # Sometimes the crs can be a datacube utils CRS object
        # so convert to string before reprojecting
        shapefile = shapefile.to_crs(crs=str(crs))

    # zero-buffer to fix some oddities.
    shapefile.geometry = shapefile.geometry.buffer(0)
    return shapefile


@click.version_option(package_name="deafrica_conflux", version=deafrica_conflux.__version__)
@click.group(help="Run deafrica-conflux.")
def main():
    pass
