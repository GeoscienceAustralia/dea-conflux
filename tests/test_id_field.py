import geopandas as gpd
from shapely.geometry import Point

from deafrica_conflux.id_field import guess_id_field, id_field_values_is_unique


def test_id_field_is_unique_true():
    d = {"col1": ["name1", "name2"], "geometry": [Point(1, 2), Point(2, 1)]}

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    if id_field_values_is_unique(input_gdf=gdf, id_field="col1") is True:
        assert True
    else:
        assert False


def test_id_field_is_unique_false():
    d = {"col1": ["name1", "name1"], "geometry": [Point(1, 2), Point(2, 1)]}

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    if id_field_values_is_unique(input_gdf=gdf, id_field="col1") is True:
        assert False
    else:
        assert True


def test_guess_id_field_with_no_matching_id_column():
    use_id = "UID"

    d = {"col1": ["name1", "name2"], "geometry": [Point(1, 2), Point(2, 1)]}

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    try:
        guess_id_field(input_gdf=gdf, use_id=use_id)
    except ValueError:
        assert True
    else:
        assert False


def test_guess_id_field_with_matching_id_column_and_no_unique_ids():
    use_id = "col1"

    d = {"col1": ["name1", "name1"], "geometry": [Point(1, 2), Point(2, 1)]}

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    try:
        guess_id_field(input_gdf=gdf, use_id=use_id)
    except ValueError:
        assert True
    else:
        assert False


def test_guess_id_field_with_matching_id_column_and_unique_ids():
    use_id = "col1"

    d = {"col1": ["name1", "name2"], "geometry": [Point(1, 2), Point(2, 1)]}

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    try:
        guess_id_field(input_gdf=gdf, use_id=use_id)
    except ValueError:
        assert False
    else:
        assert True


def test_guess_id_field_with_no_use_id():
    d = {"col1": ["name1", "name2"], "geometry": [Point(1, 2), Point(2, 1)]}

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    try:
        guess_id_field(input_gdf=gdf, use_id="")
    except ValueError:
        assert True
    else:
        assert False


def test_guess_id_field_with_multiple_id_cols():
    d = {
        "WB_ID": ["name1", "name2"],
        "ID": [2, 3],
        "UID": [5, 6],
        "geometry": [Point(1, 2), Point(2, 1)],
    }

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    id_field = guess_id_field(input_gdf=gdf, use_id="")

    assert id_field == "UID"


def test_guess_id_field_with_multiple_id_cols_small_case():
    d = {
        "wb_id": ["name1", "name2"],
        "ID": [2, 3],
        "uid": [5, 6],
        "geometry": [Point(1, 2), Point(2, 1)],
    }

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    id_field = guess_id_field(input_gdf=gdf, use_id="")

    assert id_field == "ID"


def test_guess_id_field_with_multiple_id_cols_small_case_2():
    d = {
        "the": ["name1", "name2"],
        "test": [2, 3],
        "uid": [5, 6],
        "geometry": [Point(1, 2), Point(2, 1)],
    }

    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    id_field = guess_id_field(input_gdf=gdf, use_id="")

    assert id_field == "uid"
