# TODO: Add tests for the command line tools.
from click.testing import CliRunner

from deafrica_conflux.cli.get_dataset_ids import get_dataset_ids


def test_get_dataset_ids_cli():
    runner = CliRunner(echo_stdin=True)
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-15]"
    polygons_vector_file = "data/edumesbb2.geojson"
    use_id = "UID"
    output_file_path = "data/edumesbb2_conflux_ids.txt"
    num_worker = 8
    cmd = f"{product} {expressions} --verbose --polygons-vector-file={polygons_vector_file} --use-id={use_id} --output-file-path={output_file_path} --num-worker={num_worker}"
    result = runner.invoke(get_dataset_ids, cmd)
    assert result.exit_code == 0

    with open(output_file_path, "r") as f:
        dataset_ids = f.readlines()
        dataset_ids = [idx.strip() for idx in dataset_ids]

    expected_dataset_ids = [
        "424bae19-35ac-5174-a40a-743450f86727",
        "679bd9b7-f88b-5996-bb39-43a87cbe916a",
        "3a736326-bb16-5112-a38e-8861b448cc8c",
        "d15407ff-3fe5-55ec-a713-d4cc9399e6b3",
        "c2ca91b7-1f55-5662-9e5d-3b1396af10f1",
    ]

    assert dataset_ids == expected_dataset_ids


def test_get_dataset_ids_with_existing_ids_file():
    runner = CliRunner(echo_stdin=True)
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-30]"
    polygons_vector_file = "data/edumesbb2.geojson"
    use_id = "UID"
    output_file_path = "data/edumesbb2_conflux_ids.txt"
    num_worker = 8
    cmd = f"{product} {expressions} --verbose --polygons-vector-file={polygons_vector_file} --use-id={use_id} --output-file-path={output_file_path} --num-worker={num_worker}"
    result = runner.invoke(get_dataset_ids, cmd)

    assert type(result.exception) is FileExistsError
