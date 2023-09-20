# TODO: Add tests for the command line tools.
from pathlib import Path
from click.testing import CliRunner

from deafrica_conflux.cli.get_dataset_ids import get_dataset_ids


# Test directory.
HERE = Path(__file__).parent.resolve()
TEST_WATERBODY = HERE / "data" / "edumesbb2.geojson"
TEST_CONFLUX_IDS_TXT = HERE / "data" / "edumesbb2_conflux_ids.txt"


def test_get_dataset_ids_cli():
    runner = CliRunner(echo_stdin=True)
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-15]"
    polygons_vector_file = TEST_WATERBODY
    use_id = "UID"
    output_file_path = TEST_CONFLUX_IDS_TXT
    num_worker = 8
    cmd = f"{product} {expressions} --verbose --polygons-vector-file={polygons_vector_file} --use-id={use_id} --output-file-path={output_file_path} --num-worker={num_worker}"
    result = runner.invoke(get_dataset_ids, cmd)
    assert result.exit_code == 0

    with open(output_file_path, "r") as f:
        dataset_ids = f.readlines()
        dataset_ids = [idx.strip() for idx in dataset_ids]

    assert len(dataset_ids) == 5


def test_get_dataset_ids_with_existing_ids_file():
    runner = CliRunner(echo_stdin=True)
    product = "wofs_ls"
    expressions = "time in [2023-01-01, 2023-01-30]"
    polygons_vector_file = TEST_WATERBODY
    use_id = "UID"
    output_file_path = TEST_CONFLUX_IDS_TXT
    num_worker = 8
    cmd = f"{product} {expressions} --verbose --polygons-vector-file={polygons_vector_file} --use-id={use_id} --output-file-path={output_file_path} --num-worker={num_worker}"
    result = runner.invoke(get_dataset_ids, cmd)

    assert type(result.exception) is FileExistsError
