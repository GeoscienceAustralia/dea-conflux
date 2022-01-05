# DEA Conflux testing readme

To run tests, use `pytest` from the root:

```bash
:dea-conflux$ pytest tests
```

The tests assume that `dea-conflux` is installed. To install, follow the instructions in the [main README](../README.md). You can install `dea-conflux` locally for testing using `pip`:

```bash
:dea-conflux$ pip -e .
```

Tests are automatically triggered in GitHub for any pushes to any branch. This behaviour is controlled by /.github/workflows/test.yml.
