# DEA Conflux testing readme

## Setting up Docker for testing
- we use `docker-compose` to manage the test infrastructure (packages and environment)
- use `docker-compose build` from the root directory
- to launch, `docker-compose up -d` starts the docker
- you should have three containers running. You can check   this by running `docker-compose ps`
- from outside docker, run the shell script `tests/setup_test_datacube.sh` to set up the test datacube. This initialises the docker container datacube, downloads datasets required for testing and indexes them into this datacube. 
- Execute bash within the docker container conflux `docker-compose exec conflux bash`
- this process sets up a datacube and an environment to run conflux.
- now you can run tests in docker <img src="https://emojis.slackmojis.com/emojis/images/1507772920/3024/penguin_dance.gif?1507772920" alt="dancing penguin" width="16"/>


## Running tests in Docker

- You need to have conflux installed
- You need to have a datacube
- that datacube needs to have certain things in it
- There is a script that does the initialisation
- The environment can all be configured with docker

## Running tests in sandbox
To run tests, use `pytest` from the root (on the command line):

```bash
:dea-conflux$ pytest tests
```

The tests assume that `dea-conflux` is installed. To install, follow the instructions in the [main README](../README.md). You can install `dea-conflux` locally for testing using `pip`:

```bash
:dea-conflux$ pip -e .
```

Tests are automatically triggered in GitHub for any pushes to any branch. This behaviour is controlled by /.github/workflows/test.yml.
