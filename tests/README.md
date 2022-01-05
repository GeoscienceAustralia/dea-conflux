# DEA Conflux testing readme

## Setting up Docker for testing
- we use `docker-compose` to manage the test infrastructure (packages and environment)
- use `docker-compose build` from the root directory
- to launch, `docker-compose up -d` starts the docker
- you should have three containers running. You can check   this by running `docker-compose ps`
- from outside docker, run the shell script `tests/setup_test_datacube.sh` to set up the test datacube. This initialises the docker container datacube, downloads datasets required for testing and indexes them into this datacube. 
- this process sets up a datacube and an environment to run conflux.
- now you can run tests in docker <img src="https://emojis.slackmojis.com/emojis/images/1507772920/3024/penguin_dance.gif?1507772920" alt="dancing penguin" width="16"/>
- If the docker container needs rebuilding run `docker-compose build` 
- Once you are done with testing, you can shut down the containers with `docker-compose down`


## Running tests in Docker
- Once containers are up, you can run testing with the command `docker-compose exec conflux pytest` 
- If you want to run the tests interactively and have access to the interactive debugger, 
  Execute bash within the docker container conflux `docker-compose exec conflux bash` and then run `pytest` from the code directory:

```bash
root@fe004etc:/code# pytest tests
```

## Running tests in sandbox
The tests assume that `dea-conflux` is installed. To install, follow the instructions in the [main README](../README.md). You can install `dea-conflux` locally for testing using `pip`:

```bash
jovyan@jupyter:dea-conflux$ pip install -e .
```

Remember the dot (.)! 

To run tests, use `pytest` from the dea-conflux repository root, in the terminal:

```bash
jovyan@jupyter:dea-conflux$ pytest tests
```

Tests are automatically triggered in GitHub for any pushes to any branch. This behaviour is controlled by /.github/workflows/test.yml.

## Adding new test data
- the docker test datacube needs to have datasets in it to run tests on
- to add a new test dataset, first make sure the product is indexed in the test datacube in `setup_test_datacube.sh`
- this is done with a line like the following:

```bash
docker-compose exec -T index bash -c "tail -n+2 product_list.csv | grep 'ga_ls_wo_3' | awk -F , '{print \$2}' | xargs datacube -v product add"
```
- add the individual dataset with `s3-to-dc` inside the heredoc (with the others):

```bash
s3-to-dc 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_wo_3'
```
