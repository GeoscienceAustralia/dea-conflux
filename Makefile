SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help setup up down clean test

METADATA_CATALOG = "https://raw.githubusercontent.com/digitalearthafrica/config/master/metadata/eo3_landsat_ard.odc-type.yaml" 
PRODUCT_CATALOG = "https://raw.githubusercontent.com/digitalearthafrica/config/master/prod/products_prod.csv"

help: ## Print this help
	@grep -E '^##.*$$' $(MAKEFILE_LIST) | cut -c'4-'
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## 0. Build the base image
	docker compose pull
	docker compose build

up: ## 1. Bring up your Docker environment.
	docker compose up -d postgres
	docker compose up -d conflux
	docker compose up -d index

init: ## 2. Prepare the database.
	docker compose exec -T index datacube -v system init --no-default-types --no-init-users

metadata: ## 3. Setup metadata types.
	docker compose exec -T index datacube -v metadata add https://raw.githubusercontent.com/digitalearthafrica/config/master/metadata/eo3_deafrica.odc-type.yaml
	docker compose exec -T index datacube -v metadata add https://raw.githubusercontent.com/digitalearthafrica/config/master/metadata/eo3_landsat_ard.odc-type.yaml

products: ## 3. Add the wofs_ls product for testing.
	docker compose exec -T index datacube -v product add https://raw.githubusercontent.com/digitalearthafrica/config/master/products/wofs_ls.odc-product.yaml

index: ## 4. Index test data.
	cat index_tiles.sh | docker compose exec -T index bash

install-conflux: ## 5. Install deafrica-conflux
	docker compose exec -T conflux bash -c "pip install -e ."

test-env: init metadata products index install-conflux


down: ## Bring down the system
	docker compose down

shell: ## Start an interactive shell
	docker compose exec index bash

clean: ## Delete everything
	docker compose down --rmi all -v

logs: ## Show the logs from the stack
	docker compose logs --follow

pip_compile:
	pip-compile --verbose \
		--extra-index-url=https://packages.dea.ga.gov.au \
		--output-file requirements.txt \
		requirements.in