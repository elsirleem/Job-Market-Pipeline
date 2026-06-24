# Ergonomic wrappers around docker compose. Everything runs inside the container.
.PHONY: help build up down shell bronze silver gold all test clean

DC := docker compose
EXEC := $(DC) exec -T pipeline

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Build the Spark image
	$(DC) build

up: ## Start the container in the background
	$(DC) up -d

down: ## Stop and remove the container
	$(DC) down

shell: ## Open a shell inside the running container
	$(DC) exec pipeline bash

bronze: ## Ingest jobs from Adzuna into the bronze Delta table
	$(EXEC) python pipelines/run_bronze.py

silver: ## Clean, dedupe, normalize and extract skills into silver
	$(EXEC) python pipelines/run_silver.py

gold: ## Build the gold analytics tables
	$(EXEC) python pipelines/run_gold.py

all: ## Run the full bronze -> silver -> gold pipeline
	$(EXEC) python pipelines/run_all.py

test: ## Run the unit test suite
	$(EXEC) python -m pytest -q

clean: ## Delete the local lakehouse data
	rm -rf data/lake data/raw
