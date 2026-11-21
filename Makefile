# Same steps as CI, for running locally. `make ci` mirrors the CI pipeline.
# Uses the project environment in ./deenv (see README "Setup").

export UV_PROJECT_ENVIRONMENT := deenv
RUN := uv run

.PHONY: help install hooks lint test data load build check-slos check-incremental pipeline orchestrate docs ci clean

help:  ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install:  ## Install dependencies from uv.lock
	uv sync --locked

hooks:  ## Install the git pre-commit hooks
	$(RUN) pre-commit install

lint:  ## Lint and check formatting (same hooks as CI)
	uv lock --check
	$(RUN) pre-commit run --all-files

test:  ## Unit tests for the data generator
	$(RUN) pytest

data:  ## Generate the raw CSVs
	$(RUN) python scripts/generate_raw_data.py

load: data  ## Append the raw CSVs to the raw layer in ecom.duckdb (reruns are safe)
	$(RUN) python scripts/load_raw.py

build: load  ## Build all dbt models and run all dbt tests
	$(RUN) dbt parse
	$(RUN) dbt build

check-slos:  ## Check the service-level objectives (ops/slos.yml); each breach names its runbook
	$(RUN) python scripts/check_slos.py

check-incremental:  ## Prove incremental runs give the same Gold tables as a full refresh
	$(RUN) python scripts/check_incremental.py

DAGSTER := DAGSTER_HOME=$(CURDIR)/.dagster $(RUN) dagster
pipeline:  ## Run the whole daily pipeline once with Dagster (generate, load, dbt build)
	mkdir -p .dagster
	$(DAGSTER) job execute -m orchestration.definitions -d scripts -j daily_pipeline

orchestrate:  ## Open the Dagster UI (lineage, runs, schedule) at http://localhost:3000
	mkdir -p .dagster
	$(DAGSTER) dev -m orchestration.definitions -d scripts

docs:  ## Generate and serve the dbt docs site
	$(RUN) dbt docs generate
	$(RUN) dbt docs serve

ci: lint test build  ## Everything CI runs

clean:  ## Remove build outputs
	rm -rf target logs reports site
