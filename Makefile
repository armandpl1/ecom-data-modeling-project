# Same steps as CI, for running locally. `make ci` mirrors the CI pipeline.
# Uses the project environment in ./deenv (see README "Setup").

export UV_PROJECT_ENVIRONMENT := deenv
RUN := uv run

.PHONY: help install hooks lint test data build docs ci clean

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

build: data  ## Build all dbt models and run all dbt tests
	$(RUN) dbt parse
	$(RUN) dbt build

docs:  ## Generate and serve the dbt docs site
	$(RUN) dbt docs generate
	$(RUN) dbt docs serve

ci: lint test build  ## Everything CI runs

clean:  ## Remove build outputs
	rm -rf target logs reports site
