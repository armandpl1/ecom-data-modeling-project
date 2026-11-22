# Same steps as CI, for running locally. `make ci` mirrors the CI pipeline.
# Uses the project environment in ./deenv (see README "Setup").

export UV_PROJECT_ENVIRONMENT := deenv
RUN := uv run

.PHONY: help install hooks lint test data load build check-slos check-incremental check-late-dimensions iceberg check-iceberg pipeline orchestrate docs ci clean metrics dashboard dashboard-dev cdc-up cdc-down cdc-replay cdc-load cdc-lag cdc-e2e

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

metrics:  ## Validate the semantic layer and export its metrics to dashboard/sources/metrics/
	$(RUN) mf validate-configs
	$(RUN) python scripts/export_metrics.py

# Node.js for the dashboard: on PATH, or the newest user-space install in
# ~/.local/opt/node-v*-linux-x64 (see README, "Semantic layer and dashboard")
NODE_BIN ?= $(lastword $(sort $(wildcard $(HOME)/.local/opt/node-v*-linux-x64/bin)))
NPM := PATH="$(NODE_BIN):$$PATH" npm --prefix dashboard

dashboard: metrics  ## Build the Evidence dashboard into dashboard/build (needs Node.js 18+)
	$(NPM) install --no-audit --no-fund
	$(NPM) run sources
	$(NPM) run build

dashboard-dev:  ## Serve the dashboard locally with live reload
	$(NPM) run dev

CDC := docker compose -f cdc/docker-compose.yml
cdc-up:  ## Start the CDC stack: Postgres, Redpanda, Debezium Server (waits until healthy)
	$(CDC) up -d --wait

cdc-down:  ## Stop the CDC stack and delete its data
	$(CDC) down -v

cdc-replay:  ## Replay the change history into the CDC source Postgres (UNTIL=YYYY-MM-DD to stop early)
	$(RUN) python scripts/cdc_source.py replay $(if $(UNTIL),--until $(UNTIL))

cdc-load:  ## Load new change events from Redpanda into the raw layer (exactly once)
	$(RUN) python scripts/cdc_consumer.py

cdc-e2e:  ## Full CDC check on a fresh stack: Gold equals the CSV path, exactly once, schema change
	$(CDC) down -v
	$(CDC) up -d --wait
	$(RUN) python scripts/check_cdc_e2e.py --json target/cdc_e2e_report.json

cdc-lag:  ## Measure commit-to-raw lag (run the consumer with --follow meanwhile; DB=path)
	$(RUN) python scripts/cdc_lag.py --database $(or $(DB),ecom.duckdb)

iceberg:  ## Publish the Gold tables as Apache Iceberg tables in data/lakehouse (reruns are safe)
	uv run python scripts/publish_iceberg.py

check-iceberg:  ## Prove the Iceberg tables match Gold, read back by DuckDB's iceberg extension
	uv run python scripts/check_iceberg.py

check-late-dimensions:  ## Prove early-arriving facts land on the unknown member, warn, then get re-pointed
	uv run python scripts/check_late_dimensions.py

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
