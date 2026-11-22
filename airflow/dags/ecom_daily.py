"""The ecom daily pipeline as an Airflow DAG (Dagster stays the main orchestrator).

    generate_extracts -> load_raw -> dbt (one task per model, its tests after it)
        -> check_service_levels
        -> publish_iceberg

- Airflow runs in its own environment (airflow/pyproject.toml). Every step
  runs the main project's code in the main environment (../deenv, or
  ../.venv in CI), so the two
  sets of dependencies never mix.
- dbt is rendered by Astronomer Cosmos from target/manifest.json (run `dbt
  parse` first; `make airflow-test` does): each model is a task followed by its
  tests (TestBehavior.AFTER_EACH), so a failing test stops everything
  downstream of that model only, and a retry reruns one model, not the build.
  A test that spans several models (a relationship from a fact to a
  dimension) gets its own task after all of them.
- Idempotent: the loader skips extracts it has already loaded, incremental
  models only process new rows, and the Iceberg publish skips unchanged
  tables, so a rerun or a retry is safe.
- 06:00 Toronto time, one run at a time, no catch-up of past days, two
  retries per task, and a failure callback that posts to ALERT_WEBHOOK_URL
  (Slack-compatible) when it is set.

Mapping to the Dagster version (scripts/orchestration/definitions.py):

| Dagster                          | Airflow here                                 |
| -------------------------------- | -------------------------------------------- |
| asset (source_extracts, raw)     | task (BashOperator)                          |
| dbt assets via dagster-dbt       | DbtTaskGroup via Cosmos                      |
| asset check (dbt test)           | test task after each model                   |
| job + schedule                   | DAG + schedule                               |
| run failure sensor               | on_failure_callback                          |
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior, TestIndirectSelection

ROOT = Path(os.environ.get("ECOM_ROOT", Path(__file__).resolve().parents[2]))
# The main project's environment: ./deenv locally (AGENTS.md), ./.venv in CI
VENV = ROOT / os.environ.get(
    "ECOM_VENV", "deenv" if (ROOT / "deenv").exists() else ".venv"
)
PYTHON = VENV / "bin" / "python"
DBT = VENV / "bin" / "dbt"
DATABASE = Path(os.environ.get("ECOM_DUCKDB_PATH", ROOT / "ecom.duckdb"))
# Cosmos renders the dbt tasks from the compiled manifest (`dbt parse`), so
# loading the DAG never runs dbt: fast, and what production deployments ship
MANIFEST = Path(os.environ.get("ECOM_DBT_MANIFEST", ROOT / "target" / "manifest.json"))

# The main project's environment for every step: its database and its dbt
PROJECT_ENV = {"ECOM_DUCKDB_PATH": str(DATABASE), "PYTHONUNBUFFERED": "1"}


def alert_on_failure(context: dict) -> None:
    """Post the failed task to ALERT_WEBHOOK_URL (Slack-compatible), if set."""
    url = os.environ.get("ALERT_WEBHOOK_URL")
    ti = context["task_instance"]
    message = (
        f"ecom_daily failed: task {ti.task_id}, run {context['run_id']}, "
        f"try {ti.try_number}. Runbook: docs/runbooks/pipeline-failure.md"
    )
    if not url:
        print(f"ALERT_WEBHOOK_URL not set: {message}")
        return
    body = json.dumps({"text": message}).encode()
    request = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(request, timeout=10)


def project_step(task_id: str, command: str) -> BashOperator:
    """A command run from the project root with the project's Python."""
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        cwd=str(ROOT),
        env=PROJECT_ENV,
        append_env=True,
    )


@dag(
    dag_id="ecom_daily",
    description="Generate, load, dbt build with tests, SLOs, publish Iceberg.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Toronto"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": alert_on_failure,
    },
    tags=["ecom", "dbt", "daily"],
)
def ecom_daily():
    generate = project_step(
        "generate_extracts", f"{PYTHON} scripts/generate_raw_data.py"
    )
    load = project_step(
        "load_raw", f"{PYTHON} scripts/load_raw.py --database {DATABASE}"
    )

    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(dbt_project_path=ROOT, manifest_path=MANIFEST),
        profile_config=ProfileConfig(
            profile_name="ecom",
            target_name="dev",
            profiles_yml_filepath=ROOT / "profiles.yml",
        ),
        # A model's test task runs only tests on that model alone ("cautious");
        # tests spanning several models (fact -> dimension relationships) get
        # their own task after all of their models (detached below)
        execution_config=ExecutionConfig(
            dbt_executable_path=str(DBT),
            test_indirect_selection=TestIndirectSelection.CAUTIOUS,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
            should_detach_multiple_parents_tests=True,
            env_vars=PROJECT_ENV,
        ),
        operator_args={"env": PROJECT_ENV, "append_env": True, "install_deps": False},
    )

    # dbt test results are enforced by the dbt tasks themselves (a failing
    # test fails the run), so only the data objectives are checked here
    slos = project_step(
        "check_service_levels",
        f"{PYTHON} scripts/check_slos.py --database {DATABASE} --skip dbt_tests",
    )
    iceberg = project_step(
        "publish_iceberg", f"{PYTHON} scripts/publish_iceberg.py --database {DATABASE}"
    )

    generate >> load >> dbt >> [slos, iceberg]


ecom_daily()
