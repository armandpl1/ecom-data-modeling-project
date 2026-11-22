"""Integrity tests for the Airflow DAG: it loads, is wired as intended, and
renders every dbt model. Run in the Airflow environment (make airflow-test),
after `dbt parse` has written target/manifest.json.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
ROOT = AIRFLOW_DIR.parent
MANIFEST = ROOT / "target" / "manifest.json"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    if not MANIFEST.exists():
        pytest.fail("target/manifest.json missing: run `dbt parse` first")
    start = time.monotonic()
    bag = DagBag(dag_folder=str(AIRFLOW_DIR / "dags"))
    bag.load_seconds = time.monotonic() - start
    return bag


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags["ecom_daily"]


def test_loads_without_errors_and_fast(dagbag) -> None:
    assert dagbag.import_errors == {}
    assert set(dagbag.dags) == {"ecom_daily"}
    # Parsed from the manifest, never by running dbt: well under the timeout
    assert dagbag.load_seconds < 20


def test_schedule_and_run_policy(dag) -> None:
    assert dag.schedule == "0 6 * * *" or str(dag.timetable.summary) == "0 6 * * *"
    assert str(dag.timezone) in ("America/Toronto", "Timezone('America/Toronto')")
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    for task in dag.tasks:
        assert task.retries == 2, task.task_id
        assert task.on_failure_callback, task.task_id


def test_pipeline_order(dag) -> None:
    tasks = {t.task_id: t for t in dag.tasks}
    assert tasks["generate_extracts"].downstream_task_ids == {"load_raw"}
    first_dbt = {t for t in tasks["load_raw"].downstream_task_ids}
    assert first_dbt and all(t.startswith("dbt.") for t in first_dbt)
    # The checks run after every dbt task
    dbt_tasks = {t for t in tasks if t.startswith("dbt.")}
    for final in ("check_service_levels", "publish_iceberg"):
        assert tasks[final].upstream_task_ids <= dbt_tasks
        assert dag.get_task(final).get_flat_relative_ids(upstream=True) >= dbt_tasks


def test_every_dbt_model_is_a_task_followed_by_its_tests(dag) -> None:
    manifest = json.loads(MANIFEST.read_text())
    # Ephemeral models are inlined into the models that use them: no task
    models = {
        n["name"]
        for n in manifest["nodes"].values()
        if n["resource_type"] == "model" and n["config"]["materialized"] != "ephemeral"
    }
    ids = {t.task_id for t in dag.tasks}
    rendered = {
        i.split(".")[1] for i in ids if i.startswith("dbt.") and i.endswith(".run")
    }
    assert rendered == models
    # A model's tests run right after it (TestBehavior.AFTER_EACH)
    run = dag.get_task("dbt.fact_orders.run")
    assert "dbt.fact_orders.test" in run.downstream_task_ids


def test_failure_alert_posts_to_the_webhook(monkeypatch: pytest.MonkeyPatch) -> None:
    sys.path.insert(0, str(AIRFLOW_DIR / "dags"))
    import ecom_daily

    sent = {}

    def fake_urlopen(request, timeout):
        sent["url"], sent["body"] = request.full_url, json.loads(request.data)

    class TI:
        task_id, try_number = "load_raw", 3

    monkeypatch.setattr(ecom_daily.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.com/x")
    ecom_daily.alert_on_failure({"task_instance": TI(), "run_id": "r1"})
    assert sent["url"] == "https://hooks.example.com/x"
    assert "task load_raw" in sent["body"]["text"] and "try 3" in sent["body"]["text"]
