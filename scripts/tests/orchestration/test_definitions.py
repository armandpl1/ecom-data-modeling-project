"""Tests for scripts/orchestration/definitions.py: graph, lineage, schedule, alerts."""

import json
from pathlib import Path

import dagster as dg
import pytest
from orchestration import definitions
from orchestration.definitions import (
    WarehousePaths,
    daily_pipeline,
    defs,
    failure_message,
    raw_layer,
    send_alert,
    source_extracts,
)

RAW_TABLES = ("customers", "products", "orders", "subscriptions")


@pytest.fixture(scope="module")
def graph():
    return defs.get_repository_def().asset_graph


def keys_named(graph, name: str) -> list[dg.AssetKey]:
    return [k for k in graph.get_all_asset_keys() if k.path[-1] == name]


def test_every_built_dbt_model_is_an_asset(graph) -> None:
    """Ephemeral models are inlined into others, never built, so they aren't assets."""
    manifest = json.loads(definitions.dbt_project.manifest_path.read_text())
    models = {
        n["name"]
        for n in manifest["nodes"].values()
        if n["resource_type"] == "model" and n["config"]["materialized"] != "ephemeral"
    }
    asset_names = {k.path[-1] for k in graph.get_all_asset_keys()}
    assert models <= asset_names
    assert len(models) >= 15


def test_lineage_runs_from_the_files_to_gold(graph) -> None:
    for table in RAW_TABLES:
        [staging] = keys_named(graph, f"stg_{table}")
        assert dg.AssetKey(["raw", table]) in graph.get(staging).parent_keys
        assert graph.get(dg.AssetKey(["raw", table])).parent_keys == {
            dg.AssetKey("source_extracts")
        }
    [fact] = keys_named(graph, "fact_orders")
    assert graph.get(fact).parent_keys  # Gold depends on Silver


def test_dbt_tests_become_asset_checks(graph) -> None:
    assert len(list(graph.asset_check_keys)) >= 100


def test_daily_schedule_is_stopped_by_default() -> None:
    repo = defs.get_repository_def()
    [schedule] = repo.schedule_defs
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "America/Toronto"
    assert schedule.job_name == daily_pipeline.name
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_failure_sensor_watches_the_daily_pipeline() -> None:
    [sensor] = defs.get_repository_def().sensor_defs
    assert sensor.name == "alert_on_failure"
    assert sensor.default_status == dg.DefaultSensorStatus.RUNNING
    assert daily_pipeline.name in {
        j.name for j in defs.get_repository_def().get_all_jobs()
    }


def test_raw_layer_loads_once_and_reruns_safely(tmp_path: Path) -> None:
    paths = WarehousePaths(
        raw_dir=str(tmp_path / "raw"), database=str(tmp_path / "w.duckdb")
    )
    instance = dg.DagsterInstance.ephemeral()

    def run() -> dict[str, dict]:
        result = dg.materialize(
            [source_extracts, raw_layer], resources={"paths": paths}, instance=instance
        )
        assert result.success
        return {
            event.asset_key.path[-1]: {
                k: v.value for k, v in event.materialization.metadata.items()
            }
            for event in result.get_asset_materialization_events()
        }

    first = run()
    assert first["customers"]["new_load"] is True
    assert first["customers"]["rows_in_batch"] > 0
    assert first["_load_log"]["new_batches"] == 4

    second = run()  # same deterministic extracts: nothing new to load
    assert second["customers"]["new_load"] is False
    assert second["_load_log"]["new_batches"] == 0


def test_send_alert_needs_a_webhook(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert send_alert("boom") is False


def test_send_alert_posts_json(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout):
        sent["url"] = request.full_url
        sent["body"] = json.loads(request.data)
        return Response()

    monkeypatch.setattr(definitions.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.com/x")
    assert send_alert("boom") is True
    assert sent == {"url": "https://hooks.example.com/x", "body": {"text": "boom"}}


def test_failure_message() -> None:
    assert failure_message("daily_pipeline", "abc", None) == (
        "Pipeline daily_pipeline failed (run abc): unknown error"
    )
