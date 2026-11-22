"""Dagster definitions: the daily pipeline as software-defined assets.

    source_extracts  ->  raw.customers, raw.products,   ->  dbt models (Bronze,
    (simulated source     raw.orders, raw.subscriptions,     Silver, Gold), with
     system writes CSVs)  raw._load_log (scripts/load_raw)    dbt tests as checks

- source_extracts runs the deterministic generator. It stands in for the
  source system dropping nightly files; a real deployment would replace it
  with a sensor on the landing folder.
- raw_layer appends the extracts with scripts/load_raw.py. Its asset keys
  (raw/<table>) are the keys dagster-dbt gives the dbt sources, so the
  lineage graph runs from the files to the Gold tables.
- dbt_models runs `dbt build`; every model is an asset and every dbt test
  becomes an asset check.
- daily_pipeline materializes everything; daily_schedule runs it at 06:00
  Toronto time (stopped by default, so nothing runs unless you turn it on).
- cdc_changes loads new change events from Redpanda into the same raw
  tables (scripts/cdc_consumer.py, exactly once). It has its own job,
  cdc_load, run every 5 minutes by cdc_schedule (stopped by default: it
  needs the CDC stack, `make cdc-up`). daily_pipeline leaves it out, so the
  file path still runs without Docker.
- alert_on_failure fires when a run fails: it logs the error and, if
  ALERT_WEBHOOK_URL is set, posts it to that Slack-compatible webhook.

`dbt source freshness` is not part of the run: the bundled sample data is
static, so it always reports as stale (see README, "Source freshness").

Run locally:  make orchestrate   (opens the Dagster UI at http://localhost:3000)
"""

import json
import os
import urllib.request
from pathlib import Path

import cdc_consumer
import check_slos
import dagster as dg
import generate_raw_data
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from load_raw import TABLES, load_all

ROOT = Path(__file__).resolve().parents[2]
RAW_KEYS = [dg.AssetKey(["raw", table]) for table in (*TABLES, "_load_log")]

dbt_project = DbtProject(project_dir=ROOT, profiles_dir=ROOT)
if not dbt_project.manifest_path.exists():
    # dagster-dbt reads target/manifest.json; build it with `dbt parse` if missing
    dbt_project.preparer.prepare(dbt_project)


class WarehousePaths(dg.ConfigurableResource):
    """Where the extracts land and where the DuckDB warehouse lives."""

    raw_dir: str = str(ROOT / "data" / "raw")
    database: str = os.environ.get("ECOM_DUCKDB_PATH", str(ROOT / "ecom.duckdb"))
    run_results: str = str(ROOT / "target" / "last_pipeline_run_results.json")


@dg.asset(
    group_name="source_system",
    description="Nightly CSV extracts, written by the deterministic generator.",
)
def source_extracts(paths: WarehousePaths) -> dg.MaterializeResult:
    generate_raw_data.main(["--out-dir", paths.raw_dir])
    files = sorted(Path(paths.raw_dir).glob("*.csv"))
    return dg.MaterializeResult(
        metadata={"files": len(files), "raw_dir": paths.raw_dir}
    )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key,
            deps=[source_extracts],
            group_name="raw",
            description=(
                "Load log: one row per extract, with its SHA-256."
                if key.path[-1] == "_load_log"
                else f"Append-only raw {key.path[-1]}: one row per change, all loads."
            ),
        )
        for key in RAW_KEYS
    ],
)
def raw_layer(context: dg.AssetExecutionContext, paths: WarehousePaths):
    results = load_all(Path(paths.raw_dir), Path(paths.database))
    for result in results:
        if not result.loaded:
            context.log.info(f"{result.file_name} already loaded as {result.batch_id}")
        yield dg.MaterializeResult(
            asset_key=dg.AssetKey(["raw", result.table]),
            metadata={
                "batch_id": result.batch_id,
                "rows_in_batch": result.rows,
                "new_load": result.loaded,
            },
        )
    yield dg.MaterializeResult(
        asset_key=dg.AssetKey(["raw", "_load_log"]),
        metadata={"new_batches": sum(r.loaded for r in results)},
    )


# Where the dbt step saves its run_results.json for the service-level check
LAST_RUN_RESULTS = ROOT / "target" / "last_pipeline_run_results.json"


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    invocation = dbt.cli(["build"], context=context, raise_on_error=False)
    yield from invocation.stream()
    LAST_RUN_RESULTS.parent.mkdir(parents=True, exist_ok=True)
    LAST_RUN_RESULTS.write_text(json.dumps(invocation.get_artifact("run_results.json")))
    if not invocation.is_successful():
        raise dg.Failure(description="dbt build failed: see docs/runbooks/dbt-tests.md")


def breach_summary(report: dict) -> str:
    breached = [r for r in report["results"] if not r["met"]]
    lines = [f"{len(breached)} service-level objective(s) breached:"]
    lines += [
        f"- {r['objective']} / {r['subject']}: {r['value']} "
        f"(target {r['target']}); runbook {r['runbook']}"
        for r in breached
    ]
    return "\n".join(lines)


@dg.asset(
    deps=[dbt_models],
    group_name="operations",
    description="Checks ops/slos.yml after each run; a breach fails the run, alerts.",
)
def service_levels(paths: WarehousePaths) -> dg.MaterializeResult:
    report = check_slos.evaluate(
        Path(paths.database),
        Path(paths.run_results),
        check_slos.load_config(check_slos.DEFAULT_CONFIG),
    )
    metadata = {
        "breached": report["breached"],
        "reference": report["reference"],
        "report": dg.MetadataValue.md(
            "\n".join(
                f"- {'met' if r['met'] else '**BREACHED**'}: {r['objective']} / "
                f"{r['subject']}: {r['value']}"
                for r in report["results"]
            )
        ),
    }
    if report["breached"]:
        # Fails the run, so alert_on_failure posts this message with the runbooks
        raise dg.Failure(description=breach_summary(report), metadata=metadata)
    return dg.MaterializeResult(metadata=metadata)


CDC_GROUP = "cdc"


class CdcSettings(dg.ConfigurableResource):
    """Where the CDC consumer reads change events from."""

    bootstrap: str = os.environ.get("CDC_BOOTSTRAP", cdc_consumer.DEFAULT_BOOTSTRAP)
    max_events: int | None = None


@dg.asset(
    group_name=CDC_GROUP,
    description=(
        "Change events from Postgres (Debezium -> Redpanda) appended to the raw "
        "tables, exactly once. Needs the CDC stack: make cdc-up."
    ),
)
def cdc_changes(paths: WarehousePaths, cdc: CdcSettings) -> dg.MaterializeResult:
    try:
        results = cdc_consumer.run_once(
            cdc.bootstrap, Path(paths.database), cdc.max_events
        )
    except cdc_consumer.TablesBlocked as err:
        # The other tables were loaded; fail so alert_on_failure posts the change
        raise dg.Failure(
            description=(
                "CDC load blocked by a source type change: "
                + "; ".join(
                    f"{c.table}.{c.column} expected {c.expected}, got {c.observed}"
                    for c in err.changes
                )
                + ". Runbook: docs/runbooks/cdc-type-change.md"
            ),
            metadata={f"rows_{r.table}": r.rows for r in err.results},
        ) from err
    return dg.MaterializeResult(
        metadata={
            "rows_loaded": sum(r.rows for r in results),
            "events_read": sum(r.events for r in results),
            **{f"rows_{r.table}": r.rows for r in results},
        }
    )


cdc_load = dg.define_asset_job(
    "cdc_load",
    selection=dg.AssetSelection.groups(CDC_GROUP),
    description="Load new change events from Redpanda into the raw layer.",
)

cdc_schedule = dg.ScheduleDefinition(
    job=cdc_load,
    cron_schedule="*/5 * * * *",
    execution_timezone="America/Toronto",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

daily_pipeline = dg.define_asset_job(
    "daily_pipeline",
    selection=dg.AssetSelection.all() - dg.AssetSelection.groups(CDC_GROUP),
    description=(
        "Generate extracts, load the raw layer, dbt build with all tests, then "
        "check the service-level objectives."
    ),
)

daily_schedule = dg.ScheduleDefinition(
    job=daily_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="America/Toronto",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


def send_alert(message: str, webhook_url: str | None = None) -> bool:
    """Post to a Slack-compatible webhook. Returns False when none is configured."""
    url = webhook_url or os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return False
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": message}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10):
        pass
    return True


def failure_message(job_name: str, run_id: str, error: str | None) -> str:
    return f"Pipeline {job_name} failed (run {run_id}): {error or 'unknown error'}"


@dg.run_failure_sensor(
    monitored_jobs=[daily_pipeline, cdc_load],
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Logs every failed run and posts it to ALERT_WEBHOOK_URL if set.",
)
def alert_on_failure(context: dg.RunFailureSensorContext) -> None:
    message = failure_message(
        context.dagster_run.job_name,
        context.dagster_run.run_id,
        context.failure_event.message,
    )
    context.log.error(message)
    if not send_alert(message):
        context.log.info("ALERT_WEBHOOK_URL not set: alert logged only")


defs = dg.Definitions(
    assets=[source_extracts, raw_layer, dbt_models, service_levels, cdc_changes],
    jobs=[daily_pipeline, cdc_load],
    schedules=[daily_schedule, cdc_schedule],
    sensors=[alert_on_failure],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project, profiles_dir=str(ROOT)),
        "paths": WarehousePaths(),
        "cdc": CdcSettings(),
    },
)
