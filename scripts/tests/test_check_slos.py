"""Tests for scripts/check_slos.py: each objective, reference modes, exit codes."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from check_slos import DEFAULT_CONFIG, evaluate, load_config, main
from load_raw import TABLES

T0 = datetime(2026, 11, 1, 2, 0, 0)
CONFIG = load_config(DEFAULT_CONFIG)


def make_warehouse(
    path: Path,
    batches: dict[str, list[tuple[datetime, int]]] | None = None,
    bronze_behind: set[str] = frozenset(),
    build_bronze: bool = True,
) -> Path:
    """raw._load_log, raw.<table> and bronze.stg_<table> for each table.

    batches: {table: [(loaded_at, row_count), ...]}; default one healthy batch.
    """
    batches = batches or {t: [(T0, 100)] for t in TABLES}
    con = duckdb.connect(str(path))
    con.execute("create schema raw; create schema bronze")
    con.execute(
        "create table raw._load_log (batch_id varchar, table_name varchar, "
        "file_name varchar, row_count bigint, sha256 varchar, loaded_at timestamp)"
    )
    for table in TABLES:
        con.execute(f"create table raw.{table} (_ingested_at timestamp)")
        for i, (loaded_at, rows) in enumerate(batches[table]):
            con.execute(
                "insert into raw._load_log values (?, ?, ?, ?, ?, ?)",
                [f"{table}-{i}", table, f"{table}.csv", rows, f"sha{i}", loaded_at],
            )
            con.execute(f"insert into raw.{table} values (?)", [loaded_at])
        if build_bronze:
            latest = max(b[0] for b in batches[table])
            seen = latest - timedelta(hours=5) if table in bronze_behind else latest
            con.execute(
                f"create table bronze.stg_{table} as "
                "select ?::timestamp as ingested_at",
                [seen],
            )
    con.close()
    return path


def write_run_results(path: Path, statuses: dict[str, str]) -> Path:
    # Real dbt ids end with a hash for generic tests
    results = [
        {"unique_id": f"test.ecom.{n}.89487786bc", "status": s}
        for n, s in statuses.items()
    ]
    path.write_text(json.dumps({"results": results}))
    return path


@pytest.fixture
def ok_results(tmp_path: Path) -> Path:
    return write_run_results(
        tmp_path / "run_results.json",
        {"unique_dim_customer": "pass", "delivered_orders_have_ship_event": "warn"},
    )


def breached(report: dict) -> set[tuple[str, str]]:
    return {(r["objective"], r["subject"]) for r in report["results"] if not r["met"]}


def test_healthy_pipeline_meets_every_objective(
    tmp_path: Path, ok_results: Path
) -> None:
    report = evaluate(make_warehouse(tmp_path / "w.duckdb"), ok_results, CONFIG)
    assert report["breached"] == 0
    assert len(report["results"]) == 3 * len(TABLES) + 1
    assert all(r["runbook"].startswith("docs/runbooks/") for r in report["results"])


def test_a_table_that_stopped_loading_breaches_freshness(
    tmp_path: Path, ok_results: Path
) -> None:
    batches = {t: [(T0, 100)] for t in TABLES}
    batches["products"] = [(T0 - timedelta(hours=48), 100)]
    report = evaluate(
        make_warehouse(tmp_path / "w.duckdb", batches), ok_results, CONFIG
    )
    assert breached(report) == {("raw_freshness", "products")}


def test_wall_clock_reference_makes_old_data_stale(
    tmp_path: Path, ok_results: Path
) -> None:
    db = make_warehouse(tmp_path / "w.duckdb")
    assert evaluate(db, ok_results, CONFIG, reference="latest_load")["breached"] == 0
    stale = evaluate(db, ok_results, CONFIG, as_of=str(T0 + timedelta(hours=40)))
    assert {o for o, _ in breached(stale)} == {"raw_freshness"}


def test_a_small_batch_breaches_volume(tmp_path: Path, ok_results: Path) -> None:
    batches = {t: [(T0 - timedelta(days=1), 100), (T0, 100)] for t in TABLES}
    batches["orders"] = [(T0 - timedelta(days=1), 100), (T0, 40)]  # 40% of usual
    report = evaluate(
        make_warehouse(tmp_path / "w.duckdb", batches), ok_results, CONFIG
    )
    assert breached(report) == {("batch_volume", "orders")}
    [orders] = [
        r
        for r in report["results"]
        if r["objective"] == "batch_volume" and r["subject"] == "orders"
    ]
    assert orders["value"] == "40 rows, 40% of previous average"


def add_cdc_batch(path: Path, table: str, rows: int, loaded_at: datetime) -> None:
    with duckdb.connect(str(path)) as con:
        con.execute(
            "insert into raw._load_log values (?, ?, ?, ?, ?, ?)",
            [f"{table}-cdc", table, f"cdc://ecom.public.{table}/0:0-{rows}", rows,
             "shacdc", loaded_at],
        )  # fmt: skip


def test_small_cdc_batches_dont_breach_volume(tmp_path: Path, ok_results: Path) -> None:
    """CDC batches are as big as whatever changed: only extracts are compared."""
    path = make_warehouse(tmp_path / "w.duckdb")
    add_cdc_batch(path, "orders", 2, T0)
    assert breached(evaluate(path, ok_results, CONFIG)) == set()


def test_a_cdc_only_table_reports_its_stream(tmp_path: Path, ok_results: Path) -> None:
    path = make_warehouse(tmp_path / "w.duckdb")
    with duckdb.connect(str(path)) as con:
        con.execute("delete from raw._load_log where table_name = 'orders'")
    add_cdc_batch(path, "orders", 5, T0)
    report = evaluate(path, ok_results, CONFIG)
    [orders] = [
        r
        for r in report["results"]
        if r["objective"] == "batch_volume" and r["subject"] == "orders"
    ]
    assert orders["met"] is True
    assert orders["value"] == "CDC stream (1 batches), volume not checked"


def test_an_empty_first_batch_breaches_volume(tmp_path: Path, ok_results: Path) -> None:
    batches = {t: [(T0, 100)] for t in TABLES}
    batches["customers"] = [(T0, 0)]
    report = evaluate(
        make_warehouse(tmp_path / "w.duckdb", batches), ok_results, CONFIG
    )
    assert ("batch_volume", "customers") in breached(report)


def test_bronze_behind_raw_breaches(tmp_path: Path, ok_results: Path) -> None:
    db = make_warehouse(tmp_path / "w.duckdb", bronze_behind={"subscriptions"})
    report = evaluate(db, ok_results, CONFIG)
    assert breached(report) == {("warehouse_caught_up", "subscriptions")}


def test_unbuilt_bronze_breaches(tmp_path: Path, ok_results: Path) -> None:
    db = make_warehouse(tmp_path / "w.duckdb", build_bronze=False)
    report = evaluate(db, ok_results, CONFIG)
    assert {o for o, _ in breached(report)} == {"warehouse_caught_up"}


@pytest.mark.parametrize(
    "statuses",
    [
        {"unique_dim_customer": "fail"},
        {"fact_orders": "error"},
        {"some_new_warning": "warn"},
    ],
)
def test_dbt_failures_and_unexpected_warnings_breach(
    tmp_path: Path, statuses: dict[str, str]
) -> None:
    results = write_run_results(tmp_path / "rr.json", statuses)
    report = evaluate(make_warehouse(tmp_path / "w.duckdb"), results, CONFIG)
    assert breached(report) == {("dbt_tests", "dbt build")}


def test_missing_run_results_breaches(tmp_path: Path) -> None:
    report = evaluate(
        make_warehouse(tmp_path / "w.duckdb"), tmp_path / "none.json", CONFIG
    )
    assert breached(report) == {("dbt_tests", "dbt build")}


def test_cli_exit_codes_and_json(
    tmp_path: Path, ok_results: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = make_warehouse(tmp_path / "w.duckdb")
    out = tmp_path / "report.json"
    assert (
        main(
            [
                "--database",
                str(db),
                "--run-results",
                str(ok_results),
                "--json",
                str(out),
            ]
        )
        == 0
    )
    assert "0 objective(s) breached" in capsys.readouterr().out
    assert json.loads(out.read_text())["breached"] == 0

    assert main(["--database", str(db), "--run-results", str(tmp_path / "x.json")]) == 1
    assert main(["--database", str(tmp_path / "missing.duckdb")]) == 2
