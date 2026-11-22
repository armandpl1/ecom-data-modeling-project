#!/usr/bin/env python3
"""Check the pipeline's service-level objectives (ops/slos.yml).

    raw_freshness        every raw table was loaded within max_age_hours of the
                         reference time
    batch_volume         each table's latest batch has at least min_ratio of the
                         average rows of its previous batches (and is not empty)
    warehouse_caught_up  Bronze has processed the newest raw load of each table
    dbt_tests            the last dbt build had no failures or errors, and only
                         allowed warnings

Reads raw._load_log, raw.<table> and bronze.stg_<table> from the DuckDB
warehouse, and dbt's run_results.json. Prints a table and, with --json, writes
a JSON report. Exits 1 if any objective is breached, 2 if the inputs are
missing. Every result names the runbook to follow.

Usage:
    python scripts/check_slos.py                       # reference: latest load
    python scripts/check_slos.py --reference now       # wall-clock freshness
    python scripts/check_slos.py --as-of "2026-11-01 06:00:00"
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import yaml
from load_raw import TABLES

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "ops" / "slos.yml"
DEFAULT_DATABASE = ROOT / "ecom.duckdb"
DEFAULT_RUN_RESULTS = ROOT / "target" / "run_results.json"


@dataclass(frozen=True)
class Result:
    objective: str
    subject: str  # the table or check it applies to
    met: bool
    value: str  # what was measured
    target: str
    runbook: str


def load_config(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def reference_time(con: duckdb.DuckDBPyConnection, mode: str, as_of: str | None):
    """The time freshness is measured against."""
    if as_of:
        return datetime.fromisoformat(as_of)
    if mode == "now":
        return datetime.now()
    if mode == "latest_load":
        return con.execute("select max(loaded_at) from raw._load_log").fetchone()[0]
    raise ValueError(f"unknown reference {mode!r} (use latest_load or now)")


def check_raw_freshness(con, reference: datetime, cfg: dict) -> list[Result]:
    limit = cfg["max_age_hours"]
    latest = dict(
        con.execute(
            "select table_name, max(loaded_at) from raw._load_log group by 1"
        ).fetchall()
    )
    results = []
    for table in TABLES:
        loaded = latest.get(table)
        if loaded is None:
            results.append(
                Result(
                    "raw_freshness",
                    table,
                    False,
                    "never loaded",
                    f"<= {limit} h",
                    cfg["runbook"],
                )
            )
            continue
        age = (reference - loaded).total_seconds() / 3600
        results.append(
            Result(
                "raw_freshness",
                table,
                age <= limit,
                f"{age:.1f} h old",
                f"<= {limit} h",
                cfg["runbook"],
            )
        )
    return results


def check_batch_volume(con, cfg: dict) -> list[Result]:
    ratio_limit = cfg["min_ratio"]
    results = []
    for table in TABLES:
        # Only file extracts: CDC batches (file_name cdc://...) are as big as
        # whatever changed since the last poll, so a small one is normal. A
        # CDC-fed table is watched through freshness and lag instead.
        counts = [
            r[0]
            for r in con.execute(
                "select row_count from raw._load_log where table_name = ? "
                "and file_name not like 'cdc://%' order by loaded_at, batch_id",
                [table],
            ).fetchall()
        ]
        target = f">= {ratio_limit:.0%} of previous average, > 0"
        cdc_batches = con.execute(
            "select count(*) from raw._load_log where table_name = ? "
            "and file_name like 'cdc://%'",
            [table],
        ).fetchone()[0]
        if not counts and cdc_batches:
            results.append(
                Result(
                    "batch_volume",
                    table,
                    True,
                    f"CDC stream ({cdc_batches} batches), volume not checked",
                    target,
                    cfg["runbook"],
                )
            )
            continue
        if not counts:
            results.append(
                Result(
                    "batch_volume", table, False, "no batches", target, cfg["runbook"]
                )
            )
            continue
        latest, previous = counts[-1], counts[:-1]
        if not previous:
            met, value = latest > 0, f"{latest} rows (first batch)"
        else:
            ratio = latest / (sum(previous) / len(previous))
            met = latest > 0 and ratio >= ratio_limit
            value = f"{latest} rows, {ratio:.0%} of previous average"
        results.append(
            Result("batch_volume", table, met, value, target, cfg["runbook"])
        )
    return results


def check_warehouse_caught_up(con, cfg: dict) -> list[Result]:
    results = []
    for table in TABLES:
        raw_max = con.execute(f"select max(_ingested_at) from raw.{table}").fetchone()[
            0
        ]
        try:
            bronze_max = con.execute(
                f"select max(ingested_at) from bronze.stg_{table}"
            ).fetchone()[0]
        except duckdb.CatalogException:
            bronze_max = None
        if bronze_max is None:
            met, value = False, "Bronze not built"
        else:
            lag = max((raw_max - bronze_max).total_seconds() / 3600, 0)
            met, value = bronze_max >= raw_max, f"{lag:.1f} h behind raw"
        results.append(
            Result(
                "warehouse_caught_up",
                table,
                met,
                value,
                "0 h behind raw",
                cfg["runbook"],
            )
        )
    return results


def check_dbt_tests(run_results: Path, cfg: dict) -> list[Result]:
    allowed = set(cfg.get("allowed_warnings", []))
    target = "0 failures or errors; only allowed warnings"
    if not run_results.exists():
        return [
            Result(
                "dbt_tests",
                "dbt build",
                False,
                "no run_results.json",
                target,
                cfg["runbook"],
            )
        ]
    statuses: dict[str, list[str]] = {}
    for node in json.loads(run_results.read_text())["results"]:
        # unique_id is "<type>.<project>.<name>[.<hash>]", e.g.
        # test.ecom.delivered_orders_have_ship_event.89487786bc
        name = node["unique_id"].split(".")[2]
        statuses.setdefault(node["status"], []).append(name)
    broken = statuses.get("fail", []) + statuses.get("error", [])
    unexpected = [w for w in statuses.get("warn", []) if w not in allowed]
    met = not broken and not unexpected
    value = (
        f"{len(broken)} failed or errored, {len(statuses.get('warn', []))} warnings"
        + (f" ({', '.join(sorted(broken + unexpected))})" if not met else "")
    )
    return [Result("dbt_tests", "dbt build", met, value, target, cfg["runbook"])]


def evaluate(
    database: Path,
    run_results: Path,
    config: dict,
    reference: str | None = None,
    as_of: str | None = None,
    skip: tuple[str, ...] = (),
) -> dict:
    """Evaluate every objective except those named in `skip`."""
    objectives = config["objectives"]
    if unknown := sorted(set(skip) - set(objectives)):
        raise ValueError(f"unknown objectives to skip: {unknown}")
    with duckdb.connect(str(database), read_only=True) as con:
        ref = reference_time(con, reference or config["reference"], as_of)
        results = (
            check_raw_freshness(con, ref, objectives["raw_freshness"])
            + check_batch_volume(con, objectives["batch_volume"])
            + check_warehouse_caught_up(con, objectives["warehouse_caught_up"])
        )
    results += check_dbt_tests(run_results, objectives["dbt_tests"])
    results = [r for r in results if r.objective not in skip]
    return {
        "reference": str(ref),
        "breached": sum(not r.met for r in results),
        "results": [asdict(r) for r in results],
    }


def format_report(report: dict) -> str:
    rows = [("Objective", "Subject", "Status", "Measured", "Target", "Runbook")]
    for r in report["results"]:
        status = "met" if r["met"] else "BREACHED"
        rows.append(
            (
                r["objective"],
                r["subject"],
                status,
                r["value"],
                r["target"],
                r["runbook"],
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    lines = [f"Reference time: {report['reference']}"]
    lines += [
        "  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip() for row in rows
    ]
    lines.append(f"{report['breached']} objective(s) breached")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--run-results", type=Path, default=DEFAULT_RUN_RESULTS)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--reference", choices=["latest_load", "now"])
    parser.add_argument("--as-of", help="reference time, e.g. '2026-11-01 06:00:00'")
    parser.add_argument("--json", type=Path, help="also write the report here")
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        help="objective to leave out, e.g. dbt_tests when the orchestrator "
        "already fails the run on a failing test (repeatable)",
    )
    args = parser.parse_args(argv)

    if not args.database.exists():
        print(f"No warehouse at {args.database}: run make build first", file=sys.stderr)
        return 2
    try:
        report = evaluate(
            args.database,
            args.run_results,
            load_config(args.config),
            args.reference,
            args.as_of,
            tuple(args.skip),
        )
    except duckdb.CatalogException as err:
        print(f"Raw layer missing in {args.database}: {err}", file=sys.stderr)
        return 2
    print(format_report(report))
    if args.json:
        args.json.write_text(json.dumps(report, indent=2) + "\n")
    return 1 if report["breached"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
