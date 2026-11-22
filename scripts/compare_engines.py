#!/usr/bin/env python3
"""Check that Gold is the same on DuckDB and on Databricks, row for row.

For every table in the gold, reporting and restricted schemas of the DuckDB
warehouse, read the same table from Databricks (Unity Catalog), cast every
column to text on each engine, and compare the two as multisets of rows
(so duplicates count, order doesn't). Columns holding load times
(*ingested_at*) differ by design and are left out, and so are the columns
Unity Catalog masks on Databricks.

Needs both builds first: `make build` and `make databricks-build`. Exits 1
on any difference, listing the tables and a few differing rows.

Usage:
    python scripts/compare_engines.py [--database ecom.duckdb]
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from pathlib import Path

import duckdb
from databricks_load_raw import client, settings

SCHEMAS = ("gold", "reporting", "restricted")
# Unity Catalog column masks show these as '***' on Databricks unless the
# reader is in pii_readers (macros/unity_catalog.sql), so they are compared
# only on DuckDB's side of the check (left out here)
MASKED = {
    "restricted.dim_customer_pii": {
        "first_name",
        "last_name",
        "full_name",
        "email",
        "phone_number",
        "phone_extension",
        "street_address",
        "postal_code",
    }
}


def duckdb_tables(con) -> dict[str, list[str]]:
    rows = con.execute(
        "select table_schema, table_name, column_name from information_schema.columns "
        f"where table_schema in {SCHEMAS} "
        "order by table_schema, table_name, ordinal_position"
    ).fetchall()
    tables: dict[str, list[str]] = {}
    for schema, table, column in rows:
        name = f"{schema}.{table}"
        if "ingested_at" not in column and column not in MASKED.get(name, set()):
            tables.setdefault(name, []).append(column)
    return tables


def text_select(columns: list[str], relation: str) -> str:
    casts = ", ".join(f"cast(`{c}` as string)" for c in columns)
    return f"select {casts} from {relation}"


def fetch_databricks(w, warehouse: str, statement: str) -> list[tuple]:
    """All rows of a statement, following the result chunks."""
    from databricks.sdk.service.sql import StatementState

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse, statement=statement, wait_timeout="50s"
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            response.status.error.message if response.status.error else ""
        )
    rows = list(response.result.data_array or [])
    chunk = response.result.next_chunk_index
    while chunk is not None:
        part = w.statement_execution.get_statement_result_chunk_n(
            response.statement_id, chunk
        )
        rows += part.data_array or []
        chunk = part.next_chunk_index
    return [tuple(r) for r in rows]


def compare(database: Path) -> dict[str, dict]:
    cfg = settings()
    w = client()
    con = duckdb.connect(str(database), read_only=True)
    report = {}
    for name, columns in duckdb_tables(con).items():
        local = Counter(
            tuple(r)
            for r in con.execute(
                text_select(columns, name)
                .replace("`", '"')
                .replace(" as string)", " as varchar)")
            ).fetchall()
        )
        remote = Counter(
            fetch_databricks(
                w, cfg["warehouse"], text_select(columns, f"{cfg['catalog']}.{name}")
            )
        )
        only_local, only_remote = local - remote, remote - local
        report[name] = {
            "rows": sum(local.values()),
            "databricks_rows": sum(remote.values()),
            "only_duckdb": list(only_local.elements())[:3],
            "only_databricks": list(only_remote.elements())[:3],
            "identical": not only_local and not only_remote,
        }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(os.environ.get("ECOM_DUCKDB_PATH", "ecom.duckdb")),
    )
    args = parser.parse_args(argv)
    report = compare(args.database)
    for name, r in report.items():
        status = "identical" if r["identical"] else "DIFFERENT"
        print(f"{name:<45} {r['rows']:>7} {r['databricks_rows']:>7}  {status}")
        if not r["identical"]:
            print("   only in DuckDB:    ", r["only_duckdb"])
            print("   only in Databricks:", r["only_databricks"])
    same = sum(r["identical"] for r in report.values())
    print(f"\n{same} of {len(report)} tables identical on DuckDB and Databricks")
    return 0 if same == len(report) else 1


if __name__ == "__main__":
    sys.exit(main())
