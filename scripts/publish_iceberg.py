#!/usr/bin/env python3
"""Publish the Gold tables from ecom.duckdb as Apache Iceberg tables.

For each table in ops/lakehouse.yml:

1. Read it from DuckDB, sorted by every column, so the same data always gives
   the same content hash.
2. Skip it if the hash matches the current snapshot: reruns are safe and
   create no new versions.
3. Check the schema against the Iceberg table (the schema guard):
   - new columns are added (schema evolution)
   - safe type widening is allowed: int to bigint, float to double, a
     decimal with more digits and the same scale
   - a dropped column or any other type change stops the publish with a
     list of every problem, because it would break readers
4. Overwrite the table in one atomic commit. Readers see the old or the new
   version, never a mix. Every snapshot records the dbt invocation id
   (`publish-id`) and the content hash, and every table, changed or not,
   records the latest publish that covered it (`last-publish-id`).
5. Expire snapshots older than the last `keep_publishes` publishes.

Each table commits on its own: Iceberg has no multi-table transaction.
scripts/check_iceberg.py checks that every table's last-publish-id is the same.

Usage:
    python scripts/publish_iceberg.py                 # after dbt build
    python scripts/publish_iceberg.py --database other.duckdb
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import yaml
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError, ResolveError
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import Schema, promote
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, MonthTransform, YearTransform

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "ops" / "lakehouse.yml"
DEFAULT_DATABASE = ROOT / "ecom.duckdb"
DEFAULT_RUN_RESULTS = ROOT / "target" / "run_results.json"
TRANSFORMS = {"day": DayTransform, "month": MonthTransform, "year": YearTransform}


class SchemaGuardError(RuntimeError):
    """The new Gold schema would break readers of the Iceberg table."""

    def __init__(self, table: str, problems: list[str]) -> None:
        self.table, self.problems = table, problems
        super().__init__(f"{table}: " + "; ".join(problems))


@dataclass
class Outcome:
    table: str
    action: str  # created, updated, unchanged
    rows: int
    snapshot_id: int | None
    added_columns: list[str] = field(default_factory=list)
    widened_columns: list[str] = field(default_factory=list)
    expired_snapshots: int = 0


def load_config(path: Path = DEFAULT_CONFIG) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def open_catalog(config: dict[str, Any], root: Path = ROOT) -> Catalog:
    warehouse = (root / config["warehouse"]).resolve()
    catalog_db = (root / config["catalog"]).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "ecom", uri=f"sqlite:///{catalog_db}", warehouse=warehouse.as_uri()
    )
    catalog.create_namespace_if_not_exists(config["namespace"])
    return catalog


def read_gold(con: duckdb.DuckDBPyConnection, table: str) -> pa.Table:
    """The table, sorted by every column so its bytes are deterministic."""
    return con.sql(f"select * from gold.{table} order by all").to_arrow_table()


def content_hash(con: duckdb.DuckDBPyConnection, table: str) -> str:
    """SHA-256 of the column names and types and every row as text, in sorted order.

    Computed by DuckDB from the values, not from Arrow buffers: bytes under a
    NULL are undefined in Arrow and change between runs.
    """
    columns = con.execute(
        "select string_agg(column_name || ' ' || data_type, ', '"
        " order by ordinal_position) from information_schema.columns"
        " where table_schema = 'gold' and table_name = ?",
        [table],
    ).fetchall()[0][0]
    rows = con.execute(
        f"select coalesce(string_agg(t::varchar, chr(10) order by t::varchar), '')"
        f" from gold.{table} t"
    ).fetchall()[0][0]
    return hashlib.sha256(f"{columns}\n{rows}".encode()).hexdigest()


def publish_id(run_results: Path = DEFAULT_RUN_RESULTS) -> str:
    """The dbt invocation that built Gold, so every table can be traced to it."""
    if run_results.exists():
        return json.loads(run_results.read_text())["metadata"]["invocation_id"]
    return "manual"


def check_schema(table: str, current: Schema, new: Schema) -> tuple[list, list]:
    """(added, widened) column names; raises SchemaGuardError on a breaking change."""
    problems, added, widened = [], [], []
    new_fields = {f.name: f for f in new.fields}
    for old in current.fields:
        if old.name not in new_fields:
            problems.append(f"column {old.name} was dropped")
            continue
        new_type = new_fields[old.name].field_type
        if new_type == old.field_type:
            continue
        try:
            promote(old.field_type, new_type)
            widened.append(old.name)
        except ResolveError:
            problems.append(
                f"column {old.name} changed type {old.field_type} -> {new_type}"
            )
    old_names = {f.name for f in current.fields}
    added = [f.name for f in new.fields if f.name not in old_names]
    if problems:
        raise SchemaGuardError(table, problems)
    return added, widened


def snapshot_publish_ids(table: Table) -> list[tuple[int, str]]:
    """(snapshot id, publish id) oldest first."""
    out = []
    for snap in sorted(table.snapshots(), key=lambda s: s.timestamp_ms):
        props = snap.summary.additional_properties if snap.summary else {}
        out.append((snap.snapshot_id, props.get("publish-id", "")))
    return out


def expire_old(table: Table, keep_publishes: int) -> int:
    """Expire snapshots of publishes older than the last `keep_publishes`."""
    history = snapshot_publish_ids(table)
    publishes: list[str] = []
    for _, pid in history:
        if pid not in publishes:
            publishes.append(pid)
    keep = set(publishes[-keep_publishes:])
    current = table.current_snapshot()
    old = [
        sid
        for sid, pid in history
        if pid not in keep and (current is None or sid != current.snapshot_id)
    ]
    if old:
        table.maintenance.expire_snapshots().by_ids(old).commit()
    return len(old)


def confirm_publish(table: Table, pid: str) -> None:
    """Record on the table that publish `pid` covered it, even if no data changed.

    A metadata-only commit (no new snapshot), so check_iceberg.py can confirm
    every table reflects the same dbt run.
    """
    if table.properties.get("last-publish-id") != pid:
        with table.transaction() as tx:
            tx.set_properties({"last-publish-id": pid})


def create_table(catalog: Catalog, name: str, schema: pa.Schema, spec: dict | None):
    table = catalog.create_table(name, schema=schema)
    if spec:
        transform = TRANSFORMS[spec["transform"]]()
        with table.update_spec() as update:
            update.add_field(
                spec["column"], transform, f"{spec['column']}_{spec['transform']}"
            )
    return table


def publish_table(
    catalog: Catalog,
    con: duckdb.DuckDBPyConnection,
    config: dict[str, Any],
    table: str,
    pid: str,
) -> Outcome:
    data = read_gold(con, table)
    digest = content_hash(con, table)
    name = f"{config['namespace']}.{table}"
    spec = (config["tables"].get(table) or {}).get("partition_by")
    new_schema = _pyarrow_to_schema_without_ids(data.schema)

    try:
        iceberg = catalog.load_table(name)
        action = "updated"
    except NoSuchTableError:
        iceberg = create_table(catalog, name, data.schema, spec)
        action = "created"

    current = iceberg.current_snapshot()
    props = current.summary.additional_properties if current and current.summary else {}
    if current is not None and props.get("content-sha256") == digest:
        confirm_publish(iceberg, pid)
        return Outcome(table, "unchanged", data.num_rows, current.snapshot_id)

    added, widened = check_schema(table, iceberg.schema(), new_schema)
    if added or widened:
        with iceberg.update_schema() as update:
            update.union_by_name(data.schema)

    with warnings.catch_warnings():
        # The first overwrite of a new, empty table has nothing to delete
        warnings.filterwarnings("ignore", "Delete operation did not match any records")
        iceberg.overwrite(
            data,
            snapshot_properties={
                "publish-id": pid,
                "content-sha256": digest,
                "source": f"ecom.duckdb gold.{table}",
            },
        )
    confirm_publish(iceberg, pid)
    expired = expire_old(iceberg, int(config["keep_publishes"]))
    snapshot = iceberg.current_snapshot()
    return Outcome(
        table,
        action,
        data.num_rows,
        snapshot.snapshot_id if snapshot else None,
        added,
        widened,
        expired,
    )


def publish(
    database: Path = DEFAULT_DATABASE,
    config: dict[str, Any] | None = None,
    root: Path = ROOT,
    pid: str | None = None,
) -> list[Outcome]:
    """Publish every configured table. Checks all schemas before writing any."""
    config = config or load_config()
    catalog = open_catalog(config, root)
    pid = pid or publish_id()
    con = duckdb.connect(str(database), read_only=True)
    try:
        # Guard every table first, so a breaking change leaves all tables untouched
        problems: list[str] = []
        for table in config["tables"]:
            try:
                existing = catalog.load_table(f"{config['namespace']}.{table}")
            except NoSuchTableError:
                continue
            new = _pyarrow_to_schema_without_ids(read_gold(con, table).schema)
            try:
                check_schema(table, existing.schema(), new)
            except SchemaGuardError as err:
                problems += [f"{table}: {p}" for p in err.problems]
        if problems:
            raise SchemaGuardError("gold", problems)
        return [publish_table(catalog, con, config, t, pid) for t in config["tables"]]
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args(argv)
    if not args.database.exists():
        print(f"Missing {args.database}: run `make build` first.", file=sys.stderr)
        return 2
    try:
        outcomes = publish(args.database, load_config(args.config))
    except SchemaGuardError as err:
        print("Schema guard: not published, nothing was changed.", file=sys.stderr)
        for problem in err.problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print(f"{'table':<31} {'action':<10} {'rows':>6}  notes")
    for o in outcomes:
        notes = []
        if o.added_columns:
            notes.append("added " + ", ".join(o.added_columns))
        if o.widened_columns:
            notes.append("widened " + ", ".join(o.widened_columns))
        if o.expired_snapshots:
            notes.append(f"expired {o.expired_snapshots} old snapshots")
        print(f"{o.table:<31} {o.action:<10} {o.rows:>6}  {'; '.join(notes)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
