"""Publishing Gold to Iceberg: a tiny gold schema in a temporary DuckDB file."""

from pathlib import Path

import duckdb
import pytest
from check_iceberg import check
from publish_iceberg import (
    SchemaGuardError,
    open_catalog,
    publish,
    snapshot_publish_ids,
)


def config(keep: int = 5) -> dict:
    return {
        "warehouse": "lake",
        "catalog": "lake/catalog.db",
        "namespace": "gold",
        "keep_publishes": keep,
        "tables": {
            "dim_thing": {},
            "fact_sale": {"partition_by": {"column": "sold_at", "transform": "month"}},
        },
    }


def make_gold(path: Path, extra_sale: bool = False) -> Path:
    con = duckdb.connect(str(path))
    con.execute("create schema if not exists gold")
    con.execute(
        "create or replace table gold.dim_thing as"
        " select * from (values (1, 'a'), (2, 'b'), (3, null)) t(thing_id, name)"
    )
    rows = (
        "(10, 1, timestamp '2026-01-05 10:00', 9.99),"
        " (11, 2, timestamp '2026-02-01 08:00', 5.00)"
    )
    if extra_sale:
        rows += ", (12, 3, timestamp '2026-03-03 12:00', 1.50)"
    con.execute(
        "create or replace table gold.fact_sale as select thing_id_ as sale_id,"
        " thing_id, sold_at, cast(amount as decimal(10,2)) as amount"
        f" from (values {rows}) t(thing_id_, thing_id, sold_at, amount)"
    )
    con.close()
    return path


@pytest.fixture
def lake(tmp_path: Path) -> tuple[Path, Path]:
    return tmp_path, make_gold(tmp_path / "w.duckdb")


def actions(outcomes) -> dict:
    return {o.table: o.action for o in outcomes}


def test_publish_creates_partitioned_tables_and_reruns_are_noops(lake) -> None:
    root, db = lake
    first = publish(db, config(), root, pid="run-1")
    assert actions(first) == {"dim_thing": "created", "fact_sale": "created"}
    again = publish(db, config(), root, pid="run-2")
    assert actions(again) == {"dim_thing": "unchanged", "fact_sale": "unchanged"}

    sale = open_catalog(config(), root).load_table("gold.fact_sale")
    assert len(sale.inspect.partitions()) == 2  # January and February
    assert {pid for _, pid in snapshot_publish_ids(sale)} == {"run-1"}


def test_changed_data_is_a_new_snapshot_and_time_travel_works(lake) -> None:
    root, db = lake
    publish(db, config(), root, pid="run-1")
    make_gold(db, extra_sale=True)
    out = publish(db, config(), root, pid="run-2")
    assert actions(out) == {"dim_thing": "unchanged", "fact_sale": "updated"}

    results = {r.table: r for r in check(db, config(), root)}
    sale = results["fact_sale"]
    assert sale.identical and sale.iceberg_rows == 3
    assert sale.previous_rows == 2 and sale.publishes_kept == 2


def test_new_column_and_widening_evolve_the_schema(lake) -> None:
    root, db = lake
    publish(db, config(), root, pid="run-1")
    con = duckdb.connect(str(db))
    con.execute(
        "create or replace table gold.dim_thing as select cast(thing_id as bigint) as"
        " thing_id, name, 'red' as color from (values (1, 'a'), (2, 'b'), (3, null))"
        " t(thing_id, name)"
    )
    con.close()
    out = {o.table: o for o in publish(db, config(), root, pid="run-2")}
    assert out["dim_thing"].added_columns == ["color"]
    assert out["dim_thing"].widened_columns == ["thing_id"]
    assert all(r.identical for r in check(db, config(), root))


def test_breaking_changes_stop_the_whole_publish(lake) -> None:
    root, db = lake
    publish(db, config(), root, pid="run-1")
    con = duckdb.connect(str(db))
    con.execute("alter table gold.dim_thing drop column name")
    con.execute("alter table gold.fact_sale alter amount type varchar")
    con.execute(
        "insert into gold.fact_sale values (99, 1, timestamp '2026-04-01', '1')"
    )
    con.close()
    with pytest.raises(SchemaGuardError) as err:
        publish(db, config(), root, pid="run-2")
    assert "dim_thing: column name was dropped" in err.value.problems
    assert any("fact_sale: column amount changed type" in p for p in err.value.problems)
    # Nothing was written: both tables still hold run-1 only
    catalog = open_catalog(config(), root)
    for name in ("gold.dim_thing", "gold.fact_sale"):
        pids = {pid for _, pid in snapshot_publish_ids(catalog.load_table(name))}
        assert pids == {"run-1"}


def test_old_publishes_are_expired(lake) -> None:
    root, db = lake
    for run in range(4):
        make_gold(db, extra_sale=run % 2 == 1)
        publish(db, config(keep=2), root, pid=f"run-{run}")
    sale = open_catalog(config(), root).load_table("gold.fact_sale")
    assert {pid for _, pid in snapshot_publish_ids(sale)} == {"run-2", "run-3"}


def test_unchanged_tables_are_confirmed_by_the_new_publish(lake) -> None:
    root, db = lake
    publish(db, config(), root, pid="run-1")
    make_gold(db, extra_sale=True)
    publish(db, config(), root, pid="run-2")  # only fact_sale's data changes
    ids = {r.table: r.publish_id for r in check(db, config(), root)}
    assert ids == {"dim_thing": "run-2", "fact_sale": "run-2"}
    # dim_thing's data still comes from run-1's snapshot: no copy was written
    thing = open_catalog(config(), root).load_table("gold.dim_thing")
    assert {pid for _, pid in snapshot_publish_ids(thing)} == {"run-1"}


def test_check_flags_a_table_left_behind(lake) -> None:
    root, db = lake
    publish(db, config(), root, pid="run-1")
    small = config()
    small["tables"] = {"fact_sale": {}}
    make_gold(db, extra_sale=True)
    publish(db, small, root, pid="run-2")  # dim_thing not covered by run-2
    ids = {r.table: r.publish_id for r in check(db, config(), root)}
    assert ids == {"dim_thing": "run-1", "fact_sale": "run-2"}
