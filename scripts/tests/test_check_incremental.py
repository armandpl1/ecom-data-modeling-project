"""Tests for scripts/check_incremental.py: batch split, fingerprints, comparison."""

import csv
from pathlib import Path

import duckdb
import pytest
from check_incremental import compare, split_extracts, table_fingerprints
from load_raw import TABLES

HEADER = ["customer_id", "updated_at", "_op", "_loaded_at"]


def write_source(source: Path, loaded_at: list[str]) -> None:
    source.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        with (source / f"{table}.csv").open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(HEADER)
            for i, ts in enumerate(loaded_at):
                writer.writerow([f"{table[0]}{i}", ts, "I", ts])


def test_split_puts_the_last_days_in_batch_two(tmp_path: Path) -> None:
    loads = ["2026-01-01 02:00:00", "2026-01-20 02:00:00", "2026-01-31 02:00:00"]
    write_source(tmp_path / "src", loads)
    counts = split_extracts(tmp_path / "src", tmp_path, batch2_days=15)
    assert counts == {t: [1, 2] for t in TABLES}
    batch2 = (tmp_path / "batch2" / "orders.csv").read_text().splitlines()
    assert batch2[0] == ",".join(HEADER)
    assert len(batch2) == 3


def test_split_refuses_an_empty_batch(tmp_path: Path) -> None:
    write_source(tmp_path / "src", ["2026-01-01 02:00:00", "2026-01-02 02:00:00"])
    with pytest.raises(ValueError, match="empty"):
        split_extracts(tmp_path / "src", tmp_path, batch2_days=30)


def make_warehouse(path: Path, rows: list[tuple]) -> Path:
    with duckdb.connect(str(path)) as con:
        con.execute("create schema gold")
        con.execute("create table gold.fact (id integer, amount decimal(10, 2))")
        con.executemany("insert into gold.fact values (?, ?)", rows)
        con.execute("create schema silver")  # not compared
        con.execute("create table silver.other as select 1 as x")
    return path


def test_identical_warehouses_match_regardless_of_row_order(tmp_path: Path) -> None:
    a = table_fingerprints(make_warehouse(tmp_path / "a.duckdb", [(1, 9.5), (2, 3.0)]))
    b = table_fingerprints(make_warehouse(tmp_path / "b.duckdb", [(2, 3.0), (1, 9.5)]))
    assert list(a) == ["gold.fact"]
    assert compare(a, b) == []


def test_differences_name_the_table_and_columns(tmp_path: Path) -> None:
    a = table_fingerprints(make_warehouse(tmp_path / "a.duckdb", [(1, 9.5), (2, 3.0)]))
    b = table_fingerprints(make_warehouse(tmp_path / "b.duckdb", [(1, 9.5), (2, 4.0)]))
    c = table_fingerprints(make_warehouse(tmp_path / "c.duckdb", [(1, 9.5)]))
    assert compare(a, b) == ["gold.fact: columns differ: amount"]
    assert "gold.fact: 2 rows incremental vs 1 full" in compare(a, c)
    assert compare(a, {}) == ["gold.fact: only in incremental"]
