"""Tests for scripts/load_raw.py: append-only loads, safe reruns, atomic batches."""

from pathlib import Path

import duckdb
import pytest
from load_raw import TABLES, load_all, load_file, sha256_of

HEADER = "customer_id,updated_at,_op,_loaded_at\n"


def write_extracts(raw_dir: Path, rows: int = 2) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        lines = [
            f"{table[0]}{i},2026-01-0{i + 1} 00:00:00,I,2026-01-02 00:00:00\n"
            for i in range(rows)
        ]
        (raw_dir / f"{table}.csv").write_text(HEADER + "".join(lines))


def count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return con.execute(sql).fetchone()[0]


def test_first_load_creates_tables_and_logs_each_batch(tmp_path: Path) -> None:
    write_extracts(tmp_path / "raw")
    results = load_all(tmp_path / "raw", tmp_path / "w.duckdb")
    assert [r.loaded for r in results] == [True] * len(TABLES)
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        assert count(con, "select count(*) from raw.customers") == 2
        assert count(con, "select count(*) from raw._load_log") == len(TABLES)
        row = con.execute(
            "select _batch_id, _file_name, _ingested_at is not null "
            "from raw.orders limit 1"
        ).fetchone()
    checksum = sha256_of(tmp_path / "raw" / "orders.csv")
    assert row == (f"orders-{checksum[:12]}", "orders.csv", True)


def test_rerun_skips_files_already_loaded(tmp_path: Path) -> None:
    write_extracts(tmp_path / "raw")
    load_all(tmp_path / "raw", tmp_path / "w.duckdb")
    again = load_all(tmp_path / "raw", tmp_path / "w.duckdb")
    assert [r.loaded for r in again] == [False] * len(TABLES)
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        assert count(con, "select count(*) from raw.customers") == 2
        assert count(con, "select count(*) from raw._load_log") == len(TABLES)


def test_a_new_extract_is_appended_not_replaced(tmp_path: Path) -> None:
    write_extracts(tmp_path / "raw", rows=2)
    load_all(tmp_path / "raw", tmp_path / "w.duckdb")
    write_extracts(tmp_path / "raw", rows=3)  # a new nightly file
    results = load_all(tmp_path / "raw", tmp_path / "w.duckdb")
    assert all(r.loaded for r in results)
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        assert (
            count(con, "select count(*) from raw.customers") == 5
        )  # 2 + 3, history kept
        assert count(con, "select count(distinct _batch_id) from raw.customers") == 2


def test_columns_are_matched_by_name(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    write_extracts(raw)
    load_all(raw, tmp_path / "w.duckdb")
    (raw / "customers.csv").write_text(
        "_loaded_at,_op,updated_at,customer_id\n"
        "2026-02-01 00:00:00,U,2026-02-01 00:00:00,c9\n"
    )
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        load_file(con, "customers", raw / "customers.csv")
        assert con.execute(
            "select _op, updated_at from raw.customers where customer_id = 'c9'"
        ).fetchone() == ("U", "2026-02-01 00:00:00")


def test_a_failed_load_leaves_no_partial_batch(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    write_extracts(raw)
    load_all(raw, tmp_path / "w.duckdb")
    (raw / "customers.csv").write_text("customer_id,an_unknown_column\nc9,x\n")
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        with pytest.raises(duckdb.Error):
            load_file(con, "customers", raw / "customers.csv")
        assert count(con, "select count(*) from raw.customers") == 2
        assert count(con, "select count(*) from raw._load_log") == len(TABLES)


def test_missing_extracts_fail_with_a_clear_message(tmp_path: Path) -> None:
    (tmp_path / "raw").mkdir()
    with pytest.raises(FileNotFoundError, match="generate_raw_data"):
        load_all(tmp_path / "raw", tmp_path / "w.duckdb")
