"""Tests for raw_data/writer.py."""

import csv
from datetime import date, datetime

from raw_data.writer import fmt, write_csv


def test_fmt_formats_each_type():
    assert fmt(None) == ""
    assert fmt(True) == "true" and fmt(False) == "false"
    assert fmt(datetime(2024, 5, 1, 9, 3, 7)) == "2024-05-01 09:03:07"
    assert fmt(date(2024, 5, 1)) == "2024-05-01"
    assert fmt(12.5) == "12.50"
    assert fmt(3) == "3"


def test_write_csv_orders_rows_by_load_then_change_time(tmp_path):
    rows = [
        {
            "id": "b",
            "updated_at": datetime(2024, 1, 2),
            "_loaded_at": datetime(2024, 1, 5),
        },
        {
            "id": "c",
            "updated_at": datetime(2024, 1, 3),
            "_loaded_at": datetime(2024, 1, 4),
        },
        {
            "id": "a",
            "updated_at": datetime(2024, 1, 1),
            "_loaded_at": datetime(2024, 1, 4),
        },
    ]
    path = tmp_path / "out.csv"
    write_csv(path, ["id", "updated_at", "_loaded_at"], rows)
    with path.open() as f:
        written = list(csv.DictReader(f))
    assert [r["id"] for r in written] == ["a", "c", "b"]
    assert written[0]["updated_at"] == "2024-01-01 00:00:00"
