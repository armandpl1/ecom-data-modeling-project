"""CSV formatting and output."""

from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path

from .config import TS_FMT


def fmt(value) -> str:
    """Format a value for CSV.

    None becomes empty, booleans true/false, dates ISO, floats 2 decimals.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.strftime(TS_FMT)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    """Write rows to path with the given column order."""
    # Files are ordered the way they'd arrive: by load batch, then change time.
    rows = sorted(rows, key=lambda r: (r["_loaded_at"], r["updated_at"]))
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: fmt(row.get(c)) for c in columns})
