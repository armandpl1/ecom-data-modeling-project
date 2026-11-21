"""Tests for scripts/export_metrics.py (the mf query calls are not run here)."""

import csv
from pathlib import Path

import pytest
from export_metrics import DATASETS, clean, main, query_command


def test_query_command_orders_by_the_group_by() -> None:
    cmd = query_command(["mrr", "churn_rate"], ["metric_time__month"], Path("o.csv"))
    assert cmd[:3] == ["mf", "query", "--quiet"]
    assert cmd[cmd.index("--metrics") + 1] == "mrr,churn_rate"
    assert cmd[cmd.index("--order") + 1] == "metric_time__month"
    assert cmd[cmd.index("--csv") + 1] == "o.csv"


def test_every_dataset_groups_by_time() -> None:
    for metrics, group_by in DATASETS.values():
        assert metrics
        assert group_by[0].startswith("metric_time__")


def test_clean_drops_rows_without_metric_values(tmp_path: Path) -> None:
    path = tmp_path / "m.csv"
    path.write_text(
        "metric_time__month,mrr,churn_rate\n"
        "2026-10-01,4162,0.07\n"
        "2026-11-01,,\n"  # the offset month churn_rate adds, with no data yet
        "2026-09-01,4113.53,\n"
    )
    assert clean(path) == 2
    with path.open(newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["metric_time__month", "mrr", "churn_rate"]
    assert [r[0] for r in rows[1:]] == ["2026-10-01", "2026-09-01"]


def test_main_fails_when_mf_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import export_metrics

    class Failed:
        returncode = 1
        stdout = "no warehouse"
        stderr = ""

    monkeypatch.setattr(export_metrics, "OUT_DIR", tmp_path)
    monkeypatch.setattr(export_metrics.subprocess, "run", lambda *a, **k: Failed())
    assert main() == 1
