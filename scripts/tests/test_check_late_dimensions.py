"""Tests for scripts/check_late_dimensions.py: customer choice, split, verdict."""

import csv
from pathlib import Path

from check_late_dimensions import FACTS, pick_customer, problems, split
from load_raw import TABLES


def write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def make_extract(root: Path) -> Path:
    write(
        root / "customers.csv",
        ["customer_id", "x"],
        [["C1", "a"], ["C2", "b"], ["C2", "c"]],
    )
    write(root / "products.csv", ["product_id"], [["P1"]])
    orders = [
        ["O1", "C1"],
        ["O2", "C2"],
        ["O3", "C2"],
        ["O4", "C3"],
        ["O5", "C3"],
        ["O6", "C3"],
    ]
    write(root / "orders.csv", ["order_id", "customer_id"], orders)
    write(root / "order_lines.csv", ["order_line_id", "order_id"], [["O1-1", "O1"]])
    write(
        root / "subscriptions.csv",
        ["subscription_id", "customer_id"],
        [["S1", "C1"], ["S2", "C2"]],
    )
    return root


def test_picks_the_subscriber_with_the_most_orders(tmp_path: Path) -> None:
    # C3 has more orders but no subscription, so it can't exercise every fact
    assert pick_customer(make_extract(tmp_path / "x")) == "C2"


def test_split_holds_back_only_the_customer_rows(tmp_path: Path) -> None:
    extract = make_extract(tmp_path / "x")
    info = split(extract, tmp_path, "C2")
    assert info == {"customer_id": "C2", "late_customer_rows": 2}
    batch1 = (tmp_path / "batch1" / "customers.csv").read_text().splitlines()
    batch2 = (tmp_path / "batch2" / "customers.csv").read_text().splitlines()
    assert batch1 == ["customer_id,x", "C1,a"]
    assert batch2 == ["customer_id,x", "C2,b", "C2,c"]
    for table in TABLES:
        if table != "customers":  # identical bytes: the second load skips them
            one = (tmp_path / "batch1" / f"{table}.csv").read_bytes()
            assert one == (tmp_path / "batch2" / f"{table}.csv").read_bytes()


def good_report() -> dict:
    keys = [f"{f}.{k}" for f, ks in FACTS.items() for k in ks]
    on_unknown = {k: (3 if k.endswith("customer_sk") else 0) for k in keys}
    return {
        "customer_fact_rows": dict.fromkeys(FACTS, 3),
        "unknown_for_customer_after_load1": on_unknown,
        "unknown_after_load1": dict(on_unknown),
        "warned_after_load1": {f"t{i}": 3 for i in range(len(FACTS))},
        "unknown_after_load2": dict.fromkeys(keys, 0),
        "differences": [],
    }


def test_a_clean_run_has_no_problems() -> None:
    assert problems(good_report()) == []


def test_rows_left_on_the_unknown_member_are_a_problem() -> None:
    report = good_report()
    report["unknown_after_load2"]["fact_orders.customer_sk"] = 2
    assert any("still on -1" in p for p in problems(report))


def test_other_customers_on_the_unknown_member_are_a_problem() -> None:
    report = good_report()
    report["unknown_after_load1"]["fact_orders.customer_sk"] = 5
    assert any("other customers" in p for p in problems(report))


def test_a_missing_warning_or_a_rebuild_difference_is_a_problem() -> None:
    report = good_report()
    report["warned_after_load1"] = {}
    report["differences"] = ["gold.fact_orders: row count 1 vs 2"]
    found = problems(report)
    assert any("one warning per fact" in p for p in found)
    assert "gold.fact_orders: row count 1 vs 2" in found
