"""Tests for raw_data/history.py: the CDC mechanics."""

from datetime import datetime, time, timedelta

from conftest import make_history


def test_between_stays_in_range_and_handles_empty_range(history):
    a, b = datetime(2024, 3, 1), datetime(2024, 3, 2)
    assert all(a <= history.between(a, b) <= b for _ in range(100))
    assert history.between(b, a) == b


def test_load_time_is_the_next_nightly_batch(history):
    ts = datetime(2024, 5, 10, 15, 30)
    loaded = history.load_time(ts)
    batch = datetime.combine(ts.date() + timedelta(days=1), time(2, 0))
    assert batch <= loaded <= batch + timedelta(minutes=30)


def test_late_rows_load_days_later():
    h = make_history(late_rate=1.0)
    ts = datetime(2024, 5, 10, 15, 30)
    assert h.load_time(ts) >= datetime(2024, 5, 12, 2, 0)


def test_emit_adds_metadata_and_skips_future_changes(history):
    rows = []
    loaded = history.emit(rows, {"id": 1}, datetime(2024, 6, 1), "I")
    assert rows == [
        {"id": 1, "updated_at": datetime(2024, 6, 1), "_op": "I", "_loaded_at": loaded}
    ]
    assert history.emit(rows, {"id": 1}, datetime(2025, 1, 5)) is None  # after end
    assert len(rows) == 1


def test_emit_skips_rows_not_loaded_by_the_extract(history):
    rows = []
    # Last day of the range, but forced to load after the extract is taken
    late = history.as_of + timedelta(hours=1)
    assert history.emit(rows, {"id": 1}, datetime(2024, 12, 31), loaded=late) is None
    assert rows == []


def test_emit_corrected_writes_bad_then_good_version(history):
    for _ in range(20):
        rows = []
        ts = datetime(2024, 6, 1, 12)
        fixed_at = history.emit_corrected(rows, {"v": "bad"}, {"v": "good"}, ts)
        bad, good = rows
        assert (bad["v"], bad["_op"]) == ("bad", "I")
        assert (good["v"], good["_op"]) == ("good", "U")
        assert good["updated_at"] == fixed_at >= ts
        # A fix with the same updated_at must load later, to break the tie
        assert (good["updated_at"], good["_loaded_at"]) > (
            bad["updated_at"],
            bad["_loaded_at"],
        )


def test_typo_swaps_two_neighboring_characters(history):
    word = "Daniel"
    typo = history.typo(word)
    assert typo != word and sorted(typo) == sorted(word)
    assert history.typo("Al") == "All"


def test_add_duplicates_redelivers_a_share_of_rows():
    h = make_history(dup_rate=0.2)
    rows = []
    for day in range(1, 101):
        h.emit(rows, {"id": day}, datetime(2024, 3, 1) + timedelta(days=day))
    result = h.add_duplicates(rows)
    assert result[: len(rows)] == rows
    extra = result[len(rows) :]
    assert 0 < len(extra) <= 20
    originals = {r["id"]: r for r in rows}
    for dup in extra:
        original = originals[dup["id"]]
        assert dup["updated_at"] == original["updated_at"]
        assert dup["_loaded_at"] >= original["_loaded_at"]
