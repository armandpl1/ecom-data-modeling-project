"""The CDC mechanics shared by every table.

A History holds the random sources and the simulated time range, and knows
how to write one version of a record as a CDC row: with updated_at, _op and
_loaded_at, a realistic nightly load time, the occasional late arrival,
bad-then-corrected values, and duplicate deliveries.

All randomness goes through history.rng and history.fake, both seeded, so the
same seed and arguments always produce the same files. The table generators
must call them in a fixed order for that to hold.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta

from faker import Faker


class History:
    def __init__(
        self, seed: int, start: date, end: date, late_rate: float, dup_rate: float
    ):
        """
        Args:
            seed: random seed for reproducible output.
            start: first day of the simulated history.
            end: last day of the simulated history. Changes after this don't exist.
            late_rate: share of rows that load 1-14 days later than normal.
            dup_rate: share of rows delivered a second time.
        """
        self.seed = seed
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)
        self.start = datetime.combine(start, time(0, 0))
        self.end = datetime.combine(end, time(23, 59, 59))
        # The extract is taken right after the last nightly batch.
        self.as_of = datetime.combine(end + timedelta(days=1), time(3, 0))
        self.late_rate = late_rate
        self.dup_rate = dup_rate

    def substream(self, offset: int) -> History:
        """A History with the same dates and rates but its own random source.

        Lets a generator add data (like extra order lines) without changing a
        single draw from this History, so the other tables stay identical.
        """
        return History(
            self.seed + offset,
            self.start.date(),
            self.end.date(),
            self.late_rate,
            self.dup_rate,
        )

    def between(self, a: datetime, b: datetime) -> datetime:
        """Random timestamp between a and b. Returns a if the range is empty."""
        if b <= a:
            return a
        return a + timedelta(seconds=self.rng.uniform(0, (b - a).total_seconds()))

    def load_time(self, ts: datetime) -> datetime:
        """Nightly batch at ~02:00 the next day; a few rows arrive days late."""
        loaded = datetime.combine(ts.date() + timedelta(days=1), time(2, 0))
        loaded += timedelta(minutes=self.rng.uniform(0, 30))
        if self.rng.random() < self.late_rate:
            loaded += timedelta(days=self.rng.randint(1, 14))
        return loaded

    def emit(
        self,
        rows: list,
        state: dict,
        ts: datetime,
        op: str = "U",
        loaded: datetime | None = None,
    ) -> datetime | None:
        """Append one version of a record. Skips changes that are in the future
        or haven't been loaded yet as of the extract time."""
        if ts > self.end:
            return None
        loaded = loaded or self.load_time(ts)
        if loaded > self.as_of:
            return None
        rows.append({**state, "updated_at": ts, "_op": op, "_loaded_at": loaded})
        return loaded

    def emit_corrected(
        self, rows: list, bad: dict, good: dict, ts: datetime, op: str = "I"
    ) -> datetime:
        """Write a bad version, then its correction. Returns the correction time.

        70% of fixes get a new updated_at a few hours later. The other 30% reuse
        the bad row's updated_at and only differ by a later _loaded_at.
        """
        bad_loaded = self.emit(rows, bad, ts, op)
        if self.rng.random() < 0.7 or bad_loaded is None:
            fix_ts = ts + timedelta(hours=self.rng.uniform(1, 48))
            self.emit(rows, good, fix_ts, "U")
            return fix_ts
        fix_loaded = bad_loaded + timedelta(days=self.rng.randint(1, 5))
        self.emit(rows, good, ts, "U", loaded=fix_loaded)
        return ts

    def typo(self, s: str) -> str:
        """Swap two neighboring characters, e.g. "Daniel" -> "Dainel"."""
        if len(s) < 3:
            return s + s[-1]
        i = self.rng.randrange(len(s) - 1)
        return s[:i] + s[i + 1] + s[i] + s[i + 2 :]

    def add_duplicates(self, rows: list[dict]) -> list[dict]:
        """Re-deliver some rows, either in the same batch or a later one."""
        dups = []
        for row in self.rng.sample(rows, int(len(rows) * self.dup_rate)):
            dup = dict(row)
            if self.rng.random() < 0.5:
                dup["_loaded_at"] = row["_loaded_at"] + timedelta(
                    days=self.rng.randint(1, 3)
                )
                if dup["_loaded_at"] > self.as_of:
                    continue
            dups.append(dup)
        return rows + dups
