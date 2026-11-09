#!/usr/bin/env python3
"""Generate raw e-commerce source data with change history, for dbt practice.

Writes four CSVs that look like append-only extracts from a source system
(CDC-style): every change to a record is a new row, never an in-place edit.

    customers.csv      one row per customer version
    products.csv       one row per product version
    orders.csv         one row per order status change
    subscriptions.csv  one row per subscription version

Metadata columns on every file:
    updated_at   when the change happened in the source system
    _op          I = insert, U = update, D = delete
    _loaded_at   when the row landed in the warehouse (nightly batch)

What the history contains:
    Updates       customers move, change email/phone/name/segment, opt out;
                  product prices change and products get discontinued;
                  orders move placed -> shipped -> delivered (or cancelled,
                  returned); subscriptions trial, upgrade, pause, churn, and
                  get repriced.
    Corrections   a bad value is written, then fixed a few hours later
                  (price typos, name typos, messy emails, wrong quantities).
                  Some fixes keep the same updated_at as the bad row and only
                  arrive in a later load, so _loaded_at is needed to break ties.
    Late arrivals some rows load days after they happened, out of order.
    Duplicates    some rows are re-delivered, identically or in a later batch.
    Deletes       a few customers (GDPR) and products are hard-deleted.

Usage:
    python scripts/generate_raw_data.py
    python scripts/generate_raw_data.py --customers 2000 --orders 15000 --seed 7
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

from faker import Faker

TS_FMT = "%Y-%m-%d %H:%M:%S"

CATEGORIES = {
    # category: (product nouns, min price, max price)
    "Electronics": (["Headphones", "Speaker", "Charger", "Keyboard", "Mouse", "Monitor"], 15, 400),
    "Home": (["Lamp", "Blanket", "Mug", "Vase", "Cushion", "Candle"], 8, 150),
    "Outdoors": (["Tent", "Backpack", "Water Bottle", "Hammock", "Headlamp"], 10, 300),
    "Apparel": (["Hoodie", "T-Shirt", "Jacket", "Cap", "Socks"], 5, 180),
    "Books": (["Notebook", "Planner", "Cookbook", "Novel"], 5, 60),
}
ADJECTIVES = ["Classic", "Pro", "Ultra", "Eco", "Smart", "Compact", "Deluxe", "Everyday", "Travel"]

PLAN_PRICES_OLD = {"basic": 9.99, "plus": 19.99, "premium": 49.99}
PLAN_PRICES_NEW = {"basic": 11.99, "plus": 24.99, "premium": 59.99}
PLAN_ORDER = ["basic", "plus", "premium"]

COLUMNS = {
    "customers": [
        "customer_id", "first_name", "last_name", "email", "phone", "street_address",
        "city", "state", "postal_code", "country", "customer_segment", "marketing_opt_in",
        "created_at", "updated_at", "_op", "_loaded_at",
    ],
    "products": [
        "product_id", "sku", "product_name", "category", "unit_price", "currency",
        "is_active", "created_at", "updated_at", "_op", "_loaded_at",
    ],
    "orders": [
        "order_id", "customer_id", "product_id", "quantity", "unit_price", "discount_pct",
        "order_total", "currency", "payment_method", "status", "order_placed_at",
        "updated_at", "_op", "_loaded_at",
    ],
    "subscriptions": [
        "subscription_id", "customer_id", "plan_name", "billing_period", "mrr", "status",
        "started_at", "trial_ends_at", "cancelled_at", "created_at", "updated_at", "_op",
        "_loaded_at",
    ],
}


class Generator:
    def __init__(self, seed: int, start: date, end: date, late_rate: float, dup_rate: float):
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)
        self.start = datetime.combine(start, time(0, 0))
        self.end = datetime.combine(end, time(23, 59, 59))
        # The extract is taken right after the last nightly batch.
        self.as_of = datetime.combine(end + timedelta(days=1), time(3, 0))
        self.late_rate = late_rate
        self.dup_rate = dup_rate
        self.reprice_at = self.start + (self.end - self.start) * 0.6

        self.customers: list[dict] = []
        self.products: list[dict] = []

    # ---------- helpers ----------

    def between(self, a: datetime, b: datetime) -> datetime:
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

    def emit(self, rows: list, state: dict, ts: datetime, op: str = "U",
             loaded: datetime | None = None) -> datetime | None:
        """Append one version of a record. Skips changes that are in the future
        or haven't been loaded yet as of the extract time."""
        if ts > self.end:
            return None
        loaded = loaded or self.load_time(ts)
        if loaded > self.as_of:
            return None
        rows.append({**state, "updated_at": ts, "_op": op, "_loaded_at": loaded})
        return loaded

    def emit_corrected(self, rows: list, bad: dict, good: dict, ts: datetime, op: str = "I") -> datetime:
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
        if len(s) < 3:
            return s + s[-1]
        i = self.rng.randrange(len(s) - 1)
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]

    @staticmethod
    def price_point(x: float) -> float:
        return max(1, int(x)) + 0.99

    # ---------- products ----------

    def gen_products(self, n: int) -> list[dict]:
        rng, rows = self.rng, []
        span = self.end - self.start
        for i in range(1, n + 1):
            category = rng.choice(list(CATEGORIES))
            nouns, lo, hi = CATEGORIES[category]
            price = self.price_point(rng.uniform(lo, hi))
            if rng.random() < 0.8:  # most of the catalog exists from the start
                created = self.start + span * rng.uniform(0, 0.05)
            else:
                created = self.between(self.start, self.start + span * 0.8)

            state = {
                "product_id": f"P{i:04d}",
                "sku": f"{category[:3].upper()}-{rng.randint(10000, 99999)}",
                "product_name": f"{rng.choice(ADJECTIVES)} {rng.choice(nouns)} - {self.fake.color_name()}",
                "category": category,
                "unit_price": price,
                "currency": "USD",
                "is_active": True,
                "created_at": created,
            }
            product = {"id": state["product_id"], "created": created, "inactive_at": None,
                       "prices": [(created, price)]}

            r = rng.random()
            if r < 0.10:  # price typo: shifted decimal point
                bad_price = round(price * 10, 2) if rng.random() < 0.5 else round(price / 10, 2)
                fixed_at = self.emit_corrected(rows, {**state, "unit_price": bad_price}, state, created)
                # Orders placed before the fix really were charged the bad price.
                product["prices"] = [(created, bad_price), (fixed_at, price)]
            elif r < 0.18:  # name typo
                bad = {**state, "product_name": self.typo(state["product_name"])}
                self.emit_corrected(rows, bad, state, created)
            else:
                self.emit(rows, state, created, "I")

            # Price changes over time.
            changes = sorted(self.between(created, self.end) for _ in range(rng.choice([0, 0, 1, 1, 2, 3])))
            for ts in changes:
                state["unit_price"] = self.price_point(state["unit_price"] * rng.uniform(0.85, 1.25))
                self.emit(rows, state, ts)
                product["prices"].append((ts, state["unit_price"]))

            # Discontinued, and sometimes hard-deleted afterwards.
            if rng.random() < 0.10:
                last = changes[-1] if changes else created
                ts = self.between(last, self.end)
                state["is_active"] = False
                self.emit(rows, state, ts)
                product["inactive_at"] = ts
                if rng.random() < 0.3:
                    self.emit(rows, state, self.between(ts, self.end), "D")

            self.products.append(product)
        return rows

    def price_at(self, product: dict, ts: datetime) -> float:
        price = product["prices"][0][1]
        for changed_at, p in sorted(product["prices"]):
            if changed_at <= ts:
                price = p
        return price

    # ---------- customers ----------

    def new_address(self) -> dict:
        return {
            "street_address": self.fake.street_address(),
            "city": self.fake.city(),
            "state": self.us_state(),
            "postal_code": self.fake.postcode(),
        }

    def us_state(self) -> str:
        return self.fake.state_abbr(include_territories=False, include_freely_associated_states=False)

    def new_email(self, first: str, last: str) -> str:
        suffix = str(self.rng.randint(1, 99)) if self.rng.random() < 0.4 else ""
        return f"{first}.{last}{suffix}@{self.fake.free_email_domain()}".lower()

    def gen_customers(self, n: int) -> list[dict]:
        rng, fake, rows = self.rng, self.fake, []
        span = self.end - self.start
        for i in range(1, n + 1):
            first, last = fake.first_name(), fake.last_name()
            created = self.start + span * (rng.random() ** 1.3)  # more signups early on
            state = {
                "customer_id": f"C{i:05d}",
                "first_name": first,
                "last_name": last,
                "email": self.new_email(first, last),
                "phone": fake.phone_number() if rng.random() > 0.05 else None,
                **self.new_address(),
                "country": "US",
                "customer_segment": rng.choices(["consumer", "small_business", "enterprise"], [85, 12, 3])[0],
                "marketing_opt_in": rng.random() < 0.6,
                "created_at": created,
            }
            # A few emails stay messy forever; staging models should clean these.
            if rng.random() < 0.03:
                state["email"] = state["email"].title()

            # Insert, sometimes with an error that gets corrected.
            r = rng.random()
            if r < 0.05:
                bad = {**state, "first_name": self.typo(first)}
            elif r < 0.09:
                bad = {**state, "email": f" {state['email'].upper()} "}
            elif r < 0.12:
                bad = {**state, "state": self.us_state()}
            elif r < 0.14:
                bad = {**state, "postal_code": None}
            else:
                bad = None
            if bad and bad != state:
                self.emit_corrected(rows, bad, state, created)
            else:
                self.emit(rows, state, created, "I")

            # Updates over the customer's lifetime.
            events = []
            for kind, p in [("move", 0.25), ("move", 0.08), ("email", 0.10), ("phone", 0.10),
                            ("opt_in", 0.20), ("segment", 0.08), ("last_name", 0.04)]:
                if rng.random() < p:
                    events.append((self.between(created, self.end), kind))
            for ts, kind in sorted(events):
                if kind == "move":
                    state.update(self.new_address())
                elif kind == "email":
                    state["email"] = self.new_email(state["first_name"], state["last_name"])
                elif kind == "phone":
                    state["phone"] = fake.phone_number()
                elif kind == "opt_in":
                    state["marketing_opt_in"] = not state["marketing_opt_in"]
                elif kind == "segment":
                    idx = ["consumer", "small_business", "enterprise"].index(state["customer_segment"])
                    state["customer_segment"] = ["consumer", "small_business", "enterprise"][min(idx + 1, 2)]
                elif kind == "last_name":
                    state["last_name"] = fake.last_name()
                self.emit(rows, state, ts)

            # Right-to-be-forgotten deletes.
            deleted_at = None
            if rng.random() < 0.02:
                last_event = events[-1][0] if events else created
                deleted_at = self.between(max(last_event, created), self.end)
                self.emit(rows, state, deleted_at, "D")

            self.customers.append({"id": state["customer_id"], "created": created,
                                   "deleted": deleted_at, "weight": rng.paretovariate(1.5)})
        return rows

    # ---------- orders ----------

    def gen_orders(self, n: int) -> list[dict]:
        rng, rows = self.rng, []
        weights = [c["weight"] for c in self.customers]

        # Pick order times first so order_ids increase with time.
        placed = []
        while len(placed) < n:
            c = rng.choices(self.customers, weights)[0]
            ts = self.between(c["created"], c["deleted"] or self.end)
            available = [p for p in self.products
                         if p["created"] <= ts and (p["inactive_at"] is None or p["inactive_at"] > ts)]
            if available:
                placed.append((ts, c, rng.choice(available)))
        placed.sort(key=lambda x: x[0])

        for i, (ts, customer, product) in enumerate(placed, start=1):
            qty = rng.choices([1, 2, 3, 4, 5], [60, 20, 10, 6, 4])[0]
            discount = rng.choices([0.0, 0.05, 0.10, 0.20], [75, 10, 10, 5])[0]
            unit_price = self.price_at(product, ts)
            state = {
                "order_id": f"O{i:06d}",
                "customer_id": customer["id"],
                "product_id": product["id"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount_pct": discount,
                "order_total": round(qty * unit_price * (1 - discount), 2),
                "currency": "USD",
                "payment_method": rng.choices(["card", "paypal", "apple_pay", "gift_card"], [65, 20, 12, 3])[0],
                "status": "placed",
                "order_placed_at": ts,
            }

            # Wrong quantity keyed in, then fixed.
            if rng.random() < 0.04:
                bad_qty = qty + rng.randint(1, 9)
                bad = {**state, "quantity": bad_qty,
                       "order_total": round(bad_qty * unit_price * (1 - discount), 2)}
                t = self.emit_corrected(rows, bad, state, ts)
            else:
                self.emit(rows, state, ts, "I")
                t = ts

            # Status lifecycle. Changes after the end date don't exist yet.
            if rng.random() < 0.06:
                state["status"] = "cancelled"
                self.emit(rows, state, t + timedelta(hours=rng.uniform(1, 48)))
                continue
            t += timedelta(days=rng.uniform(0.5, 4))
            state["status"] = "shipped"
            self.emit(rows, state, t)
            t += timedelta(days=rng.uniform(2, 7))
            state["status"] = "delivered"
            self.emit(rows, state, t)

            if rng.random() < 0.02:  # goodwill discount applied after delivery
                t += timedelta(days=rng.uniform(1, 10))
                state["discount_pct"] = min(discount + 0.15, 0.5)
                state["order_total"] = round(qty * unit_price * (1 - state["discount_pct"]), 2)
                self.emit(rows, state, t)
            if rng.random() < 0.05:
                t += timedelta(days=rng.uniform(3, 30))
                state["status"] = "returned"
                self.emit(rows, state, t)
        return rows

    # ---------- subscriptions ----------

    def mrr(self, plan: str, period: str, ts: datetime) -> float:
        price = (PLAN_PRICES_NEW if ts >= self.reprice_at else PLAN_PRICES_OLD)[plan]
        return price if period == "monthly" else round(price * 10 / 12, 2)

    def gen_subscriptions(self, share: float) -> list[dict]:
        rng, rows = self.rng, []
        subscribers = rng.sample(self.customers, int(len(self.customers) * share))
        subscribers.sort(key=lambda c: c["created"])

        sub_no = 0
        for customer in subscribers:
            limit = customer["deleted"] or self.end
            t = customer["created"] + timedelta(days=rng.expovariate(1 / 60))
            if t >= limit:
                continue
            sub_no += 1
            plan = rng.choices(PLAN_ORDER, [55, 35, 10])[0]
            period = rng.choices(["monthly", "annual"], [75, 25])[0]
            trial = rng.random() < 0.6
            state = {
                "subscription_id": f"S{sub_no:05d}",
                "customer_id": customer["id"],
                "plan_name": plan,
                "billing_period": period,
                "mrr": self.mrr(plan, period, t),
                "status": "trialing" if trial else "active",
                "started_at": t.date(),
                "trial_ends_at": (t + timedelta(days=14)).date() if trial else None,
                "cancelled_at": None,
                "created_at": t,
            }

            # Backdated start date, later corrected.
            if rng.random() < 0.05:
                bad = {**state, "started_at": state["started_at"] - timedelta(days=rng.choice([1, 30, 31]))}
                self.emit_corrected(rows, bad, state, t)
            else:
                self.emit(rows, state, t, "I")

            if trial:
                t += timedelta(days=14, minutes=rng.uniform(0, 120))
                if t >= limit:
                    continue
                if rng.random() < 0.75:
                    state["status"] = "active"
                    self.emit(rows, state, t)
                else:
                    state["status"] = "cancelled"
                    state["cancelled_at"] = t
                    self.emit(rows, state, t)
                    continue

            repriced = state["created_at"] >= self.reprice_at
            while True:
                nxt = t + timedelta(days=30, hours=rng.uniform(-12, 12))
                if not repriced and nxt >= self.reprice_at and state["status"] != "cancelled":
                    repriced = True
                    if self.reprice_at < limit:
                        state["mrr"] = self.mrr(state["plan_name"], state["billing_period"], self.reprice_at)
                        self.emit(rows, state, self.reprice_at + timedelta(minutes=rng.uniform(0, 90)))
                t = nxt
                if t >= limit:
                    break

                r = rng.random()
                if state["status"] == "paused":
                    if r < 0.4:
                        state["status"] = "active"
                        self.emit(rows, state, t)
                    continue
                if r < 0.025:
                    state["status"] = "cancelled"
                    state["cancelled_at"] = t
                    self.emit(rows, state, t)
                    break
                elif r < 0.065:  # plan change, mostly upgrades
                    idx = PLAN_ORDER.index(state["plan_name"])
                    idx = min(idx + 1, 2) if rng.random() < 0.7 else max(idx - 1, 0)
                    if PLAN_ORDER[idx] != state["plan_name"]:
                        state["plan_name"] = PLAN_ORDER[idx]
                        state["mrr"] = self.mrr(state["plan_name"], state["billing_period"], t)
                        self.emit(rows, state, t)
                elif r < 0.085:  # failed payment
                    state["status"] = "past_due"
                    self.emit(rows, state, t)
                    t += timedelta(days=rng.uniform(3, 10))
                    if rng.random() < 0.7:
                        state["status"] = "active"
                    else:
                        state["status"] = "cancelled"
                        state["cancelled_at"] = t
                    self.emit(rows, state, t)
                    if state["status"] == "cancelled":
                        break
                elif r < 0.095:
                    state["status"] = "paused"
                    self.emit(rows, state, t)

            # A deleted customer's live subscription is cancelled just before deletion.
            if customer["deleted"] and state["status"] != "cancelled":
                ts = customer["deleted"] - timedelta(minutes=rng.uniform(1, 30))
                state["status"] = "cancelled"
                state["cancelled_at"] = ts
                self.emit(rows, state, ts)
        return rows

    # ---------- output ----------

    def add_duplicates(self, rows: list[dict]) -> list[dict]:
        """Re-deliver some rows, either in the same batch or a later one."""
        dups = []
        for row in self.rng.sample(rows, int(len(rows) * self.dup_rate)):
            dup = dict(row)
            if self.rng.random() < 0.5:
                dup["_loaded_at"] = row["_loaded_at"] + timedelta(days=self.rng.randint(1, 3))
                if dup["_loaded_at"] > self.as_of:
                    continue
            dups.append(dup)
        return rows + dups


def fmt(value) -> str:
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
    # Files are ordered the way they'd arrive: by load batch, then change time.
    rows = sorted(rows, key=lambda r: (r["_loaded_at"], r["updated_at"]))
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: fmt(row.get(c)) for c in columns})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=Path(__file__).resolve().parent.parent / "data" / "raw")
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--products", type=int, default=80)
    parser.add_argument("--orders", type=int, default=6000)
    parser.add_argument("--subscription-share", type=float, default=0.35,
                        help="share of customers who start a subscription")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 10, 31))
    parser.add_argument("--late-rate", type=float, default=0.03, help="share of rows that load days late")
    parser.add_argument("--dup-rate", type=float, default=0.01, help="share of rows re-delivered")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    gen = Generator(args.seed, args.start, args.end, args.late_rate, args.dup_rate)
    # Order matters: orders and subscriptions depend on customers and products.
    tables = {
        "products": gen.gen_products(args.products),
        "customers": gen.gen_customers(args.customers),
    }
    tables["orders"] = gen.gen_orders(args.orders)
    tables["subscriptions"] = gen.gen_subscriptions(args.subscription_share)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    id_col = {"customers": "customer_id", "products": "product_id",
              "orders": "order_id", "subscriptions": "subscription_id"}
    for name, rows in tables.items():
        rows = gen.add_duplicates(rows)
        path = args.out_dir / f"{name}.csv"
        write_csv(path, COLUMNS[name], rows)
        ids = len({r[id_col[name]] for r in rows})
        print(f"{path}: {len(rows):>6} rows, {ids:>5} distinct {id_col[name]}s")


if __name__ == "__main__":
    main()
