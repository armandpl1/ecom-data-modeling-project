"""Customer profiles and their changes.

Possible changes per customer: an error at signup that gets corrected (name
typo, messy email, wrong state, missing postal code), then address moves,
email/phone/last-name changes, marketing opt-in toggles, segment upgrades,
and a rare hard delete.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .config import CUSTOMER_SEGMENTS
from .history import History


@dataclass
class Customer:
    """What orders and subscriptions need to know about a customer."""

    id: str
    created: datetime
    deleted: datetime | None
    weight: float  # how often this customer orders, relative to others


# Updates over a customer's lifetime: each (kind, chance) pair rolls
# independently, so a customer can move twice, etc.
UPDATE_CHANCES = [
    ("move", 0.25),
    ("move", 0.08),
    ("email", 0.10),
    ("phone", 0.10),
    ("opt_in", 0.20),
    ("segment", 0.08),
    ("last_name", 0.04),
]


def new_address(h: History) -> dict:
    """A random US street address, as the four address columns."""
    return {
        "street_address": h.fake.street_address(),
        "city": h.fake.city(),
        "state": us_state(h),
        "postal_code": h.fake.postcode(),
    }


def us_state(h: History) -> str:
    """A 50-states-plus-DC abbreviation (no territories like PR or FM)."""
    return h.fake.state_abbr(
        include_territories=False, include_freely_associated_states=False
    )


def new_email(h: History, first: str, last: str) -> str:
    """An email built from the customer's name, e.g. jane.doe42@gmail.com."""
    suffix = str(h.rng.randint(1, 99)) if h.rng.random() < 0.4 else ""
    return f"{first}.{last}{suffix}@{h.fake.free_email_domain()}".lower()


def signup_error(h: History, state: dict) -> dict | None:
    """A wrong version of the signup row, or None. Chances add up to 14%."""
    r = h.rng.random()
    if r < 0.05:
        return {**state, "first_name": h.typo(state["first_name"])}
    if r < 0.09:
        return {**state, "email": f" {state['email'].upper()} "}
    if r < 0.12:
        return {**state, "state": us_state(h)}
    if r < 0.14:
        return {**state, "postal_code": None}
    return None


def apply_update(h: History, state: dict, kind: str) -> None:
    """Change the customer's state for one kind of update."""
    if kind == "move":
        state.update(new_address(h))
    elif kind == "email":
        state["email"] = new_email(h, state["first_name"], state["last_name"])
    elif kind == "phone":
        state["phone"] = h.fake.phone_number()
    elif kind == "opt_in":
        state["marketing_opt_in"] = not state["marketing_opt_in"]
    elif kind == "segment":  # move up one tier
        idx = CUSTOMER_SEGMENTS.index(state["customer_segment"])
        state["customer_segment"] = CUSTOMER_SEGMENTS[
            min(idx + 1, len(CUSTOMER_SEGMENTS) - 1)
        ]
    elif kind == "last_name":
        state["last_name"] = h.fake.last_name()


def generate_customers(h: History, n: int) -> tuple[list[dict], list[Customer]]:
    """Generate n customers. Returns (CSV rows, customer lookups)."""
    rng, fake, rows, customers = h.rng, h.fake, [], []
    span = h.end - h.start
    for i in range(1, n + 1):
        first, last = fake.first_name(), fake.last_name()
        created = h.start + span * (rng.random() ** 1.3)  # more signups early on
        state = {
            "customer_id": f"C{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": new_email(h, first, last),
            "phone": fake.phone_number() if rng.random() > 0.05 else None,
            **new_address(h),
            "country": "US",
            "customer_segment": rng.choices(CUSTOMER_SEGMENTS, [85, 12, 3])[0],
            "marketing_opt_in": rng.random() < 0.6,
            "created_at": created,
        }
        # A few emails stay messy forever; staging models should clean these.
        if rng.random() < 0.03:
            state["email"] = state["email"].title()

        # Insert, sometimes with an error that gets corrected.
        bad = signup_error(h, state)
        if bad and bad != state:  # a random "wrong" state can match the right one
            h.emit_corrected(rows, bad, state, created)
        else:
            h.emit(rows, state, created, "I")

        # Updates, applied in time order.
        events = [
            (h.between(created, h.end), kind)
            for kind, chance in UPDATE_CHANCES
            if rng.random() < chance
        ]
        for ts, kind in sorted(events):
            apply_update(h, state, kind)
            h.emit(rows, state, ts)

        # Right-to-be-forgotten deletes.
        deleted_at = None
        if rng.random() < 0.02:
            last_event = events[-1][0] if events else created
            deleted_at = h.between(max(last_event, created), h.end)
            h.emit(rows, state, deleted_at, "D")

        # A Pareto weight makes a few customers order far more often than most.
        customers.append(
            Customer(
                id=state["customer_id"],
                created=created,
                deleted=deleted_at,
                weight=rng.paretovariate(1.5),
            )
        )
    return rows, customers
