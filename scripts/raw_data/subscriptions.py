"""Subscriptions and their month-by-month changes.

At most one subscription per customer, for a share of customers. Each
subscription optionally starts with a 14-day trial, then is simulated month
by month: upgrades/downgrades, failed payments (past_due), pauses, churn, and
the one-time price increase. Some start dates are backdated by mistake and
corrected.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from .config import PLAN_ORDER, PLAN_PRICES_NEW, PLAN_PRICES_OLD, REPRICE_POINT
from .customers import Customer
from .history import History


def mrr(plan: str, period: str, ts: datetime, reprice_at: datetime) -> float:
    """Monthly recurring revenue for a plan at time ts.

    Annual plans cost 10 months' worth per year, so their MRR is lower.
    """
    price = (PLAN_PRICES_NEW if ts >= reprice_at else PLAN_PRICES_OLD)[plan]
    return price if period == "monthly" else round(price * 10 / 12, 2)


def generate_subscriptions(
    h: History, share: float, customers: list[Customer]
) -> list[dict]:
    """Generate subscriptions for a share of customers. Returns CSV rows."""
    rng, rows = h.rng, []
    reprice_at = h.start + (h.end - h.start) * REPRICE_POINT
    subscribers = rng.sample(customers, int(len(customers) * share))
    subscribers.sort(key=lambda c: c.created)

    sub_no = 0
    for customer in subscribers:
        # The subscription can't outlive the customer. Most people
        # subscribe within a couple of months of signing up.
        limit = customer.deleted or h.end
        t = customer.created + timedelta(days=rng.expovariate(1 / 60))
        if t >= limit:
            continue
        sub_no += 1
        plan = rng.choices(PLAN_ORDER, [55, 35, 10])[0]
        period = rng.choices(["monthly", "annual"], [75, 25])[0]
        trial = rng.random() < 0.6
        state = {
            "subscription_id": f"S{sub_no:05d}",
            "customer_id": customer.id,
            "plan_name": plan,
            "billing_period": period,
            "mrr": mrr(plan, period, t, reprice_at),
            "status": "trialing" if trial else "active",
            "started_at": t.date(),
            "trial_ends_at": (t + timedelta(days=14)).date() if trial else None,
            "cancelled_at": None,
            "created_at": t,
        }

        # Backdated start date, later corrected.
        if rng.random() < 0.05:
            bad = {
                **state,
                "started_at": state["started_at"]
                - timedelta(days=rng.choice([1, 30, 31])),
            }
            h.emit_corrected(rows, bad, state, t)
        else:
            h.emit(rows, state, t, "I")

        if trial:
            t += timedelta(days=14, minutes=rng.uniform(0, 120))
            if t >= limit:
                continue
            if rng.random() < 0.75:
                state["status"] = "active"
                h.emit(rows, state, t)
            else:
                state["status"] = "cancelled"
                state["cancelled_at"] = t
                h.emit(rows, state, t)
                continue

        # Monthly simulation. Each month at most one thing happens; the
        # chances below are per month (cancel 2.5%, plan change 4%,
        # failed payment 2%, pause 1%). Paused subs resume 40%/month.
        repriced = state["created_at"] >= reprice_at
        while True:
            nxt = t + timedelta(days=30, hours=rng.uniform(-12, 12))
            if not repriced and nxt >= reprice_at and state["status"] != "cancelled":
                repriced = True
                if reprice_at < limit:
                    state["mrr"] = mrr(
                        state["plan_name"],
                        state["billing_period"],
                        reprice_at,
                        reprice_at,
                    )
                    h.emit(
                        rows, state, reprice_at + timedelta(minutes=rng.uniform(0, 90))
                    )
            t = nxt
            if t >= limit:
                break

            r = rng.random()
            if state["status"] == "paused":
                if r < 0.4:
                    state["status"] = "active"
                    h.emit(rows, state, t)
                continue
            if r < 0.025:
                state["status"] = "cancelled"
                state["cancelled_at"] = t
                h.emit(rows, state, t)
                break
            elif r < 0.065:  # plan change, mostly upgrades
                idx = PLAN_ORDER.index(state["plan_name"])
                idx = min(idx + 1, 2) if rng.random() < 0.7 else max(idx - 1, 0)
                if PLAN_ORDER[idx] != state["plan_name"]:
                    state["plan_name"] = PLAN_ORDER[idx]
                    state["mrr"] = mrr(
                        state["plan_name"], state["billing_period"], t, reprice_at
                    )
                    h.emit(rows, state, t)
            elif r < 0.085:  # failed payment
                state["status"] = "past_due"
                h.emit(rows, state, t)
                t += timedelta(days=rng.uniform(3, 10))
                if rng.random() < 0.7:
                    state["status"] = "active"
                else:
                    state["status"] = "cancelled"
                    state["cancelled_at"] = t
                h.emit(rows, state, t)
                if state["status"] == "cancelled":
                    break
            elif r < 0.095:
                state["status"] = "paused"
                h.emit(rows, state, t)

        # A deleted customer's live subscription is cancelled just before deletion.
        if customer.deleted and state["status"] != "cancelled":
            ts = customer.deleted - timedelta(minutes=rng.uniform(1, 30))
            state["status"] = "cancelled"
            state["cancelled_at"] = ts
            h.emit(rows, state, ts)
    return rows
