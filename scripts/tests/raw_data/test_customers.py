"""Tests for raw_data/customers.py."""

from raw_data.config import CUSTOMER_SEGMENTS
from raw_data.customers import apply_update, new_email


def test_new_email_is_built_from_the_name(history):
    email = new_email(history, "Jane", "Doe")
    assert email.startswith("jane.doe") and "@" in email
    assert email == email.lower()


def test_segment_update_moves_up_one_tier_and_stops_at_the_top(history):
    state = {"customer_segment": "consumer"}
    apply_update(history, state, "segment")
    assert state["customer_segment"] == "small_business"
    state = {"customer_segment": "enterprise"}
    apply_update(history, state, "segment")
    assert state["customer_segment"] == "enterprise"


def test_generated_customers(small_dataset):
    rows, customers = small_dataset["customers"]
    assert len(customers) == 300
    assert {r["customer_segment"] for r in rows} <= set(CUSTOMER_SEGMENTS)
    for customer in customers:
        history = [r for r in rows if r["customer_id"] == customer.id]
        assert history[0]["_op"] == "I"
        assert customer.weight >= 1  # Pareto weights start at 1
        deletes = [r for r in history if r["_op"] == "D"]
        if customer.deleted:
            assert deletes and deletes[0]["updated_at"] == customer.deleted
        else:
            assert not deletes
