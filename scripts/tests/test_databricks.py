"""Tests for the Databricks scripts' SQL building (no workspace needed)."""

from compare_engines import MASKED, text_select
from databricks_load_raw import copy_into_sql, landing_name


def test_landing_name_carries_the_checksum() -> None:
    assert (
        landing_name("orders", "8e47996be8f2" + "0" * 52) == "orders-8e47996be8f2.csv"
    )


def test_copy_into_loads_text_with_lineage() -> None:
    sql = copy_into_sql("ecom", "orders")
    assert sql.startswith("COPY INTO ecom.raw.orders FROM (SELECT *,")
    assert "FROM '/Volumes/ecom/raw/landing/orders/'" in sql
    assert "'inferSchema' = 'false'" in sql  # raw stays text, like DuckDB
    assert "_metadata.file_name as _file_name" in sql
    assert "concat('orders-'" in sql  # batch id from the checksum in the name


def test_text_select_casts_every_column() -> None:
    sql = text_select(["a", "b"], "gold.t")
    assert sql == "select cast(`a` as string), cast(`b` as string) from gold.t"


def test_masked_columns_cover_the_raw_pii() -> None:
    assert {"email", "phone_number", "street_address"} <= MASKED[
        "restricted.dim_customer_pii"
    ]
