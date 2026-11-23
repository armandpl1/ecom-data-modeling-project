---
name: add-dbt-model
description: Checklist and YAML templates for adding or changing a dbt model in this repo (Bronze stg_*, Silver int_*, Gold dim_*/fact_*), so it ships with its description, grain, tests and docs. Use whenever a model under models/ is created or its columns, grain or logic change.
---

# Add or change a dbt model

## 1. Pick the layer and copy a template

| Layer  | Folder                    | Schema    | Prefix              | Copy from                                   |
| ------ | ------------------------- | --------- | ------------------- | ------------------------------------------- |
| Bronze | `models/staging/`         | bronze    | `stg_`              | `stg_products.sql` (incremental)            |
| Silver | `models/intermediate/`    | silver    | `int_`              | `int_products_history.sql` (SCD2)           |
| Gold   | `models/marts/`           | gold      | `dim_` / `fact_`    | `dim_product.sql`, `fact_order_headers.sql` |
| Report | `models/marts/reporting/` | reporting | none (named by use) | `customer_rfm.sql`                          |

Rules per layer:
- **Bronze** reads one `source('raw', ...)`, trims, types, dedups. Grain is
  one row per (record key, `updated_at`). No joins, no business rules.
- **Silver** holds business logic and SCD2 history (see the
  `add-scd2-history` skill). Reads Bronze or other Silver models.
- **Gold** is the star schema. Reads Silver. Facts point at dimension
  surrogate keys (`*_sk`) valid at event time; unknown versions use
  `unknown_member_key()`.

## 2. SQL conventions

- Start with a comment header: layer, what the model is, and for a fact,
  `-- Grain: one row per ...`.
- Import CTEs first (`select * from {{ ref(...) }}`), then logic CTEs, then
  a final select.
- Qualify columns inside subqueries.
- Lowercase, trailing commas, explicit `as`, aliases aligned per select
  (`.sqlfluff`). Run `uv run sqlfluff fix <file>`.

## 3. YAML in the same change

Put it in the folder's `_staging.yml` / `_intermediate.yml` / `_marts.yml`.
Gold models enforce contracts, so every column needs `data_type`.

```yaml
  - name: fact_example
    config:
      contract: {enforced: true}
    description: >
      What it measures. Grain: one row per <thing>.
    data_tests:
      - unique_combination_of_columns:
          arguments:
            combination_of_columns: [order_id, line_number]
    columns:
      - name: customer_sk
        data_type: string
        description: '{{ doc("scd2_sk") }}'
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_customer')
                field: customer_sk
          - unknown_member_rows:
              config:
                severity: warn
      - name: net_revenue
        data_type: decimal(12, 2)
        description: Revenue after discounts, in USD.
        data_tests: [not_negative]
```

Checklist:
- [ ] Model description (Gold: says the grain).
- [ ] Every Gold column has a description; reuse `models/docs.md` doc
      blocks (`scd2_sk`, `valid_from_timestamp`, `is_current`, `date_key`,
      `loaded_at`, ...). Add a new doc block if a definition repeats.
- [ ] Primary/surrogate key: `unique` + `not_null`.
- [ ] Facts: grain test (`unique_combination_of_columns`) and a
      `relationships` test for every dimension key.
- [ ] Business-rule tests for new logic: generic (`expression_is_true`,
      `not_negative`, `accepted_values`) or a singular test in `tests/`
      named `assert_<rule>.sql`.
- [ ] Unit tests for tricky logic in `models/marts/_unit_tests.yml`.
- [ ] `severity: warn` only for a documented, self-healing data gap.
- [ ] PII columns tagged `meta: {pii: true}`; raw PII never reaches Gold
      (`assert_no_raw_pii_in_gold`).

## 4. Build and verify

```bash
uv run dbt parse                       # fails fast on broken refs or YAML
uv run dbt build -s <model>+           # the model, its tests and everything downstream
uv run sqlfluff lint models/<path>.sql
```

Incremental models: also run `make check-incremental` (incremental run must
equal a full refresh). New Gold facts: consider whether
`models/semantic/` metrics or `models/marts/reporting/` need it.

## 5. Docs

Use the `update-project-guides` skill: README model list and lineage,
PROJECT_STEPS.txt for a new step.
