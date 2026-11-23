---
name: debug-dbt-test-failure
description: Find the cause of a failing or warning dbt test in this repo by reading the stored failing rows in the test_failures schema, the compiled test SQL and the upstream models. Use when dbt build or dbt test reports FAIL, WARN or ERROR.
---

# Debug a failing dbt test

Every data test stores its failing rows as a view in the `test_failures`
schema of `ecom.duckdb` (`+store_failures: true` in `dbt_project.yml`; off
on Databricks).

## 1. Get the failing test's name

From the build output, or:

```bash
uv run dbt build -s <model>+ 2>&1 | grep -E "FAIL|WARN|ERROR"
```

## 2. Look at the failing rows

```bash
# list the stored failure views
uv run dbt show --inline "select table_name from information_schema.tables where table_schema = 'test_failures' order by 1" --limit 200

# read one
uv run dbt show --inline "select * from test_failures.<test_view_name>" --limit 20
```

The view name is the test's name (long names are shortened with a hash);
`dbt show` truncates wide cells, so filter the list, e.g.
`... and table_name like 'scd2_valid_ranges%'`.

## 3. Read what the test actually checks

```bash
uv run dbt compile -s <test_name>
ls target/compiled/ecom/models/**/  # or tests/ for singular tests
```

Generic tests live in `tests/generic/` (`scd2_valid_ranges`,
`unique_combination_of_columns`, `unknown_member_rows`, `not_negative`,
`expression_is_true`). Singular tests are `tests/assert_*.sql`; each has a
comment header saying which rule it guards.

## 4. Trace upstream

Take one failing key and follow it down the lineage with `dbt show`:
Gold → Silver → Bronze → `raw.<table>`. The first layer where the row looks
wrong is where the bug is.

```bash
uv run dbt ls -s +<model> --resource-type model   # upstream models
```

## 5. Decide

- **Logic bug** → fix the model, rebuild `-s <model>+`.
- **Test is wrong** (checks the wrong grain or column) → fix the test and
  say why in the commit.
- **Known, self-healing data gap** (e.g. facts arriving before their
  dimension) → only then `severity: warn`, and document it in the model
  description and IMPROVEMENTS.txt.

Never delete a test or loosen it just to get a green build.

## Common causes here

| Symptom                               | Usual cause                                                      |
| ------------------------------------- | ---------------------------------------------------------------- |
| `scd2_valid_ranges` overlap           | duplicate `updated_at` per key not deduplicated in Bronze        |
| fact grain not unique                 | join to a dimension without the validity-range condition         |
| `relationships` fails on `*_sk`       | fact built before dimension rebuilt; rebuild with `+` selector   |
| `unknown_member_rows` warns           | early-arriving facts: expected, see `make check-late-dimensions` |
| incremental differs from full refresh | watermark filter too narrow; see `incremental_lookback_hours`    |
