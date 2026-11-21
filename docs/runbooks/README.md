# Runbooks

One runbook per alert. Every service-level objective in `ops/slos.yml` names
its runbook, and the Dagster alert message includes it.

| Alert                                    | Runbook                                          |
| ---------------------------------------- | ------------------------------------------------ |
| `raw_freshness` breached                 | [raw-freshness.md](raw-freshness.md)             |
| `batch_volume` breached                  | [batch-volume.md](batch-volume.md)               |
| `warehouse_caught_up` breached           | [warehouse-caught-up.md](warehouse-caught-up.md) |
| `dbt_tests` breached, or dbt build fails | [dbt-tests.md](dbt-tests.md)                     |
| Any other failed `daily_pipeline` run    | [pipeline-failure.md](pipeline-failure.md)       |

Check every objective at any time:

```bash
make check-slos                                   # relative to the latest load
python scripts/check_slos.py --reference now      # relative to the clock
```

All queries below run against the warehouse with the DuckDB CLI or Python:
`python -c "import duckdb; print(duckdb.connect('ecom.duckdb', read_only=True).sql('<query>'))"`.
