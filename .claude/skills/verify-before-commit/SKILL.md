---
name: verify-before-commit
description: Which real checks to run in this repo before committing, by kind of change (dbt model, macro, data generator, Python script, CI workflow, Terraform, Airflow, docs), and how to report the results. Use before any commit, and before writing a number into the docs.
---

# Verify before committing

AGENTS.md: verify with real runs, never report a result you didn't measure.
`make ci` is the baseline for every code change. It runs the same steps as
the three required CI checks: `Lint`, `Unit tests (data generator)`,
`dbt build and test`.

## By kind of change

| Change                               | Run                                                                         |
| ------------------------------------ | --------------------------------------------------------------------------- |
| Any code change                      | `make ci`                                                                   |
| dbt model / macro / test             | `uv run dbt build -s <model>+` while iterating, then `make ci`              |
| Incremental model or watermark logic | `make check-incremental`                                                    |
| SCD2 or fact-to-dimension joins      | `make check-late-dimensions`                                                |
| Gold table shape                     | `make iceberg && make check-iceberg`, `make check-slos`                     |
| Semantic layer (`models/semantic/`)  | `make metrics`                                                              |
| Data generator (`scripts/raw_data/`) | `make test`; fingerprints must stay identical unless the data should change |
| Other Python scripts                 | `make test` (tests mirror code under `scripts/tests/`)                      |
| `.github/workflows/*.yml`            | `uvx --from actionlint-py actionlint .github/workflows/ci.yml`              |
| `infra/terraform/`                   | `make infra-up && make infra-check && make infra-down`                      |
| `airflow/`                           | `make airflow-test` (after `make airflow-init` once)                        |
| `cdc/`                               | `make cdc-up`, `make cdc-replay`, `make cdc-load`, `make cdc-down`          |
| Cross-database / Databricks SQL      | `make check-databricks` (needs `.env.databricks`; ask the owner first)      |
| Docs only                            | Rerun every query or command behind a number you changed                    |

Never use `--target prod` locally. To try slim CI: `make prod-state` then
`make slim-build`.

## Reporting

Report what each command printed, not what you expect:

```
make ci                    OK  (dbt: PASS=<n> WARN=<n> ERROR=0 SKIP=0; pytest: <n> passed)
make check-incremental     OK
make check-databricks      not run (needs workspace credentials)
```

Any WARN must be one already documented as a known, self-healing gap;
otherwise treat it as a failure (use the `debug-dbt-test-failure` skill).

## Then commit

- Stage specific files, never `git add -f` (local-only files stay out:
  `GAP_PLAN.md`, `STUDY_GUIDE.md`, `study_guide/`, `.env.databricks`).
- Message: a short summary line, a blank line, then bullets saying what
  changed and why.
- No `Co-Authored-By` or "Generated with" lines.
