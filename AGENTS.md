# AGENTS.md

Standards for anyone changing this repository, human or AI agent. Follow them
on every change.

## Workflow

1. **Branch off the latest `main`.** `main` is protected: changes go through a
   pull request, and the checks `Lint`, `Unit tests (data generator)` and
   `dbt build and test` must pass.
2. **Plan, build, verify, document, then commit**, in that order.
3. **Verify with real runs, not assumptions.** Run `dbt build`, the tests and
   the queries behind any number you write down. Never report a result that
   wasn't measured.
4. **Before committing:** `make ci` (lint, unit tests, dbt build).
5. **Commit messages:** a short summary line, a blank line, then bullet points
   saying what changed and why.
6. **No AI attribution.** No `Co-Authored-By` or "Generated with" lines in
   commits or pull requests. Commits appear as the repository owner only.
7. **Never rewrite published history** (force push) without the owner's explicit
   go-ahead.

## dbt and data modeling standards

- **Layers:** Bronze (`models/staging`, schema `bronze`) cleans and types;
  Silver (`models/intermediate`) holds business logic and SCD2 history; Gold
  (`models/marts`) is the Kimball star schema.
- **Every model has a description**, and every Gold column has one.
  Definitions used in several places go in `models/docs.md` as doc blocks.
- **Every model has tests:** keys unique and not null, facts unique at their
  declared grain, every fact key checked against its dimension. Add
  business-rule tests for new logic.
- **Declare the grain** of every fact table in its SQL comment and YAML.
- **SCD2:** build history from the source change log with the
  `scd2_from_history` macro. Validity ranges are half-open
  `[valid_from_timestamp, valid_to_timestamp)` and are checked by the
  `scd2_valid_ranges` test.
- **Known data gaps warn; broken rules fail.** Use `severity: warn` only for
  a documented, self-healing data issue.
- **SQL style:** sqlfluff with the rules in `.sqlfluff` (lowercase, trailing
  commas, explicit `as`, aliases aligned per select), also a pre-commit hook.
  `uv run sqlfluff fix <file>` applies most fixes. Qualify columns in
  subqueries so they can't bind to the outer query by accident.

## Python standards

- **Environment:** Python 3.11, dependencies managed with uv in `./deenv`
  (`export UV_PROJECT_ENVIRONMENT=deenv`), pinned in `uv.lock`. Add packages
  with `uv add`, never pip.
- **Linting:** ruff, black and flake8, run by pre-commit hooks (`make hooks`).
- **The data generator** (`scripts/raw_data/`) must be deterministic: the same
  seed gives byte-identical CSVs. `scripts/tests/test_generate_raw_data.py`
  pins their fingerprints. A refactor must keep them identical; update them
  only when the data is meant to change, and say so.
- **Tests mirror the code:** `scripts/raw_data/<module>.py` is tested in
  `scripts/tests/raw_data/test_<module>.py`.

## Documentation standards

- **README.md** is the source of truth for design, commands and results.
- **Keep the guides current** when a step changes:
  - `PROJECT_STEPS.txt`: what was built and how to check it
  - `LEARNING_GUIDE.txt`: how to learn the repo
  - `IMPROVEMENTS.txt`: gaps and recommended solutions
- **Plain language:** short sentences, no jargon without explanation.
- **Markdown tables** have their `|` characters aligned in the raw file.

## CI/CD standards

- **Pin actions to full commit SHAs,** with the version in a comment.
- **Least privilege:** read-only token by default. Only `deploy-docs` may write
  to Pages.
- **Every job has a timeout** and uploads what's needed to debug a failure.
- **Don't rename CI job names** without also updating the branch ruleset's
  required checks.
- **Validate workflow changes** with actionlint:
  `uvx --from actionlint-py actionlint .github/workflows/ci.yml`
- **Merging to `main` redeploys the docs site:**
  https://armandpl1.github.io/ecom-data-modeling-project/

## Harnesses

| Tool                                  | Use it to                                                   |
| ------------------------------------- | ----------------------------------------------------------- |
| `make help`                           | List every command                                          |
| `make ci`                             | Run everything CI runs, locally                             |
| `dbt build`                           | Build every model and run every data and unit test          |
| `dbt docs generate && dbt docs serve` | Browse model docs and the lineage graph                     |
| `pytest`                              | Test the data generator, including its output fingerprint   |
| `analyses/`                           | Ad-hoc SQL backing README claims (e.g. the 28% SCD2 impact) |
| `test_failures` schema                | Inspect the failing rows of any dbt test with plain SQL     |
