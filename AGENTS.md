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
- **`--target prod` runs only in CI on `main`.** Locally, use the default
  `dev` target, or `make prod-state` and `make slim-build` to try slim CI.
- **Validate workflow changes** with actionlint:
  `uvx --from actionlint-py actionlint .github/workflows/ci.yml`
- **Merging to `main` redeploys the docs site:**
  https://armandpl1.github.io/ecom-data-modeling-project/

## Harness

The harness is everything that runs around an agent working in this repo:
the commands it verifies with, the settings and hooks that enforce the rules
above, and the subagents and skills that package the repo's workflows.
Claude Code loads it from `CLAUDE.md` (which imports this file) and
`.claude/`. Other agents should read this file and follow the same rules.

### Commands

| Tool                                  | Use it to                                                   |
| ------------------------------------- | ----------------------------------------------------------- |
| `make help`                           | List every command                                          |
| `make ci`                             | Run everything CI runs, locally                             |
| `dbt build`                           | Build every model and run every data and unit test          |
| `dbt docs generate && dbt docs serve` | Browse model docs and the lineage graph                     |
| `pytest`                              | Test the data generator, including its output fingerprint   |
| `analyses/`                           | Ad-hoc SQL backing README claims (e.g. the 28% SCD2 impact) |
| `test_failures` schema                | Inspect the failing rows of any dbt test with plain SQL     |

### Settings and hooks (`.claude/settings.json`)

These are enforced by Claude Code, not left to the agent's memory.

| Piece                                 | What it does                                                                                                     |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `env`                                 | Sets `UV_PROJECT_ENVIRONMENT=deenv` for every command                                                            |
| `permissions.allow`                   | Runs read-only and verify commands (`make ci`, `uv run dbt build`, `git diff`) without a prompt                  |
| `permissions.deny`                    | Never reads `.env.databricks`; never edits `uv.lock` by hand                                                     |
| `hooks/guard_bash.py` (before Bash)   | Blocks `--target prod`, force push, push to `main`, `git add -f`, pip, AI attribution, reading `.env.databricks` |
| `hooks/lint_python.py` (after edits)  | Runs ruff and black on each edited `.py` file and reports failures at once                                       |
| `hooks/lint_sql_on_stop.py` (on stop) | Runs sqlfluff once on all changed SQL before the agent hands back (dbt templater is too slow per edit)           |

Personal overrides go in `.claude/settings.local.json` (git-ignored).

### Subagents (`.claude/agents/`)

| Agent                | Use it to                                                    | Can edit |
| -------------------- | ------------------------------------------------------------ | -------- |
| `dbt-modeler`        | Add or change models, macros and tests under the layer rules | yes      |
| `verifier`           | Run the real checks and report measured results only         | no       |
| `standards-reviewer` | Review the branch against this file before a commit or PR    | no       |
| `docs-keeper`        | Update README and the guides after a verified change         | docs     |

### Skills (`.claude/skills/`)

| Skill                    | Use it when                                                      |
| ------------------------ | ---------------------------------------------------------------- |
| `add-dbt-model`          | Creating a model or changing its columns, grain or logic         |
| `add-scd2-history`       | Adding SCD2 history or changing tracked columns                  |
| `debug-dbt-test-failure` | `dbt build` reports FAIL, WARN or ERROR                          |
| `verify-before-commit`   | Choosing which checks a change needs, and reporting them         |
| `update-project-guides`  | Choosing which documents a change touches                        |
| `ship-pr`                | The owner asks to commit, push or open a PR (owner-invoked only) |

## Agent guidelines

1. **This file wins.** If a skill, agent prompt or habit disagrees with
   AGENTS.md, follow AGENTS.md and fix the skill.
2. **Follow the loop:** plan → build (`dbt-modeler`) → verify (`verifier`)
   → document (`docs-keeper`) → review (`standards-reviewer`) → commit only
   when the owner asks (`ship-pr`).
3. **Measured or nothing.** A number, a "tests pass" or a "works" claim needs
   a command run in the same session. Say "not run" for checks you skipped.
4. **Don't work around the harness.** If a hook blocks a command, change the
   approach; don't rephrase the command to slip past it. Never commit with
   `--no-verify`.
5. **Stay in scope.** Change what the task needs. Don't reformat unrelated
   files or change the data generator's output as a side effect.
6. **Ask before** anything outward-facing or hard to undo: pushing,
   opening or merging PRs, Databricks or AWS calls, deleting data
   (`make clean`, `make cdc-down`, `make infra-down`).
7. **Local-only files stay local:** `GAP_PLAN.md`, `STUDY_GUIDE.md`,
   `study_guide/` (in `.git/info/exclude`) and `.env.databricks`.
8. **Keep the harness current.** A new standard, command or workflow means
   updating this file and the matching skill in the same change.
