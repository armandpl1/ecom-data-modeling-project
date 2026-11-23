---
name: ship-pr
description: Commit, push a feature branch and open a pull request in this repo the way AGENTS.md requires (branch off main, verified, clean commit message, no AI attribution, never push main or force push). Use only when the owner asks to commit, push or open a PR.
disable-model-invocation: true
---

# Ship a pull request

Only run this when the owner asks. Commits and PRs appear as the owner.

## 1. Preconditions

```bash
git fetch origin
git branch --show-current          # must not be main
git log --oneline origin/main..HEAD
git status --short
```

- On `main`? Create a branch first: `git switch -c <type>/<topic> origin/main`
  (`feat/`, `fix/`, `docs/`, `chore/`).
- Verification done in this session (`verify-before-commit` skill); docs
  updated (`update-project-guides` skill). If not, do those first.
- Optional: run the `standards-reviewer` agent on the branch.

## 2. Commit

Stage files by name. Never `git add -f`, never stage `GAP_PLAN.md`,
`STUDY_GUIDE.md`, `study_guide/` or `.env.databricks`.

```bash
git add <files>
git commit -F - <<'EOF'
Short summary in the imperative

- What changed and why
- What changed and why
EOF
```

No `Co-Authored-By`, no "Generated with" lines. Pre-commit hooks run on
commit; if they fail, fix the cause and commit again (no `--no-verify`).

## 3. Push and open the PR

```bash
git push -u origin HEAD
gh pr create --base main --title "<summary line>" --body-file - <<'EOF'
## What
- ...

## Why
- ...

## Verified
- make ci: <result line>
- <other checks and their results>
EOF
```

The required checks are `Lint`, `Unit tests (data generator)` and
`dbt build and test`. Watch them with `gh pr checks --watch`.

## Never

- Push to `main`, force push, or rewrite published history without the
  owner's explicit go-ahead.
- Merge the PR yourself unless asked.
