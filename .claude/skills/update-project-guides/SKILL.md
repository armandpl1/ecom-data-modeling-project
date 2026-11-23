---
name: update-project-guides
description: Which of this repo's documents to update for a given change (README.md, PROJECT_STEPS.txt, LEARNING_GUIDE.txt, IMPROVEMENTS.txt, models/docs.md, AGENTS.md) and the writing rules they follow (plain language, measured numbers, aligned Markdown tables). Use after a change is verified, before committing.
---

# Keep the docs current

README.md is the source of truth for design, commands and results.

## What to update

| Change                                 | Update                                                                         |
| -------------------------------------- | ------------------------------------------------------------------------------ |
| New model, column or test              | YAML descriptions; `models/docs.md` if a definition repeats; README model list |
| New or changed `make` target / command | README commands section; Makefile `##` help text                               |
| A new capability (a "step")            | PROJECT_STEPS.txt: new numbered step, what was built and how to check it       |
| Changed behaviour of an existing step  | That step in PROJECT_STEPS.txt, and any README results it moved                |
| A new concept a learner needs          | LEARNING_GUIDE.txt, in reading order                                           |
| A gap closed, or a new gap found       | IMPROVEMENTS.txt: mark done, or add problem + recommended solution             |
| New top-level folder or file           | README repository layout tree                                                  |
| New standard, agent, skill or hook     | AGENTS.md                                                                      |

## Writing rules

- Plain language: short sentences; explain jargon the first time it appears.
- Every number (row counts, percentages, timings) comes from a query or
  command you ran. Put the command next to it, or keep it in `analyses/`.
- Markdown tables: align the `|` characters in the raw file. Pad every cell
  in a column to the column's widest cell.
- Keep each document's existing structure and numbering. Don't renumber old
  steps.
- Don't touch local-only notes: `GAP_PLAN.md`, `STUDY_GUIDE.md`,
  `study_guide/`.

## Check

```bash
git diff --stat            # docs changed alongside code?
grep -n "TODO\|TBD" README.md PROJECT_STEPS.txt LEARNING_GUIDE.txt IMPROVEMENTS.txt
```

Merging to `main` redeploys the docs site
(https://armandpl1.github.io/ecom-data-modeling-project/), so model
descriptions are public documentation.
