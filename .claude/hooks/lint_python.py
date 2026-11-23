#!/usr/bin/env python3
"""PostToolUse hook for Edit/Write: lint a Python file right after it changes.

Runs ruff and black --check (both fast) on the edited file. On failure it
exits 2, which shows the output to Claude so it fixes the file in the same
turn. flake8 and the full pre-commit run stay in `make lint`.
"""

import json
import os
import subprocess
import sys


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    path = payload.get("tool_input", {}).get("file_path", "")
    if not path.endswith(".py") or not os.path.exists(path):
        return 0

    env = {**os.environ, "UV_PROJECT_ENVIRONMENT": "deenv"}
    env.pop("VIRTUAL_ENV", None)
    problems = []
    for cmd in (
        ["uv", "run", "ruff", "check", path],
        ["uv", "run", "black", "--check", "-q", path],
    ):
        result = subprocess.run(
            cmd, check=False, capture_output=True, text=True, env=env
        )
        if result.returncode != 0:
            problems.append(
                f"$ {' '.join(cmd)}\n{result.stdout}{result.stderr}".strip()
            )

    if problems:
        print(
            "Python lint failed (fix with `uv run ruff check --fix` / `uv run black`):",
            file=sys.stderr,
        )
        print("\n\n".join(problems), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
