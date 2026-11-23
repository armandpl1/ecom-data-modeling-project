#!/usr/bin/env python3
"""Stop hook: lint the SQL files changed in this working tree, once per turn.

sqlfluff with the dbt templater compiles the whole project (~25 s), too slow
to run after every edit. Running it once when Claude is about to stop catches
style problems before the change is handed back.

Exit 2 sends the output to Claude and keeps it working. If Claude is already
continuing because of this hook (stop_hook_active), report but don't block
again, so it can never loop.
"""

import json
import os
import subprocess
import sys

SQL_DIRS = ("models/", "tests/", "macros/", "analyses/")


def changed_sql_files() -> list[str]:
    tracked = subprocess.run(
        ["git", "diff", "--name-only", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    ).stdout.split()
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        check=False,
        capture_output=True,
        text=True,
    ).stdout.split()
    return sorted(
        f
        for f in set(tracked + untracked)
        if f.endswith(".sql") and f.startswith(SQL_DIRS) and os.path.exists(f)
    )


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        payload = {}
    files = changed_sql_files()
    if not files:
        return 0

    env = {**os.environ, "UV_PROJECT_ENVIRONMENT": "deenv"}
    env.pop("VIRTUAL_ENV", None)
    result = subprocess.run(
        ["uv", "run", "sqlfluff", "lint", *files],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode == 0:
        return 0

    print(
        "sqlfluff found style problems in the changed SQL "
        "(`uv run sqlfluff fix <file>` applies most fixes):\n" + result.stdout[-4000:],
        file=sys.stderr,
    )
    return 0 if payload.get("stop_hook_active") else 2


if __name__ == "__main__":
    sys.exit(main())
