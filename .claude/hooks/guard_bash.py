#!/usr/bin/env python3
"""PreToolUse hook for Bash: block commands that break the AGENTS.md rules.

Claude Code sends the tool call as JSON on stdin. Exit code 2 blocks the
command and shows the message on stderr to Claude, so it can change course.
Exit code 0 lets the command run.
"""

import json
import re
import sys

RULES = [
    (
        r"\bdbt\b[^|;&]*(--target[ =]prod\b|-t prod\b)",
        (
            "`--target prod` runs only in CI on main. Use the default dev target, "
            "or `make prod-state` and `make slim-build` to try slim CI."
        ),
    ),
    (
        r"\bgit\s+push\b[^|;&]*(--force\b|--force-with-lease\b|\s-f\b|\s\+\S)",
        (
            "Force push rewrites published history. It needs the owner's explicit "
            "go-ahead: ask first, and let the owner run it."
        ),
    ),
    (
        r"\bgit\s+push\b[^|;&]*\s(\S*:)?main\b",
        "main is protected. Push a feature branch and open a pull request.",
    ),
    (
        r"\bgit\s+add\b[^|;&]*(\s-f\b|--force\b)",
        (
            "`git add -f` would commit files that are kept local on purpose "
            "(GAP_PLAN.md, STUDY_GUIDE.md, study_guide/, .env.databricks)."
        ),
    ),
    (
        r"\bgit\s+(commit|tag)\b[^|;&]*(Co-Authored-By|Generated with)",
        "No AI attribution in commits: no Co-Authored-By or 'Generated with' lines.",
    ),
    (
        r"\bgh\s+pr\b[^|;&]*(Co-Authored-By|Generated with)",
        "No AI attribution in pull requests.",
    ),
    (
        r"(^|[\s;&|])(pip3?|python3?\s+-m\s+pip)\s+install\b",
        "Dependencies are managed with uv. Use `uv add <package>`, never pip.",
    ),
    (
        r"\.env\.databricks(?!\.example)",
        (
            ".env.databricks holds workspace secrets. Don't read or print it; "
            "use .env.databricks.example for the variable names."
        ),
    ),
]


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    command = payload.get("tool_input", {}).get("command", "")
    for pattern, reason in RULES:
        if re.search(pattern, command, flags=re.IGNORECASE):
            print(f"Blocked by .claude/hooks/guard_bash.py: {reason}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
