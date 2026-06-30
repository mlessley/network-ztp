#!/usr/bin/env python3
"""
PostToolUse hook for Edit/Write — runs ruff and mypy after every Python file change.

Output is shown to Claude as context so it can see and fix any remaining issues.
Always exits 0 — the tool already ran; we are reporting, not blocking.
"""

import json
import os
import subprocess
import sys


def run(cmd: list[str], cwd: str) -> None:
    subprocess.run(cmd, cwd=cwd)


def main() -> None:
    try:
        data = json.load(sys.stdin)
        file_path = data.get("tool_input", {}).get("file_path", "")
    except Exception:
        sys.exit(0)

    if not file_path or not file_path.endswith(".py"):
        sys.exit(0)

    # Resolve to absolute path (file_path from Claude Code may be relative)
    abs_file = os.path.abspath(file_path)

    if not os.path.isfile(abs_file):
        sys.exit(0)

    # Project root is two directories up from .claude/hooks/
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))

    print(f"── ruff format {'─' * 50}", flush=True)
    run(["uv", "run", "ruff", "format", abs_file], cwd=project_root)

    print(f"── ruff check --fix {'─' * 46}", flush=True)
    run(["uv", "run", "ruff", "check", "--fix", abs_file], cwd=project_root)

    # Run mypy on the temporal package whenever a temporal/ file is touched.
    # Scope is the full package, not the single file — cross-module type errors
    # (e.g. a model field change that breaks an activity signature) need to surface.
    # Skip for test files and hook scripts: they are not part of the typed package.
    norm = abs_file.replace("\\", "/")
    if "/temporal/" in norm and "/tests/" not in norm and "/.claude/" not in norm:
        print(f"── mypy temporal/ {'─' * 48}", flush=True)
        run(["uv", "run", "mypy", "temporal/"], cwd=project_root)

    sys.exit(0)


if __name__ == "__main__":
    main()
