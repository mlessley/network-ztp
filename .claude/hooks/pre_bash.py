#!/usr/bin/env python3
"""
PreToolUse hook for Bash — blocks dangerous or incorrect commands.

Receives Claude Code tool input as JSON on stdin.
Exit 0: allow the command.
Exit 2: block it (stdout is shown to the user as the reason).
"""

import json
import os
import re
import shlex
import sys


def block(reason: str) -> None:
    print(reason, flush=True)
    sys.exit(2)


def main() -> None:
    try:
        data = json.load(sys.stdin)
        raw = data.get("tool_input", {}).get("command", "")
    except Exception:
        sys.exit(0)

    if not raw:
        sys.exit(0)

    # Use shlex for proper tokenisation — handles quoting and escaping.
    # Fall back to whitespace split if the command has unmatched quotes.
    try:
        tokens = shlex.split(raw)
    except ValueError:
        tokens = raw.split()

    if not tokens:
        sys.exit(0)

    project = "/devx/repos/sd-branch-ztp"
    first = tokens[0]
    rest = tokens[1:]

    # ── 1. Recursive delete on absolute paths outside the project ─────────────
    #
    # rm -rf, rm -fr, rm -Rf, rm -rRf, etc. are all caught by checking whether
    # 'r' appears in the combined flag string.  Relative paths (rm -rf .venv)
    # and paths inside the project are allowed.
    if first == "rm":
        combined_flags = "".join(t.lstrip("-") for t in rest if t.startswith("-"))
        is_recursive = "r" in combined_flags.lower()
        if is_recursive:
            paths = [t for t in rest if not t.startswith("-")]
            for p in paths:
                if p.startswith("/") and not p.startswith(project):
                    block(
                        f"BLOCKED: recursive delete on absolute path outside the project.\n"
                        f"  Path    : {p}\n"
                        f"  Allowed : relative paths, or absolute paths under {project}\n"
                        f"  If you need to clean a system path, do it manually outside Claude Code."
                    )

    # ── 2. Reading .env or credential-named files ──────────────────────────────
    #
    # Catches: cat .env, head .env.local, tail /path/to/.env.production, etc.
    # .env.example is safe and is explicitly exempted.
    _READERS = {"cat", "head", "tail", "less", "more", "bat", "open"}
    if first in _READERS:
        for arg in rest:
            if arg.startswith("-"):
                continue
            name = os.path.basename(arg)
            # Block .env files but allow .env.example (the checked-in template)
            if re.match(r"^\.env(\..+)?$", name, re.IGNORECASE) and name != ".env.example":
                block(
                    f"BLOCKED: reading .env file is not permitted.\n"
                    f"  File: {arg}\n"
                    f"  Use .env.example to see which variables are needed.\n"
                    f"  Never read live credentials — they must stay out of conversation context."
                )
            if re.search(r"\b(secret|credential|password)\b", name, re.IGNORECASE):
                block(
                    f"BLOCKED: reading file with sensitive name is not permitted.\n"
                    f"  File: {arg}\n"
                    f"  If this file is needed for a task, ask the user to supply its contents."
                )

    # ── 3. git push --force / -f ───────────────────────────────────────────────
    #
    # Catches: --force, -f, and combined short flags like -fu that include 'f'.
    # git push without --force is not blocked here — it requires human approval
    # via the permissions allow-list (git push is not in the allow list).
    if tokens[:2] == ["git", "push"]:
        has_force = (
            "--force" in rest
            or "-f" in rest
            or any(t.startswith("-") and not t.startswith("--") and "f" in t[1:] for t in rest)
        )
        if has_force:
            block(
                "BLOCKED: git push --force is not allowed.\n"
                "  Force-pushing rewrites shared history and can destroy teammates' work.\n"
                "  If commits have diverged: investigate why before acting.\n"
                "  If you amended a commit that was already pushed: create a new commit instead."
            )

    # ── 4. pip install — wrong package manager ─────────────────────────────────
    #
    # This project uses uv exclusively.  pip install bypasses uv.lock and
    # .python-version, making the environment inconsistent.
    if first in {"pip", "pip3"} and rest and rest[0] == "install":
        package = rest[1] if len(rest) > 1 else "<package>"
        block(
            f"BLOCKED: use 'uv add {package}' not 'pip install {package}'.\n"
            f"  This project uses uv exclusively.  pip install bypasses uv.lock\n"
            f"  and .python-version, making the environment inconsistent with CI."
        )

    sys.exit(0)


if __name__ == "__main__":
    main()
