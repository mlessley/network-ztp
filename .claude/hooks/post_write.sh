#!/usr/bin/env bash
# Delegates to post_write.py — ruff + mypy on every modified Python file.
# stdin (JSON from Claude Code) flows through exec to the Python process.
exec python3 "$(cd "$(dirname "$0")" && pwd)/post_write.py"
