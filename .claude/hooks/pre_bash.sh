#!/usr/bin/env bash
# Delegates to pre_bash.py — using Python for proper shlex-based command parsing.
# stdin (JSON from Claude Code) flows through exec to the Python process.
exec python3 "$(cd "$(dirname "$0")" && pwd)/pre_bash.py"
