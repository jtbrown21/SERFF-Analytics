#!/usr/bin/env python3
"""Verify required dependencies are installed."""
import importlib.util
import sys

REQUIRED = [
    "pandas",
    "duckdb",
    "pyairtable",
    "jinja2",
    "dotenv",
    "pyarrow",
]

missing = []
for pkg in REQUIRED:
    if importlib.util.find_spec(pkg) is None:
        missing.append(pkg)

if missing:
    print("Missing dependencies:", ", ".join(missing))
    sys.exit(1)

print("All dependencies available.")
