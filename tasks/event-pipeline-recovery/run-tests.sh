#!/bin/sh
set -eu

python -m pip install --quiet pytest==9.1.1

cd /app

python -m pytest -v -rA /tests/test_outputs.py
