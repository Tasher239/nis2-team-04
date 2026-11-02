#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

python3.12 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install \
  --build-constraint requirements.txt \
  -r requirements.txt
