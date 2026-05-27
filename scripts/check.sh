#!/usr/bin/env sh
set -eu

PYTHON_BIN=${PYTHON_BIN:-}
if [ -z "$PYTHON_BIN" ]; then
  if [ -x ".venv/bin/python" ]; then
    PYTHON_BIN=".venv/bin/python"
  elif [ -x "/opt/homebrew/bin/python3" ]; then
    PYTHON_BIN="/opt/homebrew/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

cargo test --workspace
(cd apps/admin-ui && npm ci && npm test && npm run build)
scripts/generate-proto.sh
(cd sdk/go && go test ./...)
PYTHONPATH=sdk/python:sdk/python/pgapp_sdk/gen "$PYTHON_BIN" -m unittest discover -s sdk/python/tests
"$PYTHON_BIN" -m mypy --strict sdk/python/pgapp_sdk/client.py sdk/python/pgapp_sdk/__init__.py
