#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

PYTHON_BIN=${PYTHON_BIN:-}
if [ -z "$PYTHON_BIN" ]; then
  if [ -x "$ROOT/sdk/python/.venv/bin/python" ]; then
    PYTHON_BIN="$ROOT/sdk/python/.venv/bin/python"
  elif [ -x "$ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$ROOT/.venv/bin/python"
  elif [ -x "/opt/homebrew/bin/python3" ]; then
    PYTHON_BIN="/opt/homebrew/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

cargo test --workspace
(cd apps/admin-ui && npm ci && npm test && npm run build)
(cd sdk/typescript && npm ci && npm run generate && npm run build)
scripts/generate-proto.sh
(cd sdk/go && go test ./...)
PYTHONPATH="$ROOT/sdk/python" "$PYTHON_BIN" -m unittest discover -s sdk/python/tests
"$PYTHON_BIN" -m mypy --strict \
  sdk/python/pgapp_sdk/client.py \
  sdk/python/pgapp_sdk/__init__.py \
  sdk/python/pgapp/__init__.py
sh scripts/check-python-sdk-install.sh
