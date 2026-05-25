#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PATH="$(go env GOPATH)/bin:$PATH"
PYTHON_BIN=${PYTHON_BIN:-}
if [ -z "$PYTHON_BIN" ]; then
  if [ -x "$ROOT/.venv/bin/python" ]; then
    PYTHON_BIN="$ROOT/.venv/bin/python"
  elif [ -x "/opt/homebrew/bin/python3" ]; then
    PYTHON_BIN="/opt/homebrew/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

mkdir -p "$ROOT/sdk/go/gen"
mkdir -p "$ROOT/sdk/python/pgapp_sdk/gen"

protoc \
  -I "$ROOT/proto" \
  --go_out="$ROOT/sdk/go/gen" \
  --go_opt=paths=source_relative \
  --go-grpc_out="$ROOT/sdk/go/gen" \
  --go-grpc_opt=paths=source_relative \
  "$ROOT/proto/pgapp/v1/common.proto" \
  "$ROOT/proto/pgapp/v1/health.proto" \
  "$ROOT/proto/pgapp/v1/cache.proto" \
  "$ROOT/proto/pgapp/v1/mq.proto"

"$PYTHON_BIN" -m grpc_tools.protoc \
  -I "$ROOT/proto" \
  --python_out="$ROOT/sdk/python/pgapp_sdk/gen" \
  --grpc_python_out="$ROOT/sdk/python/pgapp_sdk/gen" \
  "$ROOT/proto/pgapp/v1/common.proto" \
  "$ROOT/proto/pgapp/v1/health.proto" \
  "$ROOT/proto/pgapp/v1/cache.proto" \
  "$ROOT/proto/pgapp/v1/mq.proto"
