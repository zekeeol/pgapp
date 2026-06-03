#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
UV_BIN=${UV_BIN:-uv}
PYTHON_BIN=${PYTHON_BIN:-}
if [ -z "$PYTHON_BIN" ]; then
  if [ -x "/opt/homebrew/bin/python3" ]; then
    PYTHON_BIN="/opt/homebrew/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

tmpdir=$(mktemp -d)
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT INT TERM

"$UV_BIN" venv --python "$PYTHON_BIN" "$tmpdir/.venv" >/dev/null
"$UV_BIN" pip install --python "$tmpdir/.venv/bin/python" "$ROOT/sdk/python" >/dev/null

"$tmpdir/.venv/bin/python" - <<'PY'
from importlib import resources

from pgapp.v1 import cache_pb2, config_pb2, mq_pb2
from pgapp_sdk import PGAppClient

typed_marker = resources.files("pgapp_sdk").joinpath("py.typed")
assert typed_marker.is_file(), "pgapp_sdk must ship py.typed for inline type hints"

client = PGAppClient()
assert client.endpoint == "127.0.0.1:50051"
assert cache_pb2.SetCacheRequest(namespace="default", key="hello").key == "hello"
assert mq_pb2.AckMessageRequest(
    queue_name="jobs",
    message_id=1,
    ack_token="token",
).ack_token == "token"
assert config_pb2.ConfigScope(app_id="billing").app_id == "billing"
PY
