#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

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

CONTAINER_NAME="pgapp-integration-$$"
SERVER_LOG="${TMPDIR:-/tmp}/pgapp-server-${CONTAINER_NAME}.log"
SERVER_PID=""

cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run \
  --name "$CONTAINER_NAME" \
  -e POSTGRES_DB=pgapp \
  -e POSTGRES_USER=pgapp \
  -e POSTGRES_PASSWORD=secret \
  -p 127.0.0.1::5432 \
  -d postgres:17 >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$CONTAINER_NAME" pg_isready -U pgapp -d pgapp >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

PG_PORT=$(docker port "$CONTAINER_NAME" 5432/tcp | sed 's/.*://')
export DATABASE_URL="postgres://pgapp:secret@127.0.0.1:${PG_PORT}/pgapp"

SERVER_PORT=$("$PYTHON_BIN" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
export PGAPP_BIND_ADDR="127.0.0.1:${SERVER_PORT}"
export PGAPP_TEST_ENDPOINT="127.0.0.1:${SERVER_PORT}"

cargo build -p pgapp-server

DATABASE_URL="$DATABASE_URL" PGAPP_BIND_ADDR="$PGAPP_BIND_ADDR" \
  cargo run -p pgapp-server >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 80); do
  if nc -z 127.0.0.1 "$SERVER_PORT" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "pgapp-server exited early. Log:" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 0.25
done

if ! nc -z 127.0.0.1 "$SERVER_PORT" >/dev/null 2>&1; then
  echo "pgapp-server did not become ready. Log:" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

scripts/check.sh

echo "Integration test passed with Docker PostgreSQL on port ${PG_PORT} and server on ${SERVER_PORT}."
