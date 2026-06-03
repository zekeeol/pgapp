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

CONTAINER_NAME="pgapp-integration-$$"
ADMIN_UI_CONTAINER_NAME="${CONTAINER_NAME}-admin-ui"
SERVER_LOG="${TMPDIR:-/tmp}/pgapp-server-${CONTAINER_NAME}.log"
SERVER_PID=""

cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  docker rm -f "$ADMIN_UI_CONTAINER_NAME" >/dev/null 2>&1 || true
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
ADMIN_PORT=$("$PYTHON_BIN" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
export PGAPP_BIND_ADDR="127.0.0.1:${SERVER_PORT}"
export PGAPP_ADMIN_BIND_ADDR="127.0.0.1:${ADMIN_PORT}"
export PGAPP_ENABLE_ADMIN=true
export PGAPP_ADMIN_TOKEN="integration-admin-token"
export PGAPP_TEST_ENDPOINT="127.0.0.1:${SERVER_PORT}"

cargo build -p pgapp-server

DATABASE_URL="$DATABASE_URL" PGAPP_BIND_ADDR="$PGAPP_BIND_ADDR" \
  PGAPP_ENABLE_ADMIN="$PGAPP_ENABLE_ADMIN" \
  PGAPP_ADMIN_BIND_ADDR="$PGAPP_ADMIN_BIND_ADDR" \
  PGAPP_ADMIN_TOKEN="$PGAPP_ADMIN_TOKEN" \
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

for _ in $(seq 1 80); do
  if nc -z 127.0.0.1 "$ADMIN_PORT" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

if ! nc -z 127.0.0.1 "$ADMIN_PORT" >/dev/null 2>&1; then
  echo "Admin HTTP server did not become ready. Log:" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

curl -fsS \
  -H "Authorization: Bearer ${PGAPP_ADMIN_TOKEN}" \
  "http://127.0.0.1:${ADMIN_PORT}/api/admin/overview" >/dev/null

curl -fsS \
  -X PUT \
  -H "Authorization: Bearer ${PGAPP_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"scope":{"app_id":"integration_config","environment":"prod","cluster":"default","namespace":"application"},"key":"feature_flags","value":{"enabled":true}}' \
  "http://127.0.0.1:${ADMIN_PORT}/api/admin/config/items" >/dev/null

curl -fsS \
  -X POST \
  -H "Authorization: Bearer ${PGAPP_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"scope":{"app_id":"integration_config","environment":"prod","cluster":"default","namespace":"application"},"message":"integration release","published_by":"integration"}' \
  "http://127.0.0.1:${ADMIN_PORT}/api/admin/config/releases" >/dev/null

curl -fsS \
  -H "Authorization: Bearer ${PGAPP_ADMIN_TOKEN}" \
  "http://127.0.0.1:${ADMIN_PORT}/api/admin/config/releases?app_id=integration_config&environment=prod&cluster=default&namespace=application" >/dev/null

docker build -t pgapp-admin-ui-integration apps/admin-ui >/dev/null
docker run \
  --name "$ADMIN_UI_CONTAINER_NAME" \
  --add-host pgapp-server:127.0.0.1 \
  -p 127.0.0.1::80 \
  -d pgapp-admin-ui-integration >/dev/null

ADMIN_UI_PORT=$(docker port "$ADMIN_UI_CONTAINER_NAME" 80/tcp | sed 's/.*://')
for _ in $(seq 1 80); do
  if curl -fsS "http://127.0.0.1:${ADMIN_UI_PORT}/" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "http://127.0.0.1:${ADMIN_UI_PORT}/" | grep -q "pgapp Admin"

scripts/check.sh

echo "Integration test passed with Docker PostgreSQL on port ${PG_PORT}, gRPC on ${SERVER_PORT}, Admin HTTP on ${ADMIN_PORT}, and Admin UI on ${ADMIN_UI_PORT}."
