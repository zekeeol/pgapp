# Local Deployment

This guide runs PGApp locally with Docker Compose. It starts PostgreSQL,
`pgapp-server`, and the React Admin UI. The server initializes the PostgreSQL
schema on startup, including Cache, MQ, DLQ, gRPC clients, Admin logs, and
Config Center tables.

## Ports

- PostgreSQL container: `127.0.0.1:15432`
- gRPC server: `127.0.0.1:50051`
- Admin HTTP API: `127.0.0.1:8080`
- Admin UI: `http://127.0.0.1:3000`

## Start

```sh
PGAPP_ADMIN_TOKEN=change-me-local-admin-token docker-compose up -d --build
```

Useful feature toggles can be passed through Compose:

```sh
PGAPP_ADMIN_TOKEN=change-me-local-admin-token \
PGAPP_ENABLE_NOTIFY=true \
PGAPP_ENABLE_AUTH=false \
PGAPP_MAX_REDELIVERY_COUNT=3 \
PGAPP_DLQ_RETENTION_DAYS=7 \
PGAPP_MAX_SCHEMA_BYTES=262144 \
docker-compose up -d --build
```

When default ports are busy, override only the host ports:

```sh
PGAPP_POSTGRES_HOST_PORT=15433 \
PGAPP_GRPC_HOST_PORT=50052 \
PGAPP_ADMIN_HOST_PORT=8081 \
PGAPP_ADMIN_UI_HOST_PORT=3001 \
PGAPP_ADMIN_TOKEN=change-me-local-admin-token \
docker-compose up -d --build
```

Inside the compose network, PostgreSQL remains `postgres:5432`, gRPC remains
`pgapp-server:50051`, and Admin HTTP remains `pgapp-server:8080`.

## Verify

```sh
docker-compose ps

docker exec pgapp-postgres-1 psql -U pgapp -d pgapp \
  -c "select table_name from information_schema.tables where table_schema='public' order by table_name;"

PGAPP_TEST_ENDPOINT=127.0.0.1:50051 \
  sh -c 'cd sdk/python && uv run python -m unittest tests/test_live.py'
```

Verify server health and readiness from the gRPC endpoint:

```sh
grpcurl -plaintext 127.0.0.1:50051 pgapp.v1.HealthService/GetHealth
grpcurl -plaintext 127.0.0.1:50051 pgapp.v1.HealthService/GetReadiness
```

If `grpcurl` is not installed, use the SDK live tests below as the functional
health check.

Expected tables:

- `cache_entries`
- `cache_namespaces`
- `cache_stats`
- `mq_archives`
- `mq_dlq`
- `mq_messages`
- `mq_queues`
- `pgapp_clients`
- `config_items`
- `config_releases`
- `config_scopes`
- `admin_log_events`

Verify Admin HTTP:

```sh
curl -H 'Authorization: Bearer change-me-local-admin-token' \
  http://127.0.0.1:8080/api/admin/overview
```

Create a gRPC client credential through Admin HTTP:

```sh
curl -X POST \
  -H 'Authorization: Bearer change-me-local-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"client_key":"svc-local","roles":["service"]}' \
  http://127.0.0.1:8080/api/admin/clients
```

The returned `secret` is shown only once. If you restart with
`PGAPP_ENABLE_AUTH=true`, SDK clients must send that key and secret as gRPC
metadata.

Verify Admin UI availability:

```sh
curl -I http://127.0.0.1:3000
```

Verify MQ delivery acknowledgement with the Python SDK:

```sh
PGAPP_TEST_ENDPOINT=127.0.0.1:50051 \
sh -c 'cd sdk/python && uv run python -' <<'PY'
from time import time_ns

from pgapp_sdk import PGAppClient

client = PGAppClient("127.0.0.1:50051", timeout=5)
queue = f"local_ack_{time_ns()}"

client.mq.create_queue(queue)
message_id = client.mq.send_json(queue, {"ok": True})
message = client.mq.read(queue, quantity=1, visibility_timeout_seconds=30)[0]

assert message.message_id == message_id
assert message.ack_token
assert client.mq.ack(queue, message.message_id, message.ack_token)
assert client.mq.read(queue, quantity=1, visibility_timeout_seconds=1) == []
print("mq ack ok")
PY
```

Create and publish a Config Center release through Admin HTTP:

```sh
curl -X PUT \
  -H 'Authorization: Bearer change-me-local-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"scope":{"app_id":"billing","environment":"prod","cluster":"default","namespace":"application"},"key":"feature_flags","value":{"enabled":true}}' \
  http://127.0.0.1:8080/api/admin/config/items

curl -X POST \
  -H 'Authorization: Bearer change-me-local-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"scope":{"app_id":"billing","environment":"prod","cluster":"default","namespace":"application"},"message":"initial","published_by":"local"}' \
  http://127.0.0.1:8080/api/admin/config/releases
```

Attach a JSON Schema to the same Config Center scope:

```sh
curl -X PUT \
  -H 'Authorization: Bearer change-me-local-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"scope":{"app_id":"billing","environment":"prod","cluster":"default","namespace":"application"},"schema":{"type":"object","additionalProperties":true}}' \
  http://127.0.0.1:8080/api/admin/config/schema

curl -H 'Authorization: Bearer change-me-local-admin-token' \
  'http://127.0.0.1:8080/api/admin/config/schema?app_id=billing&environment=prod&cluster=default&namespace=application'
```

Inspect and operate a queue DLQ:

```sh
curl -H 'Authorization: Bearer change-me-local-admin-token' \
  'http://127.0.0.1:8080/api/admin/mq/queues/orders/dlq?limit=20&offset=0'

curl -X POST \
  -H 'Authorization: Bearer change-me-local-admin-token' \
  http://127.0.0.1:8080/api/admin/mq/queues/orders/dlq/123/reprocess

curl -X POST \
  -H 'Authorization: Bearer change-me-local-admin-token' \
  http://127.0.0.1:8080/api/admin/mq/queues/orders/dlq/purge
```

Open the Admin UI and enter `change-me-local-admin-token`:

```text
http://127.0.0.1:3000
```

## SDK Install Smoke Test

Verify the Python package can be installed into a fresh `uv` environment using
the Homebrew Python requested for local development:

```sh
sh scripts/check-python-sdk-install.sh
```

## Operate

```sh
docker-compose logs -f pgapp-server
docker-compose logs -f postgres
docker-compose logs -f admin-ui

docker-compose stop
docker-compose start
```

## Remove

```sh
docker-compose down
```

This keeps the named PostgreSQL volume. To delete local data too:

```sh
docker-compose down -v
```
