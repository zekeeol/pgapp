# Local Deployment

This guide runs PGApp locally with Docker Compose. It starts PostgreSQL,
`pgapp-server`, and the React Admin UI. The server initializes the PostgreSQL
schema on startup.

## Ports

- PostgreSQL container: `127.0.0.1:15432`
- gRPC server: `127.0.0.1:50051`
- Admin HTTP API: `127.0.0.1:8080`
- Admin UI: `http://127.0.0.1:3000`

## Start

```sh
PGAPP_ADMIN_TOKEN=change-me-local-admin-token docker-compose up -d --build
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
  PYTHONPATH=sdk/python:sdk/python/pgapp_sdk/gen \
  .venv/bin/python -m unittest sdk/python/tests/test_live.py
```

Expected tables:

- `cache_entries`
- `cache_namespaces`
- `cache_stats`
- `mq_archives`
- `mq_messages`
- `mq_queues`
- `admin_log_events`

Verify Admin HTTP:

```sh
curl -H 'Authorization: Bearer change-me-local-admin-token' \
  http://127.0.0.1:8080/api/admin/overview
```

Open the Admin UI and enter `change-me-local-admin-token`:

```text
http://127.0.0.1:3000
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
