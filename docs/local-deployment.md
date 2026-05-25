# Local Deployment

This guide runs PGApp locally without taking over an existing PostgreSQL on
port `5432`.

## Ports

- PostgreSQL container: `127.0.0.1:15432`
- gRPC server: `127.0.0.1:50051`

## Start

```sh
docker network create pgapp-local-net 2>/dev/null || true

docker run --name pgapp-local-postgres \
  --network pgapp-local-net \
  -e POSTGRES_DB=pgapp \
  -e POSTGRES_USER=pgapp \
  -e POSTGRES_PASSWORD=secret \
  -p 127.0.0.1:15432:5432 \
  -d postgres:17

docker build -t pgapp-server:local .

docker run --name pgapp-local-server \
  --network pgapp-local-net \
  -e DATABASE_URL='postgres://pgapp:secret@pgapp-local-postgres:5432/pgapp' \
  -e PGAPP_BIND_ADDR='0.0.0.0:50051' \
  -p 127.0.0.1:50051:50051 \
  -d pgapp-server:local
```

The server applies the Cache and MQ schema on startup.

## Verify

```sh
docker ps --filter name=pgapp-local

docker exec pgapp-local-postgres psql -U pgapp -d pgapp \
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

## Operate

```sh
docker logs -f pgapp-local-server
docker logs -f pgapp-local-postgres

docker stop pgapp-local-server pgapp-local-postgres
docker start pgapp-local-postgres pgapp-local-server
```

## Remove

```sh
docker rm -f pgapp-local-server pgapp-local-postgres
docker network rm pgapp-local-net
```

This deletes the local PostgreSQL container and its data because the command
does not attach a persistent volume. Add a named volume before using this setup
for longer-lived local data.

## Docker Compose

The repository also includes `docker-compose.yml`, but it maps PostgreSQL to
host port `5432`. Use the manual commands above when another local PostgreSQL
is already listening there.
