# Admin UI

The Admin UI is a React + Vite operations console backed by a token-protected
HTTP API inside the `pgapp-server` binary. It is intentionally read-only for
Cache and MQ data, and it owns Config Center draft/publish workflows.

## Runtime Model

`pgapp-server` can expose two listeners:

```text
gRPC API       PGAPP_BIND_ADDR        default 127.0.0.1:50051
Admin HTTP    PGAPP_ADMIN_BIND_ADDR  default 127.0.0.1:8080
```

Admin HTTP is disabled by default.

```sh
PGAPP_ENABLE_ADMIN=true
PGAPP_ADMIN_BIND_ADDR=127.0.0.1:8080
PGAPP_ADMIN_TOKEN=change-me
PGAPP_ADMIN_MAX_PAGE_SIZE=100
```

When `PGAPP_ENABLE_ADMIN=true`, `PGAPP_ADMIN_TOKEN` is required. Admin API
requests must send it as a bearer token:

```sh
curl -H "Authorization: Bearer $PGAPP_ADMIN_TOKEN" \
  http://127.0.0.1:8080/api/admin/overview
```

The token is not accepted in query strings and request logs do not store the raw
token value.

## API Surface

Read-only routes:

```text
GET /api/admin/overview
GET /api/admin/logs
GET /api/admin/clients
GET /api/admin/cache/namespaces
GET /api/admin/cache/entries
GET /api/admin/mq/queues
GET /api/admin/mq/queues/{queue}/messages
GET /api/admin/mq/queues/{queue}/dlq
GET /api/admin/mq/queues/{queue}/dlq/{message_id}
GET /api/admin/config/scopes
GET /api/admin/config/draft
GET /api/admin/config/releases
GET /api/admin/config/schema
```

List routes support bounded pagination through `limit` and `offset`. The server
caps requested limits at `PGAPP_ADMIN_MAX_PAGE_SIZE`.

Config mutation routes:

```text
PUT /api/admin/config/items
DELETE /api/admin/config/items
POST /api/admin/config/releases
PUT /api/admin/config/schema
DELETE /api/admin/config/schema
POST /api/admin/mq/queues/{queue}/dlq/{message_id}/reprocess
POST /api/admin/mq/queues/{queue}/dlq/purge
POST /api/admin/clients
POST /api/admin/clients/{client_key}/rotate
POST /api/admin/clients/{client_key}/deactivate
```

Config item writes accept a scope, key, and JSON value. The write updates draft
state only. `POST /api/admin/config/releases` publishes the current draft as a
client-visible immutable release snapshot.

JSON Schema routes attach or remove the optional per-scope schema used to
validate draft upserts and publish attempts. DLQ routes operate only on dead
letter entries, not active MQ messages. Client credential routes create, rotate,
or deactivate gRPC key+secret credentials.

## Read-Only Limits

The Admin API does not expose Cache mutation routes:

- no set or update
- no delete
- no namespace invalidation

The Admin API does not expose MQ mutation routes:

- no send
- no ack or archive
- no purge or drop
- no visibility timeout changes

DLQ reprocess and purge are intentionally available because they operate on
dead-letter entries after a message has left the active queue.

Cache inspection reads from `cache_namespaces`, `cache_entries`, and
`cache_stats` without calling Cache `get`, so it does not increment hit/miss
counters or update access metadata.

MQ message browsing reads from `mq_messages` and `mq_archives` without calling
MQ `Read`, so it does not change `visibility_timeout_at`, `read_count`, or
delivery availability.

## Config Center

Config scopes use:

```text
app_id / environment / cluster / namespace
```

The UI shows scope rows, draft items, a JSON editor, JSON Schema editor, publish
action, and release history. Invalid JSON is rejected before submission where
possible, and server-side schema validation returns stable `invalid_argument`
errors. Config Center does not add secret-specific display behavior in this
version; sensitive values should be stored in a dedicated secret manager.

See [config-center.md](config-center.md) for the draft/publish model, long-poll
semantics, and JSON Schema behavior.

## Client Credentials

The Clients section lists active and inactive gRPC clients and lets an operator
create, rotate, or deactivate credentials. Create and rotate responses show the
plaintext secret once; after that only the Argon2 hash remains in PostgreSQL.

## Logs

Admin-visible server events are persisted to PostgreSQL in
`admin_log_events`. Records include timestamp, level, target, message,
request ID, and structured JSON fields.

The first version does not automatically prune old log rows. Operators can add
their own retention job, for example:

```sql
DELETE FROM admin_log_events
WHERE occurred_at < now() - interval '30 days';
```

## Frontend Development

```sh
cd apps/admin-ui
npm install
npm run dev
```

The Vite dev server proxies `/api/admin` to `http://127.0.0.1:8080`.

For browser sessions, store the admin token in session storage:

```js
sessionStorage.setItem("pgapp_admin_token", "change-me")
```

Production packaging can serve the built static files separately from the Rust
binary. The Admin HTTP API remains owned by `pgapp-server`.
