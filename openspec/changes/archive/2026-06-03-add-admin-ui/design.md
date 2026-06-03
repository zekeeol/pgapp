## Context

The current system has one Rust server binary with gRPC services:

```text
SDKs
  |
  | gRPC
  v
pgapp-server
  |-- health/readiness/runtime metrics
  |-- cache service
  |-- mq service
  |
  v
PostgreSQL
```

The Admin UI adds an operational read-only control plane to the same binary:

```text
Browser Admin UI
        |
        | HTTP/JSON + Bearer token
        v
pgapp-server
  |-- gRPC endpoint:  PGAPP_BIND_ADDR, default 127.0.0.1:50051
  |
  `-- Admin endpoint: PGAPP_ADMIN_BIND_ADDR, default 127.0.0.1:8080
        |
        |-- runtime metrics / readiness
        |-- persisted PostgreSQL logs
        |-- admin sessions / API activity
        |-- Cache read-only views
        `-- MQ read-only views
        |
        v
    PostgreSQL
```

The browser must not connect directly to PostgreSQL and should not need
generated gRPC clients. A thin Admin HTTP API can apply auth, redaction,
pagination, bounded result sizes, and read-only access rules before presenting
existing Cache/MQ/runtime state.

## Goals / Non-Goals

**Goals:**

- Provide a browser admin console for pgapp operators.
- Serve the Admin HTTP API from the existing `pgapp-server` binary on a separate
  bind address.
- Bind Admin HTTP to `127.0.0.1:8080` by default when enabled.
- Require `PGAPP_ADMIN_TOKEN` for Admin API requests.
- Show server health, readiness, runtime metrics, PostgreSQL pool state, Cache
  stats, MQ queue stats, recent errors, persisted logs, and client/admin session
  activity.
- Persist server log events to PostgreSQL and provide query/filter APIs for the
  UI.
- Provide read-only Cache inspection: namespaces, key searches, stats,
  paginated entries, safe value previews.
- Provide read-only MQ inspection: queue list, queue metrics, visible/in-flight
  counts, and message previews that do not claim delivery.
- Build UI with React + Vite, TypeScript, modern but compact operational design.
- Keep UI focused on repeated inspection workflows: dense tables, filters,
  detail panels, inline error states, loading states, and empty states.
- Use TDD for Admin API behavior and frontend workflows.

**Non-Goals:**

- Hosted multi-tenant SaaS control plane.
- Replacing Grafana/Prometheus or a full log aggregation system.
- Direct SQL console in the UI.
- Full RBAC, SSO, or organization management in the first Admin UI version.
- Cache mutation from Admin UI/API: no set, update, delete, or namespace
  invalidation.
- MQ mutation from Admin UI/API: no send, ack, archive, visibility change, purge, or drop.
- Changing Cache/MQ delivery semantics, including the current `Ack`/`Archive`
  acknowledgement-token model.

## Proposed Architecture

### Server Side

Add an optional Admin HTTP server path to the existing Rust binary.

Target shape:

```text
pgapp-server
  |
  |-- gRPC endpoint: PGAPP_BIND_ADDR, default 127.0.0.1:50051
  |
  `-- admin endpoint: PGAPP_ADMIN_BIND_ADDR, default 127.0.0.1:8080
        |
        |-- GET /api/admin/overview
        |-- GET /api/admin/runtime/metrics
        |-- GET /api/admin/logs
        |-- GET /api/admin/clients
        |-- GET /api/admin/cache/namespaces
        |-- GET /api/admin/cache/entries
        |-- GET /api/admin/cache/entries/{namespace}/{key}
        |-- GET /api/admin/mq/queues
        |-- GET /api/admin/mq/queues/{queue}
        `-- GET /api/admin/mq/queues/{queue}/messages
```

Exact endpoint names can be settled during implementation, but the method shape
is intentional: Admin Cache/MQ resources are read-only in this change.

### Admin HTTP Contract

The Admin API should be boring and predictable:

- All routes live under `/api/admin`.
- All API routes require `Authorization: Bearer <PGAPP_ADMIN_TOKEN>`.
- The token must not be accepted in query strings and must not be written to
  logs.
- Responses are JSON DTOs, not raw database rows.
- Error responses use a stable shape such as `code`, `message`, and
  `request_id`, with database internals and secrets redacted.
- List endpoints use bounded pagination. Prefer `limit` plus a cursor when the
  underlying ordering is stable; offset is acceptable only for small operational
  tables.
- The server enforces a maximum page size, even if the UI asks for more.
- Cache/MQ mutation attempts through Admin routes return `404 Not Found` or
  `405 Method Not Allowed`; they must not tunnel through a generic action
  endpoint.

Likely Rust stack options:

| Option | Fit |
| --- | --- |
| Axum beside tonic | Strong fit. Same Tokio runtime, ergonomic JSON, good tower middleware. |
| Tonic gRPC-Web | Possible, but adds browser protobuf complexity and still needs admin-specific auth/redaction. |
| Separate Node API | Avoid; adds another runtime and weakens the Rust-server ownership boundary. |

The preferred direction is Axum beside tonic, sharing configuration and `PgPool`.

### Frontend

Add a Vite React TypeScript app, likely:

```text
apps/admin-ui
  src/
    api/
    components/
    routes/
    styles/
```

First-screen experience should be the actual console:

```text
+--------------------------------------------------------------------+
| pgapp Admin                 env: local        health: ready         |
+---------------+----------------------------------------------------+
| Overview      |  Server                                            |
| Cache         |  +----------+ +----------+ +----------+             |
| MQ            |  | Requests | | Errors   | | PG Pool  |             |
| Logs          |  +----------+ +----------+ +----------+             |
| Clients       |                                                    |
| Settings      |  Cache                                             |
|               |  namespaces, keys, ttl, capacity, hit/miss stats    |
|               |                                                    |
|               |  MQ                                                |
|               |  queues, visible, in-flight, archived              |
+---------------+----------------------------------------------------+
```

Visual tone: quiet operational UI, not a hero/landing page. Prioritize:

- left navigation
- compact metric strips
- searchable/filterable tables
- side panels for details
- inline error states
- loading and empty states

The UI should not render mutation controls for Cache or MQ in this change.

### Data Model for UI

The Admin API should return UI-friendly DTOs rather than raw database rows.
Examples:

```text
Overview
  server_state
  uptime
  request_counts
  error_counts
  pg_pool
  cache_summary
  mq_summary

AdminLogEvent
  id
  timestamp
  level
  target
  message
  fields

CacheEntryPreview
  namespace
  key
  size_bytes
  expires_at
  last_accessed_at
  access_count
  value_preview
  value_encoding

QueueMessagePreview
  queue
  message_id
  read_count
  enqueued_at
  available_at
  visibility_timeout_at
  payload_preview
```

Payload/value previews should be truncated and explicitly encoded so binary
cache values do not break the UI. Full value inspection, if included, must still
be read-only and should be bounded or explicitly requested.

### Read-Only Data Access

Admin Cache inspection should be based on read-only projections over the
existing Cache tables:

```text
cache_namespaces
cache_entries
cache_stats
```

The implementation must avoid calling Cache `get` for browse/detail views if it
increments hit/miss counters or updates access metadata. Admin inspection should
not change `last_accessed_at`, `access_count`, hit/miss counters, expiry state,
capacity state, namespace generation, or stored values.

Admin MQ inspection should be based on read-only projections over the existing
MQ tables:

```text
mq_queues
mq_messages
mq_archives
```

The implementation must avoid calling MQ `Read` or long-polling read paths for
message browsing because those paths are delivery operations. Admin message
inspection should not change `visibility_timeout_at`, `read_count`,
`last_read_at`, archive rows, or active message availability.

### PostgreSQL Log Storage

Persist server log events in PostgreSQL so the Logs view survives restarts and
can be filtered without depending on process memory.

Suggested logical shape:

```text
admin_log_events
  id
  occurred_at
  level
  target
  message
  request_id
  fields_json
```

Implementation can choose the exact schema, but the Admin API must support
bounded, filterable reads by time range, level, text, and source/target where
available.

Recommended retention controls:

- default to bounded query windows in the UI
- document that operators can prune old rows
- consider a future `PGAPP_ADMIN_LOG_RETENTION_DAYS` setting, but do not require
  automated cleanup in the first implementation unless tests and docs define it

### MQ Read-Only Browsing

The existing MQ `Read` operation is delivery-oriented and mutates message
visibility. The Admin UI must not use it for message browsing. Admin message
previews need a separate peek/list query that reads queue rows without changing
visibility timeout, read count, archive state, or acknowledgement state.

## Safety Model

Admin UI is read-only for Cache/MQ, but it still exposes sensitive operational
data and payload previews.

Initial model:

- Admin HTTP endpoint is disabled unless `PGAPP_ENABLE_ADMIN=true`.
- When enabled, the server must require `PGAPP_ADMIN_TOKEN` to be configured.
- Admin API requests must send the token as a bearer credential.
- The default Admin bind address is `127.0.0.1:8080`.
- Cache/MQ Admin API routes must reject mutation methods.
- Server records enough Admin access/activity metadata to support the Clients
  view, without logging secrets.
- Admin API should set conservative browser-facing headers for local operator
  usage, including no-store caching for JSON responses.

This is not full RBAC, but it avoids shipping an unauthenticated observability
surface.

## Logs and Client Activity

Logs are persisted to PostgreSQL in this change. The UI should expose a bounded
default window, for example newest events first, plus filters for level, text,
and time range.

Client activity has two meanings:

- active Admin UI browser sessions
- application SDK/gRPC request activity

The first is admin session tracking. The second is derived from runtime metrics
and request metadata. The first implementation should label them clearly:
`Admin sessions` and `API activity`.

## Key Unknowns

- Should built Admin UI static assets be served by `pgapp-server`, or should the
  Vite app be deployed separately in production?
- Which log levels and structured fields should be persisted by default to keep
  the log table useful without becoming too noisy?
- Should Cache full value inspection be allowed, or should the first UI only
  expose previews?
- What default retention or cleanup strategy should apply to persisted
  `admin_log_events`?
- Should admin actions call internal service functions directly or go through
  gRPC clients to exercise the public API boundary?

## Risks / Trade-offs

- Browsing Cache/MQ data can expose sensitive payloads; previews and redaction
  need deliberate defaults.
- Large queues/caches require pagination and filtering from day one.
- Reusing existing Cache/MQ service methods can accidentally mutate read stats
  or delivery state; read-only Admin projections need explicit tests.
- Direct message `Read` mutates visibility timeout; UI browsing must avoid
  accidental message claims.
- Persisting logs adds write volume to PostgreSQL and needs retention controls.
- Serving Vite static assets from Rust adds packaging complexity but simplifies
  deployment.
- A very polished UI can distract from missing operational clarity. The first
  version should be visually clean but conservative.

## Suggested First Version

Think of the first useful slice like this:

```text
Read-only admin foundation
  |
  v
Persisted logs and client activity
  |
  v
Richer filters, retention, and redaction controls
```

Slice 1:

- Admin server enabled by config and protected by `PGAPP_ADMIN_TOKEN`.
- React shell with Overview, Cache, MQ, Logs, and Clients tabs.
- Overview shows health/readiness/runtime metrics.
- Cache tab shows namespaces/stats and paginated entries.
- MQ tab shows queues, metrics, and non-mutating message previews.
- Logs tab reads persisted PostgreSQL log events.

Slice 2:

- Admin sessions and API activity views.
- Better filtering, export, and redaction controls.
- Log retention configuration and cleanup task.
