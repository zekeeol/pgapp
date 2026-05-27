## 1. Discovery and API Contract

- [x] 1.1 Define Admin API DTOs for overview, runtime metrics, persisted logs, client activity, Cache read-only views, and MQ read-only views.
- [x] 1.2 Define configuration contract for `PGAPP_ENABLE_ADMIN`, `PGAPP_ADMIN_BIND_ADDR` defaulting to `127.0.0.1:8080`, and required `PGAPP_ADMIN_TOKEN`.
- [x] 1.3 Define read-only safety model for Cache/MQ Admin API routes, including rejection of mutation methods.
- [x] 1.4 Define Admin HTTP response contract for auth header handling, JSON errors, request IDs, pagination, and max page size enforcement.
- [x] 1.5 Write failing Admin API tests for disabled-by-default behavior, token-required startup/access, unauthorized requests, and token redaction from logs.
- [x] 1.6 Write failing Admin API tests for overview/read-only monitoring responses.

## 2. Server Admin API

- [x] 2.1 Add server configuration tests for Admin enablement, Admin bind address defaulting, and required admin token validation.
- [x] 2.2 Implement Admin HTTP server startup and shutdown behavior in the same `pgapp-server` binary after tests fail.
- [x] 2.3 Add read-only endpoints for health/readiness/runtime metrics/PostgreSQL pool overview.
- [x] 2.4 Add PostgreSQL-backed log event schema and failing tests for persisted log writes/reads.
- [x] 2.5 Add persisted log query endpoints with pagination and filters for time range, level, text, and source/target.
- [x] 2.6 Add stable JSON error responses with redacted database internals and no-store headers for Admin JSON routes.
- [x] 2.7 Add admin session/API activity tracking with clear distinction between admin sessions and application API activity.

## 3. Cache Admin Inspection

- [x] 3.1 Write failing tests for listing cache namespaces and paginated cache entries.
- [x] 3.2 Add Cache Admin read endpoints with pagination, filtering, value preview truncation, and safe binary encoding.
- [x] 3.3 Write failing tests proving Cache Admin routes do not expose set/update/delete/invalidate behavior.
- [x] 3.4 Write failing tests proving Cache Admin inspection does not update `last_accessed_at`, `access_count`, hit/miss counters, expiry state, generation, or capacity state.
- [x] 3.5 Verify Cache detail reads remain bounded and read-only for preview or full-value inspection.

## 4. MQ Admin Inspection

- [x] 4.1 Write failing tests for listing queues, queue metrics, and paginated message previews without mutating visibility timeout.
- [x] 4.2 Add MQ Admin read endpoints for queues, backlog previews, archive previews, and queue metrics.
- [x] 4.3 Write failing tests proving MQ Admin routes do not expose send/delete/archive/purge/drop/ack behavior.
- [x] 4.4 Write failing tests proving Admin message browsing does not update `visibility_timeout_at`, `read_count`, `last_read_at`, archive rows, or active message availability.
- [x] 4.5 Verify Admin message browsing uses a peek/list path instead of delivery-oriented `Read`.

## 5. React + Vite Admin UI

- [x] 5.1 Scaffold React + Vite TypeScript app and test harness without implementing product screens first.
- [x] 5.2 Build the app shell: navigation, layout, theme tokens, loading/error/empty states, and API client.
- [x] 5.3 Build Overview screen with health, readiness, runtime metrics, PostgreSQL pool, Cache summary, and MQ summary.
- [x] 5.4 Build Cache screen with namespace list, entries table, read-only detail drawer, filtering, pagination, and safe value preview.
- [x] 5.5 Build MQ screen with queue list, metrics, read-only message preview table, filtering, and pagination.
- [x] 5.6 Build Logs screen backed by persisted PostgreSQL log events with filtering and bounded result sets.
- [x] 5.7 Build Clients screen for admin sessions and API activity.
- [x] 5.8 Verify responsive layouts for desktop and smaller laptop widths.

## 6. Packaging, Verification, and Documentation

- [x] 6.1 Decide whether production serves built static assets from `pgapp-server` or deploys UI separately.
- [x] 6.2 Add build scripts for Admin UI and server integration.
- [x] 6.3 Add end-to-end tests that start PostgreSQL, server, Admin API, and UI, then exercise token auth, persisted logs, and read-only Cache/MQ views.
- [x] 6.4 Document Admin UI configuration, local startup, auth model, read-only limitations, log persistence, retention guidance, and payload preview behavior.
- [x] 6.5 Update README with Admin UI usage once implemented.
