## Why

Phase one can run Cache and MQ, but operators currently need tests, SDKs, logs,
or direct PostgreSQL queries to understand what the server is doing. That is too
slow for day-to-day operations and makes routine diagnosis harder than it should
be.

An Admin UI should make pgapp observable and inspectable from a browser: server
status, runtime metrics, persisted logs, client activity, Cache data, and MQ data
should be visible in one controlled read-only place.

## What Changes

- Add an Admin UI built with React + Vite using a modern, clean operational
  interface rather than a marketing-style landing page.
- Add an Admin HTTP API inside the existing `pgapp-server` binary, served on a
  separate bind address from the gRPC endpoint.
- Default the Admin HTTP bind address to `127.0.0.1:8080` when Admin UI is
  enabled.
- Require `PGAPP_ADMIN_TOKEN` for Admin API access.
- Add dashboards for health, readiness, runtime metrics, PostgreSQL pool state,
  Cache statistics, MQ queue metrics, and recent errors.
- Persist server log events to PostgreSQL and expose log viewing/filtering from
  the Admin UI.
- Add client connection/session visibility for active admin users and server API
  activity.
- Add read-only Cache inspection: list/query namespaces and keys, inspect values
  safely, and view cache stats.
- Add read-only MQ inspection: list queues, inspect backlog/message previews
  without mutating delivery state, and view queue metrics.

## Capabilities

### New Capabilities

- `admin-ui`: Browser-based operations console and Admin API behavior for
  observing pgapp-server, Cache, and MQ.

### Modified Capabilities

- None in this proposal. Existing Cache, MQ, and runtime semantics remain the
  source of truth. The Admin API wraps and presents those capabilities without
  adding Cache or MQ mutation paths.

## Impact

- New React + Vite frontend app, likely under `apps/admin-ui`.
- New server-side Admin HTTP API hosted by `pgapp-server` on a separate bind
  address from the gRPC endpoint.
- New PostgreSQL-backed log event storage and query paths.
- New UI build/test/tooling for TypeScript, React, and browser integration.
- New security-sensitive operational surface that must require
  `PGAPP_ADMIN_TOKEN`.
- New tests for Admin API authorization, read-only monitoring, persisted logs,
  non-mutating Cache/MQ inspection, and frontend workflows.
