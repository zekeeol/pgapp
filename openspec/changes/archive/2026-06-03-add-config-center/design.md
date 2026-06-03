## Context

PGApp is PostgreSQL-first and currently exposes Cache, MQ, health/runtime
metrics, an Admin HTTP API, and a React Admin UI. Cache can store key/value data
but has TTL, eviction, and runtime-cache semantics, so it should not become the
source of truth for application configuration.

The Config Center adds an Apollo-like workflow while staying inside the current
architecture:

```text
Admin UI / Admin HTTP
        |
        | edit draft / publish
        v
pgapp-server
        |
        | gRPC ConfigService
        v
PostgreSQL
  - config_scopes
  - config_items
  - config_releases
```

Clients consume published releases through gRPC and SDK helpers. Operators edit
draft JSON values through the Admin UI, then publish immutable release
snapshots.

## Goals / Non-Goals

**Goals:**

- Add a PostgreSQL-backed configuration center with `key -> JSON value` items.
- Use an Apollo-like scope model: `app_id`, `environment`, `cluster`, and
  `namespace`.
- Separate draft edits from published client-visible releases.
- Store published releases as immutable JSON snapshots with monotonically
  increasing revisions and checksums.
- Provide gRPC APIs and Rust/Go/Python SDK methods for reading releases and
  long-polling for changes.
- Provide Admin HTTP and Admin UI workflows for scope browsing, draft editing,
  JSON validation, publishing, and release history.
- Keep deployment PostgreSQL-only.

**Non-Goals:**

- Full Apollo compatibility or import/export format compatibility.
- Real-time streaming in the first version. The public change API is long-poll
  first, not gRPC streaming first.
- Typed schemas, JSON Schema validation, config inheritance, gray release,
  approval workflows, RBAC, or secret management.
- Client-side config caching daemon or sidecar.
- Editing Cache or MQ behavior through configuration in this change.

## Decisions

### Decision: Use a dedicated ConfigService instead of Cache

Config values are durable, authoritative, versioned data. Cache entries can
expire, be evicted, and update access metadata on reads. A dedicated
`ConfigService` avoids mixing transient cache semantics with release and audit
semantics.

Alternative considered: store configuration in Cache namespaces. This is
simpler but loses release snapshots, history, and clear client-visible publish
boundaries.

### Decision: Scope by app, environment, cluster, and namespace

The first version uses this scope:

```text
app_id / environment / cluster / namespace
```

Example:

```text
billing / prod / default / application
```

This maps cleanly to Apollo concepts while staying generic enough for non-Java
applications. The server should validate each scope component using the same
safe identifier style used elsewhere in PGApp.

### Decision: Draft items are separate from immutable releases

Draft writes update `config_items`; clients do not see these changes until
publish. Publishing runs in a transaction:

```text
lock scope
read active draft items
build JSON object snapshot
revision = current_revision + 1
insert config_releases row
update config_scopes.current_revision
notify waiting long-pollers
commit
```

Draft state remains editable after publish. A later edit creates unpublished
changes until the next publish.

### Decision: Store release snapshots as JSONB objects

Each draft item is a row:

```text
key TEXT
value_json JSONB
deleted BOOL
```

Each release stores the complete published namespace snapshot:

```text
snapshot_json JSONB
checksum TEXT
revision BIGINT
```

Storing the full snapshot makes client reads simple, immutable, and stable.
Diffs can be derived later if needed, but the first version optimizes for
correctness and simple SDK behavior.

### Decision: Long-poll API first, streaming later

The public change detection API accepts a known revision and timeout. If a newer
release exists, the server returns it immediately. Otherwise it waits until a
new publish appears or the timeout expires.

Implementation can start with a bounded PostgreSQL-backed wait loop, and may
use PostgreSQL `LISTEN/NOTIFY` internally to reduce polling. The external API
should not expose this implementation detail.

### Decision: Admin UI owns writes, SDKs focus on client reads

The Admin UI and Admin HTTP API provide operator workflows:

- list scopes
- inspect and edit draft JSON items
- validate JSON
- publish releases
- view release history and snapshots

SDKs should expose read and watch helpers for application clients. Mutating
drafts through SDKs can exist at the gRPC layer for automation, but SDK
ergonomics should make client consumption the main path.

## Data Model Sketch

```text
config_scopes
  id
  app_id
  environment
  cluster
  namespace
  current_revision
  created_at
  updated_at
  UNIQUE(app_id, environment, cluster, namespace)

config_items
  id
  scope_id
  config_key
  value_json
  deleted
  updated_at
  UNIQUE(scope_id, config_key)

config_releases
  id
  scope_id
  revision
  snapshot_json
  checksum
  message
  published_at
  published_by
  UNIQUE(scope_id, revision)
```

Indexes should support scope lookup, latest release lookup, and release history
pagination.

## API Shape

Likely protobuf shape:

```text
service ConfigService {
  rpc UpsertItem(UpsertConfigItemRequest) returns (OperationResult);
  rpc DeleteItem(DeleteConfigItemRequest) returns (OperationResult);
  rpc GetDraft(GetConfigDraftRequest) returns (ConfigDraftResponse);
  rpc Publish(PublishConfigRequest) returns (ConfigRelease);
  rpc GetRelease(GetConfigReleaseRequest) returns (ConfigRelease);
  rpc ListReleases(ListConfigReleasesRequest) returns (ListConfigReleasesResponse);
  rpc Watch(WatchConfigRequest) returns (WatchConfigResponse);
}
```

`Watch` is unary long-poll, not server streaming:

```text
scope + known_revision + timeout_seconds
  -> changed=true + release
  -> changed=false + latest_revision
```

## Admin UI Shape

The Admin UI should add a Config section to the existing operations console.
It should be work-focused rather than decorative:

```text
Config
  Scopes list
  Draft items table
  JSON editor panel
  Publish button
  Release history
```

The UI must not expose raw SQL or secrets. Invalid JSON should be caught before
submission where possible and returned as a stable server validation error when
not.

## Risks / Trade-offs

- Sensitive values may be stored as JSON config → Document that Config Center is
  not a secret manager and avoid special secret display features in v1.
- Full release snapshots can duplicate data → Accept for v1 because reads and
  rollback inspection are simple; optimize with diffs only if needed later.
- Long-poll can tie up connections → Enforce maximum timeout and use bounded
  waits; consider internal `LISTEN/NOTIFY`.
- Concurrent publishes can race → Publish must lock the scope row and increment
  revision inside one transaction.
- Admin UI JSON editing can produce malformed data → Validate JSON in UI and
  server, store only valid JSONB.
- SDK watch loops can become noisy → SDKs should expose caller-controlled
  timeout and backoff rather than hiding unbounded retry loops.

## Migration Plan

1. Add the configuration schema migration with idempotent `CREATE TABLE IF NOT
   EXISTS` statements and indexes.
2. Include the migration in `db::apply_schema` so Docker Compose and local
   startup initialize the Config Center automatically.
3. Add protobuf definitions and regenerate Rust/Go/Python clients.
4. Add core store tests and server integration tests before implementation.
5. Expose Admin HTTP routes and Admin UI screens.
6. Update Docker/local docs to include Config Center initialization and Admin UI
   usage.

Rollback strategy: stop using the ConfigService, remove the server/Admin UI
surface in a later change if necessary, and leave config tables in PostgreSQL
until an explicit schema rollback or data export is performed.

## Open Questions

- Should `Publish` reject when the draft snapshot checksum matches the latest
  published checksum, or create a new revision anyway for audit/message
  purposes?
- Should release `published_by` be a free-form string in v1, or derived only
  from Admin token/session metadata?
- Should SDKs expose draft mutation helpers, or keep SDK ergonomics read-only
  even if gRPC supports mutation for automation?
