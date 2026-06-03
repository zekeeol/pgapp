## Context

PGApp phase one established a PostgreSQL-backed Cache, MQ, and Config Center with gRPC APIs and Rust/Python/Go SDKs. Phase two adds six production-critical features: DLQ for poison messages, JSON Schema validation for config, client authentication, LISTEN/NOTIFY-based MQ push, a TypeScript SDK, and atomic cache operations.

The server is a Rust binary (tonic + SQLx) with protobuf-defined gRPC contracts. SDKs are generated from shared `.proto` files. The Admin UI is a React + Vite SPA served through an Axum HTTP API with token auth.

## Goals / Non-Goals

**Goals:**

- Prevent poison messages from indefinitely blocking MQ consumers via automatic dead-lettering
- Validate Config Center draft items against operator-defined JSON Schemas before publish
- Secure gRPC endpoints with key+secret authentication so the server can be exposed beyond localhost
- Reduce MQ consumer latency from polling intervals to near-real-time via PostgreSQL LISTEN/NOTIFY
- Provide a first-class TypeScript SDK so Node.js/Deno/browser applications can use pgapp
- Add atomic cache primitives (Increment, Decrement, SetNX, GetSet, Append, Prepend) for coordination use cases

**Non-Goals:**

- Full RBAC or OAuth2/OIDC integration in gRPC auth (key+secret is the v1 primitive)
- Schema inheritance, cross-scope schema references, or draft JSON Schema standards beyond per-scope validation
- Exactly-once MQ delivery semantics (at-least-once remains the contract)
- WebSocket or SSE-based push (gRPC server-streaming is the push transport)
- Browser-native SDK (TypeScript SDK targets Node.js; browser support may follow)
- Cache transactions or multi-key atomicity beyond single-key operations

## Decisions

### Decision 1: DLQ as per-queue table, not a single global dead letter queue

Each queue gets its own DLQ partition via a shared `mq_dlq` table with a `queue_id` column. Messages are moved to DLQ when `read_count >= max_redelivery_count` AND the message would otherwise be eligible for another redelivery.

**Alternative considered**: Single global DLQ with a `source_queue` column. Rejected because per-queue DLQ retention policies, purging, and reprocessing are clearer when scoped by queue.

**Trigger mechanism**: DLQ move is triggered during `Read`, not during `Ack`. When `Read` picks a message, if `read_count >= max_redelivery_count`, the message is moved to `mq_dlq` instead of being returned. This keeps the read path simple and avoids coupling DLQ logic to the ack path.

### Decision 2: JSON Schema stored in `config_scopes` column

Add a nullable `json_schema JSONB` column to `config_scopes`. Operators set/update it via Admin API. Validation runs inside `upsert_item` (draft validation) and `publish` (final gate). Use the `jsonschema` crate for validation.

**Alternative considered**: Separate `config_schemas` table with version history. Rejected as over-engineering for v1; a single schema per scope matches the current scope model. Schema versioning can be added later alongside config release features.

### Decision 3: gRPC auth as tonic interceptor, not middleware layer

Implement authentication as a `tonic::service::Interceptor` that extracts `x-pgapp-key` and `x-pgapp-secret` from request metadata, validates against `pgapp_clients` table, and injects client identity into request extensions. This keeps auth transparent to individual service implementations and follows tonic's interceptor pattern.

**Credentials**: `key` is a public identifier (UUID or similar), `secret` is a hashed value stored with bcrypt. On validation, the interceptor queries `SELECT id, key_hash, secret_hash, roles FROM pgapp_clients WHERE key_hash = $1 AND active = true`.

**Alternative considered**: mTLS. Rejected because key management complexity is higher for client libraries; key+secret works over any gRPC transport without certificate infrastructure.

### Decision 4: LISTEN/NOTIFY as internal optimization, with gRPC server-streaming for clients

PostgreSQL NOTIFY is an internal server mechanism. When `Send` commits, the server issues `PERFORM pg_notify('mq_' || queue_name, '')`. A background task per server instance listens on queue channels and wakes gRPC streaming responders.

The client-facing change is a new `StreamRead` server-streaming RPC on `MQService`. The existing `ReadWithPoll` unary RPC is preserved for backward compatibility and can internally use the same NOTIFY mechanism to reduce poll latency.

**Alternative considered**: Expose LISTEN/NOTIFY directly to clients via a PostgreSQL connection. Rejected because it couples clients to PostgreSQL and requires separate connection management. gRPC streaming keeps a single transport.

**Channel naming**: `pgapp_mq_<queue_name>` — prefixed to avoid collisions with other PostgreSQL NOTIFY usage.

### Decision 5: TypeScript SDK uses protobuf-ts over grpc-tools

Use `@protobuf-ts/plugin` and `@protobuf-ts/runtime` for protobuf code generation in TypeScript. This provides better TypeScript ergonomics than the older `grpc-tools` approach and supports both `@grpc/grpc-js` transport and the protobuf-ts client directly.

**Package structure**: `@pgapp/sdk` on npm. Single package containing Cache, MQ, and Config Center clients. Mirror the Python SDK's client hierarchy (`PGAppClient` → `.cache`, `.mq`, `.config`).

**Alternative considered**: `@grpc/proto-loader` with dynamic codegen. Rejected because static generation provides better type safety and IDE support, matching the Rust/Go/Python SDK experience.

### Decision 6: Cache atomic operations use SELECT FOR UPDATE

Each atomic operation executes as a short transaction:
- `Increment`/`Decrement`: `SELECT value_bytes FOR UPDATE` → parse as i64 → add/subtract → `UPDATE` → return new value
- `SetNX`: `INSERT ... ON CONFLICT DO NOTHING` → return whether inserted
- `GetSet`: `SELECT value_bytes FOR UPDATE` → `UPDATE` → return old value
- `Append`/`Prepend`: `SELECT value_bytes FOR UPDATE` → concatenate → `UPDATE` → return new length

All operations respect TTL (skip expired entries) and namespace generation (skip stale generations).

**Alternative considered**: PostgreSQL advisory locks. Rejected because row-level `FOR UPDATE` is simpler, scoped to the exact entry, and doesn't consume lock slots from a shared pool.

## Risks / Trade-offs

- **DLQ table growth**: Dead letters accumulate indefinitely if not purged → Add `PGAPP_DLQ_RETENTION_DAYS` config; periodically sweep expired DLQ entries. Expose DLQ metrics for monitoring.
- **Schema validation performance**: Large JSON Schemas or deeply nested config values could add latency to `UpsertItem` → Cache compiled JSON Schema objects per scope; set a configurable max schema size.
- **Auth interceptor DB query per request**: Every gRPC call hits `pgapp_clients` table → Connection pooling already amortizes this; consider an in-memory credential cache with TTL for high-throughput deployments later.
- **LISTEN/NOTIFY connection per server instance**: Each server process holds an extra PostgreSQL connection for LISTEN → Acceptable overhead; the connection is long-lived and idle most of the time.
- **TypeScript SDK maintenance**: Protobuf regenerations must include TypeScript → Add TypeScript codegen to `scripts/generate-proto.sh` so it stays in sync with Rust/Go/Python.
- **Cache atomic ops on non-numeric values**: `Increment` on a string value → Return a clear `InvalidArgument` error; document the type constraint in SDKs.

## Migration Plan

1. **Database migrations**: Add `0005_dlq.sql` (mq_dlq table + index), `0006_auth.sql` (pgapp_clients table + index), `0007_config_schema.sql` (ALTER config_scopes ADD json_schema). All idempotent.
2. **Proto regeneration**: Update `.proto` files with new RPCs and messages. Run `scripts/generate-proto.sh` (extended for TypeScript output).
3. **Server rollout**: New env vars have safe defaults (`PGAPP_ENABLE_AUTH=false`, `PGAPP_MAX_REDELIVERY_COUNT=0` for no DLQ, `PGAPP_ENABLE_NOTIFY=true`). Existing deployments continue unchanged.
4. **SDK releases**: Rust crate, Go module, Python package, and new TypeScript npm package released together with server.
5. **Admin UI**: New sections appear when corresponding features are enabled. No breaking changes to existing UI.
6. **Rollback**: Disable features via env vars. DLQ and auth tables can be dropped manually if needed. Schema column (`json_schema`) is nullable and has no impact when unused.

## Open Questions

- Should DLQ max redelivery count be per-queue or global? (Leaning toward per-queue for flexibility, with a global default.)
- Should the TypeScript SDK include generated gRPC types as a separate `@pgapp/sdk-grpc` package, or bundle everything in `@pgapp/sdk`? (Leaning toward single package for simpler installation.)
- Should `Increment`/`Decrement` support float values in addition to integers? (Leaning toward i64 only in v1, matching Redis INCR/DECR semantics.)
