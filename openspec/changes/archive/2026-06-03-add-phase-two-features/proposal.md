## Why

Phase one established the PostgreSQL-backed Cache, MQ, and Config Center services with gRPC APIs and multi-language SDKs. Phase two addresses production-critical gaps: poison message handling in MQ, configuration data quality, client authentication, real-time push delivery, broader SDK ecosystem coverage, and richer cache semantics. Each feature targets a specific production readiness concern that limits phase one's deployability in real environments.

## What Changes

- **MQ Dead Letter Queue (DLQ)**: Add per-queue dead letter storage. Messages exceeding a configurable `max_redelivery_count` are automatically moved to `mq_dlq` after `Ack`/`Archive` failures stop counting retries. New gRPC APIs: `ListDlqMessages`, `GetDlqMessage`, `ReprocessDlqMessage`, `PurgeDlq`. Admin UI gains a DLQ inspection panel.
- **Config JSON Schema Validation**: Operators can attach an optional JSON Schema (`$schema`) to any Config Center scope. `UpsertItem` validates draft values against the schema. `Publish` is blocked if any non-deleted draft item fails schema validation. Uses the `jsonschema` Rust crate. Admin UI exposes schema editor and inline validation feedback.
- **gRPC Client Authentication**: Introduce key+secret authentication via gRPC metadata headers (`x-pgapp-key`, `x-pgapp-secret`). Credentials are managed in a new `pgapp_clients` PostgreSQL table. When authentication is enabled, unauthenticated requests receive `UNAUTHENTICATED`. Admin UI provides client credential management (create, rotate, revoke).
- **MQ LISTEN/NOTIFY Push**: Replace internal polling in `ReadWithPoll` with PostgreSQL `LISTEN`/`NOTIFY`. When a message is `Send` to a queue, the server issues `NOTIFY` on a queue-specific channel. A new gRPC server-streaming RPC `StreamRead` allows consumers to receive messages in real time. The existing `ReadWithPoll` (unary long-poll) is preserved for backward compatibility.
- **TypeScript SDK**: A new npm package `@pgapp/sdk` providing TypeScript/JavaScript clients for Cache, MQ, and Config Center gRPC services. Generated from the same protobuf contracts using `@grpc/grpc-js` and `protobuf-ts`. API surface mirrors existing Python/Rust/Go SDKs.
- **Cache Atomic Operations**: New atomic cache operations: `Increment`/`Decrement` (numeric values), `SetNX` (set-if-not-exists), `GetSet` (atomic get-and-set), `Append`/`Prepend` (byte concatenation). Implemented with `SELECT FOR UPDATE` row-level locking in the core store and exposed via new gRPC RPCs on `CacheService`. All SDKs gain corresponding methods.

## Capabilities

### New Capabilities

- `mq-dead-letter-queue`: Dead letter queue storage for poison messages with inspection, reprocessing, and purge APIs
- `config-json-schema-validation`: Per-scope JSON Schema attachment and draft/publish validation
- `grpc-client-authentication`: Key+secret gRPC authentication via metadata headers with client credential management
- `mq-listen-notify-push`: PostgreSQL LISTEN/NOTIFY-based real-time message delivery with server-streaming gRPC
- `typescript-sdk`: TypeScript/JavaScript SDK npm package for Cache, MQ, and Config Center
- `cache-atomic-operations`: Atomic Increment, Decrement, SetNX, GetSet, Append, Prepend operations on cache values

### Modified Capabilities

- `mq-service`: New DLQ inspection/reprocess RPCs; new server-streaming `StreamRead` RPC; `Send` now issues `NOTIFY`; `ReadWithPoll` optional internal switch to LISTEN-based wake
- `cache-service`: New atomic operation RPCs (`Increment`, `Decrement`, `SetNX`, `GetSet`, `Append`, `Prepend`)
- `multi-language-sdk`: TypeScript SDK added to SDK matrix; all existing SDKs gain methods for DLQ, cache atomic ops, and MQ stream-read
- `server-runtime`: New auth interceptor (optional, configurable); new `pgapp_clients` table migration; new admin routes for client credentials

## Impact

- **Database**: New migration `0005_dlq.sql` (mq_dlq table), `0006_clients.sql` (pgapp_clients table), schema extensions to `config_scopes` (optional `json_schema` column)
- **Protobuf**: New RPCs on `MQService` (DLQ + StreamRead), new RPCs on `CacheService` (atomic ops), new messages. Requires regeneration of all SDK stubs.
- **Server config**: New env vars `PGAPP_ENABLE_AUTH`, `PGAPP_MAX_REDELIVERY_COUNT`, `PGAPP_DLQ_RETENTION_DAYS`, `PGAPP_ENABLE_NOTIFY`
- **Dependencies (Rust)**: `jsonschema` crate for JSON Schema validation
- **Dependencies (TypeScript)**: `@grpc/grpc-js`, `@protobuf-ts/runtime`, `@protobuf-ts/plugin`
- **Admin UI**: New sections for DLQ browser, schema editor in Config Center, client credentials management
- **SDK matrix**: Rust, Go, Python, and TypeScript SDKs all require additions for new RPCs
- **Backward compatibility**: All existing APIs remain unchanged; DLQ is automatic when `max_redelivery_count` is configured; auth is opt-in; `ReadWithPoll` still works; atomic cache ops are additive
