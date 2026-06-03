## 1. Database Migrations

- [x] 1.1 Add `0005_dlq.sql` migration: create `mq_dlq` table (id, queue_id, original_message_id, payload, headers, read_count, enqueued_at, dead_lettered_at, reason) with indexes
- [x] 1.2 Add `0006_auth.sql` migration: create `pgapp_clients` table (id, client_key, key_hash, secret_hash, active, roles, created_at, updated_at) with unique index on key_hash
- [x] 1.3 Add `0007_config_schema.sql` migration: ALTER `config_scopes` ADD `json_schema JSONB` (nullable)
- [x] 1.4 Add `max_redelivery_count` column to `mq_queues` table (nullable, default NULL)
- [x] 1.5 Update `db::apply_schema` in `crates/pgapp-core/src/db.rs` to include all new migrations

## 2. Protobuf Contract Changes

- [x] 2.1 Add DLQ messages and RPCs to `proto/pgapp/v1/mq.proto`: `ListDlqMessagesRequest/Response`, `GetDlqMessageRequest`, `ReprocessDlqMessageRequest`, `PurgeDlqRequest`, `DlqMessage` message type
- [x] 2.2 Add `StreamReadRequest` and `StreamRead` server-streaming RPC to `proto/pgapp/v1/mq.proto`
- [x] 2.3 Add atomic cache RPCs to `proto/pgapp/v1/cache.proto`: `IncrementRequest/Response`, `DecrementRequest/Response`, `SetNXRequest/Response`, `GetSetRequest/Response`, `AppendRequest/Response`, `PrependRequest/Response`
- [x] 2.4 Regenerate Rust stubs via `scripts/generate-proto.sh`
- [x] 2.5 Regenerate Python stubs
- [x] 2.6 Regenerate Go stubs
- [x] 2.7 Add TypeScript code generation to `scripts/generate-proto.sh`

## 3. MQ Dead Letter Queue

- [x] 3.1 Add `dead_letter` method to `MqStore` in `crates/pgapp-core/src/mq.rs`: move message from `mq_messages` to `mq_dlq` when `read_count >= max_redelivery_count`
- [x] 3.2 Integrate DLQ check into existing `read` method: skip messages at max redelivery, move them to DLQ instead
- [x] 3.3 Add `list_dlq_messages`, `get_dlq_message`, `reprocess_dlq_message`, `purge_dlq` methods to `MqStore`
- [x] 3.4 Add `dlq_count` to `QueueMetrics` and `metrics` method
- [x] 3.5 Add DLQ RPC handlers in `MqGrpc` in `crates/pgapp-server/src/lib.rs`
- [x] 3.6 Add DLQ sweep task (periodic cleanup based on `PGAPP_DLQ_RETENTION_DAYS`)
- [x] 3.7 Add `PGAPP_MAX_REDELIVERY_COUNT` and `PGAPP_DLQ_RETENTION_DAYS` to `ServerConfig`
- [x] 3.8 Add DLQ integration tests in `crates/pgapp-server/tests/grpc_integration.rs`

## 4. Config JSON Schema Validation

- [x] 4.1 Add `jsonschema` crate to workspace dependencies in `Cargo.toml`
- [x] 4.2 Add `json_schema` field to `ConfigScope` struct and `ensure_scope`/`scope_id` queries in `crates/pgapp-core/src/config_center.rs`
- [x] 4.3 Add `set_schema` and `get_schema` methods to `ConfigStore`
- [x] 4.4 Add schema validation in `upsert_item`: if scope has schema, validate draft value before insert
- [x] 4.5 Add schema validation in `publish`: validate all non-deleted items against schema, reject if any fail
- [x] 4.6 Add `SetSchema`/`GetSchema` RPCs to `proto/pgapp/v1/config.proto` and regenerate stubs
- [x] 4.7 Add schema RPC handlers in `ConfigGrpc`
- [x] 4.8 Add `PGAPP_MAX_SCHEMA_BYTES` config option
- [x] 4.9 Add config schema validation tests

## 5. gRPC Client Authentication

- [x] 5.1 Add `bcrypt` (or `argon2`) dependency for secret hashing
- [x] 5.2 Add `ClientStore` in `crates/pgapp-core/src/` for CRUD operations on `pgapp_clients` table
- [x] 5.3 Implement auth interceptor as a `tonic::service::Interceptor` fn: extract `x-pgapp-key`/`x-pgapp-secret` from metadata, validate against DB
- [x] 5.4 Add auth interceptor to the gRPC server builder in `serve()` when `PGAPP_ENABLE_AUTH=true`
- [x] 5.5 Add health check bypass logic in interceptor (allow `GetHealth`/`GetReadiness` without auth)
- [x] 5.6 Add `PGAPP_ENABLE_AUTH` to `ServerConfig`
- [x] 5.7 Add auth failure metrics to `MetricsRegistry`
- [x] 5.8 Add Admin HTTP endpoints: `GET /api/admin/clients`, `POST /api/admin/clients`, `POST /api/admin/clients/:key/rotate`, `POST /api/admin/clients/:key/deactivate`
- [x] 5.9 Add auth integration tests (valid, invalid, missing credentials)
- [x] 5.10 Add auth bypass tests (health check, disabled mode)

## 6. MQ LISTEN/NOTIFY Push

- [x] 6.1 Add `listen` module to `crates/pgapp-core/src/` for managing PostgreSQL LISTEN connection
- [x] 6.2 Implement NOTIFY on `send`/`send_batch`: `PERFORM pg_notify('pgapp_mq_<queue>', '<count>')` in send transaction
- [x] 6.3 Implement LISTEN connection manager: maintain dedicated PG connection, subscribe to queue channels, reconnect on failure
- [x] 6.4 Add `StreamRead` server-streaming RPC handler in `MqGrpc`: open stream, await NOTIFY events, push messages via `tokio::sync::broadcast`
- [x] 6.5 Update `ReadWithPoll` to optionally use NOTIFY-based wake instead of pure polling
- [x] 6.6 Add `PGAPP_ENABLE_NOTIFY` config option (default true)
- [x] 6.7 Add graceful degradation when LISTEN connection is unavailable (fall back to polling)
- [x] 6.8 Add stream-read integration tests

## 7. Cache Atomic Operations

- [x] 7.1 Add `increment` method to `CacheStore`: `SELECT value_bytes FOR UPDATE`, parse as i64, add delta, UPDATE, return new value
- [x] 7.2 Add `decrement` method to `CacheStore`: same pattern with subtraction
- [x] 7.3 Add `set_nx` method to `CacheStore`: `INSERT ... ON CONFLICT DO NOTHING`, return created status
- [x] 7.4 Add `get_set` method to `CacheStore`: `SELECT FOR UPDATE` → `UPDATE` → return old value
- [x] 7.5 Add `append` method to `CacheStore`: `SELECT FOR UPDATE` → concatenate → `UPDATE` → return new length
- [x] 7.6 Add `prepend` method to `CacheStore`: prepend bytes before existing value
- [x] 7.7 Handle edge cases: non-numeric values on incr/decr, TTL expiry, namespace invalidation
- [x] 7.8 Add atomic cache RPC handlers in `CacheGrpc`
- [x] 7.9 Add atomic cache integration tests

## 8. SDK Updates — Rust

- [x] 8.1 Add DLQ methods to Rust SDK (`list_dlq_messages`, `get_dlq_message`, `reprocess_dlq_message`, `purge_dlq`)
- [x] 8.2 Add `stream_read` method to Rust SDK MQ client returning a `Stream`
- [x] 8.3 Add atomic cache methods to Rust SDK (`increment`, `decrement`, `set_nx`, `get_set`, `append`, `prepend`)
- [x] 8.4 Add auth credential support to Rust SDK client initialization
- [x] 8.5 Update Rust SDK examples

## 9. SDK Updates — Python

- [x] 9.1 Add DLQ methods to Python SDK (`list_dlq_messages`, `get_dlq_message`, `reprocess_dlq_message`, `purge_dlq`)
- [x] 9.2 Add `stream_read` generator method to Python SDK MQ client
- [x] 9.3 Add atomic cache methods to Python SDK (`increment`, `decrement`, `set_nx`, `get_set`, `append`, `prepend`)
- [x] 9.4 Add auth credential support to `PGAppClient.__init__` (`key` and `secret` params)
- [x] 9.5 Update Python SDK examples

## 10. SDK Updates — Go

- [x] 10.1 Add DLQ methods to Go SDK
- [x] 10.2 Add `StreamRead` to Go SDK MQ client returning a channel-based iterator
- [x] 10.3 Add atomic cache methods to Go SDK
- [x] 10.4 Add auth credential support to Go SDK `Dial`
- [x] 10.5 Update Go SDK examples

## 11. TypeScript SDK

- [x] 11.1 Initialize npm package at `sdk/typescript/` with `@pgapp/sdk` name and `package.json`
- [x] 11.2 Add `@grpc/grpc-js`, `@protobuf-ts/runtime`, `@protobuf-ts/plugin` dependencies
- [x] 11.3 Configure protobuf-ts code generation from shared `.proto` files
- [x] 11.4 Implement `PGAppClient` class with endpoint, timeout, and credential options
- [x] 11.5 Implement `CacheClient` with all operations including atomic ops
- [x] 11.6 Implement `MQClient` with all operations including DLQ and `streamRead` (AsyncIterable)
- [x] 11.7 Implement `ConfigClient` with scope, getRelease, getLatestRelease, and watch methods
- [x] 11.8 Implement `PGAppError` with gRPC status code preservation
- [x] 11.9 Add TypeScript type definitions for all public APIs
- [x] 11.10 Add TypeScript SDK README with usage examples
- [x] 11.11 Add TypeScript SDK build to CI/check scripts
- [x] 11.12 Add `tsconfig.json` and build configuration

## 12. Admin UI Updates

- [x] 12.1 Add DLQ browser section to Admin UI: queue selector, DLQ message table, reprocess/purge actions
- [x] 12.2 Add JSON Schema editor to Config Center scope detail: attach/edit/remove schema, inline validation
- [x] 12.3 Display schema validation errors in Config Center draft editor
- [x] 12.4 Add client credentials management section: list, create, rotate, deactivate
- [x] 12.5 Add Admin HTTP routes for schema CRUD and DLQ inspection
- [x] 12.6 Update Admin UI navigation to include new sections

## 13. Documentation and Final Integration

- [x] 13.1 Update `README.md` with new feature descriptions and configuration options
- [x] 13.2 Update `docs/local-deployment.md` with auth, DLQ, NOTIFY configuration examples
- [x] 13.3 Add `docs/dlq.md` documenting DLQ semantics and operations
- [x] 13.4 Add `docs/auth.md` documenting gRPC client authentication setup
- [x] 13.5 Update `Dockerfile` and `docker-compose.yml` for new env vars
- [x] 13.6 Run full `scripts/check.sh` and `scripts/integration.sh` to verify no regressions
- [x] 13.7 Validate OpenSpec with `openspec validate --specs --strict`
