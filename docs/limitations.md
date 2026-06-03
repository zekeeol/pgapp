# Current Limitations

- Cache is PostgreSQL-backed and is not a Redis-compatible in-memory cache.
- Cache capacity is logical capacity. PostgreSQL physical disk usage depends on vacuum and normal storage maintenance.
- Cache data is disposable by design. Durable cache mode is a future option.
- MQ provides at-least-once processing with a visibility-timeout delivery window. Business handlers must be idempotent.
- MQ acknowledgement uses per-delivery `ack_token` values. A stale or expired token cannot acknowledge a later delivery attempt.
- MQ is implemented with owned PostgreSQL tables and row locking. Very high queue throughput may require partitioning, LISTEN/NOTIFY, or a dedicated queue system later.
- Config Center stores JSON values and immutable snapshots, but it is not a secret manager and does not yet provide RBAC, gray release, approval workflows, or streaming watch.
- Config Center JSON Schema validation is per scope. There is no shared schema registry or schema version migration workflow yet.
- Admin UI can mutate Config Center, DLQ entries, and gRPC client credentials, but Cache and active MQ views are intentionally read-only.
- TypeScript SDK support is source-level in this repository. Publishing to a public npm registry is a release process outside this local implementation.
- Production high availability depends on the PostgreSQL deployment.
