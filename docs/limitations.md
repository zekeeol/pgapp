# Phase One Limitations

- Cache is PostgreSQL-backed and is not a Redis-compatible in-memory cache.
- Cache capacity is logical capacity. PostgreSQL physical disk usage depends on vacuum and normal storage maintenance.
- Cache data is disposable by design. Durable cache mode is a future option.
- MQ provides at-least-once processing with a visibility-timeout delivery window. Business handlers must be idempotent.
- MQ acknowledgement is currently represented by `Delete` or `Archive`. There is no separate `Ack` RPC and no per-delivery receipt handle in phase one.
- MQ is implemented with owned PostgreSQL tables and row locking. Very high queue throughput may require partitioning, LISTEN/NOTIFY, or a dedicated queue system later.
- Production high availability depends on the PostgreSQL deployment.
