# Schema Rollback Notes

PGApp creates owned tables for Cache, MQ, DLQ, gRPC client auth, Admin logs, and
Config Center:

- `cache_namespaces`
- `cache_entries`
- `cache_stats`
- `mq_queues`
- `mq_messages`
- `mq_archives`
- `mq_dlq`
- `pgapp_clients`
- `admin_log_events`
- `config_scopes`
- `config_items`
- `config_releases`

Rollback of application code should not automatically drop MQ tables because
they may contain unprocessed messages. Active deliveries also store the current
per-delivery `ack_token` in `mq_messages`; rolling back to an older binary that
does not understand ack tokens can strand in-flight consumers. Operators should
disable the MQ service, let visibility timeouts expire or drain messages with a
compatible binary, archive anything that needs retention, and then drop tables
explicitly only if the data is no longer needed.

Cache data is disposable. Operators may truncate `cache_entries` during rollback
if needed.

Config Center releases are immutable application configuration history. Before
rolling back to a binary that does not serve Config Center, export any required
release snapshots from `config_releases`.

`pgapp_clients.secret_hash` values are Argon2 hashes. A rollback that disables
gRPC client authentication does not need to delete client rows, but operators
should treat rotated plaintext secrets as unavailable after the create/rotate
response has been lost.

`admin_log_events` can be retained across rollback for audit history or deleted
with an explicit retention policy.
