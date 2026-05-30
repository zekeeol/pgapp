# Schema Rollback Notes

Phase one creates owned tables for cache and MQ:

- `cache_namespaces`
- `cache_entries`
- `cache_stats`
- `mq_queues`
- `mq_messages`
- `mq_archives`

Rollback of application code should not automatically drop MQ tables because
they may contain unprocessed messages. Active deliveries also store the current
per-delivery `ack_token` in `mq_messages`; rolling back to an older binary that
does not understand ack tokens can strand in-flight consumers. Operators should
disable the MQ service, let visibility timeouts expire or drain messages with a
compatible binary, archive anything that needs retention, and then drop tables
explicitly only if the data is no longer needed.

Cache data is disposable. Operators may truncate `cache_entries` during rollback if needed.
