# Schema Rollback Notes

Phase one creates owned tables for cache and MQ:

- `cache_namespaces`
- `cache_entries`
- `cache_stats`
- `mq_queues`
- `mq_messages`
- `mq_archives`

Rollback of application code should not automatically drop MQ tables because they may contain unprocessed messages. Operators can disable the MQ service, drain or archive messages, and then drop tables explicitly if the data is no longer needed.

Cache data is disposable. Operators may truncate `cache_entries` during rollback if needed.
