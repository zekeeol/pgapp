# Dead Letter Queue

PGApp MQ is at-least-once. When a consumer reads a message but does not `Ack`
or `Archive` it before the visibility timeout expires, the message can be
redelivered. The dead letter queue (DLQ) prevents poison messages from cycling
forever.

## Configuration

```sh
PGAPP_MAX_REDELIVERY_COUNT=3
PGAPP_DLQ_RETENTION_DAYS=7
```

`PGAPP_MAX_REDELIVERY_COUNT=0` disables automatic DLQ movement. This is the
default and preserves phase-one behavior.

`PGAPP_DLQ_RETENTION_DAYS=0` disables the retention sweep. DLQ rows are retained
until an operator purges them.

Queues store their redelivery limit when they are created. Changing the global
environment value affects newly created queues; existing queues keep their
stored limit unless changed by a future queue-management operation.

## Movement Semantics

Messages move to DLQ during `Read`, not during `Ack`. When a message is visible
again and its `read_count` is already at or above the queue's
`max_redelivery_count`, PGApp moves it from `mq_messages` to `mq_dlq` and does
not return it to the consumer.

```text
Send -> Read -> handler fails -> visibility timeout expires
        Read -> handler fails -> visibility timeout expires
        Read -> read_count at limit -> move to mq_dlq
```

The DLQ entry preserves:

- original message id
- queue id
- JSON payload
- headers
- read count
- original enqueue timestamp
- dead-letter timestamp
- dead-letter reason

## Operations

gRPC and SDKs expose these operations:

```text
ListDlqMessages(queue, limit, offset)
GetDlqMessage(queue, original_message_id)
ReprocessDlqMessage(queue, original_message_id)
PurgeDlq(queue)
```

Reprocessing deletes the DLQ row and reinserts the message into the active queue
with the original message id, original payload and headers, `read_count=0`, and
immediate availability. A reprocessed message must still be acknowledged by a
consumer after successful handling.

Purging permanently deletes all DLQ rows for the queue.

## Admin UI

The Admin UI can inspect, reprocess, and purge DLQ entries. Active MQ messages
remain read-only in Admin UI: it does not send, ack, archive, drop, purge active
queues, or change visibility timeouts.

## Example

```sh
curl -H "Authorization: Bearer $PGAPP_ADMIN_TOKEN" \
  "http://127.0.0.1:8080/api/admin/mq/queues/orders/dlq?limit=20&offset=0"

curl -X POST \
  -H "Authorization: Bearer $PGAPP_ADMIN_TOKEN" \
  http://127.0.0.1:8080/api/admin/mq/queues/orders/dlq/123/reprocess

curl -X POST \
  -H "Authorization: Bearer $PGAPP_ADMIN_TOKEN" \
  http://127.0.0.1:8080/api/admin/mq/queues/orders/dlq/purge
```
