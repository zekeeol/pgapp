# MQ Semantics

PGApp MQ is implemented with owned PostgreSQL tables. It provides at-least-once
delivery using row locks and visibility timeouts.

## Message Lifecycle

```text
Send
  |
  v
visible in mq_messages
  |
  | Read / ReadWithPoll
  v
in flight until visibility_timeout_at
  |                 |
  | Delete          | timeout expires
  v                 v
removed       visible again

Archive
  |
  v
removed from mq_messages and copied to mq_archives
```

## Read

`Read` claims available messages with `FOR UPDATE SKIP LOCKED`, increments
`read_count`, and sets `visibility_timeout_at`.

While a message is in flight, other consumers should not receive it. If the
consumer crashes or does not acknowledge the message before the timeout expires,
the message becomes visible again and can be redelivered.

## Acknowledgement

Phase one does not expose an RPC literally named `Ack`.

Successful processing is acknowledged by one of these operations:

- `Delete(queue_name, message_id)`: remove the message from the active queue.
- `Archive(queue_name, message_id)`: remove the message from the active queue
  and retain a copy in `mq_archives`.

Use `Delete` for normal acknowledgement when processed messages do not need to
be retained. Use `Archive` when audit, replay, or debugging workflows need the
processed payload.

## Visibility Extension

Long-running consumers can call `SetVisibilityTimeout` to extend or shorten the
in-flight window for a message.

Setting the timeout to `0` makes the message immediately eligible for another
read if it has not been deleted or archived.

## Delivery Guarantees

- Delivery is at-least-once.
- Consumers must be idempotent.
- Concurrent readers should claim distinct visible messages.
- Ordering is best-effort by message id for visible messages, not a strict
  cross-consumer ordering guarantee.

## Current Limitations

Acknowledgement currently uses `queue_name + message_id`. There is no
per-delivery receipt handle in phase one. This means a stale consumer that still
knows a message id could delete or archive a message after it has become visible
again and been read by another consumer.

A future production-hardening change should add a delivery token or receipt
handle to `Read` responses and require that token for acknowledgement.
