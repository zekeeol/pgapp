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
  | Ack             | timeout expires
  v                 v
removed       visible again

Archive
  |
  v
removed from mq_messages and copied to mq_archives
```

## Read

`Read` claims available messages with `FOR UPDATE SKIP LOCKED`, increments
`read_count`, sets `visibility_timeout_at`, and returns an `ack_token` for that
specific delivery attempt.

While a message is in flight, other consumers should not receive it. If the
consumer crashes or does not acknowledge the message before the timeout expires,
the message becomes visible again and can be redelivered.

The active row stores the latest token in `mq_messages.ack_token`. A later read
overwrites that value with a new token, so any earlier token becomes stale.

Consumer loop:

```text
Read(queue, quantity, visibility_timeout)
  |
  v
process QueueMessage.json_payload
  |
  +-- success, no retention needed -> Ack(queue, message_id, ack_token)
  |
  +-- success, retain processed copy -> Archive(queue, message_id, ack_token)
  |
  +-- needs more time -> SetVisibilityTimeout(queue, message_id, ack_token, seconds)
  |
  +-- crash / timeout -> message becomes visible for another read
```

## Acknowledgement

Successful processing is acknowledged with:

- `Ack(queue_name, message_id, ack_token)`: remove the message from the active
  queue only when the token matches the current in-flight delivery and has not
  expired.
- `Archive(queue_name, message_id, ack_token)`: remove the message from the
  active queue and retain a copy in `mq_archives`, again only when the token
  matches the current in-flight delivery.

An `ack_token` is invalid after its visibility timeout expires or after the
message is redelivered. This prevents a stale consumer from acknowledging a
newer delivery attempt by message id alone.

`Ack` returns `success = false` when no active in-flight message matches all of:

- queue name
- message id
- current `ack_token`
- unexpired `visibility_timeout_at`

This false result is the expected response for stale tokens, expired tokens,
already acknowledged messages, and already archived messages.

## Visibility Extension

Long-running consumers can call `SetVisibilityTimeout` with the current
`ack_token` to extend or shorten the in-flight window for a message.

Setting the timeout to `0` makes the message immediately eligible for another
read if it has not been acknowledged or archived.

After visibility is shortened to `0`, the previous token should be treated as
released. A later read receives a fresh token.

## Delivery Guarantees

- Delivery is at-least-once.
- Consumers must be idempotent.
- Concurrent readers should claim distinct visible messages.
- Ordering is best-effort by message id for visible messages, not a strict
  cross-consumer ordering guarantee.
- A valid `ack_token` confirms only the current delivery attempt, not the whole
  lifetime of the message id.

## Current Limitations

Acknowledgement is at-least-once, not exactly-once. A handler can still finish
its business side effect and fail to ack before the visibility timeout expires,
so consumers must remain idempotent.
