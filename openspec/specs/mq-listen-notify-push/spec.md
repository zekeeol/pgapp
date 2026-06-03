# mq-listen-notify-push Specification

## Purpose
TBD - created by archiving change add-phase-two-features. Update Purpose after archive.
## Requirements
### Requirement: PostgreSQL NOTIFY on message send
When the `Send` or `SendBatch` RPC commits a transaction that inserts messages into a queue, the server SHALL issue a PostgreSQL `NOTIFY` on a queue-specific channel named `pgapp_mq_<queue_name>`. The NOTIFY payload SHALL indicate the number of newly available messages.

#### Scenario: NOTIFY is issued on successful send
- **WHEN** a client sends a message to queue `orders`
- **THEN** the server SHALL execute `PERFORM pg_notify('pgapp_mq_orders', '1')` within the same transaction

#### Scenario: NOTIFY is issued for batch send
- **WHEN** a client sends a batch of 5 messages to queue `orders`
- **THEN** the server SHALL execute `PERFORM pg_notify('pgapp_mq_orders', '5')` within the same transaction

### Requirement: gRPC server-streaming StreamRead RPC
The MQ service SHALL provide a new `StreamRead` server-streaming RPC. A client opens a stream for a specific queue with quantity and visibility timeout parameters. The server SHALL listen for PostgreSQL NOTIFY events on the queue's channel and push available messages to the client as they arrive. The stream SHALL remain open until the client cancels or a server-configured maximum stream duration is reached.

#### Scenario: StreamRead delivers messages in real time
- **WHEN** a client opens a `StreamRead` stream for queue `orders` and a message is subsequently sent to that queue
- **THEN** the server SHALL push the message to the client via the open stream without the client polling

#### Scenario: StreamRead delivers existing messages immediately
- **WHEN** a client opens a `StreamRead` stream for a queue that already has visible messages
- **THEN** the server SHALL immediately push available messages up to the requested quantity

#### Scenario: StreamRead respects visibility timeout
- **WHEN** a `StreamRead` stream delivers a message with a 30-second visibility timeout
- **THEN** the message SHALL be invisible to other consumers for 30 seconds, matching `Read` semantics

### Requirement: Backward compatibility with ReadWithPoll
The existing `ReadWithPoll` unary RPC SHALL remain available and unchanged in its external contract. Internally, the server MAY use the LISTEN/NOTIFY mechanism to reduce poll latency, but the client-facing behavior SHALL be identical to phase one.

#### Scenario: ReadWithPoll still works
- **WHEN** a client uses `ReadWithPoll` with a 10-second max poll on an empty queue
- **THEN** the call SHALL block until a message arrives or the timeout expires, matching phase one behavior

### Requirement: LISTEN connection management
The server SHALL maintain a dedicated PostgreSQL connection for LISTEN on all active queue channels. When a new queue is created, the server SHALL dynamically add a LISTEN for that queue's channel. The server SHALL gracefully handle PostgreSQL connection interruptions by reconnecting and re-establishing LISTEN subscriptions.

#### Scenario: LISTEN recovers from connection loss
- **WHEN** the PostgreSQL LISTEN connection is dropped
- **THEN** the server SHALL reconnect and re-issue LISTEN for all active queue channels within a configurable retry interval

### Requirement: NOTIFY feature toggle
The LISTEN/NOTIFY mechanism SHALL be enabled by default but configurable via `PGAPP_ENABLE_NOTIFY`. When disabled, the server SHALL fall back to the existing polling-based internal implementation for both `StreamRead` and `ReadWithPoll`.

#### Scenario: Fall back to polling when NOTIFY is disabled
- **WHEN** `PGAPP_ENABLE_NOTIFY` is set to false and a client opens a `StreamRead`
- **THEN** the server SHALL use a polling loop internally instead of PostgreSQL LISTEN

