# mq-service Delta Specification

## Purpose
Extend the MQ service specification with DLQ inspection/reprocess RPCs, server-streaming StreamRead, and NOTIFY integration.

## ADDED Requirements

### Requirement: DLQ management API
The MQ service SHALL provide gRPC RPCs for inspecting, reprocessing, and purging dead letter queue messages. These RPCs SHALL be available when the MQ service is enabled and SHALL operate within the scope of the caller's authenticated identity when authentication is enabled.

#### Scenario: List DLQ messages with pagination
- **WHEN** a client calls `ListDlqMessages` for a queue with DLQ entries
- **THEN** the response SHALL return DLQ messages ordered by dead-letter timestamp descending with offset/limit pagination

#### Scenario: Reprocess a DLQ message
- **WHEN** a client calls `ReprocessDlqMessage` for a valid DLQ entry
- **THEN** the message SHALL be moved to the active queue with reset read count and become available for consumption

### Requirement: Server-streaming message read
The MQ service SHALL provide a `StreamRead` server-streaming RPC that delivers messages to consumers in real time as they become available. The stream SHALL accept queue name, quantity, and visibility timeout parameters. Messages SHALL be pushed to the client as they are received, respecting the configured batch quantity per push.

#### Scenario: StreamRead pushes available messages on open
- **WHEN** a client opens a `StreamRead` for a queue with visible messages
- **THEN** the server SHALL immediately send available messages up to the requested quantity

#### Scenario: StreamRead pushes new messages in real time
- **WHEN** a `StreamRead` is open and a new message is sent to the queue
- **THEN** the server SHALL push the message to the client within a bounded latency

## MODIFIED Requirements

### Requirement: Message production
The MQ service MUST support sending one message or a batch of messages to a queue. Message payloads MUST be JSON-compatible documents. The service MUST support delayed delivery by making messages invisible until their delay expires. When the NOTIFY feature is enabled, the service MUST issue a PostgreSQL NOTIFY on the queue-specific channel upon successful message insertion.

#### Scenario: Send one message
- **WHEN** a client sends JSON payload `P` to queue `orders`
- **THEN** the service MUST return a message id for the enqueued message

#### Scenario: NOTIFY is issued on send
- **WHEN** the NOTIFY feature is enabled and a client sends a message to queue `orders`
- **THEN** the service MUST issue a PostgreSQL NOTIFY on channel `pgapp_mq_orders`

#### Scenario: Send delayed message
- **WHEN** a client sends a message with a delay of 60 seconds
- **THEN** the message MUST NOT be returned by reads until the delay has expired

#### Scenario: Send batch preserves count
- **WHEN** a client sends a batch containing three messages
- **THEN** the service MUST return three message ids

### Requirement: Long polling and metrics
The MQ service MUST support long polling reads and queue metrics. Metrics MUST include visible message count, in-flight message count when available, oldest visible message age when available, total archived message count when available, and DLQ message count when DLQ is configured.

#### Scenario: Long polling waits for a message
- **WHEN** a client starts a long poll on an empty queue and a message arrives before the poll deadline
- **THEN** the read response MUST return the newly available message

#### Scenario: Queue metrics expose backlog
- **WHEN** messages are waiting in a queue
- **THEN** queue metrics MUST report a visible message count greater than zero

#### Scenario: Queue metrics include DLQ count
- **WHEN** a queue has DLQ entries and DLQ is configured
- **THEN** queue metrics MUST report the DLQ message count
