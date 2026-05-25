## ADDED Requirements

### Requirement: Queue lifecycle
The MQ service MUST support creating, purging, and dropping queues using owned PostgreSQL schema. Queue creation MUST support durable queues. If transient queue storage is enabled by deployment configuration, queue creation MUST allow selecting it; otherwise unsupported storage modes MUST fail validation.

#### Scenario: Create a durable queue
- **WHEN** a client creates queue `orders` in durable mode
- **THEN** the queue MUST become available for sending and reading messages

#### Scenario: Purge a queue
- **WHEN** a client purges queue `orders`
- **THEN** subsequent reads from `orders` MUST return no previously enqueued messages

#### Scenario: Drop a queue
- **WHEN** a client drops queue `orders`
- **THEN** subsequent operations on `orders` MUST fail with a stable not-found error

#### Scenario: Reject unsupported storage mode
- **WHEN** a client creates a queue with a storage mode that is not enabled by deployment configuration
- **THEN** the service MUST reject the request with a stable invalid-argument error

### Requirement: Message production
The MQ service MUST support sending one message or a batch of messages to a queue. Message payloads MUST be JSON-compatible documents. The service MUST support delayed delivery by making messages invisible until their delay expires.

#### Scenario: Send one message
- **WHEN** a client sends JSON payload `P` to queue `orders`
- **THEN** the service MUST return a message id for the enqueued message

#### Scenario: Send delayed message
- **WHEN** a client sends a message with a delay of 60 seconds
- **THEN** the message MUST NOT be returned by reads until the delay has expired

#### Scenario: Send batch preserves count
- **WHEN** a client sends a batch containing three messages
- **THEN** the service MUST return three message ids

### Requirement: Message consumption and visibility timeout
The MQ service MUST support reading available messages with a requested quantity and visibility timeout. A read message MUST become invisible to other consumers until it is deleted, archived, or its visibility timeout expires.

#### Scenario: Read hides message during visibility timeout
- **WHEN** consumer `A` reads message `M` with a 30 second visibility timeout
- **THEN** consumer `B` MUST NOT receive message `M` before the timeout expires

#### Scenario: Concurrent reads claim distinct messages
- **WHEN** multiple consumers read from the same queue concurrently
- **THEN** the same visible message MUST NOT be returned to more than one consumer within the same visibility timeout window

#### Scenario: Unacknowledged message is redelivered
- **WHEN** a consumer reads message `M` and does not delete or archive it before the visibility timeout expires
- **THEN** message `M` MUST become eligible for a later read

#### Scenario: Read response includes metadata
- **WHEN** a consumer reads messages from a queue
- **THEN** each returned message MUST include message id, read count, enqueue time, visibility timeout time, and payload

### Requirement: Acknowledgement, archive, and visibility management
The MQ service MUST support deleting a message, archiving a message, and updating a message visibility timeout.

#### Scenario: Delete acknowledges a message
- **WHEN** a client deletes message `M` after reading it
- **THEN** message `M` MUST NOT be returned by future reads

#### Scenario: Archive preserves a processed message
- **WHEN** a client archives message `M`
- **THEN** message `M` MUST be removed from the active queue and retained in the queue archive

#### Scenario: Extend visibility timeout
- **WHEN** a client extends the visibility timeout for message `M`
- **THEN** message `M` MUST remain invisible until the new timeout expires

### Requirement: Long polling and metrics
The MQ service MUST support long polling reads and queue metrics. Metrics MUST include visible message count, in-flight message count when available, oldest visible message age when available, and total archived message count when available.

#### Scenario: Long polling waits for a message
- **WHEN** a client starts a long poll on an empty queue and a message arrives before the poll deadline
- **THEN** the read response MUST return the newly available message

#### Scenario: Queue metrics expose backlog
- **WHEN** messages are waiting in a queue
- **THEN** queue metrics MUST report a visible message count greater than zero
