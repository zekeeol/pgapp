# mq-dead-letter-queue Specification

## Purpose
Define the dead letter queue behavior for poison messages: automatic move to DLQ on max redelivery, inspection, reprocessing, and purge.

## ADDED Requirements

### Requirement: Automatic dead letter on max redelivery
When a queue has a configured `max_redelivery_count` greater than zero, the MQ service SHALL automatically move messages whose `read_count` equals or exceeds `max_redelivery_count` from the active `mq_messages` table into the `mq_dlq` table during the `Read` operation, instead of returning them to the consumer. The move SHALL preserve the original message id, payload, headers, read count, enqueue time, and the reason for dead-lettering.

#### Scenario: Message exceeds redelivery count
- **WHEN** a queue `orders` has `max_redelivery_count = 3` and a message has been read 3 times without successful acknowledgement
- **THEN** the next `Read` for that message SHALL move it to `mq_dlq` and SHALL NOT return it in the read response

#### Scenario: Message below redelivery count is read normally
- **WHEN** a queue `orders` has `max_redelivery_count = 3` and a message has been read only 2 times without acknowledgement
- **THEN** a `Read` SHALL return the message normally with an incremented read count

#### Scenario: No DLQ when max_redelivery_count is zero or unset
- **WHEN** a queue has `max_redelivery_count = 0` or the configuration is not set
- **THEN** messages SHALL be redelivered indefinitely regardless of read count

### Requirement: DLQ message inspection
The MQ service SHALL provide gRPC APIs to list DLQ messages for a queue with pagination and to retrieve a single DLQ message by its original message id. Each DLQ entry SHALL include the original message id, payload, read count, enqueue time, dead-letter timestamp, and dead-letter reason.

#### Scenario: List DLQ messages for a queue
- **WHEN** a client requests DLQ messages for queue `orders`
- **THEN** the response SHALL return DLQ entries ordered by dead-letter timestamp descending with pagination support

#### Scenario: Get single DLQ message
- **WHEN** a client requests a specific DLQ message by original message id
- **THEN** the response SHALL return the full DLQ entry or a not-found error

### Requirement: DLQ message reprocessing
The MQ service SHALL provide a `ReprocessDlqMessage` RPC that moves a message from `mq_dlq` back to the active `mq_messages` table with `read_count` reset to zero and `available_at` set to the current time, making it immediately eligible for consumption.

#### Scenario: Reprocess a DLQ message
- **WHEN** a client reprocesses a DLQ message with original message id `M`
- **THEN** message `M` SHALL be removed from `mq_dlq`, re-inserted into `mq_messages` with `read_count = 0`, and become available for immediate `Read`

#### Scenario: Reprocess non-existent DLQ message
- **WHEN** a client attempts to reprocess a message not in the DLQ
- **THEN** the service SHALL return a not-found error

### Requirement: DLQ purge
The MQ service SHALL provide a `PurgeDlq` RPC that removes all DLQ messages for a specified queue.

#### Scenario: Purge all DLQ messages for a queue
- **WHEN** a client purges the DLQ for queue `orders`
- **THEN** all DLQ entries for queue `orders` SHALL be permanently deleted

### Requirement: DLQ retention
The MQ service SHALL support a configurable `dlq_retention_days` setting. When set, DLQ entries older than the retention period SHALL be eligible for automatic cleanup by a periodic sweep operation. When not set, DLQ entries SHALL be retained indefinitely.

#### Scenario: DLQ entries expire after retention period
- **WHEN** a DLQ entry has a dead-letter timestamp older than the configured retention period
- **THEN** the periodic sweep SHALL remove that entry from `mq_dlq`

#### Scenario: No retention sweep when retention is unset
- **WHEN** `dlq_retention_days` is not configured
- **THEN** no automatic DLQ cleanup SHALL occur
