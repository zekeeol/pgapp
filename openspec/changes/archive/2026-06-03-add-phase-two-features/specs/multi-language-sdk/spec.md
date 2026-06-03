# multi-language-sdk Delta Specification

## Purpose
Extend the multi-language SDK specification with TypeScript SDK inclusion, new API coverage for DLQ, cache atomic operations, and MQ stream-read across all SDKs.

## ADDED Requirements

### Requirement: TypeScript SDK inclusion
The SDK matrix SHALL include a TypeScript/JavaScript SDK published as `@pgapp/sdk` on npm. The TypeScript SDK SHALL provide a `PGAppClient` class with `.cache`, `.mq`, and `.config` sub-clients matching the API surface of the Rust, Go, and Python SDKs. The TypeScript SDK SHALL be generated from the same protobuf definitions.

#### Scenario: TypeScript SDK is part of the SDK matrix
- **WHEN** a new pgapp release is published
- **THEN** the TypeScript SDK SHALL be released alongside Rust, Go, and Python SDKs with matching API coverage

### Requirement: DLQ API parity across SDKs
All SDKs (Rust, Go, Python, TypeScript) SHALL expose DLQ management methods: `listDlqMessages`, `getDlqMessage`, `reprocessDlqMessage`, and `purgeDlq`. Each SDK SHALL use idiomatic types for its language.

#### Scenario: Python SDK lists DLQ messages
- **WHEN** a Python SDK user calls `client.mq.list_dlq_messages('orders')`
- **THEN** the SDK SHALL return a list of DLQ message objects with message id, payload, read count, and dead-letter timestamp

### Requirement: Cache atomic operations parity across SDKs
All SDKs SHALL expose cache atomic operation methods: `increment`, `decrement`, `setNX`, `getSet`, `append`, `prepend`. Each SDK SHALL use idiomatic types for its language (e.g., `number` in TypeScript, `i64` in Rust, `int` in Go/Python).

#### Scenario: Rust SDK atomic increment
- **WHEN** a Rust SDK user calls `client.cache().increment("ns", "counter", 5).await`
- **THEN** the SDK SHALL return the new value as `i64`

### Requirement: MQ stream-read parity across SDKs
All SDKs SHALL expose a `streamRead` method that returns a language-idiomatic streaming/iterator interface: `AsyncIterable<MQMessage>` in TypeScript, `Stream` in Rust, channel-based iterator in Go, and generator/iterator in Python.

#### Scenario: TypeScript SDK streamRead returns async iterable
- **WHEN** a TypeScript user calls `client.mq.streamRead('orders', { quantity: 5, visibilityTimeoutSeconds: 30 })`
- **THEN** the SDK SHALL return an `AsyncIterable<MQMessage>` that yields messages as they arrive

## MODIFIED Requirements

### Requirement: Consistent client initialization
The Rust, Go, Python, and TypeScript SDKs MUST provide a top-level client that connects to a configured server endpoint and accepts timeout and transport configuration. When authentication is enabled on the server, SDKs MUST support providing credentials (key and secret) that are attached as gRPC metadata on every request.

#### Scenario: Initialize client with endpoint
- **WHEN** an SDK user creates a client with a server endpoint
- **THEN** the client MUST use that endpoint for subsequent Cache, MQ, and Config calls

#### Scenario: Initialize client with auth credentials
- **WHEN** an SDK user creates a client with key and secret credentials
- **THEN** all gRPC requests MUST include `x-pgapp-key` and `x-pgapp-secret` metadata headers

#### Scenario: Apply request timeout
- **WHEN** an SDK user configures a request timeout
- **THEN** operations started through that client MUST honor the configured timeout

### Requirement: MQ API parity
The Rust, Go, Python, and TypeScript SDKs MUST expose MQ methods corresponding to server-supported queue lifecycle, send, batch send, read, long poll, server-streaming read, acknowledgement, archive, visibility timeout, metrics, and DLQ management operations.

#### Scenario: SDK sends and reads a message
- **WHEN** an SDK user sends a JSON-compatible message and then reads from the same queue
- **THEN** the SDK MUST return the message id, read count, timestamps, acknowledgement token, and payload reported by the server

#### Scenario: SDK acknowledges a message
- **WHEN** an SDK user acknowledges a read message with its acknowledgement token
- **THEN** the SDK MUST report the acknowledgement result returned by the server

### Requirement: Cache API parity
The Rust, Go, Python, and TypeScript SDKs MUST expose Cache methods corresponding to server-supported key/value operations, TTL, deletion, namespace invalidation, stats retrieval, and atomic operations (increment, decrement, set-if-not-exists, get-and-set, append, prepend).

#### Scenario: SDK cache round trip
- **WHEN** an SDK user sets a cache key and then gets the same key
- **THEN** the SDK MUST return the value returned by the server without altering the byte content

#### Scenario: SDK cache miss representation
- **WHEN** the server reports a cache miss
- **THEN** the SDK MUST expose the miss using an idiomatic nullable, option, or result representation for that language
