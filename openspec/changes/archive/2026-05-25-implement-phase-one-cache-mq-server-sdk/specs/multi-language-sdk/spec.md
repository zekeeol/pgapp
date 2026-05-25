## ADDED Requirements

### Requirement: Consistent client initialization
The Rust, Go, and Python SDKs MUST provide a top-level client that connects to a configured server endpoint and accepts timeout and transport configuration.

#### Scenario: Initialize client with endpoint
- **WHEN** an SDK user creates a client with a server endpoint
- **THEN** the client MUST use that endpoint for subsequent Cache and MQ calls

#### Scenario: Apply request timeout
- **WHEN** an SDK user configures a request timeout
- **THEN** operations started through that client MUST honor the configured timeout

### Requirement: Cache API parity
The Rust, Go, and Python SDKs MUST expose Cache methods corresponding to server-supported key/value operations, TTL, deletion, namespace invalidation, and stats retrieval.

#### Scenario: SDK cache round trip
- **WHEN** an SDK user sets a cache key and then gets the same key
- **THEN** the SDK MUST return the value returned by the server without altering the byte content

#### Scenario: SDK cache miss representation
- **WHEN** the server reports a cache miss
- **THEN** the SDK MUST expose the miss using an idiomatic nullable, option, or result representation for that language

### Requirement: MQ API parity
The Rust, Go, and Python SDKs MUST expose MQ methods corresponding to server-supported queue lifecycle, send, batch send, read, long poll, delete, archive, visibility timeout, and metrics operations.

#### Scenario: SDK sends and reads a message
- **WHEN** an SDK user sends a JSON-compatible message and then reads from the same queue
- **THEN** the SDK MUST return the message id, read count, timestamps, and payload reported by the server

#### Scenario: SDK acknowledges a message
- **WHEN** an SDK user deletes a read message
- **THEN** the SDK MUST report the acknowledgement result returned by the server

### Requirement: Consistent serialization and errors
The Rust, Go, and Python SDKs MUST preserve cache values as opaque bytes, serialize MQ payloads as JSON-compatible documents, and convert server errors into language-native error forms while preserving machine-readable status information.

#### Scenario: Cache bytes are preserved
- **WHEN** an SDK writes arbitrary bytes to Cache and reads the key back
- **THEN** the SDK MUST return the same byte sequence

#### Scenario: MQ JSON payload is preserved
- **WHEN** an SDK sends a JSON object containing nested fields
- **THEN** the SDK MUST return an equivalent JSON object when the message is read

#### Scenario: Error status is preserved
- **WHEN** the server returns an invalid-argument or unavailable error
- **THEN** the SDK MUST expose the corresponding status code or category to the caller

### Requirement: Shared protobuf versioning
The Rust, Go, and Python SDKs MUST be generated from the same protobuf definitions as the server for a given release.

#### Scenario: SDK and server use matching contract
- **WHEN** an SDK release is built
- **THEN** its generated code MUST reflect the protobuf package and message definitions used by the matching server release
