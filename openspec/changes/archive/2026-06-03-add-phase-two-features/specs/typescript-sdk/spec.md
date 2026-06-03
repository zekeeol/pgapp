# typescript-sdk Specification

## Purpose
Define the TypeScript/JavaScript SDK for pgapp: package structure, client API surface, code generation, and idiomatic TypeScript conventions.

## ADDED Requirements

### Requirement: Package structure and distribution
The TypeScript SDK SHALL be published as an npm package named `@pgapp/sdk`. It SHALL include TypeScript type definitions, generated gRPC client stubs, and a high-level client class. The package SHALL declare `@grpc/grpc-js` as a peer dependency.

#### Scenario: Install and import the SDK
- **WHEN** a user runs `npm install @pgapp/sdk @grpc/grpc-js`
- **THEN** they SHALL be able to `import { PGAppClient } from '@pgapp/sdk'` in TypeScript without additional type packages

### Requirement: Client initialization
The SDK SHALL provide a `PGAppClient` class that accepts an endpoint string and optional configuration (timeout, credentials). It SHALL expose `.cache`, `.mq`, and `.config` sub-clients matching the Python/Rust/Go SDK hierarchy.

#### Scenario: Create client with endpoint
- **WHEN** a user creates `new PGAppClient('localhost:50051')`
- **THEN** the client SHALL connect to that endpoint for all subsequent operations

#### Scenario: Create client with auth credentials
- **WHEN** a user creates a client with `{ key: 'my-key', secret: 'my-secret' }` in options
- **THEN** all gRPC calls SHALL include `x-pgapp-key` and `x-pgapp-secret` metadata

### Requirement: Cache API parity
The TypeScript SDK SHALL expose Cache methods: `set`, `get`, `mget`, `delete`, `exists`, `invalidateNamespace`, `stats`, and atomic operations (`increment`, `decrement`, `setNX`, `getSet`, `append`, `prepend`). Method signatures SHALL use TypeScript types appropriate for each operation (e.g., `get` returns `Buffer | null`).

#### Scenario: TypeScript SDK cache round trip
- **WHEN** a user sets a cache key with a Buffer value and then gets the same key
- **THEN** the SDK SHALL return a Buffer with the same byte content

#### Scenario: TypeScript SDK cache miss returns null
- **WHEN** the server reports a cache miss
- **THEN** the SDK SHALL return `null` for `get` and `{ hit: false, value: null }` for `mget` items

### Requirement: MQ API parity
The TypeScript SDK SHALL expose MQ methods: `createQueue`, `purgeQueue`, `dropQueue`, `sendJson`, `sendBatchJson`, `read`, `readWithPoll`, `ack`, `archive`, `setVisibilityTimeout`, `metrics`, DLQ operations (`listDlqMessages`, `getDlqMessage`, `reprocessDlqMessage`, `purgeDlq`), and `streamRead` (returning an AsyncIterable).

#### Scenario: TypeScript SDK send and read a JSON message
- **WHEN** a user sends a JSON object `{ orderId: 123 }` and reads from the same queue
- **THEN** the SDK SHALL return the message with parsed JSON payload matching the sent object

#### Scenario: TypeScript SDK streamRead yields messages
- **WHEN** a user calls `streamRead('orders', { quantity: 5, visibilityTimeoutSeconds: 30 })`
- **THEN** the SDK SHALL return an `AsyncIterable<MQMessage>` that yields messages as they arrive

### Requirement: Config Center API parity
The TypeScript SDK SHALL expose Config Center methods: `scope`, `getLatestRelease`, `getRelease`, `watch`. The `ConfigRelease` type SHALL parse `snapshotJson` into a typed object.

#### Scenario: TypeScript SDK get latest release
- **WHEN** a user retrieves the latest release for a scope
- **THEN** the SDK SHALL return a `ConfigRelease` with parsed `snapshot` object and typed fields

### Requirement: Error handling
The TypeScript SDK SHALL translate gRPC errors into typed `PGAppError` instances preserving the gRPC status code and message. Errors SHALL be thrown, not returned (matching idiomatic TypeScript conventions).

#### Scenario: Error status code is preserved
- **WHEN** the server returns an `INVALID_ARGUMENT` error
- **THEN** the SDK SHALL throw a `PGAppError` with `code` property set to the gRPC status code

### Requirement: Protobuf code generation
The TypeScript SDK's generated code SHALL be produced from the same `.proto` files used for Rust/Go/Python SDKs. The generation SHALL use `@protobuf-ts/plugin` and SHALL be integrated into `scripts/generate-proto.sh`.

#### Scenario: Proto regeneration includes TypeScript
- **WHEN** `scripts/generate-proto.sh` is run
- **THEN** TypeScript stub files SHALL be generated into the SDK's source tree alongside Rust, Go, and Python stubs
