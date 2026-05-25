## 1. TDD and Project Foundation

- [x] 1.1 Create the Rust workspace structure for the server, shared libraries, generated protobuf code, and integration tests.
- [x] 1.2 Establish the TDD harness for unit tests, database integration tests, server integration tests, SDK conformance tests, and end-to-end smoke tests.
- [x] 1.3 Add a project rule or developer note that production behavior must start with a failing automated test before implementation.
- [x] 1.4 Add protobuf packages for shared types, health/capability status, Cache service, and MQ service.
- [x] 1.5 Add failing protobuf generation smoke tests for Rust, Go, and Python clients.
- [x] 1.6 Configure protobuf code generation until the generation smoke tests pass.
- [x] 1.7 Add local development assets for PostgreSQL, owned database schema bootstrap, and server configuration.

## 2. Server Runtime

- [x] 2.1 Write failing unit tests for configuration loading and validation of bind address, PostgreSQL connection string, pool settings, enabled services, request limits, and timeouts.
- [x] 2.2 Implement configuration loading and validation until the configuration tests pass.
- [x] 2.3 Write failing tests for PostgreSQL pool initialization and graceful shutdown behavior.
- [x] 2.4 Implement PostgreSQL pool initialization and shutdown handling until the pool tests pass.
- [x] 2.5 Write failing readiness tests for PostgreSQL connectivity, cache schema availability, and MQ schema/index availability.
- [x] 2.6 Implement capability checks until the readiness tests pass.
- [x] 2.7 Write failing server integration tests for feature-gated Cache and MQ service registration.
- [x] 2.8 Implement gRPC server startup and service registration until the server integration tests pass.
- [x] 2.9 Write failing tests for health/readiness responses and per-service capability status.
- [x] 2.10 Implement health and readiness reporting until those tests pass.
- [x] 2.11 Write failing tests for stable error mapping across validation, not-found, conflict, timeout, and database-unavailable failures.
- [x] 2.12 Implement stable error mapping until the error tests pass.
- [x] 2.13 Write failing metrics tests for request counts, latency, errors, and PostgreSQL pool observability.
- [x] 2.14 Implement runtime observability until the metrics tests pass.

## 3. Database Bootstrap

- [x] 3.1 Write failing migration tests that assert required cache tables, indexes, and metadata objects exist after bootstrap.
- [x] 3.2 Add cache schema migrations until the cache migration tests pass.
- [x] 3.3 Write failing migration tests that assert required MQ tables, archive tables, indexes, and readiness checks exist after bootstrap.
- [x] 3.4 Add owned MQ bootstrap SQL until the MQ migration tests pass.
- [x] 3.5 Write failing tests for database initialization in local development and integration test environments.
- [x] 3.6 Add database initialization flow until environment bootstrap tests pass.
- [x] 3.7 Add rollback-safe migration notes for cache and MQ schema setup.

## 4. Cache Service

- [x] 4.1 Write failing unit tests for cache key validation, key hashing, namespace isolation, and opaque byte preservation.
- [x] 4.2 Implement cache validation and storage primitives until those tests pass.
- [x] 4.3 Write failing service tests for single-key and batch set/get/delete/exists operations.
- [x] 4.4 Implement single-key and batch operations until those tests pass.
- [x] 4.5 Write failing tests for per-entry TTL, read-time expiration, and bounded expired-entry sweeping.
- [x] 4.6 Implement TTL and expired-entry sweeping until those tests pass.
- [x] 4.7 Write failing tests for namespace invalidation using generation tracking.
- [x] 4.8 Implement namespace invalidation until those tests pass.
- [x] 4.9 Write failing tests for logical capacity accounting by key count and byte size.
- [x] 4.10 Implement logical capacity accounting until those tests pass.
- [x] 4.11 Write failing tests for eviction order: expired entries first, then least-recently-used live entries.
- [x] 4.12 Implement eviction until those tests pass.
- [x] 4.13 Write failing tests for cache statistics: hits, misses, writes, deletes, evictions, expired removals, logical key count, logical byte size, and per-namespace usage.
- [x] 4.14 Implement cache statistics until those tests pass.
- [x] 4.15 Write failing Cache gRPC integration tests for all cache-service spec scenarios.
- [x] 4.16 Implement Cache gRPC service handlers until the integration tests pass.

## 5. MQ Service

- [x] 5.1 Write failing unit tests for queue name validation, queue lifecycle validation, and unsupported storage mode rejection.
- [x] 5.2 Implement queue validation and lifecycle handlers for create, purge, and drop until those tests pass.
- [x] 5.3 Write failing database tests for owned MQ storage access covering active messages, archived messages, delayed availability, and in-flight visibility timeout state.
- [x] 5.4 Implement owned MQ storage access until those tests pass.
- [x] 5.5 Write failing tests for single-message and batch send with JSON-compatible payload validation.
- [x] 5.6 Implement send and batch send handlers until those tests pass.
- [x] 5.7 Write failing concurrent database tests proving transactional read claiming returns distinct messages to concurrent consumers.
- [x] 5.8 Implement transactional read claiming with quantity limits, visibility timeout, read count metadata, and timestamps until the concurrency tests pass.
- [x] 5.9 Write failing tests for delayed delivery through message availability timing.
- [x] 5.10 Implement delayed delivery until those tests pass.
- [x] 5.11 Write failing tests for long polling with bounded retry behavior and request deadline handling.
- [x] 5.12 Implement long polling reads until those tests pass.
- [x] 5.13 Write failing tests for delete, archive, and visibility-timeout update transactional behavior.
- [x] 5.14 Implement delete, archive, and visibility-timeout update handlers until those tests pass.
- [x] 5.15 Write failing tests for MQ metrics: visible messages, in-flight messages when available, oldest visible message age when available, and archived message count when available.
- [x] 5.16 Implement MQ metrics until those tests pass.
- [x] 5.17 Write failing MQ gRPC integration tests for all mq-service spec scenarios.
- [x] 5.18 Implement MQ gRPC service handlers until the integration tests pass.

## 6. Multi-language SDKs

- [x] 6.1 Write failing SDK generation tests that assert Rust, Go, and Python generated clients match the shared protobuf contract.
- [x] 6.2 Generate Rust, Go, and Python clients until generation tests pass.
- [x] 6.3 Write failing Rust SDK tests for top-level client initialization, Cache wrapper, MQ wrapper, timeout handling, serialization, and error mapping.
- [x] 6.4 Implement the Rust SDK until Rust SDK tests pass.
- [x] 6.5 Write failing Go SDK tests for top-level client initialization, Cache wrapper, MQ wrapper, context/deadline handling, serialization, and error mapping.
- [x] 6.6 Implement the Go SDK until Go SDK tests pass.
- [x] 6.7 Write failing Python SDK tests for top-level client initialization, Cache wrapper, MQ wrapper, timeout handling, serialization, and exception mapping.
- [x] 6.8 Implement the Python SDK until Python SDK tests pass.
- [x] 6.9 Write failing cross-language SDK conformance tests for cache round trips, cache misses, MQ send/read, acknowledgement, and error status preservation.
- [x] 6.10 Implement SDK conformance fixes until cross-language tests pass.

## 7. End-to-End Verification and Documentation

- [x] 7.1 Write a failing end-to-end smoke test that starts PostgreSQL, starts the Rust server, performs Cache operations, and performs MQ send/read/delete through an SDK.
- [x] 7.2 Complete server, database, and SDK integration work until the end-to-end smoke test passes.
- [x] 7.3 Add examples for Rust, Go, and Python SDK usage covering Cache and MQ.
- [x] 7.4 Document the TDD workflow used for this change and how to run each test layer locally.
- [x] 7.5 Document phase-one limitations, including PostgreSQL-backed cache latency, logical capacity semantics, and MQ at-least-once processing.
