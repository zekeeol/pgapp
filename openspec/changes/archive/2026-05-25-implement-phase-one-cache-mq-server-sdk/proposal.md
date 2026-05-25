## Why

This project needs a concrete phase-one foundation for a PostgreSQL-first application service suite. The first release should prove the core product promise: expose useful cache and queue capabilities through a stable Rust gRPC server and multi-language SDKs while keeping PostgreSQL as the primary infrastructure dependency.

## What Changes

- Introduce an application-facing Cache service that provides key/value operations, TTL, invalidation, logical capacity limits, eviction policies, and cache statistics on top of PostgreSQL-backed storage.
- Introduce an MQ service implemented with owned PostgreSQL schema, transactions, row locking, and indexes for queue creation, message send/read/ack/archive, visibility timeouts, delayed delivery, long polling, retry tracking, and queue metrics.
- Introduce a Rust gRPC server runtime that owns database connectivity, service registration, configuration, health checks, capability checks, observability, and consistent error mapping.
- Introduce Rust, Go, and Python SDKs that expose a stable client API for the Cache and MQ services.
- Establish phase-one deployment and development assets for running PostgreSQL, applying the owned schema, and running the Rust service locally.
- Use test-driven development for implementation: each behavior must begin with a failing automated test, then minimal implementation, then refactoring while tests stay green.
- Defer Vector and Search modules to later phases.

## Capabilities

### New Capabilities
- `cache-service`: Key/value cache behavior including TTL, invalidation, capacity policy, eviction, and stats.
- `mq-service`: PostgreSQL-backed queue behavior including queue lifecycle, production, consumption, acknowledgement, visibility timeout, retry, archive, and metrics.
- `server-runtime`: Rust gRPC runtime behavior including configuration, database connectivity, service hosting, health, readiness, error mapping, and observability.
- `multi-language-sdk`: Rust, Go, and Python SDK behavior for connecting to the server and calling Cache and MQ APIs.

### Modified Capabilities

None.

## Impact

- New protobuf API contracts for Cache, MQ, server health, and shared error/status concepts.
- New Rust service workspace with gRPC server, SQL access layer, configuration, metrics, and integration tests.
- New PostgreSQL schema requirements for cache storage and self-managed PostgreSQL message queues, with no PGMQ dependency.
- New SDK packages for Rust, Go, and Python generated from protobuf plus ergonomic wrappers.
- New local deployment assets such as Docker Compose, database initialization SQL, and development documentation.
- Test suites become the primary acceptance gate for server runtime behavior, Cache behavior, MQ behavior, SDK parity, and end-to-end flows.
