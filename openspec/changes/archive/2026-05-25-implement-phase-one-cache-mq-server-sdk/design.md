## Context

The repository currently contains only OpenSpec scaffolding. Phase one establishes the first runnable product shape for a PostgreSQL-first application service suite:

```
Rust / Go / Python SDKs
          |
          v
     gRPC contract
          |
          v
  Rust server runtime
   |              |
   v              v
Cache service   MQ service
   |              |
   v              v
PostgreSQL     PostgreSQL
```

PostgreSQL remains the primary infrastructure dependency. The server layer exists to provide a stable protocol, safe validation, policy enforcement, observability, and language-neutral client APIs rather than forcing every application to call PostgreSQL functions directly.

## Goals / Non-Goals

**Goals:**

- Ship a Rust gRPC server that hosts phase-one Cache and MQ services.
- Provide PostgreSQL-backed key/value cache behavior with TTL, invalidation, logical capacity limits, eviction, and stats.
- Provide self-managed PostgreSQL queue behavior with queue lifecycle, send/read/delete/archive, delayed delivery, visibility timeouts, long polling, retry count exposure, and queue metrics.
- Generate and package Rust, Go, and Python SDKs from the same protobuf contract with small idiomatic wrappers.
- Provide local development and deployment assets for PostgreSQL, schema bootstrap, and service configuration.
- Use test-driven development so product behavior is specified by failing tests before implementation code is added.

**Non-Goals:**

- Vector search, full-text search, admin UI, hosted control plane, billing, or multi-cluster orchestration.
- Full Redis or RabbitMQ protocol compatibility.
- Redis-class in-memory latency or hard physical memory caps for cache storage.
- Business-level exactly-once message processing. MQ provides visibility-timeout based exclusive delivery windows; callers remain responsible for idempotent handlers.
- Production high availability beyond what the underlying PostgreSQL deployment provides.

## Decisions

### Test-Driven Development Workflow

Implementation must follow a red-green-refactor loop:

```
spec scenario
     |
     v
write failing test
     |
     v
minimal implementation
     |
     v
refactor with tests green
```

The test layer should match the behavior under development:

- Pure validation, serialization, error mapping, TTL math, capacity accounting, and SQL query helpers use unit tests.
- PostgreSQL schema, cache storage, MQ locking, visibility timeout, redelivery, archive, purge, and metrics use database integration tests.
- gRPC request/response behavior, health/readiness, and error mapping use server integration tests.
- Rust, Go, and Python SDK parity uses SDK conformance tests against the same running server.
- A final smoke test starts PostgreSQL and the Rust server, then exercises Cache and MQ through an SDK.

Production behavior is not considered complete until the corresponding failing test has been made green. Temporary spikes are allowed only for learning an API or toolchain; spike code must not be kept as production implementation unless it is covered by tests.

Alternatives considered:

- Implement first and add tests afterward: faster in the moment, but too likely to miss concurrency, TTL, and SDK parity regressions.
- Rely mainly on manual smoke testing: useful as a final check, but insufficient for queue locking and expiration edge cases.

### Single Rust gRPC Server Binary

Use one Rust binary that hosts Cache and MQ services behind one gRPC endpoint. The runtime owns configuration, PostgreSQL pool management, service registration, health checks, metrics, and error mapping.

Alternatives considered:

- Separate binaries per capability: cleaner isolation, but more deployment and configuration surface for phase one.
- Direct SDK-to-PostgreSQL calls: simpler server implementation, but pushes schema/version checks, security, error mapping, and observability into every client language.

### Protobuf-First API Contract

Use protobuf files as the canonical API contract. Generate low-level clients for Rust, Go, and Python, then add thin ergonomic wrappers for common operations.

Alternatives considered:

- REST/JSON: easier ad hoc debugging, but less precise cross-language contracts and weaker streaming/long-poll ergonomics.
- Hand-written SDK-only contracts: faster initially, but likely to drift across languages.

### PostgreSQL-Backed Cache With Opaque Values

Implement Cache using owned PostgreSQL schema rather than `pg_prewarm`. `pg_prewarm` can remain an operations enhancement later, but it does not provide application cache semantics.

The cache data model should use a small set of shared tables:

```
cache_namespaces
  name
  generation
  created_at
  updated_at

cache_entries
  namespace
  generation
  key_hash
  key
  value_bytes
  metadata
  expires_at
  size_bytes
  last_accessed_at
  access_count
  created_at
  updated_at
```

Values are opaque bytes at the protocol level. SDKs can provide helpers for strings and JSON, but the server must preserve bytes exactly.

Use `UNLOGGED` storage by default for cache entries because cache data is disposable and should avoid unnecessary WAL pressure. Provide a durable mode only if configuration requires cache data to survive PostgreSQL restart/crash recovery.

Alternatives considered:

- JSONB-only values: convenient for inspection, but constrains binary payloads and language-neutral usage.
- Table per namespace: strong isolation, but heavier migrations and operational complexity.
- External Redis: lower latency for hot paths, but contradicts the PostgreSQL-first constraint for phase one.

### TTL, Invalidation, and Capacity Policy

TTL is enforced with read-time checks plus a background sweeper. Expired rows may remain physically present until cleanup, but must be invisible to reads.

Namespace invalidation uses a generation counter. Invalidating a namespace increments its generation, making old entries unreachable without a large synchronous delete.

Logical capacity is enforced by configured maximum key count and maximum byte count. Eviction should remove expired entries first, then live entries by least-recently-used order. Access metadata should be updated with sampling or throttling so hot reads do not become excessive writes.

Alternatives considered:

- Immediate physical deletion for namespace invalidation: simple, but risky for large namespaces and lock-heavy.
- Exact LRU update on every read: precise, but creates write amplification and table bloat.
- Physical memory accounting: misleading for PostgreSQL-backed storage; phase one should expose logical size and document the distinction.

### Owned PostgreSQL MQ Backend

Implement MQ with owned PostgreSQL tables rather than PGMQ. The server owns the queue state machine and uses PostgreSQL transactions, indexes, and row-level locking to provide queue semantics.

The active queue model should use shared tables rather than one physical table per queue:

```
mq_queues
  id
  name
  durable
  max_receive_count
  default_visibility_timeout_seconds
  created_at
  updated_at

mq_messages
  id
  queue_id
  payload
  headers
  available_at
  visibility_timeout_at
  read_count
  last_read_at
  created_at
  updated_at

mq_archives
  id
  queue_id
  original_message_id
  payload
  headers
  read_count
  enqueued_at
  archived_at
```

Delayed delivery is represented by `available_at > now()`. In-flight delivery is represented by `visibility_timeout_at > now()`. A message is visible when `available_at <= now()` and `visibility_timeout_at <= now()` or `visibility_timeout_at IS NULL`.

Reads should claim messages inside one transaction using a `SELECT ... FOR UPDATE SKIP LOCKED` subquery followed by an `UPDATE ... RETURNING`:

```
WITH picked AS (
  SELECT id
  FROM mq_messages
  WHERE queue_id = $1
    AND available_at <= now()
    AND (visibility_timeout_at IS NULL OR visibility_timeout_at <= now())
  ORDER BY id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE mq_messages m
SET visibility_timeout_at = now() + $3::interval,
    read_count = read_count + 1,
    last_read_at = now(),
    updated_at = now()
FROM picked
WHERE m.id = picked.id
RETURNING m.*;
```

`SKIP LOCKED` allows concurrent consumers to claim different messages without blocking each other on already-claimed rows. Deleting a message removes it from `mq_messages`. Archiving moves it to `mq_archives` and removes it from the active table in one transaction. Visibility extension updates `visibility_timeout_at` for an active message.

Long polling should be implemented by the server as a bounded wait loop that repeatedly attempts a transactional read until a message is claimed or the poll deadline expires. A later optimization can add PostgreSQL `LISTEN/NOTIFY`, but phase one should not require a second connection-management model unless benchmarks justify it.

The MQ service contract should describe delivery as at-least-once processing with visibility-timeout based exclusive delivery. Messages that are read but not deleted or archived become visible again after the visibility timeout.

Alternatives considered:

- PGMQ extension: less code and proven queue primitives, but adds an extension dependency and gives the project less control over schema, lifecycle, and product-specific behavior.
- One physical table per queue: simpler purges and per-queue storage modes, but requires dynamic DDL, more identifier handling, and heavier operational cleanup.
- External RabbitMQ/SQS: richer ecosystem, but adds infrastructure contrary to the phase-one product promise.

### Runtime Safety and Observability

The server must validate identifiers, request sizes, timeouts, and quantities before reaching SQL. Storage and validation failures must map to stable gRPC status codes. The runtime should expose health/readiness and metrics for request counts, latencies, PostgreSQL pool state, cache stats, and queue stats.

Alternatives considered:

- Let raw database errors pass through: faster to implement, but unstable and unsafe as a public API.
- Minimal health endpoint only: insufficient for diagnosing schema readiness, pool exhaustion, cache eviction, or queue backlog.

### SDK Shape

Each SDK should expose one top-level client with Cache and MQ subclients:

```
client.cache.set(...)
client.cache.get(...)
client.mq.send(...)
client.mq.read(...)
```

Generated clients remain available for advanced users, while wrappers cover common flows with language-native errors, timeouts, and serialization helpers.

## Risks / Trade-offs

- PostgreSQL-backed cache latency is higher than an in-memory cache -> Document the intended use cases and avoid Redis-compatible claims.
- Cache reads can cause write amplification if every hit updates access metadata -> Use sampled or throttled metadata updates.
- Logical capacity can diverge from physical disk usage because PostgreSQL reclaims space later -> Report logical usage clearly and rely on autovacuum/maintenance for physical cleanup.
- Namespace invalidation leaves old rows behind until sweeping -> Make old rows unreachable immediately and clean them in bounded batches.
- Custom MQ correctness depends on transaction boundaries and indexes -> Cover concurrent consumers, redelivery, archive, purge, and metrics with database integration tests.
- MQ active tables can accumulate dead tuples under heavy delete/archive throughput -> Use bounded cleanup, proper indexes, and document autovacuum expectations.
- Long polling with a bounded retry loop can add query load on empty queues -> Enforce poll limits and keep `LISTEN/NOTIFY` available as a future optimization.
- MQ redelivery can duplicate business effects -> Return read counts, document at-least-once semantics, and encourage idempotency keys in payloads.
- Multi-language SDKs can drift -> Generate from one proto contract and add conformance tests that exercise the same scenarios in Rust, Go, and Python.
- TDD can slow early scaffolding if the test harness is weak -> Build the test harness first and keep initial tests narrow enough to drive one behavior at a time.

## Migration Plan

1. Add protobuf contracts for Cache, MQ, health/capability status, and shared error/status fields.
2. Add database bootstrap for cache and MQ schema objects, indexes, and readiness checks.
3. Add the Rust server runtime with feature flags for Cache and MQ.
4. Add SDK generation and wrappers for Rust, Go, and Python.
5. Add local Docker Compose and integration test fixtures.
6. Roll out by applying database bootstrap, starting the server with health checks enabled, then publishing SDK packages.

Rollback should prefer service rollback and feature disablement. Cache data is disposable. MQ data should remain in PostgreSQL unless an operator explicitly chooses to purge or drop queues.

## Open Questions

- Should cache durable mode be included in phase one, or should all cache storage be unlogged until a concrete need appears?
- Should tag invalidation be a phase-one feature, or should phase one stop at key delete and namespace invalidation?
- Should the first SDK release include high-level JSON helpers for Cache, or only opaque bytes plus MQ JSON helpers?
- What minimum PostgreSQL version should be supported for phase one?
