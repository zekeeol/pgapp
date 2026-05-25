# server-runtime Specification

## Purpose
Define pgapp-server runtime behavior for configuration, PostgreSQL readiness,
gRPC service hosting, stable error mapping, health checks, and operational
metrics.

## Requirements
### Requirement: Configurable server startup
The server runtime MUST load configuration for the gRPC bind address, PostgreSQL connection string, connection pool settings, enabled services, request limits, and timeout defaults before accepting traffic.

#### Scenario: Start with valid configuration
- **WHEN** the server is started with valid runtime and database configuration
- **THEN** it MUST bind the configured gRPC address and initialize the configured PostgreSQL pool

#### Scenario: Reject invalid configuration
- **WHEN** required configuration is missing or malformed
- **THEN** the server MUST fail startup with a clear configuration error

### Requirement: Capability readiness checks
The server runtime MUST verify PostgreSQL connectivity and required database capabilities for each enabled service before reporting ready.

#### Scenario: Required MQ schema missing
- **WHEN** MQ is enabled and the required MQ tables or indexes are unavailable
- **THEN** readiness MUST report MQ as unavailable and the MQ service MUST NOT accept normal queue operations

#### Scenario: Database reachable and capabilities available
- **WHEN** PostgreSQL is reachable and required schema objects are available
- **THEN** readiness MUST report enabled services as available

### Requirement: gRPC service hosting
The server runtime MUST expose Cache and MQ gRPC services when they are enabled and MUST reject calls to disabled services with a stable unavailable error.

#### Scenario: Enabled service accepts calls
- **WHEN** Cache is enabled and the server is ready
- **THEN** a Cache get request MUST reach the Cache service implementation

#### Scenario: Disabled service is unavailable
- **WHEN** MQ is disabled by configuration
- **THEN** MQ method calls MUST fail with a stable unavailable error

### Requirement: Stable error mapping
The server runtime MUST translate validation, not-found, conflict, timeout, and database-unavailable failures into stable gRPC status codes and structured error details.

#### Scenario: Invalid request maps to invalid argument
- **WHEN** a client sends a request with an invalid queue name, cache key, quantity, or timeout
- **THEN** the server MUST return an invalid-argument error with a machine-readable reason

#### Scenario: Database outage maps to unavailable
- **WHEN** PostgreSQL is unavailable during a request
- **THEN** the server MUST return an unavailable error without leaking raw database internals

### Requirement: Runtime observability
The server runtime MUST expose operational signals for health, readiness, request counts, request latency, error counts, PostgreSQL pool state, and per-service metrics.

#### Scenario: Request metrics include service and method
- **WHEN** a Cache or MQ request completes
- **THEN** runtime metrics MUST record the service name, method name, status, and latency

#### Scenario: Health reflects process liveness
- **WHEN** the server process is running
- **THEN** the health endpoint MUST report liveness independently from database readiness
