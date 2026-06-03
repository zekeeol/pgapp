# server-runtime Specification

## Purpose
Define pgapp-server runtime behavior for configuration, PostgreSQL readiness,
gRPC service hosting, stable error mapping, health checks, and operational
metrics.
## Requirements
### Requirement: Configurable server startup
The server runtime MUST load configuration for the gRPC bind address, PostgreSQL connection string, connection pool settings, enabled services, request limits, timeout defaults, authentication settings, DLQ settings, NOTIFY toggle, and schema validation limits before accepting traffic.

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
The server runtime MUST expose Cache, MQ, and Config Center gRPC services when they are enabled and MUST reject calls to disabled services with a stable unavailable error. When authentication is enabled, the server MUST apply the auth interceptor to all services except health checks.

#### Scenario: Enabled service accepts calls
- **WHEN** Cache is enabled and the server is ready
- **THEN** a Cache get request MUST reach the Cache service implementation

#### Scenario: Authenticated request reaches enabled service
- **WHEN** auth is enabled, Cache is enabled, and a client provides valid credentials
- **THEN** a Cache get request MUST reach the Cache service implementation with client identity in extensions

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
The server runtime MUST expose operational signals for health, readiness, request counts, request latency, error counts, authentication failures, PostgreSQL pool state, and per-service metrics.

#### Scenario: Request metrics include service and method
- **WHEN** a Cache or MQ request completes
- **THEN** runtime metrics MUST record the service name, method name, status, and latency

#### Scenario: Auth failure metrics are recorded
- **WHEN** an authentication attempt fails
- **THEN** runtime metrics MUST record the authentication failure count

#### Scenario: Health reflects process liveness
- **WHEN** the server process is running
- **THEN** the health endpoint MUST report liveness independently from database readiness

### Requirement: Authentication interceptor
The server runtime SHALL support an optional gRPC authentication interceptor that validates `x-pgapp-key` and `x-pgapp-secret` metadata headers against the `pgapp_clients` database table. When authentication is enabled, the interceptor SHALL reject unauthenticated requests with `UNAUTHENTICATED` status, except for health check RPCs. When disabled, all requests SHALL pass through without authentication.

#### Scenario: Auth interceptor passes valid credentials
- **WHEN** auth is enabled and a request includes valid key and secret metadata
- **THEN** the interceptor SHALL inject the client identity into request extensions and allow the request to proceed

#### Scenario: Auth interceptor rejects invalid credentials
- **WHEN** auth is enabled and a request includes invalid credentials
- **THEN** the interceptor SHALL return `UNAUTHENTICATED` without invoking the service handler

#### Scenario: Auth interceptor allows health checks
- **WHEN** auth is enabled and a request targets `GetHealth` or `GetReadiness` without credentials
- **THEN** the interceptor SHALL allow the request to proceed

### Requirement: DLQ configuration
The server runtime SHALL support configuration for DLQ behavior via environment variables: `PGAPP_MAX_REDELIVERY_COUNT` (default 0, meaning DLQ is disabled) and `PGAPP_DLQ_RETENTION_DAYS` (default 0, meaning indefinite retention). The server SHALL pass these values to the MQ store at initialization.

#### Scenario: DLQ is disabled by default
- **WHEN** `PGAPP_MAX_REDELIVERY_COUNT` is not set
- **THEN** messages SHALL be redelivered indefinitely without dead-lettering

#### Scenario: DLQ is enabled with configuration
- **WHEN** `PGAPP_MAX_REDELIVERY_COUNT` is set to 3
- **THEN** messages read more than 3 times SHALL be moved to the DLQ

### Requirement: NOTIFY configuration
The server runtime SHALL support a `PGAPP_ENABLE_NOTIFY` configuration (default true). When enabled, the server SHALL use PostgreSQL LISTEN/NOTIFY for MQ message delivery notification. When disabled, the server SHALL fall back to polling-based delivery.

#### Scenario: NOTIFY is enabled by default
- **WHEN** `PGAPP_ENABLE_NOTIFY` is not set
- **THEN** the server SHALL use LISTEN/NOTIFY for MQ message delivery

### Requirement: Client credential management endpoints
The server runtime's Admin HTTP API SHALL provide endpoints for managing gRPC client credentials: `GET /api/admin/clients` (list), `POST /api/admin/clients` (create), `POST /api/admin/clients/:key/rotate` (rotate secret), `POST /api/admin/clients/:key/deactivate` (deactivate). These endpoints SHALL require the Admin token for authorization.

#### Scenario: Admin creates a client credential
- **WHEN** an admin with a valid token creates a new client with key `svc-billing`
- **THEN** the response SHALL include the generated plaintext secret exactly once

#### Scenario: Non-admin cannot access client endpoints
- **WHEN** a request without a valid admin token attempts to list clients
- **THEN** the server SHALL return 401 Unauthorized
