## ADDED Requirements

### Requirement: Admin UI runtime and access control

The system MUST provide an optional Admin UI and Admin HTTP API for browser-based operations. The Admin HTTP API MUST run inside the same `pgapp-server` binary as the gRPC services, use a separate bind address, and require `PGAPP_ADMIN_TOKEN` when enabled.

#### Scenario: Admin UI disabled by default
- **WHEN** the server starts without Admin UI enablement
- **THEN** no Admin HTTP listener MUST accept requests

#### Scenario: Admin API uses a separate default bind address
- **WHEN** Admin UI is enabled without an explicit Admin bind address
- **THEN** the Admin HTTP listener MUST bind to `127.0.0.1:8080`

#### Scenario: Admin API requires configured token
- **WHEN** Admin UI is enabled without `PGAPP_ADMIN_TOKEN`
- **THEN** the server MUST fail startup or disable the Admin HTTP listener with an explicit configuration error

#### Scenario: Admin API requires credentials
- **WHEN** Admin UI is enabled with `PGAPP_ADMIN_TOKEN`
- **THEN** requests without a valid bearer credential MUST fail with an unauthorized response

### Requirement: Operational overview

The Admin UI MUST show a first-screen operational overview for server state, readiness, runtime metrics, PostgreSQL pool state, Cache summary, MQ summary, and recent errors.

#### Scenario: Overview shows server readiness
- **WHEN** an authenticated admin opens the Admin UI
- **THEN** the overview MUST show whether enabled services are ready or unavailable

#### Scenario: Overview shows runtime metrics
- **WHEN** runtime metrics are available from the server
- **THEN** the overview MUST show request counts, error counts, latency totals or derived latency, and PostgreSQL pool state

#### Scenario: Overview handles unavailable data
- **WHEN** a metric source is unavailable
- **THEN** the UI MUST show an explicit unavailable state rather than blank or misleading values

### Requirement: Persisted logs and client activity

The Admin UI MUST provide views for PostgreSQL-persisted server logs/events and client activity. Client activity MUST distinguish Admin UI sessions from application API request activity.

#### Scenario: Persist server logs to PostgreSQL
- **WHEN** the server emits log events selected for Admin visibility
- **THEN** those log events MUST be persisted to PostgreSQL with timestamp, level, source or target, message, and structured fields when available

#### Scenario: View persisted logs
- **WHEN** an authenticated admin opens the Logs view
- **THEN** persisted server log or event records MUST be shown with timestamp, level, source or target, and message

#### Scenario: Filter persisted logs
- **WHEN** an admin filters logs by level, text, time range, or source/target
- **THEN** the Logs view MUST show only matching persisted records

#### Scenario: Secrets are redacted from logs
- **WHEN** an Admin API request includes the admin bearer token
- **THEN** persisted logs MUST NOT contain the raw token value

#### Scenario: View client activity
- **WHEN** an authenticated admin opens the Clients view
- **THEN** the UI MUST show admin session activity separately from application API activity

### Requirement: Bounded Admin API reads

The Admin API MUST expose bounded read endpoints for operational data. List endpoints MUST support pagination or bounded loading and MUST enforce server-side maximum result sizes.

#### Scenario: List endpoint enforces maximum page size
- **WHEN** an Admin API client requests more records than the server maximum
- **THEN** the response MUST be capped to the server maximum or fail with a validation error

#### Scenario: List endpoint returns stable pagination metadata
- **WHEN** an Admin API client reads logs, Cache entries, or MQ messages
- **THEN** the response MUST include enough pagination metadata to request the next page when more records are available

#### Scenario: Admin API error shape is stable
- **WHEN** an Admin API request fails
- **THEN** the response MUST use a stable JSON error shape with a machine-readable code and human-readable message

### Requirement: Read-only Cache inspection

The Admin UI MUST allow authenticated admins to inspect Cache data through read-only Admin API routes. Cache views MUST support namespace browsing, key search or filtering, pagination, safe value previews, and cache statistics. The Admin API MUST NOT expose Cache set, update, delete, or namespace invalidation routes in this change.

#### Scenario: Browse cache namespaces
- **WHEN** an authenticated admin opens the Cache view
- **THEN** the UI MUST show cache namespaces with key counts and byte usage when available

#### Scenario: Browse cache entries safely
- **WHEN** an admin browses entries for a namespace
- **THEN** the UI MUST show paginated entries with truncated and explicitly encoded value previews

#### Scenario: Inspect cache entry without mutation
- **WHEN** an admin opens a cache entry detail
- **THEN** the Admin API MUST return read-only entry metadata and bounded value preview or value content without changing the entry

#### Scenario: Cache inspection does not affect cache statistics
- **WHEN** an admin browses or inspects Cache entries through the Admin API
- **THEN** the Admin API MUST NOT update access metadata, hit counters, miss counters, expiry state, namespace generation, or capacity state

#### Scenario: Cache mutation is unavailable
- **WHEN** an Admin API client attempts to set, update, delete, or invalidate Cache data through the Admin API
- **THEN** the request MUST fail with a method-not-allowed or not-found response

### Requirement: Read-only MQ inspection

The Admin UI MUST allow authenticated admins to inspect MQ queues and messages through read-only Admin API routes. MQ views MUST support queue listing, queue metrics, and message preview without accidental delivery mutation. The Admin API MUST NOT expose MQ send, delete, archive, purge, drop, or ack routes in this change.

#### Scenario: Browse queues
- **WHEN** an authenticated admin opens the MQ view
- **THEN** the UI MUST show queues with visible count, in-flight count, oldest visible age, and archived count when available

#### Scenario: Preview messages without claiming delivery
- **WHEN** an admin browses queue messages
- **THEN** the Admin API MUST NOT mutate message visibility timeout, read count, archive state, acknowledgement state, or delivery availability merely to display message previews

#### Scenario: Preview archived messages without mutation
- **WHEN** an admin browses archived MQ messages
- **THEN** the Admin API MUST return read-only archived message previews without moving messages between archive and active storage

#### Scenario: MQ mutation is unavailable
- **WHEN** an Admin API client attempts to send, delete, archive, purge, drop, or acknowledge MQ data through the Admin API
- **THEN** the request MUST fail with a method-not-allowed or not-found response

### Requirement: Safety feedback

The Admin UI MUST display stable success and error feedback for admin operations without exposing raw database internals or secrets.

#### Scenario: Operation error is visible
- **WHEN** an Admin API operation fails validation or server execution
- **THEN** the UI MUST show a stable, human-readable error message without exposing raw database internals

#### Scenario: Read operation updates visible state
- **WHEN** an admin refreshes or changes filters in a read-only view
- **THEN** the UI MUST update the affected view so the latest readable state is visible without a manual browser reload

### Requirement: Modern operational frontend

The Admin UI MUST be implemented with React and Vite. The first screen MUST be the actual operations console, not a landing page. The interface MUST use a clean, modern, compact operational design suitable for repeated admin workflows.

#### Scenario: App starts at console
- **WHEN** an admin opens the Admin UI root URL
- **THEN** the UI MUST render the operations console shell with navigation and overview content

#### Scenario: Mutation controls are absent
- **WHEN** an admin views Cache or MQ screens
- **THEN** the UI MUST NOT render controls for Cache mutation or MQ mutation

#### Scenario: Tables remain usable with large data
- **WHEN** Cache or MQ resources contain many records
- **THEN** the UI MUST use pagination, filtering, or bounded loading rather than rendering unbounded records
