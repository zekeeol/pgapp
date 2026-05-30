## ADDED Requirements

### Requirement: Scoped JSON configuration items
The Config Center MUST store draft configuration as scoped `key -> JSON value`
items. A scope MUST include `app_id`, `environment`, `cluster`, and `namespace`.
The service MUST validate scope components and keys using safe identifier rules
and MUST reject invalid JSON values.

#### Scenario: Store JSON value in a scope
- **WHEN** an admin stores JSON value `{"enabled": true}` for key `feature_flags` in scope `billing/prod/default/application`
- **THEN** the draft for that exact scope MUST contain key `feature_flags` with that JSON value

#### Scenario: Isolate scopes
- **WHEN** the same key is stored in scopes `billing/prod/default/application` and `billing/staging/default/application`
- **THEN** reads of each draft scope MUST return only the value stored for that scope

#### Scenario: Reject invalid JSON
- **WHEN** a client attempts to store malformed JSON for a configuration item
- **THEN** the service MUST reject the request with a stable invalid-argument error

### Requirement: Draft edits are not client-visible until publish
The Config Center MUST separate draft configuration from published releases.
Draft upserts and deletes MUST NOT change the latest published release until a
publish operation succeeds.

#### Scenario: Draft upsert does not change latest release
- **WHEN** an admin updates a draft item after revision `3` is published
- **THEN** clients reading the latest published release MUST still receive revision `3`

#### Scenario: Draft delete is hidden until publish
- **WHEN** an admin deletes a draft item after it appears in the latest published release
- **THEN** clients reading the latest published release MUST still see the item until the next publish succeeds

### Requirement: Publish creates immutable releases
The Config Center MUST publish draft configuration by creating an immutable
release snapshot for the scope. Each release MUST have a monotonically
increasing revision, a complete JSON object snapshot, a checksum, and publish
metadata.

#### Scenario: Publish creates next revision
- **WHEN** the current published revision for a scope is `4` and an admin publishes the draft
- **THEN** the service MUST create revision `5` for that scope

#### Scenario: Release snapshot includes active draft items
- **WHEN** an admin publishes a draft containing active keys `a` and `b` and deleted key `c`
- **THEN** the release snapshot MUST include keys `a` and `b` and MUST NOT include key `c`

#### Scenario: Published release is immutable
- **WHEN** revision `2` has been published and later draft edits are made
- **THEN** reading revision `2` MUST return the original snapshot for revision `2`

### Requirement: Client release reads
The Config Center MUST allow clients to read the latest published release or a
specific published revision for a scope. Client release reads MUST NOT expose
unpublished draft data.

#### Scenario: Read latest release
- **WHEN** a client requests the latest release for a scope with revision `7`
- **THEN** the response MUST include revision `7`, its checksum, publish metadata, and the JSON snapshot

#### Scenario: Read specific revision
- **WHEN** a client requests revision `3` for a scope with revisions `1` through `7`
- **THEN** the response MUST include the immutable snapshot for revision `3`

#### Scenario: Missing release is stable
- **WHEN** a client requests a scope or revision that does not exist
- **THEN** the service MUST return a stable not-found error

### Requirement: Long-poll change detection
The Config Center MUST provide unary long-poll change detection. A client MUST
send a scope, known revision, and bounded timeout. The service MUST return
immediately when a newer release exists, otherwise wait until a newer release is
published or the timeout expires.

#### Scenario: Watch returns immediately for newer release
- **WHEN** a client watches a scope with known revision `3` and the latest release is revision `4`
- **THEN** the service MUST immediately return `changed=true` with revision `4`

#### Scenario: Watch returns on publish
- **WHEN** a client watches a scope with known revision `4` and revision `5` is published before the watch timeout
- **THEN** the service MUST return `changed=true` with revision `5`

#### Scenario: Watch times out without change
- **WHEN** a client watches a scope with known revision `4` and no newer release is published before the timeout
- **THEN** the service MUST return `changed=false` and the latest known revision

#### Scenario: Watch timeout is bounded
- **WHEN** a client requests a long-poll timeout greater than the configured maximum
- **THEN** the service MUST cap the timeout or reject the request with a stable validation error

### Requirement: Config Center SDK support
The Rust, Go, and Python SDKs MUST expose Config Center helpers for reading
latest releases, reading specific revisions, and long-polling for changes using
typed JSON values in each language.

#### Scenario: Python SDK reads latest JSON release
- **WHEN** a Python client requests the latest release for a scope
- **THEN** the SDK MUST return the release revision and a Python JSON-compatible mapping

#### Scenario: SDK watch exposes no-change response
- **WHEN** an SDK client watches a scope and no new release appears before timeout
- **THEN** the SDK MUST expose a no-change result without throwing an error

### Requirement: Admin UI configuration management
The Admin UI MUST provide a Config section for managing Config Center data. The
UI MUST allow authenticated admins to browse scopes, inspect draft items, edit
JSON values, delete draft items, publish releases, and view release history.

#### Scenario: Admin edits draft JSON
- **WHEN** an authenticated admin edits a JSON value for a config key and saves it
- **THEN** the draft item MUST be updated and the UI MUST show the saved draft value

#### Scenario: Admin publishes a release
- **WHEN** an authenticated admin publishes a draft scope
- **THEN** the UI MUST show the newly published revision in release history

#### Scenario: Admin sees JSON validation feedback
- **WHEN** an admin enters malformed JSON in the Config editor
- **THEN** the UI MUST show a stable validation error and MUST NOT publish the malformed value

### Requirement: Runtime and deployment integration
The Config Center MUST integrate with the existing PGApp runtime and deployment
model. Server startup MUST initialize the Config Center schema with PostgreSQL,
health/readiness MUST report Config Center availability, and Docker Compose
deployment MUST require no external configuration service.

#### Scenario: Startup initializes config schema
- **WHEN** `pgapp-server` starts against an empty PostgreSQL database
- **THEN** the required Config Center tables and indexes MUST be created

#### Scenario: Readiness reports config capability
- **WHEN** the Config Center schema is available and the service is enabled
- **THEN** readiness MUST report the config capability as available

#### Scenario: Docker Compose uses only PostgreSQL
- **WHEN** PGApp is deployed with the repository Docker Compose configuration
- **THEN** Config Center MUST run using the existing PostgreSQL service without Redis, etcd, Consul, or Apollo dependencies
