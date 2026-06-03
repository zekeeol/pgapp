## 1. API Contract and Data Model

- [x] 1.1 Define `ConfigService` protobuf contract for scopes, draft items, releases, release lists, and unary long-poll watch responses.
- [x] 1.2 Add failing proto contract tests proving generated Rust clients/messages expose the Config service surface.
- [x] 1.3 Design PostgreSQL schema migration for `config_scopes`, `config_items`, and `config_releases` with idempotent tables, uniqueness constraints, and read indexes.
- [x] 1.4 Define validation rules for scope components, config keys, JSON value payloads, publish messages, and watch timeout bounds.
- [x] 1.5 Define runtime configuration defaults for enabling Config Center and maximum long-poll timeout.

## 2. Core Store TDD

- [x] 2.1 Write failing core integration tests for creating scopes and upserting `key -> JSON value` draft items.
- [x] 2.2 Implement Config core store module and migration wiring after the draft item tests fail.
- [x] 2.3 Write failing tests proving scopes are isolated by `app_id`, `environment`, `cluster`, and `namespace`.
- [x] 2.4 Write failing tests for draft delete/tombstone behavior before publish.
- [x] 2.5 Write failing tests for invalid scope, key, and malformed JSON validation errors.
- [x] 2.6 Write failing tests proving draft edits do not change latest published releases.

## 3. Publish and Release Semantics

- [x] 3.1 Write failing tests for publish creating monotonically increasing immutable revisions.
- [x] 3.2 Implement transactional publish with scope row locking, full JSONB snapshot creation, checksum generation, and publish metadata.
- [x] 3.3 Write failing tests proving published releases remain immutable after later draft edits.
- [x] 3.4 Write failing tests proving deleted draft items are omitted from subsequent release snapshots.
- [x] 3.5 Add release history listing with pagination and tests for stable ordering.
- [x] 3.6 Decide and test same-checksum publish behavior from the design open question.

## 4. Long-Poll Watch

- [x] 4.1 Write failing tests for watch returning immediately when a newer revision already exists.
- [x] 4.2 Write failing tests for watch returning `changed=false` when no release appears before timeout.
- [x] 4.3 Write failing tests for watch completing when another task publishes a newer release before timeout.
- [x] 4.4 Implement bounded unary long-poll using PostgreSQL-backed checks and optional `LISTEN/NOTIFY` without exposing streaming to clients.
- [x] 4.5 Add timeout cap enforcement and tests for over-limit watch requests.

## 5. Server Integration

- [x] 5.1 Wire `ConfigServiceServer` into `pgapp-server` after failing server integration tests confirm the service is missing.
- [x] 5.2 Add Config Center availability to health/readiness capability reporting.
- [x] 5.3 Add runtime metrics recording for config draft, publish, release read, and watch methods.
- [x] 5.4 Add gRPC integration tests for draft upsert, publish, latest release read, specific revision read, and watch.
- [x] 5.5 Verify disabled Config Center runtime behavior if a service toggle is introduced.

## 6. SDKs

- [x] 6.1 Regenerate protobuf clients for Rust, Go, and Python.
- [x] 6.2 Add Rust SDK Config client helpers for reading latest releases, reading specific revisions, and watching for changes.
- [x] 6.3 Add Go SDK Config client helpers with JSON map/document handling and watch no-change results.
- [x] 6.4 Add Python SDK Config client helpers with full type hints for JSON-compatible values and watch no-change results.
- [x] 6.5 Add live SDK tests for Config Center publish/read/watch behavior in Rust, Go, and Python.

## 7. Admin HTTP API

- [x] 7.1 Define Admin Config DTOs for scope lists, draft items, release snapshots, release history, publish requests, and JSON validation errors.
- [x] 7.2 Add failing Admin HTTP tests for token-protected scope browsing and draft reads.
- [x] 7.3 Add Admin HTTP routes for listing scopes, reading draft items, upserting JSON draft items, deleting draft items, publishing releases, and listing release history.
- [x] 7.4 Add tests proving Admin HTTP Config mutation requires valid Admin token and returns stable JSON errors.
- [x] 7.5 Add tests proving Admin HTTP publish creates client-visible releases while draft edits alone do not.

## 8. Admin UI

- [x] 8.1 Add failing Admin UI tests for a Config navigation item, scope list, draft item table, JSON editor, publish action, and release history.
- [x] 8.2 Build Config page shell in the existing React + Vite Admin UI without adding a landing page.
- [x] 8.3 Add scope browsing and selection UI with loading, error, and empty states.
- [x] 8.4 Add JSON draft item editor with client-side JSON parsing feedback and server error display.
- [x] 8.5 Add release publish workflow and release history/snapshot inspection.
- [x] 8.6 Verify the UI does not expose raw SQL or secret-specific display behavior.
- [x] 8.7 Verify responsive layout for the Config page at desktop and smaller laptop widths.

## 9. Deployment, Documentation, and End-to-End Verification

- [x] 9.1 Update Docker Compose/local deployment verification to show Config Center schema initialization and Admin UI availability.
- [x] 9.2 Update README and docs with Config Center concepts, scope model, draft/publish behavior, long-poll behavior, and SDK examples.
- [x] 9.3 Add Docker-backed integration coverage for PostgreSQL, server, Admin UI, Admin HTTP Config routes, and SDK Config read/watch flows.
- [x] 9.4 Run `openspec validate "add-config-center" --type change --strict`.
- [x] 9.5 Run full verification: Rust workspace tests, Admin UI tests/build, Go SDK tests, Python unittest/mypy, and Docker-backed integration.
