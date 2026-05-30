## Why

PGApp already provides PostgreSQL-backed Cache and MQ services, but applications
still need a central, versioned configuration source instead of shipping runtime
configuration through environment variables, ad hoc database tables, or cache
entries. A PostgreSQL-first configuration center gives operators Apollo-like
draft, publish, and client update workflows without introducing a separate
configuration system.

## What Changes

- Add a new `ConfigService` gRPC API for managing and consuming application
  configuration.
- Store configuration in PostgreSQL as scoped `key -> JSON value` items using
  `jsonb`.
- Support Apollo-like draft and publish semantics:
  - writes update draft state only
  - publish creates an immutable release snapshot with a monotonically
    increasing revision
  - clients read published releases, not drafts
- Support long-poll based change detection before introducing streaming:
  clients send a known revision and receive a newer release or a no-change
  response after timeout.
- Add SDK support in Rust, Go, and Python for reading latest releases and
  waiting for changes.
- Add Admin UI configuration management pages for browsing scopes, editing JSON
  draft items, publishing releases, and viewing release history.
- Add Admin HTTP endpoints for Config Center operations, protected by the
  existing Admin token.

## Capabilities

### New Capabilities

- `config-center`: PostgreSQL-backed application configuration center with
  scoped JSON config items, draft/publish releases, read APIs, long-poll change
  detection, SDK access, and Admin UI management.

### Modified Capabilities

- None. Existing Cache, MQ, SDK, server runtime, and Admin UI capabilities keep
  their current behavior. This change adds a new service and surfaces it through
  the existing server, SDK, and Admin UI integration points.

## Impact

- New PostgreSQL migration for configuration scopes, draft items, immutable
  releases, and optional release notification state.
- New protobuf file and generated clients for `ConfigService`.
- New Rust core store/module for config validation, draft mutation, publishing,
  release reads, and long-poll behavior.
- `pgapp-server` gains a `ConfigServiceServer` and config capability reporting.
- Rust, Go, and Python SDKs gain typed Config clients using JSON values.
- Admin HTTP API gains token-protected Config routes.
- Admin UI gains a Config page with scope selection, JSON item editing,
  release publishing, and release history.
- Docker/local deployment continues to use PostgreSQL only; no Redis, etcd,
  Consul, or Apollo service dependency is introduced.
