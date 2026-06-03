# Config Center

PGApp Config Center is an Apollo-like configuration service backed by
PostgreSQL. The first supported value model is `key -> JSON value` with draft
editing, explicit publish, immutable release snapshots, JSON Schema validation,
and bounded long-poll change detection.

## Scope Model

Every item belongs to one scope:

```text
app_id / environment / cluster / namespace
```

The tuple keeps application, deployment environment, rollout cluster, and
logical namespace separate. Keys are unique inside one scope.

Example:

```text
billing / prod / default / application / feature_flags
```

## Draft And Publish

Draft writes update `config_items` and are not visible to application clients
until published.

```text
UpsertItem/DeleteItem
        |
        v
draft rows in config_items
        |
        | Publish
        v
immutable config_releases snapshot
```

Publishing creates the next revision for that scope and stores a complete JSON
snapshot plus checksum. Clients read published releases through `GetRelease` or
watch for newer revisions through `Watch`.

## JSON Schema Validation

`SetSchema` attaches one JSON Schema document to a scope. When a schema exists,
the server validates draft upserts and publish attempts. Invalid values fail
with a stable invalid-argument error and do not update draft or release state.

The schema is intentionally scoped with the config namespace instead of a global
registry. This keeps early operations simple and avoids cross-application schema
coupling.

## Long Polling

`Watch(scope, known_revision, timeout_seconds)` is unary long polling:

```text
client known revision R
        |
        v
server checks latest release
        |
        +-- latest > R -> changed=true, return release immediately
        |
        +-- latest <= R -> wait up to timeout, then changed=false
```

The server caps the wait with `PGAPP_MAX_CONFIG_WATCH_SECONDS`. Clients should
loop with their last observed revision and reconnect after timeout or transient
network errors.

## Admin UI

The Admin UI owns operator workflows for Config Center:

- list scopes
- inspect draft items
- edit JSON values
- delete draft keys
- publish a release
- view release history
- edit or remove the optional JSON Schema

Cache and active MQ data are read-only in Admin UI, but Config Center is
operator-managed there by design.

## Limits

- Values are JSON documents, not typed language objects.
- Config Center is not a secret manager.
- There is no RBAC, approval workflow, gray release, or streaming watch in this
  version.
- Secret-shaped values should be stored in a dedicated secret system and
  referenced from config.
