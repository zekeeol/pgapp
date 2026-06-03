# config-json-schema-validation Specification

## Purpose
TBD - created by archiving change add-phase-two-features. Update Purpose after archive.
## Requirements
### Requirement: Schema attachment per scope
The Config Center SHALL allow operators to attach an optional JSON Schema (Draft 2020-12 or earlier) to any scope. The schema SHALL be stored as a JSONB value in the scope's configuration. Operators SHALL be able to set, update, or remove the schema via Admin API.

#### Scenario: Attach a schema to a scope
- **WHEN** an operator attaches a valid JSON Schema to scope `billing/prod/default/application`
- **THEN** subsequent draft item upserts SHALL validate against that schema

#### Scenario: Remove a schema from a scope
- **WHEN** an operator removes the schema from a scope
- **THEN** subsequent draft item upserts SHALL proceed without schema validation

#### Scenario: Reject invalid JSON Schema on attachment
- **WHEN** an operator attempts to attach a string that is not a valid JSON Schema
- **THEN** the service SHALL reject the request with an invalid-argument error describing the schema issue

### Requirement: Draft item validation on upsert
When a scope has an attached JSON Schema, the `UpsertItem` RPC SHALL validate the draft item's `json_value` against the schema. Validation SHALL reject items whose values do not conform to the schema. Deleted items (marked `deleted = true`) SHALL NOT be validated against the schema.

#### Scenario: Valid draft item is accepted
- **WHEN** a scope has a schema requiring a `host` string property and a `port` integer property
- **THEN** an upsert for key `db` with value `{"host": "localhost", "port": 5432}` SHALL succeed

#### Scenario: Invalid draft item is rejected
- **WHEN** a scope has a schema requiring integer `port` and the draft value has `"port": "not-a-number"`
- **THEN** the upsert SHALL fail with an invalid-argument error describing the validation failure

#### Scenario: Deleted items bypass validation
- **WHEN** a scope has a schema and an operator marks a key as deleted
- **THEN** the delete operation SHALL succeed regardless of whether the key's value conforms to the schema

### Requirement: Publish-time validation gate
The `Publish` RPC SHALL validate all non-deleted draft items against the scope's schema (if one is attached) before creating a release. If any non-deleted item fails validation, the publish SHALL be rejected and no release SHALL be created.

#### Scenario: Publish succeeds when all items are valid
- **WHEN** all non-deleted draft items pass schema validation
- **THEN** the publish SHALL create a new release successfully

#### Scenario: Publish is blocked by invalid items
- **WHEN** at least one non-deleted draft item fails schema validation
- **THEN** the publish SHALL fail with an error listing the keys and validation failures

#### Scenario: Publish succeeds when no schema is attached
- **WHEN** a scope has no attached schema
- **THEN** publish SHALL proceed without schema validation

### Requirement: Schema size and complexity limits
The Config Center SHALL enforce a maximum JSON Schema size to prevent resource exhaustion during validation. The limit SHALL be configurable via server configuration.

#### Scenario: Reject oversized schema
- **WHEN** an operator attempts to attach a schema exceeding the configured maximum size
- **THEN** the service SHALL reject the request with an invalid-argument error

