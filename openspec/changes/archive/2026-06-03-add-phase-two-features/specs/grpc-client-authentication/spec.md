# grpc-client-authentication Specification

## Purpose
Define key+secret gRPC client authentication: credential storage, metadata extraction, validation, and client credential management.

## ADDED Requirements

### Requirement: Client credential storage
The server SHALL store client credentials in a `pgapp_clients` PostgreSQL table. Each client record SHALL contain a unique client key (public identifier), a hashed secret (bcrypt), an active flag, optional role labels, and creation/update timestamps. The server SHALL NOT store plaintext secrets.

#### Scenario: Create a new client credential
- **WHEN** an administrator creates a new client with key `svc-billing` and a generated secret
- **THEN** the key SHALL be stored as-is and the secret SHALL be stored as a bcrypt hash

#### Scenario: Deactivate a client credential
- **WHEN** an administrator deactivates client `svc-billing`
- **THEN** subsequent authentication attempts with that key SHALL fail with an unauthenticated error

### Requirement: Metadata-based authentication
When authentication is enabled, the gRPC server SHALL require `x-pgapp-key` and `x-pgapp-secret` metadata headers on every request (except health checks). The server SHALL validate the credentials against the `pgapp_clients` table on each request. Valid credentials SHALL result in the client identity being available to service handlers via request extensions. Invalid or missing credentials SHALL result in an `UNAUTHENTICATED` gRPC status.

#### Scenario: Authenticated request succeeds
- **WHEN** a client includes valid `x-pgapp-key` and `x-pgapp-secret` metadata on a Cache `Get` request
- **THEN** the request SHALL reach the Cache service handler with the client identity available in extensions

#### Scenario: Invalid secret is rejected
- **WHEN** a client provides a valid `x-pgapp-key` but an incorrect `x-pgapp-secret`
- **THEN** the server SHALL return `UNAUTHENTICATED` without processing the request

#### Scenario: Missing metadata is rejected
- **WHEN** a client does not include `x-pgapp-key` or `x-pgapp-secret` metadata
- **THEN** the server SHALL return `UNAUTHENTICATED` without processing the request

#### Scenario: Inactive client is rejected
- **WHEN** a client provides valid credentials for a deactivated client record
- **THEN** the server SHALL return `UNAUTHENTICATED` without processing the request

### Requirement: Health check bypass
The gRPC health check RPCs (`GetHealth`, `GetReadiness`) SHALL NOT require authentication, even when authentication is enabled. This ensures load balancers and monitoring systems can check server health without credentials.

#### Scenario: Health check succeeds without credentials
- **WHEN** authentication is enabled and a client calls `GetHealth` without metadata headers
- **THEN** the server SHALL return a successful health response

### Requirement: Authentication is opt-in
Authentication SHALL be disabled by default. When `PGAPP_ENABLE_AUTH` is false or unset, all requests SHALL be processed without credential validation. This preserves backward compatibility for existing deployments.

#### Scenario: Requests succeed when auth is disabled
- **WHEN** authentication is disabled
- **THEN** all gRPC requests SHALL be processed normally regardless of metadata presence

### Requirement: Client credential management
The Admin HTTP API SHALL provide endpoints for listing, creating, rotating, and deactivating client credentials. Creating a new client SHALL return the generated secret exactly once (the plaintext secret is not stored and cannot be recovered). Rotation SHALL generate a new secret while preserving the client key.

#### Scenario: Create client returns secret once
- **WHEN** an administrator creates a new client credential
- **THEN** the response SHALL include the plaintext secret, and no subsequent API call SHALL be able to retrieve it

#### Scenario: Rotate secret invalidates old secret
- **WHEN** an administrator rotates the secret for client `svc-billing`
- **THEN** the old secret SHALL immediately stop working and the new secret SHALL be returned in the response
