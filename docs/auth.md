# gRPC Client Authentication

PGApp can require key+secret authentication for gRPC clients. Authentication is
disabled by default for local compatibility.

## Enable

```sh
PGAPP_ENABLE_AUTH=true
```

When enabled, every gRPC request must include metadata headers:

```text
x-pgapp-key: <client key>
x-pgapp-secret: <client secret>
```

Health and readiness RPCs remain unauthenticated so load balancers can probe the
server without credentials.

## Credential Storage

Credentials are stored in PostgreSQL table `pgapp_clients`.

- `client_key` is the public identifier.
- `secret_hash` stores an Argon2 password hash.
- plaintext secrets are never stored.
- create and rotate operations return the plaintext secret exactly once.
- deactivated clients fail authentication immediately.

## Admin HTTP API

All routes require `Authorization: Bearer $PGAPP_ADMIN_TOKEN`.

```text
GET  /api/admin/clients
POST /api/admin/clients
POST /api/admin/clients/{client_key}/rotate
POST /api/admin/clients/{client_key}/deactivate
```

Create a client:

```sh
curl -X POST \
  -H "Authorization: Bearer $PGAPP_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"client_key":"svc-billing","roles":["service"]}' \
  http://127.0.0.1:8080/api/admin/clients
```

Rotate a secret:

```sh
curl -X POST \
  -H "Authorization: Bearer $PGAPP_ADMIN_TOKEN" \
  http://127.0.0.1:8080/api/admin/clients/svc-billing/rotate
```

Deactivate a client:

```sh
curl -X POST \
  -H "Authorization: Bearer $PGAPP_ADMIN_TOKEN" \
  http://127.0.0.1:8080/api/admin/clients/svc-billing/deactivate
```

## SDK Examples

Python:

```python
from pgapp_sdk import PGAppClient

client = PGAppClient(
    "127.0.0.1:50051",
    timeout=5,
    key="svc-billing",
    secret="returned-once-secret",
)
```

Rust:

```rust
let client = pgapp_sdk::PgAppClient::connect_with_timeout_and_credentials(
    "http://127.0.0.1:50051",
    Some(std::time::Duration::from_secs(5)),
    "svc-billing",
    "returned-once-secret",
)
.await?;
```

Go:

```go
client, err := pgapp.DialWithCredentials(
    ctx,
    "127.0.0.1:50051",
    5*time.Second,
    "svc-billing",
    "returned-once-secret",
)
```

TypeScript:

```ts
const client = new PGAppClient("127.0.0.1:50051", {
  timeoutMs: 5000,
  credentials: {
    key: "svc-billing",
    secret: "returned-once-secret",
  },
});
```
