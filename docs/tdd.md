# Test-Driven Development

This change is implemented with a red-green-refactor loop.

1. Start each production behavior with a failing automated test.
2. Add the smallest implementation that makes the test pass.
3. Refactor while keeping the relevant tests green.

Test layers:

- Rust unit tests: validation, configuration, errors, metrics, and pure domain helpers.
- Database integration tests: Cache schema, MQ schema, Config Center schema, cache behavior, queue visibility, token-scoped ack, archive, redelivery, DLQ movement, auth storage, and concurrent reads. These run when `DATABASE_URL` points to a PostgreSQL database.
- Server integration tests: gRPC method behavior, health, readiness, auth metadata, Admin HTTP auth, read-only Admin Cache/MQ views, and error mapping.
- Admin UI tests: React component and route behavior with mocked Admin HTTP responses.
- SDK tests: Rust, Go, Python, and TypeScript client shape and conformance.
- End-to-end smoke tests: PostgreSQL plus the Rust server plus SDK calls, including Docker-backed local deployment flows.

Run everything available locally:

```sh
scripts/check.sh
```

Run the Docker-backed integration suite:

```sh
scripts/integration.sh
```
