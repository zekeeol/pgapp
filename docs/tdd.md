# Test-Driven Development

This change is implemented with a red-green-refactor loop.

1. Start each production behavior with a failing automated test.
2. Add the smallest implementation that makes the test pass.
3. Refactor while keeping the relevant tests green.

Test layers:

- Rust unit tests: validation, configuration, errors, metrics, and pure domain helpers.
- Database integration tests: cache schema, MQ schema, cache behavior, queue visibility, token-scoped ack, archive, redelivery, and concurrent reads. These run when `DATABASE_URL` points to a PostgreSQL database.
- Server integration tests: gRPC method behavior, health, readiness, and error mapping.
- SDK tests: Rust, Go, and Python client shape and conformance.
- End-to-end smoke tests: PostgreSQL plus the Rust server plus SDK calls.

Run everything available locally:

```sh
scripts/check.sh
```
