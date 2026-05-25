# PGApp

PGApp is a PostgreSQL-first application service suite. Phase one provides a
Rust gRPC server with PostgreSQL-backed Cache and MQ services, plus Rust, Go,
and Python SDKs generated from shared protobuf contracts.

The MQ implementation uses owned PostgreSQL tables, transactions, indexes, and
`FOR UPDATE SKIP LOCKED`. It does not depend on PGMQ.

## Architecture

```text
Rust / Go / Python SDKs
        |
        | gRPC
        v
pgapp-server
  - Cache service
  - MQ service
  - Health, readiness, runtime metrics
        |
        | SQLx
        v
PostgreSQL
  - cache_namespaces / cache_entries / cache_stats
  - mq_queues / mq_messages / mq_archives
```

## Features

- Cache: namespace-scoped key/value storage, TTL, batch get, exact delete,
  namespace invalidation, logical capacity limits, LRU eviction, and stats.
- MQ: queue lifecycle, JSON message production, batch send, delayed delivery,
  visibility timeout, long polling, delete acknowledgement, archive
  acknowledgement, visibility extension, and queue metrics.
- Server runtime: configurable PostgreSQL pool, service toggles, request
  limits, default request timeout, health/readiness checks, and runtime metrics.
- SDKs: idiomatic Rust, Go, and Python clients for the phase-one Cache and MQ
  APIs.

## Local Deployment

If port `5432` is already in use, run PostgreSQL on `15432` and the gRPC server
on `50051`:

```sh
docker network create pgapp-local-net 2>/dev/null || true

docker run --name pgapp-local-postgres \
  --network pgapp-local-net \
  -e POSTGRES_DB=pgapp \
  -e POSTGRES_USER=pgapp \
  -e POSTGRES_PASSWORD=secret \
  -p 127.0.0.1:15432:5432 \
  -d postgres:17

docker build -t pgapp-server:local .

docker run --name pgapp-local-server \
  --network pgapp-local-net \
  -e DATABASE_URL='postgres://pgapp:secret@pgapp-local-postgres:5432/pgapp' \
  -e PGAPP_BIND_ADDR='0.0.0.0:50051' \
  -p 127.0.0.1:50051:50051 \
  -d pgapp-server:local
```

Client endpoint:

```text
127.0.0.1:50051
```

Database URL from the host:

```text
postgres://pgapp:secret@127.0.0.1:15432/pgapp
```

See [docs/local-deployment.md](docs/local-deployment.md) for verification and
operations commands.

## Configuration

Required:

```sh
DATABASE_URL=postgres://pgapp:secret@127.0.0.1:15432/pgapp
```

Common optional settings:

```sh
PGAPP_BIND_ADDR=127.0.0.1:50051
PGAPP_MIN_CONNECTIONS=1
PGAPP_MAX_CONNECTIONS=20
PGAPP_ENABLE_CACHE=true
PGAPP_ENABLE_MQ=true
PGAPP_MAX_BATCH_SIZE=100
PGAPP_MAX_PAYLOAD_BYTES=1048576
PGAPP_MAX_VISIBILITY_TIMEOUT_SECONDS=43200
PGAPP_DEFAULT_TIMEOUT_SECONDS=30
PGAPP_CACHE_MAX_KEYS=100000
PGAPP_CACHE_MAX_BYTES=1073741824
```

Omit `PGAPP_CACHE_MAX_KEYS` or `PGAPP_CACHE_MAX_BYTES` for unbounded logical
cache limits.

## SDK Quick Start

Python:

```python
from pgapp_sdk import PGAppClient

client = PGAppClient("127.0.0.1:50051", timeout=5)

client.cache.set("default", "hello", b"world", ttl_seconds=60)
assert client.cache.get("default", "hello") == b"world"

client.mq.create_queue("orders")
message_id = client.mq.send_json("orders", {"order_id": 123})
messages = client.mq.read("orders", quantity=1, visibility_timeout_seconds=30)

client.mq.delete("orders", message_id)
```

Rust:

```rust
use pgapp_sdk::PgAppClient;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PgAppClient::connect("http://127.0.0.1:50051").await?;

    let mut cache = client.cache();
    cache.set("default", "hello", b"world".to_vec(), Some(60)).await?;

    let mut mq = client.mq();
    mq.create_queue("orders").await?;
    let message_id = mq.send_json("orders", &json!({"order_id": 123})).await?;
    mq.delete("orders", message_id).await?;

    Ok(())
}
```

Go:

```go
package main

import (
    "context"
    "time"

    pgapp "github.com/zekee/pgapp/sdk/go/pgapp"
)

func main() {
    ctx := context.Background()
    client, err := pgapp.Dial(ctx, "127.0.0.1:50051", 5*time.Second)
    if err != nil {
        panic(err)
    }

    _, err = client.Cache().Set(ctx, "default", "hello", []byte("world"), 60)
    if err != nil {
        panic(err)
    }

    _, err = client.MQ().CreateQueue(ctx, "orders")
    if err != nil {
        panic(err)
    }
}
```

## MQ Acknowledgement Model

PGApp MQ is at-least-once. `Read` makes messages invisible for the requested
visibility timeout. Successful processing is acknowledged by either:

- `Delete`: remove the message from the active queue.
- `Archive`: remove the message from the active queue and retain it in
  `mq_archives`.

There is no separate `Ack` RPC in phase one. See
[docs/mq-semantics.md](docs/mq-semantics.md) for details and current
limitations.

## Testing

Run unit, integration, SDK, and type checks available in the current
environment:

```sh
scripts/check.sh
```

Run the full Docker-backed integration test:

```sh
scripts/integration.sh
```

Validate archived OpenSpec specs:

```sh
openspec validate --specs --strict
```

## Documentation

- [docs/local-deployment.md](docs/local-deployment.md)
- [docs/mq-semantics.md](docs/mq-semantics.md)
- [docs/limitations.md](docs/limitations.md)
- [docs/schema-rollback.md](docs/schema-rollback.md)
- [docs/tdd.md](docs/tdd.md)
