# PGApp Rust SDK

Typed Rust client for PGApp Cache, MQ, and Config gRPC services.

## Usage

```rust
use pgapp_sdk::PgAppClient;
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PgAppClient::connect_with_timeout_and_credentials(
        "http://127.0.0.1:50051",
        Some(Duration::from_secs(5)),
        "svc-billing",
        "secret",
    )
    .await?;

    let mut cache = client.cache();
    cache.set("default", "hello", b"world".to_vec(), Some(60)).await?;
    let counter = cache.increment("default", "counter", 1, Some(60)).await?;
    let lock_created = cache
        .set_nx("default", "lock:orders", b"1".to_vec(), Some(30))
        .await?;
    let old_value = cache
        .get_set("default", "last-release", b"v2".to_vec(), Some(60))
        .await?;
    let new_len = cache.append("default", "log", b"\nentry".to_vec(), Some(60)).await?;

    println!("counter={counter} lock_created={lock_created} old={old_value:?} len={new_len}");

    let mut mq = client.mq();
    mq.create_queue("orders").await?;
    let message_id = mq.send_json("orders", &json!({"order_id": 123})).await?;
    let messages = mq.read("orders", 1, 30).await?;
    if let Some(message) = messages.first() {
        assert_eq!(message.message_id, message_id);
        mq.ack("orders", message.message_id, &message.ack_token).await?;
    }

    let dlq_messages = mq.list_dlq_messages("orders", 10, 0).await?;
    if let Some(message) = dlq_messages.first() {
        let fetched = mq.get_dlq_message("orders", message.original_message_id).await?;
        mq.reprocess_dlq_message("orders", fetched.original_message_id).await?;
    }

    let stream_message_id = mq.send_json("orders", &json!({"stream": true})).await?;
    let mut stream = mq.stream_read("orders", 1, 30).await?;
    if let Some(batch) = stream.message().await? {
        assert_eq!(batch.messages[0].message_id, stream_message_id);
    }

    let mut config = client.config();
    let scope = pgapp_sdk::ConfigClient::scope("billing", "prod", "default", "application");
    let release = config.get_latest_release(scope).await?;
    println!("config revision {}", release.revision);

    Ok(())
}
```

When `PGAPP_ENABLE_AUTH=true`, create or rotate client credentials through the
Admin HTTP API and pass the returned secret to
`connect_with_timeout_and_credentials`. The plaintext secret is shown only once.
