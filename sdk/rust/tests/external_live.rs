use pgapp_sdk::PgAppClient;
use serde_json::json;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tonic::Code;

static NEXT_ID: AtomicU64 = AtomicU64::new(30_000);

fn external_endpoint() -> Option<String> {
    let raw = std::env::var("PGAPP_TEST_ENDPOINT").ok()?;
    if raw.starts_with("http://") || raw.starts_with("https://") {
        Some(raw)
    } else {
        Some(format!("http://{raw}"))
    }
}

fn unique(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}_{id}")
}

#[tokio::test]
async fn external_rust_sdk_cache_and_mq_round_trip() {
    let Some(endpoint) = external_endpoint() else {
        eprintln!("skipping external live test: PGAPP_TEST_ENDPOINT is not set");
        return;
    };
    let client = PgAppClient::connect_with_timeout(endpoint, Some(Duration::from_secs(5)))
        .await
        .unwrap();

    let namespace = unique("rust_external_cache");
    let queue = unique("rust_external_orders");

    let mut cache = client.cache();
    cache
        .set(&namespace, "hello", b"world".to_vec(), Some(60))
        .await
        .unwrap();
    assert_eq!(
        cache.get(&namespace, "hello").await.unwrap(),
        Some(b"world".to_vec())
    );

    let mut mq = client.mq();
    assert!(mq.create_queue(&queue).await.unwrap());
    let message_id = mq.send_json(&queue, &json!({"ok": true})).await.unwrap();
    let messages = mq.read(&queue, 1, 30).await.unwrap();
    assert_eq!(messages[0].message_id, message_id);
    assert!(mq.delete(&queue, message_id).await.unwrap());
}

#[tokio::test]
async fn external_rust_sdk_preserves_error_status() {
    let Some(endpoint) = external_endpoint() else {
        eprintln!("skipping external live test: PGAPP_TEST_ENDPOINT is not set");
        return;
    };
    let client = PgAppClient::connect_with_timeout(endpoint, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let mut cache = client.cache();
    let err = cache
        .set("bad namespace", "key", b"value".to_vec(), Some(60))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
