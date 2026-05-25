use pgapp_core::{config::ServerConfig, db};
use pgapp_sdk::PgAppClient;
use serde_json::json;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tonic::Code;

static NEXT_ID: AtomicU64 = AtomicU64::new(20_000);

async fn spawn_server() -> Option<String> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&database_url, 1, 5).await.ok()?;
    db::apply_schema(&pool).await.ok()?;
    let addr = free_addr();
    let cfg = ServerConfig::from_map(HashMap::from([
        ("DATABASE_URL".to_string(), database_url),
        ("PGAPP_BIND_ADDR".to_string(), addr.to_string()),
        ("PGAPP_MAX_CONNECTIONS".to_string(), "5".to_string()),
    ]))
    .ok()?;
    tokio::spawn(async move {
        let _ = pgapp_server::serve(addr, pool, cfg).await;
    });
    let endpoint = format!("http://{addr}");
    for _ in 0..20 {
        if PgAppClient::connect(endpoint.clone()).await.is_ok() {
            return Some(endpoint);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    None
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn unique(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}_{id}")
}

#[tokio::test]
async fn rust_sdk_cache_and_mq_round_trip() {
    let Some(endpoint) = spawn_server().await else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = PgAppClient::connect(endpoint).await.unwrap();

    let namespace = unique("sdk_cache");
    let mut cache = client.cache();
    cache
        .set(&namespace, "hello", b"world".to_vec(), Some(60))
        .await
        .unwrap();
    assert_eq!(
        cache.get(&namespace, "hello").await.unwrap(),
        Some(b"world".to_vec())
    );

    let queue = unique("sdk_orders");
    let mut mq = client.mq();
    assert!(mq.create_queue(&queue).await.unwrap());
    let message_id = mq.send_json(&queue, &json!({"ok": true})).await.unwrap();
    let messages = mq.read(&queue, 1, 30).await.unwrap();
    assert_eq!(messages[0].message_id, message_id);
    assert!(mq.delete(&queue, message_id).await.unwrap());
}

#[tokio::test]
async fn rust_sdk_exposes_phase_one_cache_and_mq_surface() {
    let Some(endpoint) = spawn_server().await else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = PgAppClient::connect(endpoint).await.unwrap();

    let namespace = unique("sdk_surface_cache");
    let mut cache = client.cache();
    assert!(
        cache
            .set(&namespace, "a", b"one".to_vec(), Some(60))
            .await
            .unwrap()
    );
    assert!(
        cache
            .set(&namespace, "b", b"two".to_vec(), Some(60))
            .await
            .unwrap()
    );
    let items = cache
        .mget(&namespace, &["a".to_string(), "missing".to_string()])
        .await
        .unwrap();
    assert_eq!(items.len(), 2);
    assert!(cache.exists(&namespace, "a").await.unwrap());
    assert!(cache.delete(&namespace, "b").await.unwrap());
    assert!(cache.invalidate_namespace(&namespace).await.unwrap());
    let stats = cache.stats().await.unwrap();
    assert!(stats.writes >= 2);

    let queue = unique("sdk_surface_orders");
    let mut mq = client.mq();
    assert!(mq.create_queue(&queue).await.unwrap());
    let ids = mq
        .send_json_batch(&queue, &[json!({"n": 1}), json!({"n": 2})], 0)
        .await
        .unwrap();
    assert_eq!(ids.len(), 2);
    let messages = mq.read_with_poll(&queue, 1, 30, 1, 25).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert!(
        mq.set_visibility_timeout(&queue, messages[0].message_id, 0)
            .await
            .unwrap()
    );
    assert!(mq.archive(&queue, messages[0].message_id).await.unwrap());
    let metrics = mq.metrics(&queue).await.unwrap();
    assert_eq!(metrics.archived_message_count, 1);
    assert!(mq.purge_queue(&queue).await.unwrap());
    assert!(mq.drop_queue(&queue).await.unwrap());
}

#[tokio::test]
async fn rust_sdk_preserves_endpoint_and_timeout_configuration() {
    let Some(endpoint) = spawn_server().await else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = PgAppClient::connect_with_timeout(endpoint.clone(), Some(Duration::from_secs(3)))
        .await
        .unwrap();
    assert_eq!(client.endpoint(), endpoint);
    assert_eq!(client.timeout(), Some(Duration::from_secs(3)));
}

#[tokio::test]
async fn rust_sdk_preserves_error_status() {
    let Some(endpoint) = spawn_server().await else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = PgAppClient::connect(endpoint).await.unwrap();
    let mut cache = client.cache();
    let err = cache
        .set("bad namespace", "key", b"value".to_vec(), Some(60))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
