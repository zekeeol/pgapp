use pgapp_core::{client_auth::ClientStore, config::ServerConfig, db};
use pgapp_proto::pgapp::v1::{
    PublishConfigRequest, UpsertConfigItemRequest,
    config_service_client::ConfigServiceClient as GeneratedConfigClient,
};
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
    spawn_server_with_env(HashMap::new()).await
}

async fn spawn_server_with_env(extra_env: HashMap<String, String>) -> Option<String> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&database_url, 1, 5).await.ok()?;
    db::apply_schema(&pool).await.ok()?;
    let addr = free_addr();
    let mut cfg_map = HashMap::from([
        ("DATABASE_URL".to_string(), database_url),
        ("PGAPP_BIND_ADDR".to_string(), addr.to_string()),
        ("PGAPP_MAX_CONNECTIONS".to_string(), "5".to_string()),
    ]);
    cfg_map.extend(extra_env);
    let cfg = ServerConfig::from_map(cfg_map).ok()?;
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

async fn create_client_credentials() -> Option<(String, String)> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&database_url, 1, 5).await.ok()?;
    db::apply_schema(&pool).await.ok()?;
    let key = unique("rust_sdk_auth_client");
    let created = ClientStore::new(pool)
        .create_client(&key, vec!["cache".to_string(), "mq".to_string()])
        .await
        .ok()?;
    Some((created.client_key, created.secret))
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn unique(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{id}_{nanos}")
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
    assert!(!messages[0].ack_token.is_empty());
    assert!(
        mq.ack(&queue, message_id, &messages[0].ack_token)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn rust_sdk_config_read_and_watch_round_trip() {
    let Some(endpoint) = spawn_server().await else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };
    let scope =
        pgapp_sdk::ConfigClient::scope(unique("rust_sdk_config"), "prod", "default", "application");
    let mut generated = GeneratedConfigClient::connect(endpoint.clone())
        .await
        .unwrap();
    generated
        .upsert_item(UpsertConfigItemRequest {
            scope: Some(scope.clone()),
            key: "feature_flags".to_string(),
            json_value: r#"{"enabled":true}"#.to_string(),
        })
        .await
        .unwrap();
    generated
        .publish(PublishConfigRequest {
            scope: Some(scope.clone()),
            message: "sdk release".to_string(),
            published_by: "rust-sdk-test".to_string(),
        })
        .await
        .unwrap();

    let client = PgAppClient::connect(endpoint).await.unwrap();
    let mut config = client.config();
    let release = config.get_latest_release(scope.clone()).await.unwrap();
    assert_eq!(release.revision, 1);
    assert_eq!(release.snapshot["feature_flags"]["enabled"], true);

    let watch = config.watch(scope, release.revision, 0).await.unwrap();
    assert!(!watch.changed);
    assert_eq!(watch.latest_revision, release.revision);
    assert!(watch.release.is_none());
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
        mq.set_visibility_timeout(&queue, messages[0].message_id, &messages[0].ack_token, 30,)
            .await
            .unwrap()
    );
    assert!(
        mq.archive(&queue, messages[0].message_id, &messages[0].ack_token)
            .await
            .unwrap()
    );
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

#[tokio::test]
async fn rust_sdk_phase_two_cache_mq_dlq_and_stream_surface() {
    let Some(endpoint) = spawn_server_with_env(HashMap::from([(
        "PGAPP_MAX_REDELIVERY_COUNT".to_string(),
        "1".to_string(),
    )]))
    .await
    else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = PgAppClient::connect(endpoint).await.unwrap();

    let namespace = unique("rust_sdk_phase_two_cache");
    let mut cache = client.cache();
    assert_eq!(
        cache
            .increment(&namespace, "counter", 2, Some(60))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        cache
            .decrement(&namespace, "counter", 1, None)
            .await
            .unwrap(),
        1
    );
    assert!(
        cache
            .set_nx(&namespace, "lock", b"first".to_vec(), Some(60))
            .await
            .unwrap()
    );
    assert!(
        !cache
            .set_nx(&namespace, "lock", b"second".to_vec(), Some(60))
            .await
            .unwrap()
    );
    assert_eq!(
        cache
            .get_set(&namespace, "slot", b"new".to_vec(), Some(60))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        cache
            .get_set(&namespace, "slot", b"newer".to_vec(), Some(60))
            .await
            .unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        cache
            .append(&namespace, "log", b"tail".to_vec(), None)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        cache
            .prepend(&namespace, "log", b"head-".to_vec(), None)
            .await
            .unwrap(),
        9
    );

    let queue = unique("rust_sdk_phase_two_orders");
    let mut mq = client.mq();
    assert!(mq.create_queue(&queue).await.unwrap());
    let poison_id = mq
        .send_json(&queue, &json!({"poison": true}))
        .await
        .unwrap();
    let first = mq.read(&queue, 1, 0).await.unwrap().remove(0);
    assert_eq!(first.message_id, poison_id);
    assert!(mq.read(&queue, 1, 0).await.unwrap().is_empty());
    let dlq = mq.list_dlq_messages(&queue, 10, 0).await.unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(
        mq.get_dlq_message(&queue, poison_id)
            .await
            .unwrap()
            .original_message_id,
        poison_id
    );
    assert!(mq.reprocess_dlq_message(&queue, poison_id).await.unwrap());
    assert!(mq.purge_dlq(&queue).await.unwrap());

    let stream_queue = unique("rust_sdk_stream_orders");
    assert!(mq.create_queue(&stream_queue).await.unwrap());
    let mut stream = mq.stream_read(&stream_queue, 1, 30).await.unwrap();
    let sent = mq
        .send_json(&stream_queue, &json!({"stream": true}))
        .await
        .unwrap();
    let pushed = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(pushed.messages[0].message_id, sent);
}

#[tokio::test]
async fn rust_sdk_attaches_auth_credentials() {
    let Some((key, secret)) = create_client_credentials().await else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };
    let Some(endpoint) = spawn_server_with_env(HashMap::from([(
        "PGAPP_ENABLE_AUTH".to_string(),
        "true".to_string(),
    )]))
    .await
    else {
        eprintln!("skipping SDK live test: DATABASE_URL is not set or unavailable");
        return;
    };

    let plain = PgAppClient::connect(endpoint.clone()).await.unwrap();
    let mut plain_cache = plain.cache();
    let err = plain_cache.get("authsdk", "missing").await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    let authed = PgAppClient::connect_with_timeout_and_credentials(
        endpoint,
        Some(Duration::from_secs(3)),
        key,
        secret,
    )
    .await
    .unwrap();
    let mut cache = authed.cache();
    assert_eq!(cache.get("authsdk", "missing").await.unwrap(), None);
}
