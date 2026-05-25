use pgapp_core::{config::ServerConfig, db};
use pgapp_proto::pgapp::v1::{
    CreateQueueRequest, GetCacheRequest, HealthRequest, QueueStorageMode, ReadMessagesRequest,
    ReadinessRequest, RuntimeMetricsRequest, SendMessageRequest, SetCacheRequest,
    cache_service_client::CacheServiceClient, health_service_client::HealthServiceClient,
    mq_service_client::MqServiceClient,
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};
use tonic::Code;

static NEXT_ID: AtomicU64 = AtomicU64::new(10_000);

async fn spawn_server(cache_enabled: bool, mq_enabled: bool) -> Option<String> {
    spawn_server_with_env(cache_enabled, mq_enabled, HashMap::new()).await
}

async fn spawn_server_with_env(
    cache_enabled: bool,
    mq_enabled: bool,
    extra_env: HashMap<String, String>,
) -> Option<String> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&database_url, 1, 5).await.ok()?;
    db::apply_schema(&pool).await.ok()?;
    let addr = free_addr();
    let mut cfg_map = HashMap::from([
        ("DATABASE_URL".to_string(), database_url),
        ("PGAPP_BIND_ADDR".to_string(), addr.to_string()),
        ("PGAPP_ENABLE_CACHE".to_string(), cache_enabled.to_string()),
        ("PGAPP_ENABLE_MQ".to_string(), mq_enabled.to_string()),
    ]);
    cfg_map.insert("PGAPP_MAX_CONNECTIONS".to_string(), "5".to_string());
    cfg_map.extend(extra_env);
    let cfg = ServerConfig::from_map(cfg_map).ok()?;
    tokio::spawn(async move {
        let _ = pgapp_server::serve(addr, pool, cfg).await;
    });
    let endpoint = format!("http://{addr}");
    wait_for_server(&endpoint).await.ok()?;
    Some(endpoint)
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn wait_for_server(endpoint: &str) -> Result<(), tonic::transport::Error> {
    for _ in 0..20 {
        if HealthServiceClient::connect(endpoint.to_string())
            .await
            .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    HealthServiceClient::connect(endpoint.to_string())
        .await
        .map(|_| ())
}

fn unique(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}_{id}")
}

#[tokio::test]
async fn health_readiness_reports_enabled_capabilities() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = HealthServiceClient::connect(endpoint).await.unwrap();
    assert!(
        client
            .get_health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .live
    );
    let readiness = client
        .get_readiness(ReadinessRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(readiness.ready);
    assert!(readiness.capabilities.iter().any(|cap| cap.name == "cache"));
    assert!(readiness.capabilities.iter().any(|cap| cap.name == "mq"));
}

#[tokio::test]
async fn cache_grpc_round_trip() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = CacheServiceClient::connect(endpoint).await.unwrap();
    let namespace = unique("grpc_cache");
    client
        .set(SetCacheRequest {
            namespace: namespace.clone(),
            key: "hello".to_string(),
            value: b"world".to_vec(),
            ttl_seconds: 60,
        })
        .await
        .unwrap();
    let response = client
        .get(GetCacheRequest {
            namespace,
            key: "hello".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.hit);
    assert_eq!(response.value, b"world");
}

#[tokio::test]
async fn mq_grpc_send_read_delete() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = MqServiceClient::connect(endpoint).await.unwrap();
    let queue = unique("grpc_orders");
    client
        .create_queue(CreateQueueRequest {
            queue_name: queue.clone(),
            storage_mode: QueueStorageMode::Durable as i32,
        })
        .await
        .unwrap();
    let sent = client
        .send(SendMessageRequest {
            queue_name: queue.clone(),
            json_payload: r#"{"ok":true}"#.to_string(),
            delay_seconds: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let read = client
        .read(ReadMessagesRequest {
            queue_name: queue,
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0].message_id, sent.message_id);
}

#[tokio::test]
async fn disabled_mq_rejects_calls() {
    let Some(endpoint) = spawn_server(true, false).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = MqServiceClient::connect(endpoint).await.unwrap();
    let err = client
        .create_queue(CreateQueueRequest {
            queue_name: unique("disabled_orders"),
            storage_mode: QueueStorageMode::Durable as i32,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
}

#[tokio::test]
async fn configured_request_limits_are_enforced() {
    let Some(endpoint) = spawn_server_with_env(
        true,
        true,
        HashMap::from([
            ("PGAPP_MAX_BATCH_SIZE".to_string(), "1".to_string()),
            ("PGAPP_MAX_PAYLOAD_BYTES".to_string(), "4".to_string()),
            (
                "PGAPP_MAX_VISIBILITY_TIMEOUT_SECONDS".to_string(),
                "1".to_string(),
            ),
        ]),
    )
    .await
    else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };

    let mut cache = CacheServiceClient::connect(endpoint.clone()).await.unwrap();
    let err = cache
        .set(SetCacheRequest {
            namespace: unique("limited_cache"),
            key: "too_big".to_string(),
            value: b"12345".to_vec(),
            ttl_seconds: 60,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let queue = unique("limited_orders");
    let mut mq = MqServiceClient::connect(endpoint).await.unwrap();
    mq.create_queue(CreateQueueRequest {
        queue_name: queue.clone(),
        storage_mode: QueueStorageMode::Durable as i32,
    })
    .await
    .unwrap();

    let err = mq
        .send(SendMessageRequest {
            queue_name: queue.clone(),
            json_payload: r#"{"too":"large"}"#.to_string(),
            delay_seconds: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = mq
        .read(ReadMessagesRequest {
            queue_name: queue,
            quantity: 2,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn runtime_metrics_record_completed_requests_and_pool_state() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };

    let namespace = unique("metrics_cache");
    let mut cache = CacheServiceClient::connect(endpoint.clone()).await.unwrap();
    cache
        .set(SetCacheRequest {
            namespace: namespace.clone(),
            key: "hello".to_string(),
            value: b"world".to_vec(),
            ttl_seconds: 60,
        })
        .await
        .unwrap();
    cache
        .get(GetCacheRequest {
            namespace,
            key: "hello".to_string(),
        })
        .await
        .unwrap();

    let mut health = HealthServiceClient::connect(endpoint).await.unwrap();
    let metrics = health
        .get_runtime_metrics(RuntimeMetricsRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(metrics.pg_pool.is_some());
    assert!(
        metrics
            .methods
            .iter()
            .any(|metric| metric.service == "cache"
                && metric.method == "get"
                && metric.status == "ok"
                && metric.count >= 1)
    );
}

#[tokio::test]
async fn configured_default_request_timeout_is_enforced() {
    let Some(endpoint) = spawn_server_with_env(
        true,
        true,
        HashMap::from([("PGAPP_DEFAULT_TIMEOUT_SECONDS".to_string(), "1".to_string())]),
    )
    .await
    else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };

    let queue = unique("timeout_orders");
    let mut mq = MqServiceClient::connect(endpoint).await.unwrap();
    mq.create_queue(CreateQueueRequest {
        queue_name: queue.clone(),
        storage_mode: QueueStorageMode::Durable as i32,
    })
    .await
    .unwrap();

    let err = mq
        .read_with_poll(pgapp_proto::pgapp::v1::ReadWithPollRequest {
            queue_name: queue,
            quantity: 1,
            visibility_timeout_seconds: 30,
            max_poll_seconds: 3,
            poll_interval_millis: 100,
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::DeadlineExceeded);
}
