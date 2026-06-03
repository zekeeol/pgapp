use pgapp_core::{client_auth::ClientStore, config::ServerConfig, db};
use pgapp_proto::pgapp::v1::{
    AckMessageRequest, AppendRequest, ConfigScope, CreateQueueRequest, DecrementRequest,
    GetCacheRequest, GetConfigReleaseRequest, GetConfigSchemaRequest, GetDlqMessageRequest,
    GetSetRequest, HealthRequest, IncrementRequest, ListDlqMessagesRequest, PrependRequest,
    PublishConfigRequest, QueueMetricsRequest, QueueStorageMode, ReadMessagesRequest,
    ReadinessRequest, ReprocessDlqMessageRequest, RuntimeMetricsRequest, SendMessageRequest,
    SetCacheRequest, SetConfigSchemaRequest, SetNxRequest, SetVisibilityTimeoutRequest,
    StreamReadRequest, UpsertConfigItemRequest, WatchConfigRequest,
    cache_service_client::CacheServiceClient, config_service_client::ConfigServiceClient,
    health_service_client::HealthServiceClient, mq_service_client::MqServiceClient,
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

async fn create_test_client_credentials() -> Option<(String, String)> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&database_url, 1, 5).await.ok()?;
    db::apply_schema(&pool).await.ok()?;
    let key = unique("grpc_auth_client");
    let created = ClientStore::new(pool)
        .create_client(&key, vec!["cache".to_string()])
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
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{id}_{nanos}")
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
    assert!(
        readiness
            .capabilities
            .iter()
            .any(|cap| cap.name == "config")
    );
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
async fn grpc_auth_requires_valid_credentials_and_bypasses_health() {
    let Some((key, secret)) = create_test_client_credentials().await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let Some(endpoint) = spawn_server_with_env(
        true,
        true,
        HashMap::from([("PGAPP_ENABLE_AUTH".to_string(), "true".to_string())]),
    )
    .await
    else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };

    let mut health = HealthServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    assert!(
        health
            .get_health(HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .live
    );

    let mut unauthenticated = CacheServiceClient::connect(endpoint.clone()).await.unwrap();
    let err = unauthenticated
        .get(GetCacheRequest {
            namespace: "auth_ns".to_string(),
            key: "missing".to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    let mut wrong_secret = CacheServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut request = tonic::Request::new(GetCacheRequest {
        namespace: "auth_ns".to_string(),
        key: "wrong".to_string(),
    });
    request
        .metadata_mut()
        .insert("x-pgapp-key", key.parse().unwrap());
    request
        .metadata_mut()
        .insert("x-pgapp-secret", "wrong-secret".parse().unwrap());
    let err = wrong_secret.get(request).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    let mut authenticated = CacheServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut request = tonic::Request::new(GetCacheRequest {
        namespace: "auth_ns".to_string(),
        key: "allowed".to_string(),
    });
    request
        .metadata_mut()
        .insert("x-pgapp-key", key.parse().unwrap());
    request
        .metadata_mut()
        .insert("x-pgapp-secret", secret.parse().unwrap());
    let response = authenticated.get(request).await.unwrap().into_inner();
    assert!(!response.hit);

    let mut health = HealthServiceClient::connect(endpoint).await.unwrap();
    let mut metrics_request = tonic::Request::new(RuntimeMetricsRequest {});
    metrics_request
        .metadata_mut()
        .insert("x-pgapp-key", key.parse().unwrap());
    metrics_request
        .metadata_mut()
        .insert("x-pgapp-secret", secret.parse().unwrap());
    let metrics = health
        .get_runtime_metrics(metrics_request)
        .await
        .unwrap()
        .into_inner();
    assert!(
        metrics.methods.iter().any(|metric| {
            metric.service == "auth"
                && metric.method == "authenticate"
                && metric.status == "unauthenticated"
                && metric.errors >= 1
        }),
        "expected auth failure metrics, got {:?}",
        metrics.methods
    );
}

#[tokio::test]
async fn grpc_auth_disabled_accepts_requests_without_credentials() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = CacheServiceClient::connect(endpoint).await.unwrap();
    let response = client
        .get(GetCacheRequest {
            namespace: "authdisabled".to_string(),
            key: "missing".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!response.hit);
}

#[tokio::test]
async fn cache_grpc_atomic_operations_round_trip() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = CacheServiceClient::connect(endpoint).await.unwrap();
    let namespace = unique("grpc_atomic_cache");

    let increment = client
        .increment(IncrementRequest {
            namespace: namespace.clone(),
            key: "counter".to_string(),
            delta: 5,
            ttl_seconds: 60,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(increment.value, 5);
    let decrement = client
        .decrement(DecrementRequest {
            namespace: namespace.clone(),
            key: "counter".to_string(),
            delta: 2,
            ttl_seconds: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(decrement.value, 3);

    let set_nx = client
        .set_nx(SetNxRequest {
            namespace: namespace.clone(),
            key: "lock".to_string(),
            value: b"first".to_vec(),
            ttl_seconds: 60,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(set_nx.created);
    let set_nx_again = client
        .set_nx(SetNxRequest {
            namespace: namespace.clone(),
            key: "lock".to_string(),
            value: b"second".to_vec(),
            ttl_seconds: 60,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!set_nx_again.created);

    let first_get_set = client
        .get_set(GetSetRequest {
            namespace: namespace.clone(),
            key: "slot".to_string(),
            value: b"new".to_vec(),
            ttl_seconds: 60,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!first_get_set.hit);
    let second_get_set = client
        .get_set(GetSetRequest {
            namespace: namespace.clone(),
            key: "slot".to_string(),
            value: b"newer".to_vec(),
            ttl_seconds: 60,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(second_get_set.hit);
    assert_eq!(second_get_set.old_value, b"new");

    assert_eq!(
        client
            .append(AppendRequest {
                namespace: namespace.clone(),
                key: "log".to_string(),
                value: b"tail".to_vec(),
                ttl_seconds: 0,
            })
            .await
            .unwrap()
            .into_inner()
            .length,
        4
    );
    assert_eq!(
        client
            .prepend(PrependRequest {
                namespace: namespace.clone(),
                key: "log".to_string(),
                value: b"head-".to_vec(),
                ttl_seconds: 0,
            })
            .await
            .unwrap()
            .into_inner()
            .length,
        9
    );
    let log = client
        .get(GetCacheRequest {
            namespace: namespace.clone(),
            key: "log".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(log.value, b"head-tail");

    client
        .set(SetCacheRequest {
            namespace: namespace.clone(),
            key: "not_numeric".to_string(),
            value: b"abc".to_vec(),
            ttl_seconds: 0,
        })
        .await
        .unwrap();
    let err = client
        .increment(IncrementRequest {
            namespace,
            key: "not_numeric".to_string(),
            delta: 1,
            ttl_seconds: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn mq_grpc_send_read_ack() {
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
            queue_name: queue.clone(),
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0].message_id, sent.message_id);
    assert!(!read.messages[0].ack_token.is_empty());

    let ack = client
        .ack(AckMessageRequest {
            queue_name: queue,
            message_id: sent.message_id,
            ack_token: read.messages[0].ack_token.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(ack.success);
}

#[tokio::test]
async fn mq_grpc_dead_letter_queue_round_trip() {
    let Some(endpoint) = spawn_server_with_env(
        true,
        true,
        HashMap::from([("PGAPP_MAX_REDELIVERY_COUNT".to_string(), "2".to_string())]),
    )
    .await
    else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = MqServiceClient::connect(endpoint).await.unwrap();
    let queue = unique("grpc_dlq_orders");
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
            json_payload: r#"{"poison":true}"#.to_string(),
            delay_seconds: 0,
        })
        .await
        .unwrap()
        .into_inner();

    let first = client
        .read(ReadMessagesRequest {
            queue_name: queue.clone(),
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner()
        .messages
        .remove(0);
    client
        .set_visibility_timeout(SetVisibilityTimeoutRequest {
            queue_name: queue.clone(),
            message_id: first.message_id,
            ack_token: first.ack_token,
            visibility_timeout_seconds: 0,
        })
        .await
        .unwrap();
    let second = client
        .read(ReadMessagesRequest {
            queue_name: queue.clone(),
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner()
        .messages
        .remove(0);
    assert_eq!(second.read_count, 2);
    client
        .set_visibility_timeout(SetVisibilityTimeoutRequest {
            queue_name: queue.clone(),
            message_id: second.message_id,
            ack_token: second.ack_token,
            visibility_timeout_seconds: 0,
        })
        .await
        .unwrap();

    let third = client
        .read(ReadMessagesRequest {
            queue_name: queue.clone(),
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(third.messages.is_empty());

    let dlq = client
        .list_dlq_messages(ListDlqMessagesRequest {
            queue_name: queue.clone(),
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dlq.messages.len(), 1);
    assert_eq!(dlq.messages[0].original_message_id, sent.message_id);
    assert!(dlq.messages[0].json_payload.contains("poison"));

    let fetched = client
        .get_dlq_message(GetDlqMessageRequest {
            queue_name: queue.clone(),
            original_message_id: sent.message_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.original_message_id, sent.message_id);
    assert!(
        client
            .metrics(QueueMetricsRequest {
                queue_name: queue.clone()
            })
            .await
            .unwrap()
            .into_inner()
            .dlq_message_count
            >= 1
    );

    let reprocessed = client
        .reprocess_dlq_message(ReprocessDlqMessageRequest {
            queue_name: queue.clone(),
            original_message_id: sent.message_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(reprocessed.success);
    let read = client
        .read(ReadMessagesRequest {
            queue_name: queue,
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.messages[0].message_id, sent.message_id);
    assert_eq!(read.messages[0].read_count, 1);
}

#[tokio::test]
async fn mq_stream_read_delivers_existing_and_future_messages() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut producer = MqServiceClient::connect(endpoint.clone()).await.unwrap();
    let queue = unique("grpc_stream_orders");
    producer
        .create_queue(CreateQueueRequest {
            queue_name: queue.clone(),
            storage_mode: QueueStorageMode::Durable as i32,
        })
        .await
        .unwrap();
    let first = producer
        .send(SendMessageRequest {
            queue_name: queue.clone(),
            json_payload: r#"{"stream":1}"#.to_string(),
            delay_seconds: 0,
        })
        .await
        .unwrap()
        .into_inner();

    let mut consumer = MqServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut stream = consumer
        .stream_read(StreamReadRequest {
            queue_name: queue.clone(),
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner();
    let existing = tokio::time::timeout(std::time::Duration::from_secs(2), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(existing.messages.len(), 1);
    assert_eq!(existing.messages[0].message_id, first.message_id);
    drop(stream);

    let mut live_consumer = MqServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut live_stream = live_consumer
        .stream_read(StreamReadRequest {
            queue_name: queue.clone(),
            quantity: 1,
            visibility_timeout_seconds: 30,
        })
        .await
        .unwrap()
        .into_inner();
    let second = producer
        .send(SendMessageRequest {
            queue_name: queue,
            json_payload: r#"{"stream":2}"#.to_string(),
            delay_seconds: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let pushed = tokio::time::timeout(std::time::Duration::from_secs(2), live_stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(pushed.messages.len(), 1);
    assert_eq!(pushed.messages[0].message_id, second.message_id);
}

#[tokio::test]
async fn mq_read_with_poll_wakes_when_message_is_sent() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut producer = MqServiceClient::connect(endpoint.clone()).await.unwrap();
    let queue = unique("grpc_poll_notify_orders");
    producer
        .create_queue(CreateQueueRequest {
            queue_name: queue.clone(),
            storage_mode: QueueStorageMode::Durable as i32,
        })
        .await
        .unwrap();

    let poll_endpoint = endpoint.clone();
    let poll_queue = queue.clone();
    let poll = tokio::spawn(async move {
        let mut consumer = MqServiceClient::connect(poll_endpoint).await.unwrap();
        consumer
            .read_with_poll(pgapp_proto::pgapp::v1::ReadWithPollRequest {
                queue_name: poll_queue,
                quantity: 1,
                visibility_timeout_seconds: 30,
                max_poll_seconds: 10,
                poll_interval_millis: 5_000,
            })
            .await
            .unwrap()
            .into_inner()
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let sent = producer
        .send(SendMessageRequest {
            queue_name: queue,
            json_payload: r#"{"poll":true}"#.to_string(),
            delay_seconds: 0,
        })
        .await
        .unwrap()
        .into_inner();

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), poll)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.messages[0].message_id, sent.message_id);
}

#[tokio::test]
async fn config_grpc_publish_read_and_watch() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = ConfigServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let scope = ConfigScope {
        app_id: unique("grpc_config"),
        environment: "prod".to_string(),
        cluster: "default".to_string(),
        namespace: "application".to_string(),
    };

    client
        .upsert_item(UpsertConfigItemRequest {
            scope: Some(scope.clone()),
            key: "feature_flags".to_string(),
            json_value: r#"{"enabled":true}"#.to_string(),
        })
        .await
        .unwrap();
    let release = client
        .publish(PublishConfigRequest {
            scope: Some(scope.clone()),
            message: "initial".to_string(),
            published_by: "grpc-test".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(release.revision, 1);
    assert!(release.snapshot_json.contains("feature_flags"));

    client
        .upsert_item(UpsertConfigItemRequest {
            scope: Some(scope.clone()),
            key: "feature_flags".to_string(),
            json_value: r#"{"enabled":false}"#.to_string(),
        })
        .await
        .unwrap();
    let latest = client
        .get_release(GetConfigReleaseRequest {
            scope: Some(scope.clone()),
            revision: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(latest.revision, 1);
    assert!(latest.snapshot_json.contains(r#""enabled":true"#));

    let watch = client
        .watch(WatchConfigRequest {
            scope: Some(scope),
            known_revision: 0,
            timeout_seconds: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(watch.changed);
    assert_eq!(watch.latest_revision, 1);

    let mut health = HealthServiceClient::connect(endpoint).await.unwrap();
    let metrics = health
        .get_runtime_metrics(RuntimeMetricsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(
        metrics
            .methods
            .iter()
            .any(|metric| metric.service == "config"
                && metric.method == "publish"
                && metric.status == "ok"
                && metric.count >= 1)
    );
}

#[tokio::test]
async fn config_grpc_schema_validation_round_trip() {
    let Some(endpoint) = spawn_server(true, true).await else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = ConfigServiceClient::connect(endpoint).await.unwrap();
    let scope = ConfigScope {
        app_id: unique("grpc_schema_config"),
        environment: "prod".to_string(),
        cluster: "default".to_string(),
        namespace: "application".to_string(),
    };
    let schema =
        r#"{"type":"object","required":["port"],"properties":{"port":{"type":"integer"}}}"#;

    client
        .set_schema(SetConfigSchemaRequest {
            scope: Some(scope.clone()),
            json_schema: schema.to_string(),
        })
        .await
        .unwrap();
    let fetched = client
        .get_schema(GetConfigSchemaRequest {
            scope: Some(scope.clone()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(fetched.has_schema);
    assert!(fetched.json_schema.contains("port"));

    let invalid = client
        .upsert_item(UpsertConfigItemRequest {
            scope: Some(scope.clone()),
            key: "db".to_string(),
            json_value: r#"{"port":"5432"}"#.to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);

    client
        .upsert_item(UpsertConfigItemRequest {
            scope: Some(scope.clone()),
            key: "db".to_string(),
            json_value: r#"{"port":5432}"#.to_string(),
        })
        .await
        .unwrap();

    let bad_schema = client
        .set_schema(SetConfigSchemaRequest {
            scope: Some(scope.clone()),
            json_schema: r#"{"type":7}"#.to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(bad_schema.code(), Code::InvalidArgument);

    client
        .set_schema(SetConfigSchemaRequest {
            scope: Some(scope.clone()),
            json_schema: String::new(),
        })
        .await
        .unwrap();
    let removed = client
        .get_schema(GetConfigSchemaRequest { scope: Some(scope) })
        .await
        .unwrap()
        .into_inner();
    assert!(!removed.has_schema);
}

#[tokio::test]
async fn disabled_config_rejects_calls() {
    let Some(endpoint) = spawn_server_with_env(
        true,
        true,
        HashMap::from([("PGAPP_ENABLE_CONFIG".to_string(), "false".to_string())]),
    )
    .await
    else {
        eprintln!("skipping gRPC integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let mut client = ConfigServiceClient::connect(endpoint).await.unwrap();
    let err = client
        .get_release(GetConfigReleaseRequest {
            scope: Some(ConfigScope {
                app_id: unique("disabled_config"),
                environment: "prod".to_string(),
                cluster: "default".to_string(),
                namespace: "application".to_string(),
            }),
            revision: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
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
