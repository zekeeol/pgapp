use pgapp_proto::pgapp::v1::{
    AckMessageRequest, AppendRequest, CacheStatsRequest, ConfigScope, CreateQueueRequest,
    DecrementRequest, DlqMessage, GetConfigReleaseRequest, GetConfigSchemaRequest, GetSetRequest,
    HealthRequest, IncrementRequest, ListDlqMessagesRequest, PrependRequest, QueueMessage,
    QueueStorageMode, SetCacheRequest, SetConfigSchemaRequest, SetNxRequest, StreamReadRequest,
    WatchConfigRequest, cache_service_client::CacheServiceClient,
    config_service_client::ConfigServiceClient, health_service_client::HealthServiceClient,
    mq_service_client::MqServiceClient,
};

#[test]
fn generated_rust_clients_and_messages_are_available() {
    let _cache_client = std::any::type_name::<CacheServiceClient<tonic::transport::Channel>>();
    let _mq_client = std::any::type_name::<MqServiceClient<tonic::transport::Channel>>();
    let _health_client = std::any::type_name::<HealthServiceClient<tonic::transport::Channel>>();
    let _config_client = std::any::type_name::<ConfigServiceClient<tonic::transport::Channel>>();

    let set = SetCacheRequest {
        namespace: "default".to_string(),
        key: "hello".to_string(),
        value: b"world".to_vec(),
        ttl_seconds: 30,
    };
    assert_eq!(set.value, b"world");

    let queue = CreateQueueRequest {
        queue_name: "orders".to_string(),
        storage_mode: QueueStorageMode::Durable as i32,
    };
    assert_eq!(queue.queue_name, "orders");
    let message = QueueMessage {
        message_id: 42,
        ack_token: "receipt".to_string(),
        ..Default::default()
    };
    assert_eq!(message.ack_token, "receipt");
    let ack = AckMessageRequest {
        queue_name: "orders".to_string(),
        message_id: message.message_id,
        ack_token: message.ack_token,
    };
    assert_eq!(ack.ack_token, "receipt");

    let dlq_request = ListDlqMessagesRequest {
        queue_name: "orders".to_string(),
        limit: 10,
        offset: 0,
    };
    assert_eq!(dlq_request.limit, 10);
    let dlq_message = DlqMessage {
        original_message_id: 42,
        reason: "max_redelivery_count".to_string(),
        ..Default::default()
    };
    assert_eq!(dlq_message.reason, "max_redelivery_count");
    let stream = StreamReadRequest {
        queue_name: "orders".to_string(),
        quantity: 1,
        visibility_timeout_seconds: 30,
    };
    assert_eq!(stream.quantity, 1);

    let increment = IncrementRequest {
        namespace: "default".to_string(),
        key: "counter".to_string(),
        delta: 5,
        ttl_seconds: 60,
    };
    assert_eq!(increment.delta, 5);
    let decrement = DecrementRequest {
        namespace: "default".to_string(),
        key: "counter".to_string(),
        delta: 2,
        ttl_seconds: 0,
    };
    assert_eq!(decrement.delta, 2);
    let set_nx = SetNxRequest {
        namespace: "default".to_string(),
        key: "lock".to_string(),
        value: b"token".to_vec(),
        ttl_seconds: 30,
    };
    assert_eq!(set_nx.value, b"token");
    let get_set = GetSetRequest {
        namespace: "default".to_string(),
        key: "slot".to_string(),
        value: b"new".to_vec(),
        ttl_seconds: 0,
    };
    assert_eq!(get_set.value, b"new");
    let append = AppendRequest {
        namespace: "default".to_string(),
        key: "log".to_string(),
        value: b"tail".to_vec(),
        ttl_seconds: 0,
    };
    assert_eq!(append.value, b"tail");
    let prepend = PrependRequest {
        namespace: "default".to_string(),
        key: "log".to_string(),
        value: b"head".to_vec(),
        ttl_seconds: 0,
    };
    assert_eq!(prepend.value, b"head");

    let _ = CacheStatsRequest {};
    let _ = HealthRequest {};

    let scope = ConfigScope {
        app_id: "billing".to_string(),
        environment: "prod".to_string(),
        cluster: "default".to_string(),
        namespace: "application".to_string(),
    };
    let release = GetConfigReleaseRequest {
        scope: Some(scope.clone()),
        revision: 7,
    };
    assert_eq!(release.scope.as_ref().unwrap().app_id, "billing");

    let watch = WatchConfigRequest {
        scope: Some(scope),
        known_revision: 7,
        timeout_seconds: 30,
    };
    assert_eq!(watch.known_revision, 7);

    let schema_scope = ConfigScope {
        app_id: "billing".to_string(),
        environment: "prod".to_string(),
        cluster: "default".to_string(),
        namespace: "application".to_string(),
    };
    let set_schema = SetConfigSchemaRequest {
        scope: Some(schema_scope.clone()),
        json_schema: r#"{"type":"object"}"#.to_string(),
    };
    assert!(set_schema.json_schema.contains("object"));
    let get_schema = GetConfigSchemaRequest {
        scope: Some(schema_scope),
    };
    assert_eq!(get_schema.scope.unwrap().namespace, "application");
}
