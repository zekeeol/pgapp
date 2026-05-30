use pgapp_proto::pgapp::v1::{
    AckMessageRequest, CacheStatsRequest, ConfigScope, CreateQueueRequest, GetConfigReleaseRequest,
    HealthRequest, QueueMessage, QueueStorageMode, SetCacheRequest, WatchConfigRequest,
    cache_service_client::CacheServiceClient, config_service_client::ConfigServiceClient,
    health_service_client::HealthServiceClient, mq_service_client::MqServiceClient,
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
}
