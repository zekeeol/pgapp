use pgapp_proto::pgapp::v1::{
    CacheStatsRequest, CreateQueueRequest, HealthRequest, QueueStorageMode, SetCacheRequest,
    cache_service_client::CacheServiceClient, health_service_client::HealthServiceClient,
    mq_service_client::MqServiceClient,
};

#[test]
fn generated_rust_clients_and_messages_are_available() {
    let _cache_client = std::any::type_name::<CacheServiceClient<tonic::transport::Channel>>();
    let _mq_client = std::any::type_name::<MqServiceClient<tonic::transport::Channel>>();
    let _health_client = std::any::type_name::<HealthServiceClient<tonic::transport::Channel>>();

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

    let _ = CacheStatsRequest {};
    let _ = HealthRequest {};
}
