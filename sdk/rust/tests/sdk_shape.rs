use pgapp_sdk::PgAppClient;
use std::time::Duration;

#[test]
fn exposes_expected_client_type() {
    let _ = std::any::type_name::<PgAppClient>();
    let _timeout = Duration::from_secs(1);
}

#[test]
fn exposes_config_client_type() {
    let _ = std::any::type_name::<pgapp_sdk::ConfigClient>();
}

#[allow(dead_code)]
async fn phase_two_sdk_surface_compiles(client: PgAppClient) {
    let mut cache = client.cache();
    let _ = cache.increment("ns", "counter", 1, Some(60)).await;
    let _ = cache.decrement("ns", "counter", 1, None).await;
    let _ = cache.set_nx("ns", "lock", b"1".to_vec(), Some(60)).await;
    let _ = cache.get_set("ns", "slot", b"v2".to_vec(), None).await;
    let _ = cache.append("ns", "log", b"tail".to_vec(), None).await;
    let _ = cache.prepend("ns", "log", b"head".to_vec(), None).await;

    let mut mq = client.mq();
    let _ = mq.list_dlq_messages("orders", 10, 0).await;
    let _ = mq.get_dlq_message("orders", 1).await;
    let _ = mq.reprocess_dlq_message("orders", 1).await;
    let _ = mq.purge_dlq("orders").await;
    let _ = mq.stream_read("orders", 1, 30).await;

    let _ = PgAppClient::connect_with_timeout_and_credentials(
        "http://127.0.0.1:50051",
        Some(Duration::from_secs(1)),
        "key",
        "secret",
    )
    .await;
}
