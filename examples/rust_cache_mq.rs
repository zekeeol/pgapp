use pgapp_sdk::PgAppClient;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PgAppClient::connect("http://127.0.0.1:50051").await?;

    let mut cache = client.cache();
    cache
        .set("default", "hello", b"world".to_vec(), Some(60))
        .await?;
    let value = cache.get("default", "hello").await?;
    println!("cache value: {:?}", value);

    let mut mq = client.mq();
    mq.create_queue("orders").await?;
    let message_id = mq.send_json("orders", &json!({"order_id": 123})).await?;
    let messages = mq.read("orders", 1, 30).await?;
    println!("sent {message_id}, read {} message(s)", messages.len());

    Ok(())
}
