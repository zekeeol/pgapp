use pgapp_core::{
    cache::{CacheLimits, CacheStore},
    client_auth::ClientStore,
    config::ServerConfig,
    config_center::{ConfigLimits, ConfigScope, ConfigStore},
    db,
    mq::{MqLimits, MqStore, QueueStorageMode},
};
use reqwest::{StatusCode, header};
use serde_json::Value;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(20_000);

async fn spawn_server_with_env(
    grpc_addr: SocketAddr,
    admin_addr: SocketAddr,
    extra_env: HashMap<String, String>,
) -> Option<(String, sqlx::PgPool)> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&database_url, 1, 5).await.ok()?;
    db::apply_schema(&pool).await.ok()?;
    let mut cfg_map = HashMap::from([
        ("DATABASE_URL".to_string(), database_url),
        ("PGAPP_BIND_ADDR".to_string(), grpc_addr.to_string()),
        ("PGAPP_MAX_CONNECTIONS".to_string(), "5".to_string()),
    ]);
    cfg_map.extend(extra_env);
    let cfg = ServerConfig::from_map(cfg_map).ok()?;
    tokio::spawn({
        let pool = pool.clone();
        async move {
            let _ = pgapp_server::serve(grpc_addr, pool, cfg).await;
        }
    });
    Some((format!("http://{admin_addr}"), pool))
}

fn free_distinct_addrs() -> (SocketAddr, SocketAddr) {
    let first = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let second = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    (first.local_addr().unwrap(), second.local_addr().unwrap())
}

fn unique(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{id}_{nanos}")
}

async fn wait_for_http(client: &reqwest::Client, url: &str) -> bool {
    for _ in 0..40 {
        if client.get(url).send().await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn admin_http_listener_is_disabled_by_default() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let Some((admin_endpoint, _pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string())]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();

    let err = client
        .get(format!("{admin_endpoint}/api/admin/overview"))
        .send()
        .await
        .unwrap_err();

    assert!(err.is_connect() || err.is_timeout());
}

#[tokio::test]
async fn admin_api_requires_bearer_token_and_returns_overview() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "super-secret-admin-token";
    let Some((admin_endpoint, pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    let unauthorized = client
        .get(format!("{admin_endpoint}/api/admin/overview"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        unauthorized.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    let error_body: Value = unauthorized.json().await.unwrap();
    assert_eq!(error_body["code"], "unauthorized");
    assert!(error_body["request_id"].as_str().unwrap().len() > 8);

    let overview = client
        .get(format!("{admin_endpoint}/api/admin/overview"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(overview.status(), StatusCode::OK);
    assert_eq!(
        overview.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    let body: Value = overview.json().await.unwrap();
    assert_eq!(body["server_state"], "ready");
    assert!(body["pg_pool"]["size"].as_u64().is_some());
    assert!(
        body["cache_summary"]["logical_key_count"]
            .as_i64()
            .is_some()
    );
    assert!(body["mq_summary"]["queue_count"].as_i64().is_some());

    let logs = client
        .get(format!("{admin_endpoint}/api/admin/logs"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!logs.contains(token));

    let persisted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM admin_log_events WHERE message ILIKE '%admin request%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(persisted >= 1);
}

#[tokio::test]
async fn admin_cache_routes_are_read_only() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "cache-admin-token";
    let Some((admin_endpoint, pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
            ("PGAPP_ADMIN_MAX_PAGE_SIZE".to_string(), "100".to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let namespace = unique("admin_http_cache_read_only");
    let cache = CacheStore::new(pool.clone(), CacheLimits::default());
    cache.set(&namespace, "a", b"one", None).await.unwrap();
    cache.set(&namespace, "b", b"two", None).await.unwrap();
    let mut stats_lock = pool.begin().await.unwrap();
    let before_hits: i64 =
        sqlx::query_scalar("SELECT hits FROM cache_stats WHERE singleton FOR UPDATE")
            .fetch_one(&mut *stats_lock)
            .await
            .unwrap();

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    let namespaces: Value = client
        .get(format!("{admin_endpoint}/api/admin/cache/namespaces"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        namespaces["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| { item["name"] == namespace && item["key_count"].as_i64().unwrap() >= 2 })
    );

    let entries: Value = client
        .get(format!(
            "{admin_endpoint}/api/admin/cache/entries?namespace={namespace}&limit=1"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(entries["items"].as_array().unwrap().len(), 1);
    assert_eq!(entries["limit"], 1);
    assert_eq!(entries["items"][0]["value_encoding"], "hex");
    assert!(entries["next_offset"].as_i64().is_some());

    let delete = client
        .delete(format!(
            "{admin_endpoint}/api/admin/cache/entries/{namespace}/a"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert!(
        delete.status() == StatusCode::NOT_FOUND
            || delete.status() == StatusCode::METHOD_NOT_ALLOWED
    );

    let after_hits: i64 = sqlx::query_scalar("SELECT hits FROM cache_stats WHERE singleton")
        .fetch_one(&mut *stats_lock)
        .await
        .unwrap();
    stats_lock.commit().await.unwrap();
    assert_eq!(after_hits, before_hits);
}

#[tokio::test]
async fn admin_mq_routes_are_read_only() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "mq-admin-token";
    let Some((admin_endpoint, pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let queue = unique("admin_http_mq_read_only");
    let mq = MqStore::new(pool.clone(), false);
    mq.create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let message_id = mq.send(&queue, r#"{"kind":"preview"}"#, 0).await.unwrap();

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    let queues: Value = client
        .get(format!("{admin_endpoint}/api/admin/mq/queues"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(queues["items"].as_array().unwrap().iter().any(|item| {
        item["name"] == queue && item["visible_message_count"].as_i64().unwrap() >= 1
    }));

    let messages: Value = client
        .get(format!(
            "{admin_endpoint}/api/admin/mq/queues/{queue}/messages"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(messages["items"][0]["message_id"], message_id);
    assert_eq!(messages["items"][0]["read_count"], 0);

    let send = client
        .post(format!(
            "{admin_endpoint}/api/admin/mq/queues/{queue}/messages"
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({"payload": {"no": "writes"}}))
        .send()
        .await
        .unwrap();
    assert!(
        send.status() == StatusCode::NOT_FOUND || send.status() == StatusCode::METHOD_NOT_ALLOWED
    );

    let row: (
        i32,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    ) = sqlx::query_as(
        "SELECT read_count, visibility_timeout_at, last_read_at FROM mq_messages WHERE id = $1",
    )
    .bind(message_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 0);
    assert!(row.1.is_none());
    assert!(row.2.is_none());
}

#[tokio::test]
async fn admin_mq_dlq_routes_inspect_reprocess_and_purge_messages() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "mq-dlq-admin-token";
    let Some((admin_endpoint, pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
            ("PGAPP_MAX_REDELIVERY_COUNT".to_string(), "1".to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let queue = unique("admin_http_mq_dlq");
    let mq = MqStore::with_limits(
        pool.clone(),
        false,
        MqLimits {
            max_redelivery_count: 1,
            ..MqLimits::default()
        },
    );
    mq.create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let message_id = mq.send(&queue, r#"{"poison":true}"#, 0).await.unwrap();
    let read = mq.read(&queue, 1, 30).await.unwrap().remove(0);
    mq.set_visibility_timeout(&queue, read.id, &read.ack_token, 0)
        .await
        .unwrap();
    assert!(mq.read(&queue, 1, 30).await.unwrap().is_empty());

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    let dlq: Value = client
        .get(format!("{admin_endpoint}/api/admin/mq/queues/{queue}/dlq"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dlq["items"][0]["original_message_id"], message_id);
    assert_eq!(dlq["items"][0]["read_count"], 1);
    assert_eq!(dlq["items"][0]["payload"]["poison"], true);
    assert!(
        dlq["items"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("max_redelivery_count")
    );

    let fetched: Value = client
        .get(format!(
            "{admin_endpoint}/api/admin/mq/queues/{queue}/dlq/{message_id}"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched["original_message_id"], message_id);
    assert_eq!(fetched["payload"]["poison"], true);

    let reprocessed: Value = client
        .post(format!(
            "{admin_endpoint}/api/admin/mq/queues/{queue}/dlq/{message_id}/reprocess"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reprocessed["success"], true);
    assert_eq!(
        mq.list_dlq_messages(&queue, 10, 0)
            .await
            .unwrap()
            .messages
            .len(),
        0
    );
    let active = mq.read(&queue, 1, 30).await.unwrap().remove(0);
    assert_eq!(active.id, message_id);
    assert_eq!(active.read_count, 1);
    mq.dead_letter(&queue, message_id, "admin purge test")
        .await
        .unwrap();

    let purged: Value = client
        .post(format!(
            "{admin_endpoint}/api/admin/mq/queues/{queue}/dlq/purge"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(purged["deleted_count"], 1);
    assert!(
        mq.list_dlq_messages(&queue, 10, 0)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
}

#[tokio::test]
async fn admin_config_routes_are_token_protected_and_manage_drafts() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "config-admin-token";
    let Some((admin_endpoint, _pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let scope = serde_json::json!({
        "app_id": unique("admin_config"),
        "environment": "prod",
        "cluster": "default",
        "namespace": "application"
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    let unauthorized = client
        .get(format!("{admin_endpoint}/api/admin/config/scopes"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let saved: Value = client
        .put(format!("{admin_endpoint}/api/admin/config/items"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "scope": scope,
            "key": "feature_flags",
            "value": {"enabled": true}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(saved["success"], true);

    let scopes: Value = client
        .get(format!("{admin_endpoint}/api/admin/config/scopes"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        scopes["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["scope"]["app_id"] == scope["app_id"])
    );

    let draft: Value = client
        .get(format!(
            "{admin_endpoint}/api/admin/config/draft?app_id={}&environment=prod&cluster=default&namespace=application",
            scope["app_id"].as_str().unwrap()
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(draft["items"][0]["key"], "feature_flags");
    assert_eq!(draft["items"][0]["value"]["enabled"], true);

    let invalid: Value = client
        .put(format!("{admin_endpoint}/api/admin/config/items"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "scope": {
                "app_id": "1bad",
                "environment": "prod",
                "cluster": "default",
                "namespace": "application"
            },
            "key": "feature_flags",
            "value": {"enabled": true}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(invalid["code"], "invalid_argument");
}

#[tokio::test]
async fn admin_config_schema_routes_set_get_validate_and_remove_schema() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "config-schema-admin-token";
    let Some((admin_endpoint, _pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let scope = serde_json::json!({
        "app_id": unique("admin_schema_config"),
        "environment": "prod",
        "cluster": "default",
        "namespace": "application"
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    let schema = serde_json::json!({
        "type": "object",
        "required": ["port"],
        "properties": {"port": {"type": "integer"}}
    });
    let set: Value = client
        .put(format!("{admin_endpoint}/api/admin/config/schema"))
        .bearer_auth(token)
        .json(&serde_json::json!({ "scope": scope, "schema": schema }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(set["success"], true);

    let query = format!(
        "app_id={}&environment=prod&cluster=default&namespace=application",
        scope["app_id"].as_str().unwrap()
    );
    let fetched: Value = client
        .get(format!("{admin_endpoint}/api/admin/config/schema?{query}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched["has_schema"], true);
    assert_eq!(fetched["schema"]["required"][0], "port");

    let invalid_item = client
        .put(format!("{admin_endpoint}/api/admin/config/items"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "scope": scope,
            "key": "db",
            "value": {"port": "5432"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_item.status(), StatusCode::BAD_REQUEST);
    let invalid_body: Value = invalid_item.json().await.unwrap();
    assert_eq!(invalid_body["code"], "invalid_argument");

    let removed: Value = client
        .delete(format!("{admin_endpoint}/api/admin/config/schema?{query}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(removed["success"], true);

    let no_schema: Value = client
        .get(format!("{admin_endpoint}/api/admin/config/schema?{query}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(no_schema["has_schema"], false);
}

#[tokio::test]
async fn admin_config_publish_makes_release_client_visible_without_exposing_draft() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "config-publish-admin-token";
    let Some((admin_endpoint, pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let scope = ConfigScope {
        app_id: unique("admin_publish_config"),
        environment: "prod".to_string(),
        cluster: "default".to_string(),
        namespace: "application".to_string(),
    };
    let store = ConfigStore::new(pool, ConfigLimits::default());
    store
        .upsert_item(
            &scope,
            "feature_flags",
            serde_json::json!({"enabled": true}),
        )
        .await
        .unwrap();

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );
    let publish: Value = client
        .post(format!("{admin_endpoint}/api/admin/config/releases"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "scope": scope,
            "message": "initial",
            "published_by": "admin-http-test"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(publish["revision"], 1);

    let update: Value = client
        .put(format!("{admin_endpoint}/api/admin/config/items"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "scope": publish["scope"],
            "key": "feature_flags",
            "value": {"enabled": false}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(update["success"], true);

    let releases: Value = client
        .get(format!(
            "{admin_endpoint}/api/admin/config/releases?app_id={}&environment=prod&cluster=default&namespace=application",
            publish["scope"]["app_id"].as_str().unwrap()
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(releases["items"][0]["revision"], 1);
    assert_eq!(
        releases["items"][0]["snapshot"]["feature_flags"]["enabled"],
        true
    );
}

#[tokio::test]
async fn admin_clients_view_separates_sessions_from_api_activity() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "clients-admin-token";
    let Some((admin_endpoint, _pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    client
        .get(format!("{admin_endpoint}/api/admin/overview"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();

    let clients: Value = client
        .get(format!("{admin_endpoint}/api/admin/clients"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(
        clients["admin_sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| {
                session["path"] == "/api/admin/overview"
                    && session["request_id"].as_str().unwrap().len() > 8
            })
    );
    assert!(clients["api_activity"].as_array().is_some());
}

#[tokio::test]
async fn admin_client_credentials_can_be_created_rotated_and_deactivated() {
    let (grpc_addr, admin_addr) = free_distinct_addrs();
    let token = "client-credentials-admin-token";
    let Some((admin_endpoint, pool)) = spawn_server_with_env(
        grpc_addr,
        admin_addr,
        HashMap::from([
            ("PGAPP_ENABLE_ADMIN".to_string(), "true".to_string()),
            ("PGAPP_ADMIN_BIND_ADDR".to_string(), admin_addr.to_string()),
            ("PGAPP_ADMIN_TOKEN".to_string(), token.to_string()),
        ]),
    )
    .await
    else {
        eprintln!("skipping Admin integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    assert!(
        wait_for_http(&client, &format!("{admin_endpoint}/api/admin/overview")).await,
        "Admin HTTP listener did not start"
    );

    let client_key = unique("admin_client_credential");
    let unauthorized = client
        .post(format!("{admin_endpoint}/api/admin/clients"))
        .json(&serde_json::json!({
            "client_key": client_key,
            "roles": ["cache", "mq"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let created: Value = client
        .post(format!("{admin_endpoint}/api/admin/clients"))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "client_key": client_key,
            "roles": ["cache", "mq"]
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let first_secret = created["secret"].as_str().unwrap().to_string();
    assert_eq!(created["client_key"], client_key);
    assert!(!first_secret.is_empty());

    let store = ClientStore::new(pool.clone());
    assert!(
        store
            .authenticate(&client_key, &first_secret)
            .await
            .unwrap()
            .is_some()
    );

    let listed: Value = client
        .get(format!("{admin_endpoint}/api/admin/clients"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["client_key"] == client_key
                && item["active"] == true
                && item["roles"].as_array().unwrap().len() == 2)
    );

    let rotated: Value = client
        .post(format!(
            "{admin_endpoint}/api/admin/clients/{client_key}/rotate"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let second_secret = rotated["secret"].as_str().unwrap().to_string();
    assert_ne!(second_secret, first_secret);
    assert!(
        store
            .authenticate(&client_key, &first_secret)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .authenticate(&client_key, &second_secret)
            .await
            .unwrap()
            .is_some()
    );

    let deactivated: Value = client
        .post(format!(
            "{admin_endpoint}/api/admin/clients/{client_key}/deactivate"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(deactivated["success"], true);
    assert!(
        store
            .authenticate(&client_key, &second_secret)
            .await
            .unwrap()
            .is_none()
    );
}
