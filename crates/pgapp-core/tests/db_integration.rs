use pgapp_core::{
    admin::{AdminLogInput, AdminStore, ListQuery, LogFilter},
    cache::{CacheLimits, CacheStore},
    client_auth::ClientStore,
    config_center::{ConfigLimits, ConfigScope, ConfigStore},
    db,
    mq::{MqLimits, MqStore, QueueStorageMode},
};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

async fn pool() -> Option<PgPool> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let pool = db::connect(&database_url, 1, 5).await.ok()?;
    db::apply_schema(&pool).await.ok()?;
    Some(pool)
}

fn unique(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{id}_{nanos}")
}

fn config_scope(app_id: &str, environment: &str, cluster: &str, namespace: &str) -> ConfigScope {
    let suffix = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ConfigScope {
        app_id: format!("{app_id}_{suffix}_{nanos}"),
        environment: environment.to_string(),
        cluster: cluster.to_string(),
        namespace: namespace.to_string(),
    }
}

#[tokio::test]
async fn migrations_create_required_cache_and_mq_schema() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };

    let cache = db::check_cache_schema(&pool).await;
    let mq = db::check_mq_schema(&pool).await;
    let config = db::check_config_schema(&pool).await;

    assert!(
        cache.available,
        "cache schema unavailable: {}",
        cache.message
    );
    assert!(mq.available, "mq schema unavailable: {}", mq.message);
    assert!(
        config.available,
        "config schema unavailable: {}",
        config.message
    );

    let cache_indexes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM pg_indexes WHERE tablename = 'cache_entries'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cache_indexes >= 3);
}

#[tokio::test]
async fn config_draft_items_are_scoped_json_documents() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ConfigStore::new(pool, ConfigLimits::default());
    let scope = config_scope("billing", "prod", "default", "application");
    let other_scope = config_scope("billing", "staging", "default", "application");

    store
        .upsert_item(
            &scope,
            "feature_flags",
            serde_json::json!({"enabled": true}),
        )
        .await
        .unwrap();
    store
        .upsert_item(
            &other_scope,
            "feature_flags",
            serde_json::json!({"enabled": false}),
        )
        .await
        .unwrap();

    let draft = store.get_draft(&scope).await.unwrap();
    assert_eq!(draft.len(), 1);
    assert_eq!(draft[0].key, "feature_flags");
    assert_eq!(draft[0].value, serde_json::json!({"enabled": true}));
    assert!(!draft[0].deleted);

    let other = store.get_draft(&other_scope).await.unwrap();
    assert_eq!(other[0].value, serde_json::json!({"enabled": false}));
}

#[tokio::test]
async fn config_draft_delete_tombstones_before_publish() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ConfigStore::new(pool, ConfigLimits::default());
    let scope = config_scope("deleteapp", "prod", "default", "application");

    store
        .upsert_item(
            &scope,
            "feature_flags",
            serde_json::json!({"enabled": true}),
        )
        .await
        .unwrap();
    assert!(store.delete_item(&scope, "feature_flags").await.unwrap());

    let draft = store.get_draft(&scope).await.unwrap();
    assert_eq!(draft.len(), 1);
    assert!(draft[0].deleted);
}

#[tokio::test]
async fn config_json_schema_validates_upserts_and_publish() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ConfigStore::new(
        pool.clone(),
        ConfigLimits {
            max_schema_bytes: 512,
            ..ConfigLimits::default()
        },
    );
    let scope = config_scope("schemaapp", "prod", "default", "application");
    let schema = serde_json::json!({
        "type": "object",
        "required": ["host", "port"],
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"}
        },
        "additionalProperties": false
    });

    store
        .set_schema(&scope, Some(schema.clone()))
        .await
        .unwrap();
    assert_eq!(store.get_schema(&scope).await.unwrap(), Some(schema));
    assert!(
        store
            .set_schema(&scope, Some(serde_json::json!({"type": 7})))
            .await
            .is_err()
    );

    store
        .upsert_item(
            &scope,
            "db",
            serde_json::json!({"host": "localhost", "port": 5432}),
        )
        .await
        .unwrap();
    assert!(
        store
            .upsert_item(
                &scope,
                "bad_db",
                serde_json::json!({"host": "localhost", "port": "5432"}),
            )
            .await
            .is_err()
    );

    let invalid_scope = config_scope("schemaapp_invalid", "prod", "default", "application");
    store
        .upsert_item(
            &invalid_scope,
            "bad_db",
            serde_json::json!({"port": "5432"}),
        )
        .await
        .unwrap();
    store
        .set_schema(
            &invalid_scope,
            Some(serde_json::json!({
                "type": "object",
                "properties": {"port": {"type": "integer"}},
                "required": ["port"]
            })),
        )
        .await
        .unwrap();
    assert!(
        store
            .publish(&invalid_scope, "blocked", "test")
            .await
            .is_err()
    );
    assert!(store.delete_item(&invalid_scope, "bad_db").await.unwrap());
    assert!(store.publish(&invalid_scope, "ok", "test").await.is_ok());

    let too_big = serde_json::json!({
        "type": "object",
        "description": "x".repeat(1024)
    });
    assert!(store.set_schema(&scope, Some(too_big)).await.is_err());
    store.set_schema(&scope, None).await.unwrap();
    assert_eq!(store.get_schema(&scope).await.unwrap(), None);
}

#[tokio::test]
async fn client_store_hashes_validates_rotates_and_deactivates_credentials() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ClientStore::new(pool.clone());
    let client_key = unique("svc-billing");

    let created = store
        .create_client(&client_key, vec!["cache".to_string(), "mq".to_string()])
        .await
        .unwrap();
    assert_eq!(created.client_key, client_key);
    assert!(!created.secret.is_empty());

    let row: (String, String) =
        sqlx::query_as("SELECT key_hash, secret_hash FROM pgapp_clients WHERE client_key = $1")
            .bind(&client_key)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(row.0, client_key);
    assert_ne!(row.1, created.secret);

    let identity = store
        .authenticate(&client_key, &created.secret)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identity.client_key, client_key);
    assert_eq!(identity.roles, vec!["cache".to_string(), "mq".to_string()]);
    assert!(
        store
            .authenticate(&client_key, "wrong-secret")
            .await
            .unwrap()
            .is_none()
    );

    let rotated = store.rotate_secret(&client_key).await.unwrap().unwrap();
    assert!(
        store
            .authenticate(&client_key, &created.secret)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .authenticate(&client_key, &rotated.secret)
            .await
            .unwrap()
            .is_some()
    );

    assert!(store.deactivate(&client_key).await.unwrap());
    assert!(
        store
            .authenticate(&client_key, &rotated.secret)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn config_validation_rejects_invalid_scope_key_and_watch_timeout() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ConfigStore::new(
        pool,
        ConfigLimits {
            max_watch_seconds: 2,
            max_payload_bytes: 128,
            max_page_size: 50,
            max_schema_bytes: 256 * 1024,
        },
    );
    let good = config_scope("validapp", "prod", "default", "application");
    let bad = config_scope("1bad", "prod", "default", "application");

    assert!(
        store
            .upsert_item(&bad, "feature_flags", serde_json::json!({"enabled": true}))
            .await
            .is_err()
    );
    assert!(
        store
            .upsert_item(&good, "feature flags", serde_json::json!({"enabled": true}))
            .await
            .is_err()
    );
    assert!(store.watch(&good, 0, 3, 25).await.is_err());
}

#[tokio::test]
async fn config_publish_creates_immutable_revisions_and_hides_drafts_until_publish() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ConfigStore::new(pool, ConfigLimits::default());
    let scope = config_scope("releaseapp", "prod", "default", "application");

    store
        .upsert_item(
            &scope,
            "feature_flags",
            serde_json::json!({"enabled": true}),
        )
        .await
        .unwrap();
    store
        .upsert_item(&scope, "limits", serde_json::json!({"qps": 10}))
        .await
        .unwrap();
    let first = store
        .publish(&scope, "initial release", "admin")
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    assert_eq!(
        first.snapshot,
        serde_json::json!({"feature_flags": {"enabled": true}, "limits": {"qps": 10}})
    );

    store
        .upsert_item(
            &scope,
            "feature_flags",
            serde_json::json!({"enabled": false}),
        )
        .await
        .unwrap();
    store.delete_item(&scope, "limits").await.unwrap();

    let latest_before_publish = store.get_latest_release(&scope).await.unwrap();
    assert_eq!(latest_before_publish.revision, 1);
    assert_eq!(
        latest_before_publish.snapshot["feature_flags"]["enabled"],
        true
    );
    assert_eq!(
        store.get_release(&scope, 1).await.unwrap().snapshot,
        first.snapshot
    );

    let second = store.publish(&scope, "second", "admin").await.unwrap();
    assert_eq!(second.revision, 2);
    assert_eq!(
        second.snapshot,
        serde_json::json!({"feature_flags": {"enabled": false}})
    );
    assert_eq!(
        store.get_release(&scope, 1).await.unwrap().snapshot,
        first.snapshot
    );

    let history = store.list_releases(&scope, 10, 0).await.unwrap();
    assert_eq!(
        history
            .items
            .iter()
            .map(|release| release.revision)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
}

#[tokio::test]
async fn config_publish_creates_revision_even_when_checksum_is_unchanged() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ConfigStore::new(pool, ConfigLimits::default());
    let scope = config_scope("samechecksum", "prod", "default", "application");

    store
        .upsert_item(
            &scope,
            "feature_flags",
            serde_json::json!({"enabled": true}),
        )
        .await
        .unwrap();
    let first = store.publish(&scope, "one", "admin").await.unwrap();
    let second = store.publish(&scope, "two", "admin").await.unwrap();

    assert_eq!(second.revision, first.revision + 1);
    assert_eq!(second.checksum, first.checksum);
}

#[tokio::test]
async fn config_watch_returns_newer_release_or_no_change() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = ConfigStore::new(
        pool,
        ConfigLimits {
            max_watch_seconds: 2,
            max_payload_bytes: 1024 * 1024,
            max_page_size: 100,
            max_schema_bytes: 256 * 1024,
        },
    );
    let scope = config_scope("watchapp", "prod", "default", "application");
    store
        .upsert_item(
            &scope,
            "feature_flags",
            serde_json::json!({"enabled": true}),
        )
        .await
        .unwrap();
    let first = store.publish(&scope, "one", "admin").await.unwrap();

    let immediate = store.watch(&scope, 0, 1, 25).await.unwrap();
    assert!(immediate.changed);
    assert_eq!(immediate.release.unwrap().revision, first.revision);

    let no_change = store.watch(&scope, first.revision, 0, 25).await.unwrap();
    assert!(!no_change.changed);
    assert_eq!(no_change.latest_revision, first.revision);

    let publisher = store.clone();
    let publish_scope = scope.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        publisher
            .upsert_item(
                &publish_scope,
                "feature_flags",
                serde_json::json!({"enabled": false}),
            )
            .await
            .unwrap();
        publisher
            .publish(&publish_scope, "two", "admin")
            .await
            .unwrap();
    });

    let changed = store.watch(&scope, first.revision, 2, 25).await.unwrap();
    assert!(changed.changed);
    assert_eq!(changed.release.unwrap().revision, first.revision + 1);
}

#[tokio::test]
async fn admin_logs_are_persisted_and_filterable() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = AdminStore::new(pool, 100);
    let request_id = unique("admin_req");

    store
        .record_log(AdminLogInput {
            level: "INFO".to_string(),
            target: "pgapp_server::admin".to_string(),
            message: "admin console opened".to_string(),
            request_id: Some(request_id.clone()),
            fields: serde_json::json!({"path": "/api/admin/overview"}),
        })
        .await
        .unwrap();

    let logs = store
        .list_logs(
            LogFilter {
                level: Some("INFO".to_string()),
                text: Some("console".to_string()),
                target: Some("admin".to_string()),
            },
            ListQuery {
                limit: Some(10),
                offset: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(logs.items.len(), 1);
    assert_eq!(
        logs.items[0].request_id.as_deref(),
        Some(request_id.as_str())
    );
    assert_eq!(logs.items[0].fields["path"], "/api/admin/overview");
}

#[tokio::test]
async fn admin_cache_inspection_is_paginated_and_read_only() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let namespace = unique("admin_cache");
    let cache = CacheStore::new(pool.clone(), CacheLimits::default());
    cache.set(&namespace, "a", b"one", None).await.unwrap();
    cache.set(&namespace, "b", b"two", None).await.unwrap();

    let before_access_count: i64 = sqlx::query_scalar(
        "SELECT access_count FROM cache_entries WHERE namespace = $1 AND cache_key = 'a'",
    )
    .bind(&namespace)
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut stats_lock = pool.begin().await.unwrap();
    let before_hits: i64 =
        sqlx::query_scalar("SELECT hits FROM cache_stats WHERE singleton FOR UPDATE")
            .fetch_one(&mut *stats_lock)
            .await
            .unwrap();

    let admin = AdminStore::new(pool.clone(), 1);
    let first_page = admin
        .list_cache_entries(
            Some(namespace.clone()),
            ListQuery {
                limit: Some(50),
                offset: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(first_page.items.len(), 1);
    assert_eq!(first_page.limit, 1);
    assert_eq!(first_page.next_offset, Some(1));
    assert_eq!(first_page.items[0].value_encoding, "hex");

    let after_access_count: i64 = sqlx::query_scalar(
        "SELECT access_count FROM cache_entries WHERE namespace = $1 AND cache_key = 'a'",
    )
    .bind(&namespace)
    .fetch_one(&pool)
    .await
    .unwrap();
    let after_hits: i64 = sqlx::query_scalar("SELECT hits FROM cache_stats WHERE singleton")
        .fetch_one(&mut *stats_lock)
        .await
        .unwrap();
    stats_lock.commit().await.unwrap();

    assert_eq!(after_access_count, before_access_count);
    assert_eq!(after_hits, before_hits);
}

#[tokio::test]
async fn admin_mq_previews_do_not_claim_delivery() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let queue = unique("admin_mq");
    let mq = MqStore::new(pool.clone(), false);
    mq.create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let message_id = mq.send(&queue, r#"{"hello":"world"}"#, 0).await.unwrap();

    let admin = AdminStore::new(pool.clone(), 100);
    let messages = admin
        .list_mq_messages(
            &queue,
            ListQuery {
                limit: Some(10),
                offset: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(messages.items.len(), 1);
    assert_eq!(messages.items[0].message_id, message_id);
    assert_eq!(messages.items[0].read_count, 0);
    assert!(messages.items[0].visibility_timeout_at.is_none());

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
async fn cache_round_trip_ttl_namespace_invalidation_and_stats() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let namespace = unique("cache_ns");
    let store = CacheStore::new(
        pool,
        CacheLimits {
            max_keys: Some(2),
            max_bytes: None,
        },
    );

    store.set(&namespace, "a", b"one", None).await.unwrap();
    assert_eq!(
        store.get(&namespace, "a").await.unwrap(),
        Some(b"one".to_vec())
    );
    assert!(store.exists(&namespace, "a").await.unwrap());

    store
        .set(&namespace, "expired", b"gone", Some(1))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(store.get(&namespace, "expired").await.unwrap(), None);

    store.invalidate_namespace(&namespace).await.unwrap();
    assert_eq!(store.get(&namespace, "a").await.unwrap(), None);

    let stats = store.stats().await.unwrap();
    assert!(stats.hits >= 1);
    assert!(stats.misses >= 2);
}

#[tokio::test]
async fn cache_batch_delete_and_capacity_eviction() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let namespace = unique("cache_batch");
    let store = CacheStore::new(
        pool,
        CacheLimits {
            max_keys: Some(2),
            max_bytes: None,
        },
    );

    store.set(&namespace, "a", b"one", None).await.unwrap();
    store.set(&namespace, "b", b"two", None).await.unwrap();
    let keys = vec!["a".to_string(), "b".to_string(), "missing".to_string()];
    let batch = store.mget(&namespace, &keys).await.unwrap();
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].1, Some(b"one".to_vec()));
    assert_eq!(batch[2].1, None);

    assert!(store.delete(&namespace, "b").await.unwrap());
    assert_eq!(store.get(&namespace, "b").await.unwrap(), None);

    store.set(&namespace, "c", b"three", None).await.unwrap();
    store.set(&namespace, "d", b"four", None).await.unwrap();
    let stats = store.stats().await.unwrap();
    let usage = stats.namespace_usage.get(&namespace).unwrap();
    assert!(usage.key_count <= 2);
    assert!(stats.evictions >= 1);
}

#[tokio::test]
async fn cache_atomic_operations_are_single_key_and_respect_visibility_rules() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = CacheStore::new(pool, CacheLimits::default());
    let namespace = unique("atomic_cache");

    assert_eq!(
        store
            .increment(&namespace, "counter", 1, Some(60))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .increment(&namespace, "counter", 5, Some(60))
            .await
            .unwrap(),
        6
    );
    assert_eq!(
        store
            .decrement(&namespace, "counter", 2, None)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store.get(&namespace, "counter").await.unwrap().unwrap(),
        b"4"
    );

    let mut increments = Vec::new();
    for _ in 0..10 {
        let store = store.clone();
        let namespace = namespace.clone();
        increments.push(tokio::spawn(async move {
            store
                .increment(&namespace, "parallel_counter", 1, None)
                .await
                .unwrap()
        }));
    }
    for increment in increments {
        increment.await.unwrap();
    }
    assert_eq!(
        store
            .get(&namespace, "parallel_counter")
            .await
            .unwrap()
            .unwrap(),
        b"10"
    );

    store
        .set(&namespace, "not_numeric", b"abc", None)
        .await
        .unwrap();
    let err = store
        .increment(&namespace, "not_numeric", 1, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("numeric"));

    assert!(
        store
            .set_nx(&namespace, "lock", b"first", Some(60))
            .await
            .unwrap()
    );
    assert!(
        !store
            .set_nx(&namespace, "lock", b"second", Some(60))
            .await
            .unwrap()
    );
    assert_eq!(
        store.get(&namespace, "lock").await.unwrap().unwrap(),
        b"first"
    );

    assert_eq!(
        store
            .get_set(&namespace, "slot", b"new", Some(60))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .get_set(&namespace, "slot", b"newer", Some(60))
            .await
            .unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        store.get(&namespace, "slot").await.unwrap().unwrap(),
        b"newer"
    );

    assert_eq!(
        store
            .append(&namespace, "log", b"tail", None)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store
            .prepend(&namespace, "log", b"head-", None)
            .await
            .unwrap(),
        9
    );
    assert_eq!(
        store.get(&namespace, "log").await.unwrap().unwrap(),
        b"head-tail"
    );

    store
        .set(&namespace, "ttl_counter", b"100", Some(1))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(
        store
            .increment(&namespace, "ttl_counter", 3, Some(60))
            .await
            .unwrap(),
        3
    );

    store
        .set(&namespace, "old_generation", b"value", None)
        .await
        .unwrap();
    store.invalidate_namespace(&namespace).await.unwrap();
    assert!(
        store
            .set_nx(&namespace, "old_generation", b"fresh", None)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .get(&namespace, "old_generation")
            .await
            .unwrap()
            .unwrap(),
        b"fresh"
    );
}

#[tokio::test]
async fn cache_namespace_invalidation_removes_superseded_rows() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let namespace = unique("cache_cleanup");
    let store = CacheStore::new(pool.clone(), CacheLimits::default());

    store.set(&namespace, "a", b"one", None).await.unwrap();
    store.set(&namespace, "b", b"two", None).await.unwrap();
    store.invalidate_namespace(&namespace).await.unwrap();

    let old_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::bigint
        FROM cache_entries e
        JOIN cache_namespaces n ON n.name = e.namespace
        WHERE e.namespace = $1 AND e.generation < n.generation
        "#,
    )
    .bind(&namespace)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(old_rows, 0);
}

#[tokio::test]
async fn mq_send_read_redeliver_archive_and_metrics() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let queue = unique("orders");
    let store = MqStore::new(pool, false);

    store
        .create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let id = store.send(&queue, r#"{"order_id":123}"#, 0).await.unwrap();
    let first = store.read(&queue, 1, 1).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].id, id);
    assert!(!first[0].ack_token.is_empty());

    let hidden = store.read(&queue, 1, 1).await.unwrap();
    assert!(hidden.is_empty());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(!store.ack(&queue, id, &first[0].ack_token).await.unwrap());
    let redelivered = store.read(&queue, 1, 30).await.unwrap();
    assert_eq!(redelivered[0].id, id);
    assert!(redelivered[0].read_count >= 2);
    assert_ne!(redelivered[0].ack_token, first[0].ack_token);

    assert!(
        store
            .archive(&queue, id, &redelivered[0].ack_token)
            .await
            .unwrap()
    );
    let metrics = store.metrics(&queue).await.unwrap();
    assert_eq!(metrics.archived_message_count, 1);
}

#[tokio::test]
async fn mq_dead_letters_after_max_redelivery_and_can_reprocess() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = MqStore::with_limits(
        pool,
        false,
        MqLimits {
            max_redelivery_count: 2,
            ..MqLimits::default()
        },
    );
    let queue = unique("dlq_orders");
    store
        .create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let message_id = store.send(&queue, r#"{"poison":true}"#, 0).await.unwrap();

    let first = store.read(&queue, 1, 30).await.unwrap().remove(0);
    assert_eq!(first.id, message_id);
    assert_eq!(first.read_count, 1);
    assert!(
        store
            .set_visibility_timeout(&queue, first.id, &first.ack_token, 0)
            .await
            .unwrap()
    );

    let second = store.read(&queue, 1, 30).await.unwrap().remove(0);
    assert_eq!(second.id, message_id);
    assert_eq!(second.read_count, 2);
    assert!(
        store
            .set_visibility_timeout(&queue, second.id, &second.ack_token, 0)
            .await
            .unwrap()
    );

    let third = store.read(&queue, 1, 30).await.unwrap();
    assert!(third.is_empty());

    let page = store.list_dlq_messages(&queue, 10, 0).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].original_message_id, message_id);
    assert_eq!(page.messages[0].read_count, 2);
    assert!(page.messages[0].reason.contains("max_redelivery_count"));

    let fetched = store.get_dlq_message(&queue, message_id).await.unwrap();
    assert_eq!(fetched.payload, serde_json::json!({"poison": true}));
    let metrics = store.metrics(&queue).await.unwrap();
    assert_eq!(metrics.dlq_message_count, 1);

    assert!(
        store
            .reprocess_dlq_message(&queue, message_id)
            .await
            .unwrap()
    );
    assert_eq!(store.metrics(&queue).await.unwrap().dlq_message_count, 0);
    let reprocessed = store.read(&queue, 1, 30).await.unwrap().remove(0);
    assert_eq!(reprocessed.id, message_id);
    assert_eq!(reprocessed.read_count, 1);
}

#[tokio::test]
async fn mq_send_and_batch_emit_postgres_notifications() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let queue = unique("notify_orders");
    let store = MqStore::new(pool, false);
    store
        .create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let channel = format!("pgapp_mq_{queue}");
    let mut listener = PgListener::connect(&database_url).await.unwrap();
    listener.listen(&channel).await.unwrap();

    store.send(&queue, r#"{"n":1}"#, 0).await.unwrap();
    let single = tokio::time::timeout(std::time::Duration::from_secs(2), listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(single.channel(), channel);
    assert_eq!(single.payload(), "1");

    store
        .send_batch(
            &queue,
            &[r#"{"n":2}"#.to_string(), r#"{"n":3}"#.to_string()],
            0,
        )
        .await
        .unwrap();
    let batch = tokio::time::timeout(std::time::Duration::from_secs(2), listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.channel(), channel);
    assert_eq!(batch.payload(), "2");
}

#[tokio::test]
async fn mq_dlq_sweep_removes_entries_older_than_retention() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let store = MqStore::with_limits(
        pool.clone(),
        false,
        MqLimits {
            max_redelivery_count: 1,
            ..MqLimits::default()
        },
    );
    let queue = unique("dlq_sweep_orders");
    store
        .create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let message_id = store.send(&queue, r#"{"old":true}"#, 0).await.unwrap();
    let message = store.read(&queue, 1, 30).await.unwrap().remove(0);
    assert!(
        store
            .set_visibility_timeout(&queue, message.id, &message.ack_token, 0)
            .await
            .unwrap()
    );
    assert!(store.read(&queue, 1, 30).await.unwrap().is_empty());
    let queue_id: i64 = sqlx::query_scalar("SELECT id FROM mq_queues WHERE name = $1")
        .bind(&queue)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE mq_dlq SET dead_lettered_at = now() - interval '2 days' WHERE queue_id = $1 AND original_message_id = $2",
    )
    .bind(queue_id)
    .bind(message_id)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(store.sweep_dlq(1).await.unwrap(), 1);
    assert_eq!(store.metrics(&queue).await.unwrap().dlq_message_count, 0);
}

#[tokio::test]
async fn mq_lifecycle_purge_drop_delayed_ack_and_visibility_extension() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let queue = unique("lifecycle_orders");
    let store = MqStore::new(pool, false);

    assert!(
        store
            .create_queue(&queue, QueueStorageMode::Transient)
            .await
            .is_err()
    );
    store
        .create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();

    let delayed_id = store.send(&queue, r#"{"delayed":true}"#, 1).await.unwrap();
    assert!(store.read(&queue, 1, 30).await.unwrap().is_empty());
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let delayed = store.read(&queue, 1, 30).await.unwrap();
    assert_eq!(delayed[0].id, delayed_id);

    assert!(
        store
            .set_visibility_timeout(&queue, delayed_id, &delayed[0].ack_token, 30)
            .await
            .unwrap()
    );
    assert!(store.read(&queue, 1, 1).await.unwrap().is_empty());
    assert!(
        store
            .ack(&queue, delayed_id, &delayed[0].ack_token)
            .await
            .unwrap()
    );

    store.send(&queue, r#"{"purge":true}"#, 0).await.unwrap();
    store.purge_queue(&queue).await.unwrap();
    assert!(store.read(&queue, 1, 1).await.unwrap().is_empty());

    store.drop_queue(&queue).await.unwrap();
    assert!(store.send(&queue, r#"{"gone":true}"#, 0).await.is_err());
}

#[tokio::test]
async fn mq_long_poll_waits_until_message_arrives() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let queue = unique("poll_orders");
    let store = MqStore::new(pool, false);
    store
        .create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();

    let sender = store.clone();
    let sender_queue = queue.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        sender
            .send(&sender_queue, r#"{"ready":true}"#, 0)
            .await
            .unwrap();
    });

    let messages = store.read_with_poll(&queue, 1, 30, 2, 50).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].payload["ready"], true);
}

#[tokio::test]
async fn mq_concurrent_reads_claim_distinct_messages() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };
    let queue = unique("concurrent_orders");
    let store = MqStore::new(pool, false);
    store
        .create_queue(&queue, QueueStorageMode::Durable)
        .await
        .unwrap();
    let payloads = vec![
        r#"{"n":1}"#.to_string(),
        r#"{"n":2}"#.to_string(),
        r#"{"n":3}"#.to_string(),
    ];
    store.send_batch(&queue, &payloads, 0).await.unwrap();

    let a = store.clone();
    let b = store.clone();
    let q1 = queue.clone();
    let q2 = queue.clone();
    let (left, right) = tokio::join!(a.read(&q1, 2, 30), b.read(&q2, 2, 30));

    let mut ids = left.unwrap().into_iter().map(|m| m.id).collect::<Vec<_>>();
    ids.extend(right.unwrap().into_iter().map(|m| m.id));
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 3);
}
