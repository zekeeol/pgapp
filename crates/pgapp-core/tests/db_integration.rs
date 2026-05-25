use pgapp_core::{
    cache::{CacheLimits, CacheStore},
    db,
    mq::{MqStore, QueueStorageMode},
};
use sqlx::PgPool;
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
    format!("{prefix}_{id}")
}

#[tokio::test]
async fn migrations_create_required_cache_and_mq_schema() {
    let Some(pool) = pool().await else {
        eprintln!("skipping integration test: DATABASE_URL is not set or unavailable");
        return;
    };

    let cache = db::check_cache_schema(&pool).await;
    let mq = db::check_mq_schema(&pool).await;

    assert!(
        cache.available,
        "cache schema unavailable: {}",
        cache.message
    );
    assert!(mq.available, "mq schema unavailable: {}", mq.message);

    let cache_indexes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM pg_indexes WHERE tablename = 'cache_entries'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cache_indexes >= 3);
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

    let hidden = store.read(&queue, 1, 1).await.unwrap();
    assert!(hidden.is_empty());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let redelivered = store.read(&queue, 1, 30).await.unwrap();
    assert_eq!(redelivered[0].id, id);
    assert!(redelivered[0].read_count >= 2);

    assert!(store.archive(&queue, id).await.unwrap());
    let metrics = store.metrics(&queue).await.unwrap();
    assert_eq!(metrics.archived_message_count, 1);
}

#[tokio::test]
async fn mq_lifecycle_purge_drop_delayed_delete_and_visibility_extension() {
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
            .set_visibility_timeout(&queue, delayed_id, 30)
            .await
            .unwrap()
    );
    assert!(store.read(&queue, 1, 1).await.unwrap().is_empty());
    assert!(store.delete(&queue, delayed_id).await.unwrap());

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
