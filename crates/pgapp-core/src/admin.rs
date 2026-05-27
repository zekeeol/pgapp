use crate::{PgAppError, PgAppResult, validation::validate_queue_name};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row};

#[derive(Debug, Clone)]
pub struct AdminStore {
    pool: PgPool,
    max_page_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListQuery {
    pub limit: Option<i64>,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub limit: usize,
    pub offset: i64,
    pub next_offset: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminLogInput {
    pub level: String,
    pub target: String,
    pub message: String,
    pub request_id: Option<String>,
    pub fields: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFilter {
    pub level: Option<String>,
    pub text: Option<String>,
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminLogEvent {
    pub id: i64,
    pub occurred_at: DateTime<Utc>,
    pub level: String,
    pub target: String,
    pub message: String,
    pub request_id: Option<String>,
    pub fields: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheNamespaceSummary {
    pub name: String,
    pub generation: i64,
    pub key_count: i64,
    pub byte_size: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntryPreview {
    pub namespace: String,
    pub key: String,
    pub size_bytes: i64,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_accessed_at: DateTime<Utc>,
    pub access_count: i64,
    pub value_preview: String,
    pub value_encoding: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqQueueSummary {
    pub name: String,
    pub durable: bool,
    pub visible_message_count: i64,
    pub in_flight_message_count: i64,
    pub oldest_visible_message_age_seconds: Option<i64>,
    pub archived_message_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MqMessagePreview {
    pub queue_name: String,
    pub message_id: i64,
    pub read_count: i32,
    pub enqueued_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub visibility_timeout_at: Option<DateTime<Utc>>,
    pub payload_preview: String,
}

impl AdminStore {
    pub fn new(pool: PgPool, max_page_size: usize) -> Self {
        Self {
            pool,
            max_page_size: max_page_size.max(1),
        }
    }

    pub async fn record_log(&self, input: AdminLogInput) -> PgAppResult<i64> {
        validate_non_empty("level", &input.level)?;
        validate_non_empty("target", &input.target)?;
        validate_non_empty("message", &input.message)?;
        let id = sqlx::query_scalar(
            r#"
            INSERT INTO admin_log_events(level, target, message, request_id, fields_json)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(input.level)
        .bind(input.target)
        .bind(input.message)
        .bind(input.request_id)
        .bind(input.fields)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn list_logs(
        &self,
        filter: LogFilter,
        query: ListQuery,
    ) -> PgAppResult<Page<AdminLogEvent>> {
        let page = self.page(query)?;
        let rows = sqlx::query(
            r#"
            SELECT id, occurred_at, level, target, message, request_id, fields_json
            FROM admin_log_events
            WHERE ($1::text IS NULL OR level = $1)
              AND ($2::text IS NULL OR message ILIKE '%' || $2 || '%')
              AND ($3::text IS NULL OR target ILIKE '%' || $3 || '%')
            ORDER BY occurred_at DESC, id DESC
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(filter.level)
        .bind(filter.text)
        .bind(filter.target)
        .bind(page.fetch_limit)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(page.finish(
            rows.into_iter()
                .map(|row| AdminLogEvent {
                    id: row.get("id"),
                    occurred_at: row.get("occurred_at"),
                    level: row.get("level"),
                    target: row.get("target"),
                    message: row.get("message"),
                    request_id: row.get("request_id"),
                    fields: row.get("fields_json"),
                })
                .collect(),
        ))
    }

    pub async fn list_cache_namespaces(
        &self,
        query: ListQuery,
    ) -> PgAppResult<Page<CacheNamespaceSummary>> {
        let page = self.page(query)?;
        let rows = sqlx::query(
            r#"
            SELECT
              n.name,
              n.generation,
              n.created_at,
              n.updated_at,
              COALESCE(COUNT(e.id), 0)::bigint AS key_count,
              COALESCE(SUM(e.size_bytes), 0)::bigint AS byte_size
            FROM cache_namespaces n
            LEFT JOIN cache_entries e
              ON e.namespace = n.name
             AND e.generation = n.generation
             AND (e.expires_at IS NULL OR e.expires_at > now())
            GROUP BY n.name, n.generation, n.created_at, n.updated_at
            ORDER BY n.name
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(page.fetch_limit)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(page.finish(
            rows.into_iter()
                .map(|row| CacheNamespaceSummary {
                    name: row.get("name"),
                    generation: row.get("generation"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                    key_count: row.get("key_count"),
                    byte_size: row.get("byte_size"),
                })
                .collect(),
        ))
    }

    pub async fn list_cache_entries(
        &self,
        namespace: Option<String>,
        query: ListQuery,
    ) -> PgAppResult<Page<CacheEntryPreview>> {
        let page = self.page(query)?;
        let rows = sqlx::query(
            r#"
            SELECT namespace, cache_key, size_bytes, expires_at, last_accessed_at, access_count, value_bytes
            FROM cache_entries e
            WHERE ($1::text IS NULL OR e.namespace = $1)
              AND (e.expires_at IS NULL OR e.expires_at > now())
              AND e.generation = (
                SELECT n.generation FROM cache_namespaces n WHERE n.name = e.namespace
              )
            ORDER BY namespace, cache_key
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(namespace)
        .bind(page.fetch_limit)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(page.finish(
            rows.into_iter()
                .map(|row| {
                    let bytes: Vec<u8> = row.get("value_bytes");
                    CacheEntryPreview {
                        namespace: row.get("namespace"),
                        key: row.get("cache_key"),
                        size_bytes: row.get("size_bytes"),
                        expires_at: row.get("expires_at"),
                        last_accessed_at: row.get("last_accessed_at"),
                        access_count: row.get("access_count"),
                        value_preview: hex::encode(
                            bytes.iter().take(64).copied().collect::<Vec<_>>(),
                        ),
                        value_encoding: "hex".to_string(),
                    }
                })
                .collect(),
        ))
    }

    pub async fn list_mq_queues(&self, query: ListQuery) -> PgAppResult<Page<MqQueueSummary>> {
        let page = self.page(query)?;
        let rows = sqlx::query(
            r#"
            SELECT
              q.name,
              q.durable,
              q.created_at,
              q.updated_at,
              COALESCE(COUNT(m.id) FILTER (
                WHERE m.available_at <= now()
                  AND (m.visibility_timeout_at IS NULL OR m.visibility_timeout_at <= now())
              ), 0)::bigint AS visible_message_count,
              COALESCE(COUNT(m.id) FILTER (WHERE m.visibility_timeout_at > now()), 0)::bigint
                AS in_flight_message_count,
              EXTRACT(EPOCH FROM now() - MIN(m.created_at) FILTER (
                WHERE m.available_at <= now()
                  AND (m.visibility_timeout_at IS NULL OR m.visibility_timeout_at <= now())
              ))::bigint AS oldest_visible_message_age_seconds,
              COALESCE((
                SELECT COUNT(*)::bigint FROM mq_archives a WHERE a.queue_id = q.id
              ), 0)::bigint AS archived_message_count
            FROM mq_queues q
            LEFT JOIN mq_messages m ON m.queue_id = q.id
            GROUP BY q.id, q.name, q.durable, q.created_at, q.updated_at
            ORDER BY q.name
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(page.fetch_limit)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(page.finish(
            rows.into_iter()
                .map(|row| MqQueueSummary {
                    name: row.get("name"),
                    durable: row.get("durable"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                    visible_message_count: row.get("visible_message_count"),
                    in_flight_message_count: row.get("in_flight_message_count"),
                    oldest_visible_message_age_seconds: row
                        .get("oldest_visible_message_age_seconds"),
                    archived_message_count: row.get("archived_message_count"),
                })
                .collect(),
        ))
    }

    pub async fn list_mq_messages(
        &self,
        queue_name: &str,
        query: ListQuery,
    ) -> PgAppResult<Page<MqMessagePreview>> {
        validate_queue_name(queue_name)?;
        let page = self.page(query)?;
        let rows = sqlx::query(
            r#"
            SELECT q.name AS queue_name, m.id, m.read_count, m.created_at, m.available_at,
                   m.visibility_timeout_at, m.payload
            FROM mq_messages m
            JOIN mq_queues q ON q.id = m.queue_id
            WHERE q.name = $1
            ORDER BY m.created_at, m.id
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(queue_name)
        .bind(page.fetch_limit)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(page.finish(
            rows.into_iter()
                .map(|row| {
                    let payload: Value = row.get("payload");
                    MqMessagePreview {
                        queue_name: row.get("queue_name"),
                        message_id: row.get("id"),
                        read_count: row.get("read_count"),
                        enqueued_at: row.get("created_at"),
                        available_at: row.get("available_at"),
                        visibility_timeout_at: row.get("visibility_timeout_at"),
                        payload_preview: truncate(&payload.to_string(), 512),
                    }
                })
                .collect(),
        ))
    }

    fn page(&self, query: ListQuery) -> PgAppResult<PageState> {
        if query.offset < 0 {
            return Err(PgAppError::InvalidArgument(
                "offset must not be negative".to_string(),
            ));
        }
        let requested = query.limit.unwrap_or(self.max_page_size as i64);
        if requested <= 0 {
            return Err(PgAppError::InvalidArgument(
                "limit must be positive".to_string(),
            ));
        }
        let limit = requested.min(self.max_page_size as i64) as usize;
        Ok(PageState {
            limit,
            fetch_limit: limit as i64 + 1,
            offset: query.offset,
        })
    }
}

struct PageState {
    limit: usize,
    fetch_limit: i64,
    offset: i64,
}

impl PageState {
    fn finish<T>(self, mut items: Vec<T>) -> Page<T> {
        let has_more = items.len() > self.limit;
        if has_more {
            items.truncate(self.limit);
        }
        Page {
            items,
            limit: self.limit,
            offset: self.offset,
            next_offset: has_more.then_some(self.offset + self.limit as i64),
        }
    }
}

fn validate_non_empty(name: &str, value: &str) -> PgAppResult<()> {
    if value.trim().is_empty() {
        return Err(PgAppError::InvalidArgument(format!(
            "{name} must not be empty"
        )));
    }
    Ok(())
}

fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}
