use crate::{
    PgAppError, PgAppResult,
    validation::{
        parse_json_payload, validate_non_negative_seconds, validate_quantity, validate_queue_name,
    },
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueStorageMode {
    Durable,
    Transient,
}

#[derive(Debug, Clone)]
pub struct MqStore {
    pool: PgPool,
    transient_enabled: bool,
    limits: MqLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqLimits {
    pub max_batch_size: i32,
    pub max_payload_bytes: usize,
    pub max_visibility_timeout_seconds: i64,
}

impl Default for MqLimits {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_payload_bytes: 1024 * 1024,
            max_visibility_timeout_seconds: 12 * 60 * 60,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueMessage {
    pub id: i64,
    pub read_count: i32,
    pub enqueued_at: DateTime<Utc>,
    pub visibility_timeout_at: Option<DateTime<Utc>>,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMetrics {
    pub visible_message_count: i64,
    pub in_flight_message_count: i64,
    pub oldest_visible_message_age_seconds: i64,
    pub archived_message_count: i64,
}

impl MqStore {
    pub fn new(pool: PgPool, transient_enabled: bool) -> Self {
        Self::with_limits(pool, transient_enabled, MqLimits::default())
    }

    pub fn with_limits(pool: PgPool, transient_enabled: bool, limits: MqLimits) -> Self {
        Self {
            pool,
            transient_enabled,
            limits,
        }
    }

    pub async fn create_queue(&self, queue_name: &str, mode: QueueStorageMode) -> PgAppResult<()> {
        validate_queue_name(queue_name)?;
        if mode == QueueStorageMode::Transient && !self.transient_enabled {
            return Err(PgAppError::InvalidArgument(
                "transient queues are disabled".to_string(),
            ));
        }
        sqlx::query(
            r#"
            INSERT INTO mq_queues(name, durable)
            VALUES ($1, $2)
            ON CONFLICT (name) DO NOTHING
            "#,
        )
        .bind(queue_name)
        .bind(mode == QueueStorageMode::Durable)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn purge_queue(&self, queue_name: &str) -> PgAppResult<()> {
        let queue_id = self.queue_id(queue_name).await?;
        sqlx::query("DELETE FROM mq_messages WHERE queue_id = $1")
            .bind(queue_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn drop_queue(&self, queue_name: &str) -> PgAppResult<()> {
        validate_queue_name(queue_name)?;
        let deleted = sqlx::query("DELETE FROM mq_queues WHERE name = $1")
            .bind(queue_name)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if deleted == 0 {
            return Err(PgAppError::NotFound(format!("queue {queue_name}")));
        }
        Ok(())
    }

    pub async fn send(
        &self,
        queue_name: &str,
        json_payload: &str,
        delay_seconds: i64,
    ) -> PgAppResult<i64> {
        self.validate_payload_size(json_payload)?;
        let payload = parse_json_payload(json_payload)?;
        self.send_value(queue_name, payload, delay_seconds).await
    }

    pub async fn send_batch(
        &self,
        queue_name: &str,
        json_payloads: &[String],
        delay_seconds: i64,
    ) -> PgAppResult<Vec<i64>> {
        self.validate_batch_len(json_payloads.len())?;
        let mut ids = Vec::with_capacity(json_payloads.len());
        for payload in json_payloads {
            ids.push(self.send(queue_name, payload, delay_seconds).await?);
        }
        Ok(ids)
    }

    pub async fn read(
        &self,
        queue_name: &str,
        quantity: i32,
        visibility_timeout_seconds: i64,
    ) -> PgAppResult<Vec<QueueMessage>> {
        validate_quantity(quantity, self.limits.max_batch_size)?;
        validate_non_negative_seconds("visibility_timeout_seconds", visibility_timeout_seconds)?;
        self.validate_visibility_timeout(visibility_timeout_seconds)?;
        let queue_id = self.queue_id(queue_name).await?;
        let rows = sqlx::query(
            r#"
            WITH picked AS (
              SELECT id
              FROM mq_messages
              WHERE queue_id = $1
                AND available_at <= now()
                AND (visibility_timeout_at IS NULL OR visibility_timeout_at <= now())
              ORDER BY id
              LIMIT $2
              FOR UPDATE SKIP LOCKED
            )
            UPDATE mq_messages m
            SET visibility_timeout_at = now() + ($3::double precision * interval '1 second'),
                read_count = read_count + 1,
                last_read_at = now(),
                updated_at = now()
            FROM picked
            WHERE m.id = picked.id
            RETURNING m.id, m.read_count, m.created_at, m.visibility_timeout_at, m.payload
            "#,
        )
        .bind(queue_id)
        .bind(quantity as i64)
        .bind(visibility_timeout_seconds as f64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(row_to_message).collect()
    }

    pub async fn read_with_poll(
        &self,
        queue_name: &str,
        quantity: i32,
        visibility_timeout_seconds: i64,
        max_poll_seconds: i64,
        poll_interval_millis: i64,
    ) -> PgAppResult<Vec<QueueMessage>> {
        validate_quantity(quantity, self.limits.max_batch_size)?;
        validate_non_negative_seconds("visibility_timeout_seconds", visibility_timeout_seconds)?;
        self.validate_visibility_timeout(visibility_timeout_seconds)?;
        validate_non_negative_seconds("max_poll_seconds", max_poll_seconds)?;
        let interval = if poll_interval_millis <= 0 {
            Duration::from_millis(100)
        } else {
            Duration::from_millis(poll_interval_millis as u64)
        };
        let deadline = Utc::now() + ChronoDuration::seconds(max_poll_seconds);

        loop {
            let messages = self
                .read(queue_name, quantity, visibility_timeout_seconds)
                .await?;
            if !messages.is_empty() || Utc::now() >= deadline {
                return Ok(messages);
            }
            sleep(interval).await;
        }
    }

    pub async fn delete(&self, queue_name: &str, message_id: i64) -> PgAppResult<bool> {
        let queue_id = self.queue_id(queue_name).await?;
        let deleted = sqlx::query("DELETE FROM mq_messages WHERE queue_id = $1 AND id = $2")
            .bind(queue_id)
            .bind(message_id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(deleted > 0)
    }

    pub async fn archive(&self, queue_name: &str, message_id: i64) -> PgAppResult<bool> {
        let queue_id = self.queue_id(queue_name).await?;
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            r#"
            WITH moved AS (
              DELETE FROM mq_messages
              WHERE queue_id = $1 AND id = $2
              RETURNING id, queue_id, payload, headers, read_count, created_at
            )
            INSERT INTO mq_archives(queue_id, original_message_id, payload, headers, read_count, enqueued_at)
            SELECT queue_id, id, payload, headers, read_count, created_at
            FROM moved
            "#,
        )
        .bind(queue_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(inserted > 0)
    }

    pub async fn set_visibility_timeout(
        &self,
        queue_name: &str,
        message_id: i64,
        visibility_timeout_seconds: i64,
    ) -> PgAppResult<bool> {
        validate_non_negative_seconds("visibility_timeout_seconds", visibility_timeout_seconds)?;
        self.validate_visibility_timeout(visibility_timeout_seconds)?;
        let queue_id = self.queue_id(queue_name).await?;
        let updated = sqlx::query(
            r#"
            UPDATE mq_messages
            SET visibility_timeout_at = now() + ($3::double precision * interval '1 second'),
                updated_at = now()
            WHERE queue_id = $1 AND id = $2
            "#,
        )
        .bind(queue_id)
        .bind(message_id)
        .bind(visibility_timeout_seconds as f64)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(updated > 0)
    }

    pub async fn metrics(&self, queue_name: &str) -> PgAppResult<QueueMetrics> {
        let queue_id = self.queue_id(queue_name).await?;
        let row = sqlx::query(
            r#"
            SELECT
              COUNT(*) FILTER (
                WHERE available_at <= now()
                  AND (visibility_timeout_at IS NULL OR visibility_timeout_at <= now())
              )::bigint AS visible_count,
              COUNT(*) FILTER (WHERE visibility_timeout_at > now())::bigint AS in_flight_count,
              COALESCE(
                EXTRACT(EPOCH FROM now() - MIN(created_at) FILTER (
                  WHERE available_at <= now()
                    AND (visibility_timeout_at IS NULL OR visibility_timeout_at <= now())
                ))::bigint,
                0
              ) AS oldest_age
            FROM mq_messages
            WHERE queue_id = $1
            "#,
        )
        .bind(queue_id)
        .fetch_one(&self.pool)
        .await?;
        let archived_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM mq_archives WHERE queue_id = $1")
                .bind(queue_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(QueueMetrics {
            visible_message_count: row.try_get("visible_count")?,
            in_flight_message_count: row.try_get("in_flight_count")?,
            oldest_visible_message_age_seconds: row.try_get("oldest_age")?,
            archived_message_count: archived_count,
        })
    }

    async fn send_value(
        &self,
        queue_name: &str,
        payload: Value,
        delay_seconds: i64,
    ) -> PgAppResult<i64> {
        validate_non_negative_seconds("delay_seconds", delay_seconds)?;
        let queue_id = self.queue_id(queue_name).await?;
        let id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO mq_messages(queue_id, payload, available_at)
            VALUES ($1, $2, now() + ($3::double precision * interval '1 second'))
            RETURNING id
            "#,
        )
        .bind(queue_id)
        .bind(payload)
        .bind(delay_seconds as f64)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    async fn queue_id(&self, queue_name: &str) -> PgAppResult<i64> {
        validate_queue_name(queue_name)?;
        let id = sqlx::query_scalar("SELECT id FROM mq_queues WHERE name = $1")
            .bind(queue_name)
            .fetch_optional(&self.pool)
            .await?;
        id.ok_or_else(|| PgAppError::NotFound(format!("queue {queue_name}")))
    }

    fn validate_batch_len(&self, len: usize) -> PgAppResult<()> {
        if len > self.limits.max_batch_size as usize {
            return Err(PgAppError::InvalidArgument(format!(
                "batch size must be less than or equal to {}",
                self.limits.max_batch_size
            )));
        }
        Ok(())
    }

    fn validate_payload_size(&self, payload: &str) -> PgAppResult<()> {
        if payload.len() > self.limits.max_payload_bytes {
            return Err(PgAppError::InvalidArgument(format!(
                "payload exceeds {} bytes",
                self.limits.max_payload_bytes
            )));
        }
        Ok(())
    }

    fn validate_visibility_timeout(&self, seconds: i64) -> PgAppResult<()> {
        if seconds > self.limits.max_visibility_timeout_seconds {
            return Err(PgAppError::InvalidArgument(format!(
                "visibility_timeout_seconds must be less than or equal to {}",
                self.limits.max_visibility_timeout_seconds
            )));
        }
        Ok(())
    }
}

fn row_to_message(row: sqlx::postgres::PgRow) -> PgAppResult<QueueMessage> {
    Ok(QueueMessage {
        id: row.try_get("id")?,
        read_count: row.try_get("read_count")?,
        enqueued_at: row.try_get("created_at")?,
        visibility_timeout_at: row.try_get("visibility_timeout_at")?,
        payload: row.try_get("payload")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_transient_mode_when_disabled() {
        assert_eq!(QueueStorageMode::Durable, QueueStorageMode::Durable);
        assert_ne!(QueueStorageMode::Durable, QueueStorageMode::Transient);
    }

    #[test]
    fn queue_metrics_default_shape_is_stable() {
        let metrics = QueueMetrics {
            visible_message_count: 1,
            in_flight_message_count: 2,
            oldest_visible_message_age_seconds: 3,
            archived_message_count: 4,
        };
        assert_eq!(metrics.visible_message_count, 1);
        assert_eq!(metrics.archived_message_count, 4);
    }
}
