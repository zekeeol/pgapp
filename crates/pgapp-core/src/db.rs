use crate::{PgAppError, PgAppResult};
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use std::time::Duration;

const CACHE_SCHEMA: &str = include_str!("../../../migrations/0001_cache.sql");
const MQ_SCHEMA: &str = include_str!("../../../migrations/0002_mq.sql");
const ADMIN_SCHEMA: &str = include_str!("../../../migrations/0003_admin.sql");
const CONFIG_SCHEMA: &str = include_str!("../../../migrations/0004_config.sql");
const DLQ_SCHEMA: &str = include_str!("../../../migrations/0005_dlq.sql");
const AUTH_SCHEMA: &str = include_str!("../../../migrations/0006_auth.sql");
const CONFIG_SCHEMA_VALIDATION: &str = include_str!("../../../migrations/0007_config_schema.sql");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityStatus {
    pub name: &'static str,
    pub available: bool,
    pub message: String,
}

pub async fn connect(database_url: &str, min: u32, max: u32) -> PgAppResult<PgPool> {
    PgPoolOptions::new()
        .min_connections(min)
        .max_connections(max)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
        .map_err(|err| PgAppError::DatabaseUnavailable(err.to_string()))
}

pub async fn apply_schema(pool: &PgPool) -> PgAppResult<()> {
    pool.execute(CACHE_SCHEMA).await?;
    pool.execute(MQ_SCHEMA).await?;
    pool.execute(ADMIN_SCHEMA).await?;
    pool.execute(CONFIG_SCHEMA).await?;
    pool.execute(DLQ_SCHEMA).await?;
    pool.execute(AUTH_SCHEMA).await?;
    pool.execute(CONFIG_SCHEMA_VALIDATION).await?;
    Ok(())
}

pub async fn check_cache_schema(pool: &PgPool) -> CapabilityStatus {
    check_table(pool, "cache_entries", "cache").await
}

pub async fn check_mq_schema(pool: &PgPool) -> CapabilityStatus {
    check_table(pool, "mq_messages", "mq").await
}

pub async fn check_config_schema(pool: &PgPool) -> CapabilityStatus {
    check_table(pool, "config_releases", "config").await
}

async fn check_table(pool: &PgPool, table: &'static str, name: &'static str) -> CapabilityStatus {
    let result: Result<bool, sqlx::Error> = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = 'public' AND table_name = $1
        )
        "#,
    )
    .bind(table)
    .fetch_one(pool)
    .await;

    match result {
        Ok(true) => CapabilityStatus {
            name,
            available: true,
            message: "available".to_string(),
        },
        Ok(false) => CapabilityStatus {
            name,
            available: false,
            message: format!("missing table {table}"),
        },
        Err(err) => CapabilityStatus {
            name,
            available: false,
            message: err.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_schema_embeds_phase_two_migrations() {
        assert!(DLQ_SCHEMA.contains("CREATE TABLE IF NOT EXISTS mq_dlq"));
        assert!(DLQ_SCHEMA.contains("max_redelivery_count"));
        assert!(AUTH_SCHEMA.contains("CREATE TABLE IF NOT EXISTS pgapp_clients"));
        assert!(CONFIG_SCHEMA_VALIDATION.contains("json_schema JSONB"));
    }
}
