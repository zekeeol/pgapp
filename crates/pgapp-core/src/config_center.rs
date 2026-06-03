use crate::{
    PgAppError, PgAppResult,
    validation::{validate_config_component, validate_config_key},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigScope {
    pub app_id: String,
    pub environment: String,
    pub cluster: String,
    pub namespace: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigItem {
    pub key: String,
    pub value: Value,
    pub deleted: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigRelease {
    pub scope: ConfigScope,
    pub revision: i64,
    pub checksum: String,
    pub snapshot: Value,
    pub message: String,
    pub published_by: String,
    pub published_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigScopeSummary {
    pub scope: ConfigScope,
    pub current_revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigWatchResult {
    pub changed: bool,
    pub latest_revision: i64,
    pub release: Option<ConfigRelease>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigLimits {
    pub max_watch_seconds: i64,
    pub max_payload_bytes: usize,
    pub max_page_size: usize,
    pub max_schema_bytes: usize,
}

impl Default for ConfigLimits {
    fn default() -> Self {
        Self {
            max_watch_seconds: 30,
            max_payload_bytes: 1024 * 1024,
            max_page_size: 100,
            max_schema_bytes: 256 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigPage<T> {
    pub items: Vec<T>,
    pub limit: usize,
    pub offset: i64,
    pub next_offset: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ConfigStore {
    pool: PgPool,
    limits: ConfigLimits,
}

impl ConfigStore {
    pub fn new(pool: PgPool, limits: ConfigLimits) -> Self {
        Self { pool, limits }
    }

    pub async fn list_scopes(
        &self,
        limit: impl Into<Option<i64>>,
        offset: i64,
    ) -> PgAppResult<ConfigPage<ConfigScopeSummary>> {
        let page = self.page(limit.into(), offset)?;
        let rows = sqlx::query(
            r#"
            SELECT app_id, environment, cluster, namespace, current_revision, created_at, updated_at
            FROM config_scopes
            ORDER BY app_id, environment, cluster, namespace
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(page.fetch_limit)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(page.finish(
            rows.into_iter()
                .map(|row| ConfigScopeSummary {
                    scope: row_to_scope(&row),
                    current_revision: row.get("current_revision"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                })
                .collect(),
        ))
    }

    pub async fn upsert_item(
        &self,
        scope: &ConfigScope,
        key: &str,
        value: Value,
    ) -> PgAppResult<()> {
        validate_scope(scope)?;
        validate_config_key(key)?;
        self.validate_payload_size(&value)?;
        let mut tx = self.pool.begin().await?;
        let scope_id = ensure_scope(&mut tx, scope).await?;
        if let Some(schema) = schema_for_scope_id(&mut tx, scope_id).await? {
            validate_value_against_schema(key, &schema, &value)?;
        }
        sqlx::query(
            r#"
            INSERT INTO config_items(scope_id, config_key, value_json, deleted, updated_at)
            VALUES ($1, $2, $3, false, now())
            ON CONFLICT (scope_id, config_key)
            DO UPDATE SET value_json = EXCLUDED.value_json, deleted = false, updated_at = now()
            "#,
        )
        .bind(scope_id)
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn set_schema(&self, scope: &ConfigScope, schema: Option<Value>) -> PgAppResult<()> {
        validate_scope(scope)?;
        if let Some(schema) = &schema {
            self.validate_schema_size(schema)?;
            validate_json_schema(schema)?;
        }
        let mut tx = self.pool.begin().await?;
        let scope_id = ensure_scope(&mut tx, scope).await?;
        sqlx::query(
            r#"
            UPDATE config_scopes
            SET json_schema = $2, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(scope_id)
        .bind(schema)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_schema(&self, scope: &ConfigScope) -> PgAppResult<Option<Value>> {
        validate_scope(scope)?;
        let schema = sqlx::query_scalar(
            r#"
            SELECT json_schema
            FROM config_scopes
            WHERE app_id = $1 AND environment = $2 AND cluster = $3 AND namespace = $4
            "#,
        )
        .bind(&scope.app_id)
        .bind(&scope.environment)
        .bind(&scope.cluster)
        .bind(&scope.namespace)
        .fetch_optional(&self.pool)
        .await?;
        Ok(schema.flatten())
    }

    pub async fn delete_item(&self, scope: &ConfigScope, key: &str) -> PgAppResult<bool> {
        validate_scope(scope)?;
        validate_config_key(key)?;
        let mut tx = self.pool.begin().await?;
        let scope_id = ensure_scope(&mut tx, scope).await?;
        sqlx::query(
            r#"
            INSERT INTO config_items(scope_id, config_key, value_json, deleted, updated_at)
            VALUES ($1, $2, 'null'::jsonb, true, now())
            ON CONFLICT (scope_id, config_key)
            DO UPDATE SET value_json = 'null'::jsonb, deleted = true, updated_at = now()
            "#,
        )
        .bind(scope_id)
        .bind(key)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn get_draft(&self, scope: &ConfigScope) -> PgAppResult<Vec<ConfigItem>> {
        validate_scope(scope)?;
        let Some(scope_id) = self.scope_id(scope).await? else {
            return Ok(Vec::new());
        };
        let rows = sqlx::query(
            r#"
            SELECT config_key, value_json, deleted, updated_at
            FROM config_items
            WHERE scope_id = $1
            ORDER BY config_key
            "#,
        )
        .bind(scope_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ConfigItem {
                    key: row.try_get("config_key")?,
                    value: row.try_get("value_json")?,
                    deleted: row.try_get("deleted")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }

    pub async fn publish(
        &self,
        scope: &ConfigScope,
        message: &str,
        published_by: &str,
    ) -> PgAppResult<ConfigRelease> {
        validate_scope(scope)?;
        validate_publish_field("message", message)?;
        validate_publish_field("published_by", published_by)?;
        let mut tx = self.pool.begin().await?;
        let scope_id = ensure_scope(&mut tx, scope).await?;
        let current_revision: i64 = sqlx::query_scalar(
            "SELECT current_revision FROM config_scopes WHERE id = $1 FOR UPDATE",
        )
        .bind(scope_id)
        .fetch_one(&mut *tx)
        .await?;

        let rows = sqlx::query(
            r#"
            SELECT config_key, value_json
            FROM config_items
            WHERE scope_id = $1 AND deleted = false
            ORDER BY config_key
            "#,
        )
        .bind(scope_id)
        .fetch_all(&mut *tx)
        .await?;
        let schema = schema_for_scope_id(&mut tx, scope_id).await?;
        let mut snapshot = Map::new();
        for row in rows {
            let key: String = row.get("config_key");
            let value: Value = row.get("value_json");
            if let Some(schema) = &schema {
                validate_value_against_schema(&key, schema, &value)?;
            }
            snapshot.insert(key, value);
        }
        let snapshot = Value::Object(snapshot);
        let checksum = checksum_json(&snapshot)?;
        let revision = current_revision + 1;
        let row = sqlx::query(
            r#"
            INSERT INTO config_releases(scope_id, revision, snapshot_json, checksum, message, published_by)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING published_at
            "#,
        )
        .bind(scope_id)
        .bind(revision)
        .bind(&snapshot)
        .bind(&checksum)
        .bind(message)
        .bind(published_by)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE config_scopes
            SET current_revision = $2, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(scope_id)
        .bind(revision)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(ConfigRelease {
            scope: scope.clone(),
            revision,
            checksum,
            snapshot,
            message: message.to_string(),
            published_by: published_by.to_string(),
            published_at: row.get("published_at"),
        })
    }

    pub async fn get_latest_release(&self, scope: &ConfigScope) -> PgAppResult<ConfigRelease> {
        validate_scope(scope)?;
        let row = sqlx::query(
            r#"
            SELECT s.app_id, s.environment, s.cluster, s.namespace,
                   r.revision, r.checksum, r.snapshot_json, r.message, r.published_by, r.published_at
            FROM config_releases r
            JOIN config_scopes s ON s.id = r.scope_id
            WHERE s.app_id = $1 AND s.environment = $2 AND s.cluster = $3 AND s.namespace = $4
            ORDER BY r.revision DESC
            LIMIT 1
            "#,
        )
        .bind(&scope.app_id)
        .bind(&scope.environment)
        .bind(&scope.cluster)
        .bind(&scope.namespace)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_release)
            .transpose()?
            .ok_or_else(|| PgAppError::NotFound(format!("config release {}", scope_label(scope))))
    }

    pub async fn get_release(
        &self,
        scope: &ConfigScope,
        revision: i64,
    ) -> PgAppResult<ConfigRelease> {
        validate_scope(scope)?;
        if revision <= 0 {
            return self.get_latest_release(scope).await;
        }
        let row = sqlx::query(
            r#"
            SELECT s.app_id, s.environment, s.cluster, s.namespace,
                   r.revision, r.checksum, r.snapshot_json, r.message, r.published_by, r.published_at
            FROM config_releases r
            JOIN config_scopes s ON s.id = r.scope_id
            WHERE s.app_id = $1 AND s.environment = $2 AND s.cluster = $3 AND s.namespace = $4
              AND r.revision = $5
            "#,
        )
        .bind(&scope.app_id)
        .bind(&scope.environment)
        .bind(&scope.cluster)
        .bind(&scope.namespace)
        .bind(revision)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_release)
            .transpose()?
            .ok_or_else(|| PgAppError::NotFound(format!("config release {}", scope_label(scope))))
    }

    pub async fn list_releases(
        &self,
        scope: &ConfigScope,
        limit: impl Into<Option<i64>>,
        offset: i64,
    ) -> PgAppResult<ConfigPage<ConfigRelease>> {
        validate_scope(scope)?;
        let page = self.page(limit.into(), offset)?;
        let rows = sqlx::query(
            r#"
            SELECT s.app_id, s.environment, s.cluster, s.namespace,
                   r.revision, r.checksum, r.snapshot_json, r.message, r.published_by, r.published_at
            FROM config_releases r
            JOIN config_scopes s ON s.id = r.scope_id
            WHERE s.app_id = $1 AND s.environment = $2 AND s.cluster = $3 AND s.namespace = $4
            ORDER BY r.revision DESC
            LIMIT $5 OFFSET $6
            "#,
        )
        .bind(&scope.app_id)
        .bind(&scope.environment)
        .bind(&scope.cluster)
        .bind(&scope.namespace)
        .bind(page.fetch_limit)
        .bind(page.offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(page.finish(
            rows.into_iter()
                .map(row_to_release)
                .collect::<PgAppResult<Vec<_>>>()?,
        ))
    }

    pub async fn watch(
        &self,
        scope: &ConfigScope,
        known_revision: i64,
        timeout_seconds: i64,
        poll_interval_millis: i64,
    ) -> PgAppResult<ConfigWatchResult> {
        validate_scope(scope)?;
        if known_revision < 0 {
            return Err(PgAppError::InvalidArgument(
                "known_revision must not be negative".to_string(),
            ));
        }
        if timeout_seconds < 0 {
            return Err(PgAppError::InvalidArgument(
                "timeout_seconds must not be negative".to_string(),
            ));
        }
        if timeout_seconds > self.limits.max_watch_seconds {
            return Err(PgAppError::InvalidArgument(format!(
                "timeout_seconds must be less than or equal to {}",
                self.limits.max_watch_seconds
            )));
        }
        let interval = if poll_interval_millis <= 0 {
            Duration::from_millis(100)
        } else {
            Duration::from_millis(poll_interval_millis as u64)
        };
        let deadline = Utc::now() + ChronoDuration::seconds(timeout_seconds);

        loop {
            if let Some(release) = self.latest_newer_than(scope, known_revision).await? {
                return Ok(ConfigWatchResult {
                    changed: true,
                    latest_revision: release.revision,
                    release: Some(release),
                });
            }
            let latest_revision = self.current_revision(scope).await?;
            if Utc::now() >= deadline {
                return Ok(ConfigWatchResult {
                    changed: false,
                    latest_revision,
                    release: None,
                });
            }
            sleep(interval).await;
        }
    }

    async fn latest_newer_than(
        &self,
        scope: &ConfigScope,
        known_revision: i64,
    ) -> PgAppResult<Option<ConfigRelease>> {
        let row = sqlx::query(
            r#"
            SELECT s.app_id, s.environment, s.cluster, s.namespace,
                   r.revision, r.checksum, r.snapshot_json, r.message, r.published_by, r.published_at
            FROM config_releases r
            JOIN config_scopes s ON s.id = r.scope_id
            WHERE s.app_id = $1 AND s.environment = $2 AND s.cluster = $3 AND s.namespace = $4
              AND r.revision > $5
            ORDER BY r.revision DESC
            LIMIT 1
            "#,
        )
        .bind(&scope.app_id)
        .bind(&scope.environment)
        .bind(&scope.cluster)
        .bind(&scope.namespace)
        .bind(known_revision)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_release).transpose()
    }

    async fn scope_id(&self, scope: &ConfigScope) -> PgAppResult<Option<i64>> {
        let id = sqlx::query_scalar(
            r#"
            SELECT id
            FROM config_scopes
            WHERE app_id = $1 AND environment = $2 AND cluster = $3 AND namespace = $4
            "#,
        )
        .bind(&scope.app_id)
        .bind(&scope.environment)
        .bind(&scope.cluster)
        .bind(&scope.namespace)
        .fetch_optional(&self.pool)
        .await?;
        Ok(id)
    }

    async fn current_revision(&self, scope: &ConfigScope) -> PgAppResult<i64> {
        let revision = sqlx::query_scalar(
            r#"
            SELECT current_revision
            FROM config_scopes
            WHERE app_id = $1 AND environment = $2 AND cluster = $3 AND namespace = $4
            "#,
        )
        .bind(&scope.app_id)
        .bind(&scope.environment)
        .bind(&scope.cluster)
        .bind(&scope.namespace)
        .fetch_optional(&self.pool)
        .await?;
        Ok(revision.unwrap_or(0))
    }

    fn validate_payload_size(&self, value: &Value) -> PgAppResult<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|err| PgAppError::InvalidArgument(format!("invalid JSON value: {err}")))?;
        if bytes.len() > self.limits.max_payload_bytes {
            return Err(PgAppError::InvalidArgument(format!(
                "json_value exceeds {} bytes",
                self.limits.max_payload_bytes
            )));
        }
        Ok(())
    }

    fn validate_schema_size(&self, value: &Value) -> PgAppResult<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|err| PgAppError::InvalidArgument(format!("invalid JSON schema: {err}")))?;
        if bytes.len() > self.limits.max_schema_bytes {
            return Err(PgAppError::InvalidArgument(format!(
                "json_schema exceeds {} bytes",
                self.limits.max_schema_bytes
            )));
        }
        Ok(())
    }

    fn page(&self, limit: Option<i64>, offset: i64) -> PgAppResult<PageState> {
        if offset < 0 {
            return Err(PgAppError::InvalidArgument(
                "offset must not be negative".to_string(),
            ));
        }
        let requested = limit.unwrap_or(self.limits.max_page_size as i64);
        if requested <= 0 {
            return Err(PgAppError::InvalidArgument(
                "limit must be positive".to_string(),
            ));
        }
        let limit = requested.min(self.limits.max_page_size as i64) as usize;
        Ok(PageState {
            limit,
            fetch_limit: limit as i64 + 1,
            offset,
        })
    }
}

struct PageState {
    limit: usize,
    fetch_limit: i64,
    offset: i64,
}

impl PageState {
    fn finish<T>(self, mut items: Vec<T>) -> ConfigPage<T> {
        let has_more = items.len() > self.limit;
        if has_more {
            items.truncate(self.limit);
        }
        ConfigPage {
            items,
            limit: self.limit,
            offset: self.offset,
            next_offset: has_more.then_some(self.offset + self.limit as i64),
        }
    }
}

async fn ensure_scope(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &ConfigScope,
) -> PgAppResult<i64> {
    let id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO config_scopes(app_id, environment, cluster, namespace)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (app_id, environment, cluster, namespace)
        DO UPDATE SET updated_at = config_scopes.updated_at
        RETURNING id
        "#,
    )
    .bind(&scope.app_id)
    .bind(&scope.environment)
    .bind(&scope.cluster)
    .bind(&scope.namespace)
    .fetch_one(&mut **tx)
    .await?;
    Ok(id)
}

async fn schema_for_scope_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope_id: i64,
) -> PgAppResult<Option<Value>> {
    let schema: Option<Value> =
        sqlx::query_scalar("SELECT json_schema FROM config_scopes WHERE id = $1")
            .bind(scope_id)
            .fetch_one(&mut **tx)
            .await?;
    Ok(schema)
}

fn validate_json_schema(schema: &Value) -> PgAppResult<()> {
    jsonschema::validator_for(schema)
        .map(|_| ())
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid JSON schema: {err}")))
}

fn validate_value_against_schema(key: &str, schema: &Value, value: &Value) -> PgAppResult<()> {
    let validator = jsonschema::validator_for(schema)
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid JSON schema: {err}")))?;
    validator.validate(value).map_err(|err| {
        PgAppError::InvalidArgument(format!(
            "config key {key} failed JSON schema validation: {err}"
        ))
    })
}

fn validate_scope(scope: &ConfigScope) -> PgAppResult<()> {
    validate_config_component("app_id", &scope.app_id)?;
    validate_config_component("environment", &scope.environment)?;
    validate_config_component("cluster", &scope.cluster)?;
    validate_config_component("namespace", &scope.namespace)?;
    Ok(())
}

fn validate_publish_field(field: &str, value: &str) -> PgAppResult<()> {
    if value.len() > 512 {
        return Err(PgAppError::InvalidArgument(format!(
            "{field} exceeds 512 bytes"
        )));
    }
    Ok(())
}

fn checksum_json(value: &Value) -> PgAppResult<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid JSON value: {err}")))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}

fn row_to_scope(row: &sqlx::postgres::PgRow) -> ConfigScope {
    ConfigScope {
        app_id: row.get("app_id"),
        environment: row.get("environment"),
        cluster: row.get("cluster"),
        namespace: row.get("namespace"),
    }
}

fn row_to_release(row: sqlx::postgres::PgRow) -> PgAppResult<ConfigRelease> {
    Ok(ConfigRelease {
        scope: row_to_scope(&row),
        revision: row.try_get("revision")?,
        checksum: row.try_get("checksum")?,
        snapshot: row.try_get("snapshot_json")?,
        message: row.try_get("message")?,
        published_by: row.try_get("published_by")?,
        published_at: row.try_get("published_at")?,
    })
}

fn scope_label(scope: &ConfigScope) -> String {
    format!(
        "{}/{}/{}/{}",
        scope.app_id, scope.environment, scope.cluster, scope.namespace
    )
}
