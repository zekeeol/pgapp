use crate::{
    PgAppError, PgAppResult,
    validation::{hash_key, validate_cache_key, validate_namespace},
};
use sqlx::{PgPool, Row};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheLimits {
    pub max_keys: Option<i64>,
    pub max_bytes: Option<i64>,
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            max_keys: None,
            max_bytes: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStore {
    pool: PgPool,
    limits: CacheLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: i64,
    pub misses: i64,
    pub writes: i64,
    pub deletes: i64,
    pub evictions: i64,
    pub expired_removals: i64,
    pub logical_key_count: i64,
    pub logical_byte_size: i64,
    pub namespace_usage: HashMap<String, NamespaceUsage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceUsage {
    pub key_count: i64,
    pub byte_size: i64,
}

impl CacheStore {
    pub fn new(pool: PgPool, limits: CacheLimits) -> Self {
        Self { pool, limits }
    }

    pub async fn set(
        &self,
        namespace: &str,
        key: &str,
        value: &[u8],
        ttl_seconds: Option<i64>,
    ) -> PgAppResult<()> {
        validate_namespace(namespace)?;
        validate_cache_key(key)?;
        if let Some(ttl) = ttl_seconds {
            if ttl <= 0 {
                return Err(PgAppError::InvalidArgument(
                    "ttl_seconds must be positive when provided".to_string(),
                ));
            }
        }
        self.ensure_namespace(namespace).await?;
        let generation = self.namespace_generation(namespace).await?;
        let key_hash = hash_key(key);
        let size_bytes = value.len() as i64;

        sqlx::query(
            r#"
            INSERT INTO cache_entries(
              namespace, generation, key_hash, cache_key, value_bytes, expires_at, size_bytes
            )
            VALUES (
              $1,
              $2,
              $3,
              $4,
              $5,
              CASE
                WHEN $6::bigint IS NULL THEN NULL
                ELSE now() + ($6::double precision * interval '1 second')
              END,
              $7
            )
            ON CONFLICT (namespace, generation, key_hash)
            DO UPDATE SET
              cache_key = EXCLUDED.cache_key,
              value_bytes = EXCLUDED.value_bytes,
              expires_at = EXCLUDED.expires_at,
              size_bytes = EXCLUDED.size_bytes,
              updated_at = now()
            "#,
        )
        .bind(namespace)
        .bind(generation)
        .bind(key_hash)
        .bind(key)
        .bind(value)
        .bind(ttl_seconds)
        .bind(size_bytes)
        .execute(&self.pool)
        .await?;

        self.increment_counter("writes", 1).await?;
        self.enforce_capacity(namespace).await?;
        Ok(())
    }

    pub async fn get(&self, namespace: &str, key: &str) -> PgAppResult<Option<Vec<u8>>> {
        validate_namespace(namespace)?;
        validate_cache_key(key)?;
        let Some(generation) = self.try_namespace_generation(namespace).await? else {
            self.increment_counter("misses", 1).await?;
            return Ok(None);
        };
        let key_hash = hash_key(key);
        let row = sqlx::query(
            r#"
            SELECT id, value_bytes
            FROM cache_entries
            WHERE namespace = $1
              AND generation = $2
              AND key_hash = $3
              AND cache_key = $4
              AND (expires_at IS NULL OR expires_at > now())
            "#,
        )
        .bind(namespace)
        .bind(generation)
        .bind(key_hash)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let id: i64 = row.try_get("id")?;
            let value: Vec<u8> = row.try_get("value_bytes")?;
            sqlx::query(
                r#"
                UPDATE cache_entries
                SET last_accessed_at = now(), access_count = access_count + 1
                WHERE id = $1
                "#,
            )
            .bind(id)
            .execute(&self.pool)
            .await?;
            self.increment_counter("hits", 1).await?;
            Ok(Some(value))
        } else {
            self.increment_counter("misses", 1).await?;
            Ok(None)
        }
    }

    pub async fn mget(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> PgAppResult<Vec<(String, Option<Vec<u8>>)>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push((key.clone(), self.get(namespace, key).await?));
        }
        Ok(results)
    }

    pub async fn exists(&self, namespace: &str, key: &str) -> PgAppResult<bool> {
        Ok(self.get(namespace, key).await?.is_some())
    }

    pub async fn delete(&self, namespace: &str, key: &str) -> PgAppResult<bool> {
        validate_namespace(namespace)?;
        validate_cache_key(key)?;
        let Some(generation) = self.try_namespace_generation(namespace).await? else {
            return Ok(false);
        };
        let result = sqlx::query(
            r#"
            DELETE FROM cache_entries
            WHERE namespace = $1 AND generation = $2 AND key_hash = $3 AND cache_key = $4
            "#,
        )
        .bind(namespace)
        .bind(generation)
        .bind(hash_key(key))
        .bind(key)
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected() as i64;
        if deleted > 0 {
            self.increment_counter("deletes", deleted).await?;
        }
        Ok(deleted > 0)
    }

    pub async fn invalidate_namespace(&self, namespace: &str) -> PgAppResult<()> {
        validate_namespace(namespace)?;
        self.ensure_namespace(namespace).await?;
        sqlx::query(
            r#"
            WITH updated AS (
              UPDATE cache_namespaces
              SET generation = generation + 1, updated_at = now()
              WHERE name = $1
              RETURNING generation
            )
            DELETE FROM cache_entries
            USING updated
            WHERE cache_entries.namespace = $1
              AND cache_entries.generation < updated.generation
            "#,
        )
        .bind(namespace)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn sweep_expired(&self, limit: i64) -> PgAppResult<i64> {
        let row = sqlx::query(
            r#"
            WITH doomed AS (
              SELECT id
              FROM cache_entries
              WHERE expires_at IS NOT NULL AND expires_at <= now()
              ORDER BY expires_at
              LIMIT $1
            )
            DELETE FROM cache_entries
            USING doomed
            WHERE cache_entries.id = doomed.id
            "#,
        )
        .bind(limit)
        .execute(&self.pool)
        .await?;
        let removed = row.rows_affected() as i64;
        if removed > 0 {
            self.increment_counter("expired_removals", removed).await?;
        }
        Ok(removed)
    }

    pub async fn stats(&self) -> PgAppResult<CacheStats> {
        let counters = sqlx::query(
            r#"
            SELECT hits, misses, writes, deletes, evictions, expired_removals
            FROM cache_stats
            WHERE singleton = true
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let usage_rows = sqlx::query(
            r#"
            SELECT e.namespace, COUNT(*)::bigint AS key_count, COALESCE(SUM(e.size_bytes), 0)::bigint AS byte_size
            FROM cache_entries e
            JOIN cache_namespaces n ON n.name = e.namespace AND n.generation = e.generation
            WHERE e.expires_at IS NULL OR e.expires_at > now()
            GROUP BY e.namespace
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut namespace_usage = HashMap::new();
        let mut logical_key_count = 0;
        let mut logical_byte_size = 0;
        for row in usage_rows {
            let namespace: String = row.try_get("namespace")?;
            let key_count: i64 = row.try_get("key_count")?;
            let byte_size: i64 = row.try_get("byte_size")?;
            logical_key_count += key_count;
            logical_byte_size += byte_size;
            namespace_usage.insert(
                namespace,
                NamespaceUsage {
                    key_count,
                    byte_size,
                },
            );
        }

        Ok(CacheStats {
            hits: counters.try_get("hits")?,
            misses: counters.try_get("misses")?,
            writes: counters.try_get("writes")?,
            deletes: counters.try_get("deletes")?,
            evictions: counters.try_get("evictions")?,
            expired_removals: counters.try_get("expired_removals")?,
            logical_key_count,
            logical_byte_size,
            namespace_usage,
        })
    }

    async fn enforce_capacity(&self, namespace: &str) -> PgAppResult<()> {
        let removed = self.sweep_expired(1_000).await?;
        if removed > 0 {
            return Ok(());
        }
        let Some(generation) = self.try_namespace_generation(namespace).await? else {
            return Ok(());
        };

        let usage = sqlx::query(
            r#"
            SELECT COUNT(*)::bigint AS key_count, COALESCE(SUM(size_bytes), 0)::bigint AS byte_size
            FROM cache_entries
            WHERE namespace = $1 AND generation = $2
            "#,
        )
        .bind(namespace)
        .bind(generation)
        .fetch_one(&self.pool)
        .await?;
        let mut excess_keys = self
            .limits
            .max_keys
            .map(|max| usage.get::<i64, _>("key_count") - max)
            .unwrap_or(0);
        let mut over_bytes = self
            .limits
            .max_bytes
            .map(|max| usage.get::<i64, _>("byte_size") > max)
            .unwrap_or(false);

        while excess_keys > 0 || over_bytes {
            let deleted = sqlx::query(
                r#"
                WITH victim AS (
                  SELECT id
                  FROM cache_entries
                  WHERE namespace = $1 AND generation = $2
                  ORDER BY last_accessed_at ASC, id ASC
                  LIMIT 1
                )
                DELETE FROM cache_entries
                USING victim
                WHERE cache_entries.id = victim.id
                "#,
            )
            .bind(namespace)
            .bind(generation)
            .execute(&self.pool)
            .await?
            .rows_affected() as i64;
            if deleted == 0 {
                break;
            }
            self.increment_counter("evictions", deleted).await?;
            excess_keys -= deleted;
            if self.limits.max_bytes.is_some() {
                let byte_size: i64 = sqlx::query_scalar(
                    r#"
                    SELECT COALESCE(SUM(size_bytes), 0)::bigint
                    FROM cache_entries
                    WHERE namespace = $1 AND generation = $2
                    "#,
                )
                .bind(namespace)
                .bind(generation)
                .fetch_one(&self.pool)
                .await?;
                over_bytes = self
                    .limits
                    .max_bytes
                    .map(|max| byte_size > max)
                    .unwrap_or(false);
            }
        }

        Ok(())
    }

    async fn ensure_namespace(&self, namespace: &str) -> PgAppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO cache_namespaces(name)
            VALUES ($1)
            ON CONFLICT (name) DO NOTHING
            "#,
        )
        .bind(namespace)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn namespace_generation(&self, namespace: &str) -> PgAppResult<i64> {
        self.try_namespace_generation(namespace)
            .await?
            .ok_or_else(|| PgAppError::NotFound(format!("namespace {namespace}")))
    }

    async fn try_namespace_generation(&self, namespace: &str) -> PgAppResult<Option<i64>> {
        let generation = sqlx::query_scalar(
            r#"
            SELECT generation
            FROM cache_namespaces
            WHERE name = $1
            "#,
        )
        .bind(namespace)
        .fetch_optional(&self.pool)
        .await?;
        Ok(generation)
    }

    async fn increment_counter(&self, counter: &str, amount: i64) -> PgAppResult<()> {
        let sql = match counter {
            "hits" => "UPDATE cache_stats SET hits = hits + $1 WHERE singleton = true",
            "misses" => "UPDATE cache_stats SET misses = misses + $1 WHERE singleton = true",
            "writes" => "UPDATE cache_stats SET writes = writes + $1 WHERE singleton = true",
            "deletes" => "UPDATE cache_stats SET deletes = deletes + $1 WHERE singleton = true",
            "evictions" => {
                "UPDATE cache_stats SET evictions = evictions + $1 WHERE singleton = true"
            }
            "expired_removals" => {
                "UPDATE cache_stats SET expired_removals = expired_removals + $1 WHERE singleton = true"
            }
            _ => {
                return Err(PgAppError::InvalidArgument(
                    "unknown cache counter".to_string(),
                ));
            }
        };
        sqlx::query(sql).bind(amount).execute(&self.pool).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limits_are_unbounded() {
        let limits = CacheLimits::default();
        assert_eq!(limits.max_keys, None);
        assert_eq!(limits.max_bytes, None);
    }

    #[test]
    fn namespace_usage_tracks_counts_and_bytes() {
        let usage = NamespaceUsage {
            key_count: 2,
            byte_size: 10,
        };
        assert_eq!(usage.key_count, 2);
        assert_eq!(usage.byte_size, 10);
    }
}
