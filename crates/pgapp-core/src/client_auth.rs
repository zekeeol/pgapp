use crate::{
    PgAppError, PgAppResult,
    validation::{hash_key, validate_config_component},
};
use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Clone)]
pub struct ClientStore {
    pool: PgPool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedClient {
    pub id: i64,
    pub client_key: String,
    pub secret: String,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    pub id: i64,
    pub client_key: String,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientRecord {
    pub id: i64,
    pub client_key: String,
    pub active: bool,
    pub roles: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ClientStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_client(
        &self,
        client_key: &str,
        roles: Vec<String>,
    ) -> PgAppResult<CreatedClient> {
        validate_client_key(client_key)?;
        validate_roles(&roles)?;
        let secret = generate_secret();
        let key_hash = hash_key(client_key);
        let secret_hash = hash_secret(&secret)?;
        let roles_json = serde_json::to_value(&roles)
            .map_err(|err| PgAppError::InvalidArgument(format!("invalid roles: {err}")))?;

        let row = sqlx::query(
            r#"
            INSERT INTO pgapp_clients (client_key, key_hash, secret_hash, active, roles)
            VALUES ($1, $2, $3, true, $4)
            RETURNING id
            "#,
        )
        .bind(client_key)
        .bind(key_hash)
        .bind(secret_hash)
        .bind(roles_json)
        .fetch_one(&self.pool)
        .await
        .map_err(map_create_error)?;

        Ok(CreatedClient {
            id: row.try_get("id")?,
            client_key: client_key.to_string(),
            secret,
            roles,
        })
    }

    pub async fn authenticate(
        &self,
        client_key: &str,
        secret: &str,
    ) -> PgAppResult<Option<ClientIdentity>> {
        if client_key.is_empty() || secret.is_empty() {
            return Ok(None);
        }

        let key_hash = hash_key(client_key);
        let row = sqlx::query(
            r#"
            SELECT id, client_key, secret_hash, roles
            FROM pgapp_clients
            WHERE key_hash = $1 AND active = true
            "#,
        )
        .bind(key_hash)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let secret_hash: String = row.try_get("secret_hash")?;
        if !verify_secret(&secret_hash, secret)? {
            return Ok(None);
        }

        Ok(Some(ClientIdentity {
            id: row.try_get("id")?,
            client_key: row.try_get("client_key")?,
            roles: parse_roles(row.try_get("roles")?)?,
        }))
    }

    pub async fn rotate_secret(&self, client_key: &str) -> PgAppResult<Option<CreatedClient>> {
        validate_client_key(client_key)?;
        let secret = generate_secret();
        let secret_hash = hash_secret(&secret)?;
        let key_hash = hash_key(client_key);

        let row = sqlx::query(
            r#"
            UPDATE pgapp_clients
            SET secret_hash = $2, updated_at = now()
            WHERE key_hash = $1 AND active = true
            RETURNING id, client_key, roles
            "#,
        )
        .bind(key_hash)
        .bind(secret_hash)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(CreatedClient {
                id: row.try_get("id")?,
                client_key: row.try_get("client_key")?,
                secret,
                roles: parse_roles(row.try_get("roles")?)?,
            })
        })
        .transpose()
    }

    pub async fn deactivate(&self, client_key: &str) -> PgAppResult<bool> {
        validate_client_key(client_key)?;
        let result = sqlx::query(
            r#"
            UPDATE pgapp_clients
            SET active = false, updated_at = now()
            WHERE key_hash = $1 AND active = true
            "#,
        )
        .bind(hash_key(client_key))
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_clients(&self) -> PgAppResult<Vec<ClientRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, client_key, active, roles, created_at, updated_at
            FROM pgapp_clients
            ORDER BY created_at DESC, id DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ClientRecord {
                    id: row.try_get("id")?,
                    client_key: row.try_get("client_key")?,
                    active: row.try_get("active")?,
                    roles: parse_roles(row.try_get("roles")?)?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }
}

fn validate_client_key(client_key: &str) -> PgAppResult<()> {
    validate_config_component("client_key", client_key)
}

fn validate_roles(roles: &[String]) -> PgAppResult<()> {
    for role in roles {
        validate_config_component("role", role)?;
    }
    Ok(())
}

fn generate_secret() -> String {
    format!("pgapp_{}_{}", Uuid::new_v4(), Uuid::new_v4())
}

fn hash_secret(secret: &str) -> PgAppResult<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| PgAppError::InvalidArgument(format!("failed to hash client secret: {err}")))
}

fn verify_secret(secret_hash: &str, secret: &str) -> PgAppResult<bool> {
    let parsed = PasswordHash::new(secret_hash)
        .map_err(|err| PgAppError::Database(format!("invalid stored client secret hash: {err}")))?;
    Ok(Argon2::default()
        .verify_password(secret.as_bytes(), &parsed)
        .is_ok())
}

fn parse_roles(value: Value) -> PgAppResult<Vec<String>> {
    serde_json::from_value(value)
        .map_err(|err| PgAppError::Database(format!("invalid stored client roles: {err}")))
}

fn map_create_error(err: sqlx::Error) -> PgAppError {
    if let sqlx::Error::Database(db_err) = &err
        && db_err.is_unique_violation()
    {
        return PgAppError::Conflict("client key already exists".to_string());
    }
    err.into()
}
