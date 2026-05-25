use crate::{PgAppError, PgAppResult, cache::CacheLimits};
use std::{collections::HashMap, net::SocketAddr, time::Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceToggles {
    pub cache: bool,
    pub mq: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestLimits {
    pub max_batch_size: i32,
    pub max_payload_bytes: usize,
    pub max_visibility_timeout_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub services: ServiceToggles,
    pub limits: RequestLimits,
    pub cache_limits: CacheLimits,
    pub default_request_timeout: Duration,
    pub transient_queues_enabled: bool,
}

impl ServerConfig {
    pub fn from_env() -> PgAppResult<Self> {
        Self::from_map(std::env::vars().collect())
    }

    pub fn from_map(map: HashMap<String, String>) -> PgAppResult<Self> {
        let database_url = required(&map, "DATABASE_URL")?;
        let bind_addr = optional(&map, "PGAPP_BIND_ADDR", "127.0.0.1:50051")
            .parse()
            .map_err(|err| {
                PgAppError::InvalidArgument(format!("invalid PGAPP_BIND_ADDR: {err}"))
            })?;
        let max_connections = parse_u32(&map, "PGAPP_MAX_CONNECTIONS", 20)?;
        let min_connections = parse_u32(&map, "PGAPP_MIN_CONNECTIONS", 1)?;
        if min_connections > max_connections {
            return Err(PgAppError::InvalidArgument(
                "PGAPP_MIN_CONNECTIONS must be <= PGAPP_MAX_CONNECTIONS".to_string(),
            ));
        }

        Ok(Self {
            bind_addr,
            database_url,
            max_connections,
            min_connections,
            services: ServiceToggles {
                cache: parse_bool(&map, "PGAPP_ENABLE_CACHE", true)?,
                mq: parse_bool(&map, "PGAPP_ENABLE_MQ", true)?,
            },
            limits: RequestLimits {
                max_batch_size: parse_i32(&map, "PGAPP_MAX_BATCH_SIZE", 100)?,
                max_payload_bytes: parse_usize(&map, "PGAPP_MAX_PAYLOAD_BYTES", 1024 * 1024)?,
                max_visibility_timeout_seconds: parse_i64(
                    &map,
                    "PGAPP_MAX_VISIBILITY_TIMEOUT_SECONDS",
                    12 * 60 * 60,
                )?,
            },
            cache_limits: CacheLimits {
                max_keys: parse_optional_i64(&map, "PGAPP_CACHE_MAX_KEYS")?,
                max_bytes: parse_optional_i64(&map, "PGAPP_CACHE_MAX_BYTES")?,
            },
            default_request_timeout: Duration::from_secs(parse_u64(
                &map,
                "PGAPP_DEFAULT_TIMEOUT_SECONDS",
                30,
            )?),
            transient_queues_enabled: parse_bool(&map, "PGAPP_ENABLE_TRANSIENT_QUEUES", false)?,
        })
    }
}

fn required(map: &HashMap<String, String>, key: &str) -> PgAppResult<String> {
    map.get(key)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| PgAppError::InvalidArgument(format!("{key} is required")))
}

fn optional<'a>(map: &'a HashMap<String, String>, key: &str, default: &'a str) -> &'a str {
    map.get(key).map(String::as_str).unwrap_or(default)
}

fn parse_bool(map: &HashMap<String, String>, key: &str, default: bool) -> PgAppResult<bool> {
    match optional(map, key, if default { "true" } else { "false" }) {
        "1" | "true" | "TRUE" | "yes" | "YES" => Ok(true),
        "0" | "false" | "FALSE" | "no" | "NO" => Ok(false),
        other => Err(PgAppError::InvalidArgument(format!(
            "{key} must be boolean, got {other}"
        ))),
    }
}

fn parse_u32(map: &HashMap<String, String>, key: &str, default: u32) -> PgAppResult<u32> {
    optional(map, key, &default.to_string())
        .parse()
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid {key}: {err}")))
}

fn parse_u64(map: &HashMap<String, String>, key: &str, default: u64) -> PgAppResult<u64> {
    optional(map, key, &default.to_string())
        .parse()
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid {key}: {err}")))
}

fn parse_i32(map: &HashMap<String, String>, key: &str, default: i32) -> PgAppResult<i32> {
    optional(map, key, &default.to_string())
        .parse()
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid {key}: {err}")))
}

fn parse_i64(map: &HashMap<String, String>, key: &str, default: i64) -> PgAppResult<i64> {
    optional(map, key, &default.to_string())
        .parse()
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid {key}: {err}")))
}

fn parse_optional_i64(map: &HashMap<String, String>, key: &str) -> PgAppResult<Option<i64>> {
    let Some(raw) = map.get(key).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let parsed: i64 = raw
        .parse()
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid {key}: {err}")))?;
    if parsed <= 0 {
        return Err(PgAppError::InvalidArgument(format!(
            "{key} must be positive when provided"
        )));
    }
    Ok(Some(parsed))
}

fn parse_usize(map: &HashMap<String, String>, key: &str, default: usize) -> PgAppResult<usize> {
    optional(map, key, &default.to_string())
        .parse()
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid {key}: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> HashMap<String, String> {
        HashMap::from([(
            "DATABASE_URL".to_string(),
            "postgres://pgapp:secret@localhost/pgapp".to_string(),
        )])
    }

    #[test]
    fn loads_defaults_with_required_database_url() {
        let cfg = ServerConfig::from_map(base()).unwrap();
        assert_eq!(cfg.bind_addr.to_string(), "127.0.0.1:50051");
        assert!(cfg.services.cache);
        assert!(cfg.services.mq);
        assert_eq!(cfg.max_connections, 20);
    }

    #[test]
    fn rejects_missing_database_url() {
        let err = ServerConfig::from_map(HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("DATABASE_URL"));
    }

    #[test]
    fn rejects_min_connections_above_max_connections() {
        let mut env = base();
        env.insert("PGAPP_MIN_CONNECTIONS".to_string(), "5".to_string());
        env.insert("PGAPP_MAX_CONNECTIONS".to_string(), "2".to_string());
        assert!(ServerConfig::from_map(env).is_err());
    }

    #[test]
    fn loads_request_and_cache_limits() {
        let mut env = base();
        env.insert("PGAPP_MAX_BATCH_SIZE".to_string(), "9".to_string());
        env.insert("PGAPP_MAX_PAYLOAD_BYTES".to_string(), "512".to_string());
        env.insert(
            "PGAPP_MAX_VISIBILITY_TIMEOUT_SECONDS".to_string(),
            "120".to_string(),
        );
        env.insert("PGAPP_CACHE_MAX_KEYS".to_string(), "1000".to_string());
        env.insert("PGAPP_CACHE_MAX_BYTES".to_string(), "4096".to_string());

        let cfg = ServerConfig::from_map(env).unwrap();

        assert_eq!(cfg.limits.max_batch_size, 9);
        assert_eq!(cfg.limits.max_payload_bytes, 512);
        assert_eq!(cfg.limits.max_visibility_timeout_seconds, 120);
        assert_eq!(cfg.cache_limits.max_keys, Some(1000));
        assert_eq!(cfg.cache_limits.max_bytes, Some(4096));
    }
}
