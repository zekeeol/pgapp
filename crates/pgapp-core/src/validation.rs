use crate::{PgAppError, PgAppResult};
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_IDENTIFIER_LEN: usize = 128;
const MAX_CACHE_KEY_LEN: usize = 1024;

pub fn validate_namespace(namespace: &str) -> PgAppResult<()> {
    validate_identifier("namespace", namespace)
}

pub fn validate_queue_name(queue_name: &str) -> PgAppResult<()> {
    validate_identifier("queue_name", queue_name)
}

pub fn validate_cache_key(key: &str) -> PgAppResult<()> {
    if key.is_empty() {
        return Err(PgAppError::InvalidArgument(
            "cache key must not be empty".to_string(),
        ));
    }
    if key.len() > MAX_CACHE_KEY_LEN {
        return Err(PgAppError::InvalidArgument(format!(
            "cache key exceeds {MAX_CACHE_KEY_LEN} bytes"
        )));
    }
    Ok(())
}

pub fn validate_quantity(quantity: i32, max: i32) -> PgAppResult<i32> {
    if quantity <= 0 {
        return Err(PgAppError::InvalidArgument(
            "quantity must be greater than zero".to_string(),
        ));
    }
    if quantity > max {
        return Err(PgAppError::InvalidArgument(format!(
            "quantity must be less than or equal to {max}"
        )));
    }
    Ok(quantity)
}

pub fn validate_non_negative_seconds(field: &str, seconds: i64) -> PgAppResult<i64> {
    if seconds < 0 {
        return Err(PgAppError::InvalidArgument(format!(
            "{field} must not be negative"
        )));
    }
    Ok(seconds)
}

pub fn parse_json_payload(payload: &str) -> PgAppResult<Value> {
    serde_json::from_str(payload)
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid JSON payload: {err}")))
}

pub fn hash_key(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

fn validate_identifier(field: &str, value: &str) -> PgAppResult<()> {
    if value.is_empty() {
        return Err(PgAppError::InvalidArgument(format!(
            "{field} must not be empty"
        )));
    }
    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(PgAppError::InvalidArgument(format!(
            "{field} exceeds {MAX_IDENTIFIER_LEN} bytes"
        )));
    }

    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(PgAppError::InvalidArgument(format!(
            "{field} must not be empty"
        )));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(PgAppError::InvalidArgument(format!(
            "{field} must start with an ASCII letter or underscore"
        )));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-') {
        return Err(PgAppError::InvalidArgument(format!(
            "{field} may only contain ASCII letters, digits, underscore, or dash"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_safe_names_and_keys() {
        validate_namespace("tenant_a").unwrap();
        validate_queue_name("orders-1").unwrap();
        validate_cache_key("user:123").unwrap();
    }

    #[test]
    fn rejects_unsafe_names() {
        assert!(validate_queue_name("1bad").is_err());
        assert!(validate_queue_name("orders;drop").is_err());
        assert!(validate_namespace("").is_err());
    }

    #[test]
    fn hashes_keys_stably() {
        assert_eq!(hash_key("abc"), hash_key("abc"));
        assert_ne!(hash_key("abc"), hash_key("abcd"));
        assert_eq!(hash_key("abc").len(), 64);
    }

    #[test]
    fn parses_json_payloads() {
        assert_eq!(parse_json_payload(r#"{"a":1}"#).unwrap()["a"], 1);
        assert!(parse_json_payload("{").is_err());
    }
}
