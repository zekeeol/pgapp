use thiserror::Error;
use tonic::{Code, Status};

pub type PgAppResult<T> = Result<T, PgAppError>;

#[derive(Debug, Error)]
pub enum PgAppError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("database unavailable: {0}")]
    DatabaseUnavailable(String),
    #[error("database error: {0}")]
    Database(String),
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl PgAppError {
    pub fn code(&self) -> Code {
        match self {
            Self::InvalidArgument(_) => Code::InvalidArgument,
            Self::NotFound(_) => Code::NotFound,
            Self::Conflict(_) => Code::AlreadyExists,
            Self::Timeout(_) => Code::DeadlineExceeded,
            Self::DatabaseUnavailable(_) | Self::ServiceUnavailable(_) => Code::Unavailable,
            Self::Database(_) => Code::Internal,
        }
    }
}

impl From<PgAppError> for Status {
    fn from(value: PgAppError) -> Self {
        Status::new(value.code(), value.to_string())
    }
}

impl From<sqlx::Error> for PgAppError {
    fn from(value: sqlx::Error) -> Self {
        match value {
            sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => {
                PgAppError::DatabaseUnavailable(value.to_string())
            }
            sqlx::Error::RowNotFound => PgAppError::NotFound("row not found".to_string()),
            other => PgAppError::Database(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_validation_to_invalid_argument() {
        let status: Status = PgAppError::InvalidArgument("bad key".to_string()).into();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("bad key"));
    }

    #[test]
    fn maps_database_outage_to_unavailable() {
        let status: Status = PgAppError::DatabaseUnavailable("down".to_string()).into();
        assert_eq!(status.code(), Code::Unavailable);
    }
}
