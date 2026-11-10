//! Error types for githubdw.

use thiserror::Error;

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Unified error enum for all githubdw operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("migration error: {0}")]
    Migration(#[from] rusqlite_migration::Error),

    #[error("schema integrity check failed: {0}")]
    SchemaIntegrity(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("gh CLI is not available or not authenticated: {0}")]
    GhUnavailable(String),

    #[error("gh command failed (exit {exit_code:?}): {stderr}")]
    GhCommand {
        exit_code: Option<i32>,
        stderr: String,
    },

    #[error("GitHub API error: {0}")]
    GitHubApi(String),

    /// Git-backed file-set recovery failed or could not be validated.
    #[error("git file recovery: {0}")]
    GitRecovery(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("sync already in progress for {0}")]
    SyncInProgress(String),

    /// A request was refused locally because it would spend quota the
    /// caller reserved. Nothing was sent to the provider.
    #[error("quota reserve reached: {0}")]
    QuotaReserve(String),

    /// The caller cancelled the operation. Nothing partial was claimed.
    #[error("cancelled")]
    Cancelled,

    /// The provider's rate limit requires a wait longer than the caller
    /// allows. Nothing was slept; retry at `wake_at`.
    #[error("rate limited on {resource} until {wake_at}: {reason}")]
    RateLimited {
        resource: String,
        wake_at: chrono::DateTime<chrono::Utc>,
        reason: String,
    },

    /// LLM / agent failure (only present with the `summaries` feature).
    #[cfg(feature = "summaries")]
    #[error("LLM error: {0}")]
    Llm(String),
}

impl Error {
    /// True when the error ends the whole run rather than one item: the
    /// caller cancelled, the quota or rate limit is spent, `gh` is unusable,
    /// or the database itself is failing. Retrying the next item cannot
    /// succeed, so walks propagate these instead of recording an item failure.
    ///
    /// A SQLite constraint violation (including a trigger `RAISE(ABORT)`) is
    /// specific to one item's rows; its savepoint is already rolled back, so
    /// it stays an item failure.
    pub fn is_run_fatal(&self) -> bool {
        match self {
            Error::Cancelled
            | Error::RateLimited { .. }
            | Error::QuotaReserve(_)
            | Error::GhUnavailable(_)
            | Error::Migration(_)
            | Error::SchemaIntegrity(_) => true,
            Error::Database(rusqlite::Error::SqliteFailure(failure, _)) => {
                failure.code != rusqlite::ErrorCode::ConstraintViolation
            }
            _ => false,
        }
    }

    /// True when a smaller page could cure the failure: the provider timed out,
    /// answered 502/504, or reported its resource limits. Any other error
    /// would fail again at every size.
    pub fn is_page_size_symptom(&self) -> bool {
        let text = match self {
            Error::GhCommand { stderr, .. } => stderr,
            Error::GitHubApi(message) => message,
            _ => return false,
        }
        .to_lowercase();
        [
            "http 502",
            "http 504",
            "timeout",
            "resource_limits",
            "something went wrong",
        ]
        .iter()
        .any(|symptom| text.contains(symptom))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sqlite(code: rusqlite::ErrorCode, extended: i32) -> Error {
        Error::Database(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code,
                extended_code: extended,
            },
            None,
        ))
    }

    #[test]
    fn run_fatal_errors_end_the_run_and_item_errors_do_not() {
        assert!(Error::Cancelled.is_run_fatal());
        assert!(Error::QuotaReserve("x".into()).is_run_fatal());
        assert!(Error::GhUnavailable("x".into()).is_run_fatal());
        assert!(sqlite(rusqlite::ErrorCode::DatabaseBusy, 5).is_run_fatal());
        assert!(sqlite(rusqlite::ErrorCode::DiskFull, 13).is_run_fatal());
        // A trigger abort or constraint is one item's rows.
        assert!(!sqlite(rusqlite::ErrorCode::ConstraintViolation, 1811).is_run_fatal());
        assert!(!Error::GitHubApi("Could not resolve".into()).is_run_fatal());
        assert!(!Error::NotFound("x".into()).is_run_fatal());
    }

    #[test]
    fn page_size_symptoms_are_only_size_and_timeout_failures() {
        let gh = |stderr: &str| Error::GhCommand {
            exit_code: Some(1),
            stderr: stderr.into(),
        };
        assert!(gh("HTTP 502: Bad Gateway").is_page_size_symptom());
        assert!(gh("HTTP 504").is_page_size_symptom());
        assert!(gh("request timeout").is_page_size_symptom());
        assert!(Error::GitHubApi("RESOURCE_LIMITS_EXCEEDED".into()).is_page_size_symptom());
        assert!(!gh("HTTP 401: Bad credentials").is_page_size_symptom());
        assert!(!gh("HTTP 403").is_page_size_symptom());
        assert!(!Error::Cancelled.is_page_size_symptom());
    }
}
