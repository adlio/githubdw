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
