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

    #[error("configuration error: {0}")]
    Config(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("sync already in progress for {0}")]
    SyncInProgress(String),
}
