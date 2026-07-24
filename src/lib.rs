//! # githubdw
//!
//! A local, SQLite-based data warehouse for GitHub repositories.
//!
//! `githubdw` syncs pull requests (with reviews, comments, file diffs, and
//! check runs) and issues (with labels, milestones, and comments) from GitHub
//! into a star-schema SQLite database, then provides querying, metrics,
//! fulltext search (FTS5 trigram), and an MCP server for AI assistants.
//!
//! Data is fetched via the `gh` CLI as a subprocess, so authentication is
//! entirely delegated to `gh auth`. GitHub Enterprise works via `GH_HOST`.

pub mod error;
mod githubdw;
pub mod storage;

pub use error::{Error, Result};
pub use githubdw::GithubDW;
