//! Command-line definitions (clap).

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "githubdw",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_SHA"), ")"),
    about = "Local SQLite data warehouse for GitHub: sync PRs and issues, query, metrics, search, MCP"
)]
pub struct CommandLine {
    /// Path to the SQLite database (default: ~/.githubdw/githubdw.db)
    #[arg(long, global = true)]
    pub db: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Sync data from GitHub into the warehouse
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    /// Fulltext search over PR/issue titles, bodies, and comments
    Search(SearchArguments),
    /// Query pull requests with filters
    Query(Box<QueryArguments>),
    /// Metrics with period-over-period deltas
    Metrics {
        #[command(subcommand)]
        command: MetricsCommand,
    },
    /// Manage monitored users, repositories, and organizations
    Monitor {
        #[command(subcommand)]
        command: MonitorCommand,
    },
    /// Manage custom user and repository groups
    Group {
        #[command(subcommand)]
        command: GroupCommand,
    },
    /// Read or write configuration values
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Run the MCP (Model Context Protocol) stdio server
    Mcp,
}

#[derive(clap::Args)]
pub struct SearchArguments {
    /// Search terms (trigram fulltext; 3+ character substrings match)
    pub query: String,
    /// Search only pull requests
    #[arg(long)]
    pub pull_requests: bool,
    /// Search only issues
    #[arg(long)]
    pub issues: bool,
    /// Search only comments
    #[arg(long)]
    pub comments: bool,
    /// Restrict to one repository ("owner/name")
    #[arg(long)]
    pub repo: Option<String>,
    /// Maximum results
    #[arg(long, default_value_t = 20)]
    pub limit: u32,
    /// Emit JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args)]
pub struct QueryArguments {
    /// Filter by author login
    #[arg(long)]
    pub author: Option<String>,
    /// Filter by reviewer login
    #[arg(long)]
    pub reviewer: Option<String>,
    /// Filter by repository ("owner/name")
    #[arg(long)]
    pub repo: Option<String>,
    /// Filter by organization (repo owner)
    #[arg(long)]
    pub org: Option<String>,
    /// Filter by group (user- or repo-group)
    #[arg(long)]
    pub group: Option<String>,
    /// Filter by label name
    #[arg(long)]
    pub label: Option<String>,
    /// Filter by state (open|merged|closed)
    #[arg(long)]
    pub state: Option<String>,
    /// Shorthand for --state merged
    #[arg(long)]
    pub merged: bool,
    /// Period filter (2026-Q1, 2026-01, 2026-W02, last-30, this-quarter, ...)
    #[arg(long)]
    pub period: Option<String>,
    /// Inclusive start date (YYYY-MM-DD)
    #[arg(long)]
    pub since: Option<String>,
    /// Inclusive end date (YYYY-MM-DD)
    #[arg(long)]
    pub until: Option<String>,
    /// Maximum rows
    #[arg(long)]
    pub limit: Option<u32>,
    /// Row offset
    #[arg(long)]
    pub offset: Option<u32>,
    /// Output format
    #[arg(long, default_value = "table")]
    pub output: String,
}

#[derive(Subcommand)]
pub enum SyncCommand {
    /// Sync a single repository ("owner/name")
    Repo {
        /// Repository in "owner/name" form
        repository: String,
        /// Limit the sync window to the last N days
        #[arg(long)]
        days: Option<u32>,
        /// Skip fetching per-file patch text (fast mode)
        #[arg(long)]
        skip_diffs: bool,
        /// Sync only pull requests
        #[arg(long)]
        pull_requests_only: bool,
        /// Sync only issues
        #[arg(long)]
        issues_only: bool,
    },
    /// Sync one user's pull requests and reviews across every repository
    User {
        /// GitHub login
        login: String,
        /// Limit the sync window to the last N days
        #[arg(long)]
        days: Option<u32>,
        /// Skip fetching per-file patch text (fast mode)
        #[arg(long)]
        skip_diffs: bool,
    },
    /// Re-fetch exact pull requests and issues (1-64) through the normal
    /// hydrate/store path. Never moves cursors or coverage, never enrolls
    /// monitoring; waits for no lock (a repository mid-sync fails its targets).
    ///
    /// Example: githubdw sync targets --pr octocat/hello#1467 --issue octocat/hello#12
    Targets {
        /// Pull request: owner/name#NUMBER or https://github.com/owner/name/pull/NUMBER
        #[arg(long = "pr", value_name = "REF")]
        pull_requests: Vec<String>,
        /// Issue: owner/name#NUMBER or https://github.com/owner/name/issues/NUMBER
        #[arg(long = "issue", value_name = "REF")]
        issues: Vec<String>,
        /// Skip fetching per-file patch text (fast mode)
        #[arg(long)]
        skip_diffs: bool,
        /// Emit the per-target report as JSON
        #[arg(long)]
        json: bool,
    },
    /// List stored items whose row counts suggest first-slice truncation
    /// (read-only, no network). Prints `--pr`/`--issue` arguments for
    /// `sync targets`.
    RepairPlan {
        /// Restrict to one repository ("owner/name")
        #[arg(long)]
        repo: Option<String>,
        /// Maximum candidates (one `sync targets` run accepts 64)
        #[arg(long, default_value_t = githubdw::refresh::MAX_REFRESH_TARGETS)]
        limit: usize,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
    /// Re-read check runs on every head commit a pull request has had
    /// (stored heads, current head, force pushes, listed commits) with
    /// `filter=all`, and store each run under its own commit. Fetches
    /// everything before writing; never deletes or moves stored runs.
    /// Idempotent: re-run the same targets to resume.
    ///
    /// Example: githubdw sync check-history --pr octocat/hello#1467 --json
    CheckHistory {
        /// Pull request: owner/name#NUMBER or a pull URL (1-64)
        #[arg(long = "pr", value_name = "REF", required = true)]
        pull_requests: Vec<String>,
        /// Only stored heads plus the current head (never reported complete)
        #[arg(long)]
        observed_only: bool,
        /// Hard cap on provider requests for the whole batch (every attempt,
        /// retry and page). Reaching it stops the batch with nothing written
        /// for the current target. The 500-request quota reserve, read from
        /// response headers, always applies as well.
        #[arg(long, value_name = "N")]
        max_requests: Option<u64>,
        /// Emit the per-target report as JSON
        #[arg(long)]
        json: bool,
    },
    /// List stored pull requests with several check-run heads, the
    /// signature of partly observed history (read-only, no network). Prints
    /// `--pr` arguments for `sync check-history`.
    CheckHistoryPlan {
        /// Restrict to one repository ("owner/name")
        #[arg(long)]
        repo: Option<String>,
        /// Minimum distinct stored heads
        #[arg(long, default_value_t = 2)]
        min_heads: i64,
        /// Maximum candidates (one `sync check-history` run accepts 64)
        #[arg(long, default_value_t = githubdw::refresh::MAX_REFRESH_TARGETS)]
        limit: usize,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
    /// Sync every enabled monitored source
    All,
    /// Watch live progress of a running sync
    Watch,
}

#[derive(Subcommand)]
pub enum MetricsCommand {
    /// Metrics for a single user
    User {
        /// GitHub login
        login: String,
        /// Period (default: this-quarter)
        #[arg(long, default_value = "this-quarter")]
        period: String,
        /// Leaderboard size
        #[arg(long, default_value_t = 5)]
        top: u32,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
    /// Metrics for a single repository
    Repo {
        /// Repository in "owner/name" form
        repository: String,
        /// Period (default: this-quarter)
        #[arg(long, default_value = "this-quarter")]
        period: String,
        /// Leaderboard size
        #[arg(long, default_value_t = 5)]
        top: u32,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
    /// Metrics for a custom group
    Group {
        /// Group name
        name: String,
        /// Period (default: this-quarter)
        #[arg(long, default_value = "this-quarter")]
        period: String,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub enum GroupCommand {
    /// Manage user groups
    User {
        #[command(subcommand)]
        action: GroupAction,
    },
    /// Manage repository groups
    Repo {
        #[command(subcommand)]
        action: GroupAction,
    },
}

#[derive(Subcommand)]
pub enum GroupAction {
    /// Create a group
    Create {
        name: String,
        /// Optional description
        #[arg(long)]
        description: Option<String>,
    },
    /// Delete a group
    Delete { name: String },
    /// Show a group's members
    Show { name: String },
    /// List all groups
    List,
    /// Add members to a group
    Add {
        name: String,
        /// Members (logins or owner/name repos)
        members: Vec<String>,
    },
    /// Remove members from a group
    Remove { name: String, members: Vec<String> },
    /// Replace a group's member set
    Set { name: String, members: Vec<String> },
}

#[derive(Subcommand)]
pub enum MonitorCommand {
    /// Add a repository to the monitored set
    AddRepo {
        /// Repository in "owner/name" form
        repository: String,
    },
    /// Add a user to the monitored set
    AddUser {
        /// GitHub login
        login: String,
    },
    /// Add an organization to the monitored set
    AddOrg {
        /// Organization login
        org: String,
    },
    /// List monitored sources
    List,
    /// Remove a monitored source
    Remove {
        /// Source identifier (login or "owner/name")
        source: String,
    },
}

#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Get a configuration value
    Get {
        /// Configuration key
        key: String,
    },
    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,
        /// New value
        value: String,
    },
    /// List all configuration values
    List,
}
