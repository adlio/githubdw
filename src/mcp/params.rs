//! Parameter structs for the MCP tools (schemars-derived schemas).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parameters for `query_pull_requests`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct QueryParams {
    /// Filter by PR author login (e.g. "alice").
    #[serde(default)]
    pub author: Option<String>,
    /// Filter by reviewer login.
    #[serde(default)]
    pub reviewer: Option<String>,
    /// Filter by repository ("owner/name").
    #[serde(default)]
    pub repo: Option<String>,
    /// Filter by repository owner/org.
    #[serde(default)]
    pub org: Option<String>,
    /// Filter by user-group or repo-group name (expands to members).
    #[serde(default)]
    pub group: Option<String>,
    /// Filter by label name.
    #[serde(default)]
    pub label: Option<String>,
    /// PR state: "open", "merged", or "closed".
    #[serde(default)]
    pub state: Option<String>,
    /// Period filter (e.g. "2026-Q1", "2026-01", "2025-H2", "last-30").
    #[serde(default)]
    pub period: Option<String>,
    /// Max rows to return. Default: 50.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Rows to skip (pagination).
    #[serde(default)]
    pub offset: Option<u32>,
    /// Output: "pull_requests" (default), "count", "count_by_author",
    /// "count_by_repo", "count_by_state", "count_by_period".
    #[serde(default)]
    pub output: Option<String>,
}

/// Parameters for `get_metrics`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct MetricsParams {
    /// Entity type: "user", "repo", "user_group", or "repo_group".
    pub entity_type: String,
    /// Entity name (login, "owner/name", or group name).
    pub name: String,
    /// Period, e.g. "2026-Q1", "2026-01". Required.
    pub period: String,
}

/// Parameters for `search`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SearchParams {
    /// Full-text query over PR/issue titles, bodies, and comments.
    pub query: String,
    /// Scope: "all" (default), "pull_requests", "issues", or "comments".
    #[serde(default)]
    pub scope: Option<String>,
    /// Restrict to one repository ("owner/name").
    #[serde(default)]
    pub repository: Option<String>,
    /// Max rows to return. Default: 50.
    #[serde(default)]
    pub limit: Option<u32>,
}

/// Parameters for `manage_monitors`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct MonitorParams {
    /// "list" (default), "add", or "remove".
    #[serde(default)]
    pub action: Option<String>,
    /// "user", "repo", or "org". Required for add/remove.
    #[serde(default)]
    pub entity_type: Option<String>,
    /// login or "owner/name". Required for add/remove.
    #[serde(default)]
    pub name: Option<String>,
}

/// Parameters for `trigger_sync`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SyncParams {
    /// "repo" (currently the only supported entity type).
    pub entity_type: String,
    /// Repository in "owner/name" form.
    pub name: String,
    /// Days to sync. Default: 30.
    #[serde(default)]
    pub days: Option<u32>,
}
