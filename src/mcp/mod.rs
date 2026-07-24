//! MCP server (stdio): exposes the warehouse to AI assistants via rmcp.

pub mod params;

use std::sync::{Arc, Mutex};

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, ServerCapabilities, ServerInfo};
use rmcp::{ErrorData as McpError, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use serde_json::json;

use crate::GithubDW;
use crate::groups::{self, GroupKind};
use crate::metrics::MetricsEngine;
use crate::period::Period;
use crate::query::QueryBuilder;
use crate::search::{SearchOptions, SearchScope};
use crate::storage::monitor_repository;
use params::*;

/// The MCP server. The warehouse sits behind a Mutex: stdio MCP dispatches
/// one call at a time, so there is never contention — the lock simply
/// satisfies Send/Sync bounds without unsafe code.
#[derive(Clone)]
pub struct GithubDwServer {
    warehouse: Arc<Mutex<GithubDW>>,
    #[expect(
        dead_code,
        reason = "the tool_handler macro accesses this router field"
    )]
    tool_router: ToolRouter<Self>,
}

fn internal_error(error: impl std::fmt::Display) -> McpError {
    McpError::internal_error(error.to_string(), None)
}

fn invalid_params(message: impl Into<String>) -> McpError {
    McpError::invalid_params(message.into(), None)
}

/// Pretty-print a JSON value into a single text content block.
fn json_result(value: serde_json::Value) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(&value).map_err(internal_error)?;
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

#[tool_router]
impl GithubDwServer {
    pub fn new(warehouse: GithubDW) -> Self {
        Self {
            warehouse: Arc::new(Mutex::new(warehouse)),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Query pull requests from the local warehouse. Filter by author, \
        reviewer, repo, org, user/repo group, state (open/merged/closed), label, and \
        period. Use output='count' for totals or 'count_by_author'/'count_by_repo'/\
        'count_by_state'/'count_by_period' for breakdowns."
    )]
    async fn query_pull_requests(
        &self,
        Parameters(params): Parameters<QueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let warehouse = self.warehouse.lock().map_err(internal_error)?;
        let connection = warehouse.connection();
        let mut builder = QueryBuilder::new(connection);
        if let Some(author) = params.author.as_deref() {
            builder = builder.author(author);
        }
        if let Some(reviewer) = params.reviewer.as_deref() {
            builder = builder.reviewer(reviewer);
        }
        if let Some(repo) = params.repo.as_deref() {
            builder = builder.repo(repo);
        }
        if let Some(org) = params.org.as_deref() {
            builder = builder.org(org);
        }
        if let Some(group_name) = params.group.as_deref() {
            match groups::kind_of(connection, group_name)
                .map_err(|e| invalid_params(e.to_string()))?
            {
                GroupKind::User => {
                    let members = groups::members(connection, GroupKind::User, group_name)
                        .map_err(internal_error)?;
                    builder = builder.authors(&members);
                }
                GroupKind::Repo => {
                    let members = groups::members(connection, GroupKind::Repo, group_name)
                        .map_err(internal_error)?;
                    builder = builder.repos(&members);
                }
            }
        }
        if let Some(label) = params.label.as_deref() {
            builder = builder.label(label);
        }
        if let Some(state) = params.state.as_deref() {
            let parsed = match state.to_lowercase().as_str() {
                "open" => crate::PrState::Open,
                "merged" => crate::PrState::Merged,
                "closed" => crate::PrState::Closed,
                other => {
                    return Err(invalid_params(format!(
                        "unknown state '{other}', use: open, merged, closed"
                    )));
                }
            };
            builder = builder.state(parsed);
        }
        if let Some(period_text) = params.period.as_deref() {
            let parsed =
                Period::parse(period_text).map_err(|error| invalid_params(error.to_string()))?;
            match parsed {
                Period::Rolling(..) => {
                    let (start, end) = parsed.date_range();
                    builder = builder.between(start, end);
                }
                other => builder = builder.period(other),
            }
        }

        let output = params.output.as_deref().unwrap_or("pull_requests");
        let response = match output {
            "count" => json!({ "count": builder.count().map_err(internal_error)? }),
            "count_by_author" => {
                json!({ "results": builder.count_by_author().map_err(internal_error)? })
            }
            "count_by_repo" => {
                json!({ "results": builder.count_by_repo().map_err(internal_error)? })
            }
            "count_by_state" => {
                json!({ "results": builder.count_by_state().map_err(internal_error)? })
            }
            "count_by_period" => {
                json!({ "results": builder.count_by_period().map_err(internal_error)? })
            }
            "pull_requests" => {
                builder = builder.limit(params.limit.unwrap_or(50));
                if let Some(offset) = params.offset {
                    builder = builder.offset(offset);
                }
                let rows = builder.pull_requests().map_err(internal_error)?;
                json!({ "_meta": { "result_count": rows.len() }, "pull_requests": rows })
            }
            other => {
                return Err(invalid_params(format!(
                    "unknown output '{other}', use: pull_requests, count, count_by_author, \
                     count_by_repo, count_by_state, count_by_period"
                )));
            }
        };
        json_result(response)
    }

    #[tool(
        description = "Period-over-period metrics for a user, repo, user_group, or \
        repo_group. Returns PR counts, reviews, comments, and churn with deltas vs. \
        the previous period, plus ranked leaderboards."
    )]
    async fn get_metrics(
        &self,
        Parameters(params): Parameters<MetricsParams>,
    ) -> Result<CallToolResult, McpError> {
        let warehouse = self.warehouse.lock().map_err(internal_error)?;
        let connection = warehouse.connection();
        let period =
            Period::parse(&params.period).map_err(|error| invalid_params(error.to_string()))?;
        let engine = MetricsEngine::new(connection);
        let response = match params.entity_type.as_str() {
            "user" => {
                let metrics = engine
                    .user_metrics(&params.name, &period)
                    .map_err(internal_error)?;
                let aggregations = engine
                    .user_aggregations(&params.name, &period, 10)
                    .map_err(internal_error)?;
                json!({ "metrics": metrics, "aggregations": aggregations })
            }
            "repo" => {
                let metrics = engine
                    .repo_metrics(&params.name, &period)
                    .map_err(internal_error)?;
                let aggregations = engine
                    .repo_aggregations(&params.name, &period, 10)
                    .map_err(internal_error)?;
                json!({ "metrics": metrics, "aggregations": aggregations })
            }
            "user_group" => {
                let members = groups::members(connection, GroupKind::User, &params.name)
                    .map_err(|error| invalid_params(error.to_string()))?;
                let metrics = engine
                    .user_group_metrics(&params.name, &members, &period)
                    .map_err(internal_error)?;
                json!({ "metrics": metrics })
            }
            "repo_group" => {
                let members = groups::members(connection, GroupKind::Repo, &params.name)
                    .map_err(|error| invalid_params(error.to_string()))?;
                let metrics = engine
                    .repo_group_metrics(&params.name, &members, &period)
                    .map_err(internal_error)?;
                json!({ "metrics": metrics })
            }
            other => {
                return Err(invalid_params(format!(
                    "unknown entity_type '{other}', use: user, repo, user_group, repo_group"
                )));
            }
        };
        json_result(response)
    }

    #[tool(
        description = "Full-text search across synced pull request and issue titles, \
        bodies, and comments (SQLite FTS5 trigram — substrings of 3+ characters match)."
    )]
    async fn search(
        &self,
        Parameters(params): Parameters<SearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let warehouse = self.warehouse.lock().map_err(internal_error)?;
        let scope = match params.scope.as_deref().unwrap_or("all") {
            "all" => SearchScope::All,
            "pull_requests" => SearchScope::PullRequests,
            "issues" => SearchScope::Issues,
            "comments" => SearchScope::Comments,
            other => {
                return Err(invalid_params(format!(
                    "unknown scope '{other}', use: all, pull_requests, issues, comments"
                )));
            }
        };
        let options = SearchOptions {
            scope,
            repository: params.repository.map(|value| value.to_lowercase()),
            limit: params.limit.unwrap_or(50),
        };
        let hits = crate::search::search(warehouse.connection(), &params.query, &options)
            .map_err(internal_error)?;
        json_result(json!({ "_meta": { "result_count": hits.len() }, "results": hits }))
    }

    #[tool(
        description = "List, add, or remove monitored users, repos, and orgs. \
        action='list' (default) shows everything currently tracked."
    )]
    async fn manage_monitors(
        &self,
        Parameters(params): Parameters<MonitorParams>,
    ) -> Result<CallToolResult, McpError> {
        let warehouse = self.warehouse.lock().map_err(internal_error)?;
        let connection = warehouse.connection();
        let action = params.action.as_deref().unwrap_or("list");
        match action {
            "list" => {
                let sources = monitor_repository::list(connection).map_err(internal_error)?;
                let users: Vec<_> = sources
                    .iter()
                    .filter(|s| s.source_type == "user")
                    .map(|s| s.identifier.clone())
                    .collect();
                let repos: Vec<_> = sources
                    .iter()
                    .filter(|s| s.source_type == "repo")
                    .map(|s| s.identifier.clone())
                    .collect();
                let orgs: Vec<_> = sources
                    .iter()
                    .filter(|s| s.source_type == "org")
                    .map(|s| s.identifier.clone())
                    .collect();
                json_result(json!({ "users": users, "repos": repos, "orgs": orgs }))
            }
            "add" | "remove" => {
                let entity_type = params
                    .entity_type
                    .as_deref()
                    .ok_or_else(|| invalid_params("entity_type is required for add/remove"))?;
                let name = params
                    .name
                    .as_deref()
                    .ok_or_else(|| invalid_params("name is required for add/remove"))?;
                if action == "add" {
                    match entity_type {
                        "user" => monitor_repository::add_user(connection, name),
                        "repo" => monitor_repository::add_repo(connection, name),
                        "org" => monitor_repository::add_org(connection, name),
                        other => {
                            return Err(invalid_params(format!(
                                "unknown entity_type '{other}', use: user, repo, org"
                            )));
                        }
                    }
                    .map_err(|error| invalid_params(error.to_string()))?;
                } else {
                    let removed =
                        monitor_repository::remove(connection, name).map_err(internal_error)?;
                    if removed == 0 {
                        return Err(invalid_params(format!("'{name}' is not monitored")));
                    }
                }
                json_result(json!({ "ok": true }))
            }
            other => Err(invalid_params(format!(
                "unknown action '{other}', use: list, add, remove"
            ))),
        }
    }

    #[tool(
        description = "Fetch fresh pull-request and issue data for a repository from \
        the GitHub API into the local database. Specify days to bound the window. \
        Requires an authenticated gh CLI."
    )]
    async fn trigger_sync(
        &self,
        Parameters(params): Parameters<SyncParams>,
    ) -> Result<CallToolResult, McpError> {
        if params.entity_type != "repo" {
            return Err(invalid_params(format!(
                "unknown entity_type '{}', only 'repo' is supported",
                params.entity_type
            )));
        }
        let warehouse = self.warehouse.lock().map_err(internal_error)?;
        let mut client = crate::fetch::GhClient::new();
        client.preflight().map_err(internal_error)?;
        let options = crate::sync::SyncOptions {
            days: Some(params.days.unwrap_or(30)),
            ..Default::default()
        };
        let mut syncer = crate::sync::Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer
            .sync_repository(&params.name, &options)
            .map_err(internal_error)?;
        json_result(json!({
            "status": "Completed",
            "pull_requests_synced": summary.pull_requests_synced,
            "issues_synced": summary.issues_synced,
            "skipped": summary.skipped,
            "failed": summary
                .failed
                .iter()
                .map(|(item, error)| json!({ "item": item, "error": error }))
                .collect::<Vec<_>>(),
            "up_to_date": summary.up_to_date,
        }))
    }
}

#[tool_handler]
impl ServerHandler for GithubDwServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "githubdw is a local SQLite warehouse of GitHub pull requests, reviews, \
                 comments, and issues. Use query_pull_requests for filtered listings and \
                 counts, get_metrics for period-over-period analytics and leaderboards, \
                 search for full-text lookup, manage_monitors to control what is tracked, \
                 and trigger_sync to refresh data from GitHub.",
        )
    }
}

/// Run the stdio MCP server until the client disconnects.
pub async fn serve_stdio(warehouse: GithubDW) -> crate::Result<()> {
    let server = GithubDwServer::new(warehouse);
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|error| crate::Error::Config(format!("MCP serve failed: {error}")))?;
    service
        .waiting()
        .await
        .map_err(|error| crate::Error::Config(format!("MCP wait failed: {error}")))?;
    Ok(())
}
