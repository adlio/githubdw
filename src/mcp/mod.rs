//! MCP server (stdio): exposes the warehouse to AI assistants via rmcp.
//!
//! Tool registration is written out by hand rather than using rmcp's
//! `#[tool]` / `#[tool_router]` / `#[tool_handler]` macros. Those macros only
//! exist in rmcp 2.x, and this crate pins rmcp 0.12 so that it stays resolvable
//! from vendored/mirrored registries that lag crates.io. The tool names,
//! descriptions, and JSON contract are unchanged.

pub mod params;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rmcp::handler::server::router::tool::{ToolRoute, ToolRouter};
use rmcp::handler::server::tool::ToolCallContext;
use rmcp::model::{
    CallToolRequestParam, CallToolResult, Content, Implementation, ListToolsResult,
    PaginatedRequestParam, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};
use serde_json::{Map, json};

use crate::GithubDW;
use crate::groups::{self, GroupKind};
use crate::metrics::MetricsEngine;
use crate::period::Period;
use crate::query::{PrFilters, PrState};
use crate::search::{SearchOptions, SearchScope};
use crate::storage::monitor_repository;
use crate::storage::time_dimension;
use params::*;

/// Pull requests one `trigger_sync` call may process before it yields, as in
/// the CLI's `sync all`. The position is checkpointed; calling again resumes.
const SYNC_ITEM_BUDGET: i64 = 500;

/// A rate-limit wait longer than this is reported to the caller instead of
/// slept through, so a tool call never parks for minutes.
const SYNC_MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(30);

/// The MCP server. Quick tools take the warehouse Mutex; `trigger_sync` never
/// does: it runs on a blocking thread with its own connection to the same
/// database file, so other calls, pings and cancellation stay responsive.
#[derive(Clone)]
pub struct GithubDwServer {
    warehouse: Arc<Mutex<GithubDW>>,
    database_path: std::path::PathBuf,
    /// Builds the GitHub client for each sync. Replaceable in tests.
    client_factory: Arc<dyn Fn() -> crate::fetch::GhClient + Send + Sync>,
    sync_item_budget: i64,
    tool_router: ToolRouter<Self>,
}

fn internal_error(error: impl std::fmt::Display) -> McpError {
    McpError::internal_error(error.to_string(), None)
}

fn invalid_params(message: impl Into<String>) -> McpError {
    McpError::invalid_params(message.into(), None)
}

/// A bad argument or unknown name is the caller's to fix; anything else is ours.
fn caller_error(error: crate::Error) -> McpError {
    match error {
        crate::Error::InvalidArgument(_) | crate::Error::NotFound(_) => {
            invalid_params(error.to_string())
        }
        other => internal_error(other),
    }
}

/// Pretty-print a JSON value into a single text content block.
fn json_result(value: serde_json::Value) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(&value).map_err(internal_error)?;
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

/// Build a `Tool` descriptor with a schemars-derived input schema.
fn tool<T: schemars::JsonSchema + 'static>(name: &'static str, description: &'static str) -> Tool {
    Tool {
        name: name.into(),
        title: None,
        description: Some(description.into()),
        input_schema: rmcp::handler::server::common::schema_for_type::<T>(),
        output_schema: None,
        annotations: None,
        icons: None,
        meta: None,
    }
}

/// Deserialize a tool call's arguments into its parameter struct.
///
/// `arguments` is optional in the MCP spec, so an absent map must behave like
/// `{}` — defaulting to `Value::Null` instead would reject calls to tools whose
/// parameters are all optional (e.g. `manage_monitors` with no action).
fn parse_params<T: serde::de::DeserializeOwned>(
    args: Option<&Map<String, serde_json::Value>>,
) -> Result<T, McpError> {
    let value = serde_json::Value::Object(args.cloned().unwrap_or_default());
    serde_json::from_value(value).map_err(|e| invalid_params(format!("invalid params: {e}")))
}

/// Shorthand for the boxed, `Send` future the tool router expects. The quick
/// tools are synchronous warehouse work, so their result is computed eagerly
/// and wrapped — no lock guard is ever held across an await point.
/// `trigger_sync` is the exception: it awaits a blocking thread.
type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<CallToolResult, McpError>> + Send + 'a>>;

fn ready(result: Result<CallToolResult, McpError>) -> ToolFuture<'static> {
    Box::pin(std::future::ready(result))
}

impl GithubDwServer {
    pub fn new(warehouse: GithubDW) -> Self {
        Self {
            database_path: warehouse.database_path().to_path_buf(),
            warehouse: Arc::new(Mutex::new(warehouse)),
            client_factory: Arc::new(crate::fetch::GhClient::new),
            sync_item_budget: SYNC_ITEM_BUDGET,
            tool_router: Self::build_tool_router(),
        }
    }

    fn build_tool_router() -> ToolRouter<Self> {
        let mut router = ToolRouter::<Self>::new();
        router.add_route(ToolRoute::<Self>::new_dyn(
            tool::<QueryParams>(
                "query_pull_requests",
                "Query pull requests from the local warehouse. Filter by author, \
                 reviewer, repo, org, user/repo group, state (open/merged/closed), label, and \
                 period. Use output='count' for totals or 'count_by_author'/'count_by_repo'/\
                 'count_by_state'/'count_by_period' for breakdowns.",
            ),
            |ctx: ToolCallContext<'_, Self>| {
                ready(ctx.service.query_pull_requests(ctx.arguments.as_ref()))
            },
        ));
        router.add_route(ToolRoute::<Self>::new_dyn(
            tool::<MetricsParams>(
                "get_metrics",
                "Period-over-period metrics for a user, repo, user_group, or \
                 repo_group. Returns PR counts, reviews, comments, and churn with deltas vs. \
                 the previous period, plus ranked leaderboards.",
            ),
            |ctx: ToolCallContext<'_, Self>| ready(ctx.service.get_metrics(ctx.arguments.as_ref())),
        ));
        router.add_route(ToolRoute::<Self>::new_dyn(
            tool::<SearchParams>(
                "search",
                "Full-text search across synced pull request and issue titles, \
                 bodies, and comments (SQLite FTS5 trigram — substrings of 3+ characters match).",
            ),
            |ctx: ToolCallContext<'_, Self>| ready(ctx.service.search(ctx.arguments.as_ref())),
        ));
        router.add_route(ToolRoute::<Self>::new_dyn(
            tool::<MonitorParams>(
                "manage_monitors",
                "List, add, or remove monitored users, repos, and orgs. \
                 action='list' (default) shows everything currently tracked.",
            ),
            |ctx: ToolCallContext<'_, Self>| {
                ready(ctx.service.manage_monitors(ctx.arguments.as_ref()))
            },
        ));
        router.add_route(ToolRoute::<Self>::new_dyn(
            tool::<SyncParams>(
                "trigger_sync",
                "Fetch fresh pull-request and issue data for a repository from \
                 the GitHub API into the local database. Specify days to bound the window. \
                 Requires an authenticated gh CLI.",
            ),
            |ctx: ToolCallContext<'_, Self>| {
                let sync = ctx.service.prepare_sync(ctx.arguments.as_ref());
                Box::pin(async move { sync?.run().await })
            },
        ));
        router
    }

    // ─── Tool implementations ────────────────────────────────────────────────

    fn query_pull_requests(
        &self,
        args: Option<&Map<String, serde_json::Value>>,
    ) -> Result<CallToolResult, McpError> {
        let params: QueryParams = parse_params(args)?;
        let state = params
            .state
            .as_deref()
            .map(PrState::parse)
            .transpose()
            .map_err(|error| match error {
                // The tool has always answered with the bare message here.
                crate::Error::InvalidArgument(message) => invalid_params(message),
                other => caller_error(other),
            })?;
        let warehouse = self.warehouse.lock().map_err(internal_error)?;
        let filters = PrFilters {
            author: params.author,
            reviewer: params.reviewer,
            repo: params.repo,
            org: params.org,
            group: params.group,
            label: params.label,
            state,
            period: params.period,
            ..PrFilters::default()
        };
        let mut builder = filters
            .builder(warehouse.connection())
            .map_err(caller_error)?;

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

    fn get_metrics(
        &self,
        args: Option<&Map<String, serde_json::Value>>,
    ) -> Result<CallToolResult, McpError> {
        let params: MetricsParams = parse_params(args)?;
        let warehouse = self.warehouse.lock().map_err(internal_error)?;
        let connection = warehouse.connection();
        let reference = time_dimension::today(connection).map_err(internal_error)?;
        let period = Period::parse_with_reference(&params.period, reference)
            .map_err(|error| invalid_params(error.to_string()))?;
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

    fn search(
        &self,
        args: Option<&Map<String, serde_json::Value>>,
    ) -> Result<CallToolResult, McpError> {
        let params: SearchParams = parse_params(args)?;
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

    fn manage_monitors(
        &self,
        args: Option<&Map<String, serde_json::Value>>,
    ) -> Result<CallToolResult, McpError> {
        let params: MonitorParams = parse_params(args)?;
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

    /// Validate a `trigger_sync` call and capture what the blocking thread
    /// needs, so the returned job borrows nothing from the request.
    fn prepare_sync(
        &self,
        args: Option<&Map<String, serde_json::Value>>,
    ) -> Result<SyncJob, McpError> {
        let params: SyncParams = parse_params(args)?;
        if params.entity_type != "repo" {
            return Err(invalid_params(format!(
                "unknown entity_type '{}', only 'repo' is supported",
                params.entity_type
            )));
        }
        Ok(SyncJob {
            params,
            database_path: self.database_path.clone(),
            client_factory: Arc::clone(&self.client_factory),
            item_budget: self.sync_item_budget,
        })
    }
}

/// One validated `trigger_sync` call.
struct SyncJob {
    params: SyncParams,
    database_path: std::path::PathBuf,
    client_factory: Arc<dyn Fn() -> crate::fetch::GhClient + Send + Sync>,
    item_budget: i64,
}

impl SyncJob {
    async fn run(self) -> Result<CallToolResult, McpError> {
        tokio::task::spawn_blocking(move || self.run_blocking())
            .await
            .map_err(internal_error)?
    }

    fn run_blocking(self) -> Result<CallToolResult, McpError> {
        // shortcut: an in-memory warehouse has no file to share, so a sync
        // there would write to a private empty database; the server only runs
        // on a file, and tests that sync use one.
        let warehouse = GithubDW::open(&self.database_path).map_err(internal_error)?;
        let mut client = (self.client_factory)();
        client.preflight().map_err(internal_error)?;
        client.set_defer_waits_over(Some(SYNC_MAX_RATE_LIMIT_WAIT));
        let options = crate::sync::SyncOptions {
            days: Some(self.params.days.unwrap_or(30)),
            ..Default::default()
        };
        let mut syncer = crate::sync::Syncer::new(warehouse.connection(), &mut client)
            .with_item_budget(self.item_budget);
        let summary = match syncer.sync_repository(&self.params.name, &options) {
            Ok(summary) => summary,
            Err(crate::Error::RateLimited { wake_at, .. }) => {
                let until = wake_at.to_rfc3339();
                return json_result(json!({
                    "status": "RateLimited",
                    "rate_limited_until": until,
                    "message": format!("rate limited until {until}, call again"),
                }));
            }
            Err(error) => return Err(caller_error(error)),
        };
        json_result(json!({
            "status": if summary.partial { "Partial" } else { "Completed" },
            "partial": summary.partial,
            "pull_requests_synced": summary.pull_requests_synced,
            "issues_synced": summary.issues_synced,
            "skipped": summary.skipped,
            "failed": summary
                .failed
                .iter()
                .map(|(item, error)| json!({ "item": item, "error": error }))
                .collect::<Vec<_>>(),
            "notes": summary.notes,
            "up_to_date": summary.up_to_date,
        }))
    }
}

impl ServerHandler for GithubDwServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: Default::default(),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "githubdw is a local SQLite warehouse of GitHub pull requests, reviews, \
                 comments, and issues. Use query_pull_requests for filtered listings and \
                 counts, get_metrics for period-over-period analytics and leaderboards, \
                 search for full-text lookup, manage_monitors to control what is tracked, \
                 and trigger_sync to refresh data from GitHub."
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult {
            tools: self.tool_router.list_all(),
            next_cursor: None,
            meta: None,
        }))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParam,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let ctx = ToolCallContext::new(self, request, context);
        self.tool_router.call(ctx)
    }
}

/// Run the stdio MCP server until the client disconnects.
pub async fn serve_stdio(warehouse: GithubDW) -> crate::Result<()> {
    use rmcp::ServiceExt;

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

#[cfg(test)]
mod tests;
