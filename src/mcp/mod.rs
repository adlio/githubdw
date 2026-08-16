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
use crate::query::QueryBuilder;
use crate::search::{SearchOptions, SearchScope};
use crate::storage::monitor_repository;
use crate::storage::time_dimension;
use params::*;

/// The MCP server. The warehouse sits behind a Mutex: stdio MCP dispatches
/// one call at a time, so there is never contention — the lock simply
/// satisfies Send/Sync bounds without unsafe code.
#[derive(Clone)]
pub struct GithubDwServer {
    warehouse: Arc<Mutex<GithubDW>>,
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

/// Shorthand for the boxed, `Send` future the tool router expects. Every tool
/// body here is synchronous warehouse work, so the result is computed eagerly
/// and wrapped — no lock guard is ever held across an await point.
type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<CallToolResult, McpError>> + Send + 'a>>;

fn ready(result: Result<CallToolResult, McpError>) -> ToolFuture<'static> {
    Box::pin(std::future::ready(result))
}

impl GithubDwServer {
    pub fn new(warehouse: GithubDW) -> Self {
        Self {
            warehouse: Arc::new(Mutex::new(warehouse)),
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
                ready(ctx.service.trigger_sync(ctx.arguments.as_ref()))
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
            let reference = time_dimension::today(connection).map_err(internal_error)?;
            let parsed = Period::parse_with_reference(period_text, reference)
                .map_err(|error| invalid_params(error.to_string()))?;
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

    fn trigger_sync(
        &self,
        args: Option<&Map<String, serde_json::Value>>,
    ) -> Result<CallToolResult, McpError> {
        let params: SyncParams = parse_params(args)?;
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
mod tests {
    use super::*;

    /// A server over an empty in-memory warehouse.
    fn server() -> GithubDwServer {
        GithubDwServer::new(GithubDW::open_in_memory().unwrap())
    }

    /// Build a tool-arguments map from a JSON object literal.
    fn args(value: serde_json::Value) -> Map<String, serde_json::Value> {
        match value {
            serde_json::Value::Object(map) => map,
            other => panic!("expected a JSON object, got {other}"),
        }
    }

    /// Extract the text of a successful single-block tool result.
    fn text_of(result: CallToolResult) -> String {
        let block = result.content.first().expect("one content block").clone();
        block.as_text().expect("a text block").text.clone()
    }

    // ─── Router wiring ───────────────────────────────────────────────────────

    #[test]
    fn router_registers_every_tool_with_a_schema() {
        let tools = server().tool_router.list_all();
        let mut names: Vec<_> = tools.iter().map(|t| t.name.to_string()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "get_metrics",
                "manage_monitors",
                "query_pull_requests",
                "search",
                "trigger_sync",
            ]
        );
        for tool in &tools {
            assert!(
                tool.description.as_ref().is_some_and(|d| !d.is_empty()),
                "{} is missing a description",
                tool.name
            );
            assert!(
                tool.input_schema.contains_key("properties"),
                "{} is missing schema properties",
                tool.name
            );
        }
    }

    #[test]
    fn server_info_advertises_tools_and_instructions() {
        let info = server().get_info();
        assert!(info.capabilities.tools.is_some(), "tools not advertised");
        let instructions = info.instructions.expect("instructions");
        assert!(instructions.contains("githubdw"));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    #[test]
    fn parse_params_defaults_when_arguments_are_absent() {
        let params: QueryParams = parse_params(None).expect("defaults");
        assert!(params.author.is_none());
        assert!(params.output.is_none());
    }

    #[test]
    fn parse_params_reads_supplied_fields() {
        let map = args(json!({ "author": "alice", "output": "count" }));
        let params: QueryParams = parse_params(Some(&map)).expect("parsed");
        assert_eq!(params.author.as_deref(), Some("alice"));
        assert_eq!(params.output.as_deref(), Some("count"));
    }

    #[test]
    fn parse_params_rejects_wrong_types() {
        let map = args(json!({ "author": 42 }));
        let parsed: Result<QueryParams, _> = parse_params(Some(&map));
        assert!(parsed.is_err(), "expected a type error");
    }

    #[test]
    fn json_result_pretty_prints_into_a_text_block() {
        let result = json_result(json!({ "count": 7 })).expect("result");
        let text = text_of(result);
        assert!(text.contains("\"count\""));
        assert!(text.contains('\n'), "expected pretty-printed JSON");
    }

    // ─── query_pull_requests ─────────────────────────────────────────────────

    #[test]
    fn query_counts_zero_on_an_empty_warehouse() {
        let map = args(json!({ "output": "count" }));
        let text = text_of(server().query_pull_requests(Some(&map)).expect("ok"));
        assert!(text.contains("\"count\": 0"), "got {text}");
    }

    #[test]
    fn query_returns_an_empty_list_with_meta() {
        let text = text_of(server().query_pull_requests(None).expect("ok"));
        assert!(text.contains("\"result_count\": 0"), "got {text}");
        assert!(text.contains("pull_requests"));
    }

    #[test]
    fn query_supports_every_breakdown_output() {
        for output in [
            "count_by_author",
            "count_by_repo",
            "count_by_state",
            "count_by_period",
        ] {
            let map = args(json!({ "output": output }));
            let text = text_of(
                server()
                    .query_pull_requests(Some(&map))
                    .unwrap_or_else(|e| panic!("{output} failed: {e}")),
            );
            assert!(text.contains("results"), "{output} -> {text}");
        }
    }

    #[test]
    fn query_rejects_an_unknown_output() {
        let map = args(json!({ "output": "nonsense" }));
        let error = server().query_pull_requests(Some(&map)).expect_err("error");
        assert!(error.message.contains("unknown output"), "{error:?}");
    }

    #[test]
    fn query_rejects_an_unknown_state() {
        let map = args(json!({ "state": "nonsense" }));
        let error = server().query_pull_requests(Some(&map)).expect_err("error");
        assert!(error.message.contains("unknown state"), "{error:?}");
    }

    #[test]
    fn query_accepts_the_documented_states() {
        for state in ["open", "merged", "closed", "MERGED"] {
            let map = args(json!({ "state": state, "output": "count" }));
            assert!(
                server().query_pull_requests(Some(&map)).is_ok(),
                "state {state} rejected"
            );
        }
    }

    #[test]
    fn query_rejects_an_unparseable_period() {
        let map = args(json!({ "period": "not-a-period" }));
        assert!(server().query_pull_requests(Some(&map)).is_err());
    }

    #[test]
    fn query_accepts_calendar_and_rolling_periods() {
        for period in ["2026-Q1", "last-30"] {
            let map = args(json!({ "period": period, "output": "count" }));
            assert!(
                server().query_pull_requests(Some(&map)).is_ok(),
                "period {period} rejected"
            );
        }
    }

    #[test]
    fn query_rejects_an_unknown_group() {
        let map = args(json!({ "group": "no-such-group" }));
        assert!(server().query_pull_requests(Some(&map)).is_err());
    }

    // ─── get_metrics ─────────────────────────────────────────────────────────

    #[test]
    fn metrics_returns_user_and_repo_shapes() {
        for entity in ["user", "repo"] {
            let map =
                args(json!({ "entity_type": entity, "name": "someone", "period": "2026-Q1" }));
            let text = text_of(
                server()
                    .get_metrics(Some(&map))
                    .unwrap_or_else(|e| panic!("{entity} failed: {e}")),
            );
            assert!(text.contains("metrics"), "{entity} -> {text}");
            assert!(text.contains("aggregations"), "{entity} -> {text}");
        }
    }

    #[test]
    fn metrics_rejects_an_unknown_entity_type() {
        let map = args(json!({ "entity_type": "planet", "name": "x", "period": "2026-Q1" }));
        let error = server().get_metrics(Some(&map)).expect_err("error");
        assert!(error.message.contains("unknown entity_type"), "{error:?}");
    }

    #[test]
    fn metrics_rejects_an_invalid_period() {
        let map = args(json!({ "entity_type": "user", "name": "x", "period": "nope" }));
        assert!(server().get_metrics(Some(&map)).is_err());
    }

    #[test]
    fn metrics_rejects_an_unknown_group() {
        for entity in ["user_group", "repo_group"] {
            let map = args(json!({ "entity_type": entity, "name": "ghost", "period": "2026-Q1" }));
            assert!(
                server().get_metrics(Some(&map)).is_err(),
                "{entity} should reject a missing group"
            );
        }
    }

    // ─── search ──────────────────────────────────────────────────────────────

    #[test]
    fn search_returns_no_hits_on_an_empty_warehouse() {
        let map = args(json!({ "query": "anything" }));
        let text = text_of(server().search(Some(&map)).expect("ok"));
        assert!(text.contains("\"result_count\": 0"), "got {text}");
    }

    #[test]
    fn search_accepts_every_scope() {
        for scope in ["all", "pull_requests", "issues", "comments"] {
            let map = args(json!({ "query": "abc", "scope": scope }));
            assert!(
                server().search(Some(&map)).is_ok(),
                "scope {scope} rejected"
            );
        }
    }

    #[test]
    fn search_rejects_an_unknown_scope() {
        let map = args(json!({ "query": "abc", "scope": "everywhere" }));
        let error = server().search(Some(&map)).expect_err("error");
        assert!(error.message.contains("unknown scope"), "{error:?}");
    }

    // ─── manage_monitors ─────────────────────────────────────────────────────

    #[test]
    fn monitors_list_is_empty_by_default() {
        let text = text_of(server().manage_monitors(None).expect("ok"));
        for key in ["users", "repos", "orgs"] {
            assert!(text.contains(key), "missing {key} in {text}");
        }
    }

    #[test]
    fn monitors_add_then_list_then_remove_round_trips() {
        let server = server();

        let add = args(json!({ "action": "add", "entity_type": "repo", "name": "adlio/githubdw" }));
        assert!(server.manage_monitors(Some(&add)).is_ok());

        let listed = text_of(server.manage_monitors(None).expect("list"));
        assert!(listed.contains("adlio/githubdw"), "got {listed}");

        let remove =
            args(json!({ "action": "remove", "entity_type": "repo", "name": "adlio/githubdw" }));
        assert!(server.manage_monitors(Some(&remove)).is_ok());

        let after = text_of(server.manage_monitors(None).expect("list"));
        assert!(!after.contains("adlio/githubdw"), "got {after}");
    }

    #[test]
    fn monitors_add_requires_entity_type_and_name() {
        let server = server();
        let missing_type = args(json!({ "action": "add", "name": "x" }));
        assert!(server.manage_monitors(Some(&missing_type)).is_err());
        let missing_name = args(json!({ "action": "add", "entity_type": "user" }));
        assert!(server.manage_monitors(Some(&missing_name)).is_err());
    }

    #[test]
    fn monitors_reject_unknown_entity_type_and_action() {
        let server = server();
        let bad_type = args(json!({ "action": "add", "entity_type": "planet", "name": "x" }));
        let error = server.manage_monitors(Some(&bad_type)).expect_err("error");
        assert!(error.message.contains("unknown entity_type"), "{error:?}");

        let bad_action = args(json!({ "action": "explode" }));
        let error = server
            .manage_monitors(Some(&bad_action))
            .expect_err("error");
        assert!(error.message.contains("unknown action"), "{error:?}");
    }

    #[test]
    fn monitors_removing_an_untracked_name_errors() {
        let map = args(json!({ "action": "remove", "entity_type": "repo", "name": "no/such" }));
        let error = server().manage_monitors(Some(&map)).expect_err("error");
        assert!(error.message.contains("not monitored"), "{error:?}");
    }

    // ─── trigger_sync ────────────────────────────────────────────────────────

    #[test]
    fn sync_rejects_a_non_repo_entity_type() {
        // Guard runs before any network/gh access, so this stays hermetic.
        let map = args(json!({ "entity_type": "user", "name": "alice" }));
        let error = server().trigger_sync(Some(&map)).expect_err("error");
        assert!(error.message.contains("only 'repo'"), "{error:?}");
    }
}
