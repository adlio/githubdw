use super::*;

/// A server over an empty in-memory warehouse.
fn server() -> GithubDwServer {
    GithubDwServer::new(GithubDW::open_in_memory().unwrap())
}

/// A `--read-only` server over an empty in-memory warehouse.
fn read_only_server() -> GithubDwServer {
    GithubDwServer::new_read_only(GithubDW::open_in_memory().unwrap())
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

// ─── Tool annotations ─────────────────────────────────────────────────────

/// Hand-written expected hints per tool, independent of the production code.
fn expected_annotations(name: &str) -> ToolAnnotations {
    match name {
        "query_pull_requests" | "get_metrics" | "search" => ToolAnnotations::new().read_only(true),
        "manage_monitors" => ToolAnnotations::new()
            .read_only(false)
            .destructive(true)
            .idempotent(true),
        "trigger_sync" => ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .open_world(true),
        other => panic!("no expected annotations for {other}"),
    }
}

#[test]
fn every_tool_carries_its_expected_annotations() {
    for tool in server().tool_router.list_all() {
        let annotations = tool
            .annotations
            .clone()
            .unwrap_or_else(|| panic!("{} has no annotations", tool.name));
        assert_eq!(
            annotations,
            expected_annotations(&tool.name),
            "{} annotations",
            tool.name
        );
    }
}

// ─── Read-only mode ────────────────────────────────────────────────────────

/// Serve `server` over an in-process duplex pipe and return a line-based
/// JSON-RPC client, matching the wire-level setup already used for the
/// `trigger_sync` concurrency test below.
async fn serve_over_duplex(
    server: GithubDwServer,
) -> (
    tokio::io::WriteHalf<tokio::io::DuplexStream>,
    tokio::io::Lines<tokio::io::BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
) {
    use rmcp::ServiceExt;
    use tokio::io::AsyncBufReadExt;

    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    tokio::spawn(async move {
        let running = server.serve(server_io).await.unwrap();
        let _ = running.waiting().await;
    });
    let (reader, mut writer) = tokio::io::split(client_io);
    let mut lines = tokio::io::BufReader::new(reader).lines();

    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "test", "version": "0"}
        }}),
    )
    .await;
    assert_eq!(receive(&mut lines).await["id"], 1);
    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    )
    .await;
    (writer, lines)
}

/// A read-only server's `tools/list` omits `trigger_sync`; a default
/// server's `tools/list` carries all five, each with its annotations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_tools_list_omits_trigger_sync() {
    let (mut writer, mut lines) = serve_over_duplex(read_only_server()).await;
    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    )
    .await;
    let reply = receive(&mut lines).await;
    let names: Vec<String> = reply["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert!(!names.contains(&"trigger_sync".to_string()), "{names:?}");
    assert_eq!(names.len(), 4, "{names:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_tools_list_carries_all_five_with_annotations() {
    let (mut writer, mut lines) = serve_over_duplex(server()).await;
    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    )
    .await;
    let reply = receive(&mut lines).await;
    let tools = reply["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 5, "{tools:?}");
    for tool in tools {
        assert!(
            tool.get("annotations").is_some(),
            "{} has no annotations: {tool}",
            tool["name"]
        );
    }
}

/// A direct `trigger_sync` call on a read-only server is refused before any
/// `gh` call: no transport is installed on this server, so a sync that
/// reached `gh` would panic the spawned task instead of returning cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_only_trigger_sync_call_is_refused_without_a_gh_call() {
    let (mut writer, mut lines) = serve_over_duplex(read_only_server()).await;
    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "trigger_sync",
            "arguments": {"entity_type": "repo", "name": "octocat/hello", "days": 30}
        }}),
    )
    .await;
    let reply = receive(&mut lines).await;
    let error = reply
        .get("error")
        .unwrap_or_else(|| panic!("an error reply: {reply}"));
    assert!(
        error["message"].as_str().unwrap().contains("read-only"),
        "{error}"
    );
}

#[test]
fn read_only_manage_monitors_list_works_add_is_refused() {
    let server = read_only_server();

    let listed = text_of(server.manage_monitors(None).expect("list still works"));
    assert!(listed.contains("users"));

    let add = args(json!({ "action": "add", "entity_type": "repo", "name": "adlio/githubdw" }));
    let error = server.manage_monitors(Some(&add)).expect_err("add refused");
    assert!(error.message.contains("read-only"), "{error:?}");

    let after = text_of(server.manage_monitors(None).expect("list"));
    assert!(
        !after.contains("adlio/githubdw"),
        "monitor table unchanged: {after}"
    );
}

#[test]
fn read_only_manage_monitors_remove_is_refused() {
    let server = read_only_server();
    let remove =
        args(json!({ "action": "remove", "entity_type": "repo", "name": "adlio/githubdw" }));
    let error = server
        .manage_monitors(Some(&remove))
        .expect_err("remove refused");
    assert!(error.message.contains("read-only"), "{error:?}");
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
    assert_eq!(
        error.message,
        "unknown state 'nonsense', use: open, merged, closed"
    );
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
        let map = args(json!({ "entity_type": entity, "name": "someone", "period": "2026-Q1" }));
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

// ─── Identity rendering ──────────────────────────────────────────────────

/// A warehouse holding one PR by a person and one by a bot.
fn server_with_two_authors() -> GithubDwServer {
    let warehouse = GithubDW::open_in_memory().unwrap();
    warehouse
        .connection()
        .execute_batch(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES ('user:octocat', 'user', 'octocat', 1, 0, 'Octocat'),
                    ('bot:builder', 'bot', 'builder', 0, 1, 'builder');
             INSERT INTO dim_repositories (repo_key, owner, name)
             VALUES ('octo/alpha', 'octo', 'alpha');
             INSERT INTO dim_date (date_key, year, quarter, month, day_of_month, day_of_week,
                 is_weekend, week_of_year, week_key, month_key, quarter_key, year_key, half_key)
             VALUES ('2026-01-05', 2026, 1, 1, 5, 1, 0, 2, '2026-W02', '2026-01', '2026-Q1',
                 '2026', '2026-H1');
             INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
             VALUES ('10:00', 10, 10, 'AM', 'Morning', 1);
             INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
                 title, created_at, created_date_key, created_time_key)
             VALUES ('octo/alpha#1', 1, 'octo/alpha', 'user:octocat', 'OPEN',
                     'A hand-written change', '2026-01-05T18:00:00Z', '2026-01-05', '10:00'),
                    ('octo/alpha#2', 2, 'octo/alpha', 'bot:builder', 'OPEN',
                     'An automated change', '2026-01-05T19:00:00Z', '2026-01-05', '10:00');",
        )
        .unwrap();
    GithubDwServer::new(warehouse)
}

/// Every identity a client reads is the login GitHub uses. The namespaced
/// key stays available, but only in a field that says so by name — a client
/// reading `author` or `login` must get a value it can pass straight to the
/// GitHub API.
#[test]
fn tool_payloads_name_identities_the_way_github_does() {
    let server = server_with_two_authors();

    let listing: serde_json::Value = serde_json::from_str(&text_of(
        server
            .query_pull_requests(Some(&args(json!({}))))
            .expect("ok"),
    ))
    .expect("json");
    let rows = listing["pull_requests"].as_array().expect("rows");
    assert_eq!(rows.len(), 2);
    for row in rows {
        let author = row["author"].as_str().expect("an author");
        assert!(!author.contains(':'), "author '{author}' is bare");
        let entity_type = row["author_type"].as_str().expect("a type");
        assert_eq!(
            row["author_key"].as_str().expect("a key"),
            format!("{entity_type}:{author}"),
            "the key remains available under its own name"
        );
    }

    let grouped: serde_json::Value = serde_json::from_str(&text_of(
        server
            .query_pull_requests(Some(&args(json!({ "output": "count_by_author" }))))
            .expect("ok"),
    ))
    .expect("json");
    for group in grouped["results"].as_array().expect("groups") {
        let label = group[0].as_str().expect("a label");
        assert!(!label.contains(':'), "group label '{label}' is bare");
    }
    let labels: Vec<&str> = grouped["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|group| group[0].as_str().unwrap())
        .collect();
    assert!(labels.contains(&"builder [bot]"), "got {labels:?}");

    let hits: serde_json::Value = serde_json::from_str(&text_of(
        server
            .search(Some(&args(json!({ "query": "change" }))))
            .expect("ok"),
    ))
    .expect("json");
    let results = hits["results"].as_array().expect("hits");
    assert!(!results.is_empty());
    for hit in results {
        if let Some(author) = hit["author"].as_str() {
            assert!(!author.contains(':'), "hit author '{author}' is bare");
            assert!(hit["author_type"].is_string(), "the type travels with it");
        }
    }

    let report: serde_json::Value = serde_json::from_str(&text_of(
        server
            .get_metrics(Some(&args(json!({
                "entity_type": "user",
                "name": "builder",
                "period": "2026-Q1",
            }))))
            .expect("ok"),
    ))
    .expect("json");
    assert_eq!(report["metrics"]["login"].as_str(), Some("builder"));
    assert_eq!(report["metrics"]["entity_type"].as_str(), Some("bot"));
    assert_eq!(
        report["metrics"]["entity_key"].as_str(),
        Some("bot:builder"),
        "the key remains available under its own name"
    );
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
    let error = server().prepare_sync(Some(&map)).err().expect("error");
    assert!(error.message.contains("only 'repo'"), "{error:?}");
}

/// Fake `gh`: `auth status` succeeds at once; the first data call announces
/// itself on `started`, then waits for `release`. Pull-request calls are
/// served from `pages` in order; issue calls get an empty page.
struct GatedTransport {
    started: std::sync::mpsc::Sender<()>,
    release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    pages: Mutex<Vec<String>>,
}

impl crate::fetch::GhTransport for GatedTransport {
    fn run(&self, arguments: &[String]) -> crate::Result<String> {
        if arguments.first().map(String::as_str) == Some("auth") {
            return Ok(String::new());
        }
        if let Some(release) = self.release.lock().unwrap().take() {
            let _ = self.started.send(());
            let _ = release.recv();
        }
        if arguments.iter().any(|a| a.contains("issues(")) {
            return Ok(empty_issue_page());
        }
        let mut pages = self.pages.lock().unwrap();
        if pages.is_empty() {
            return Err(crate::Error::GitHubApi("no more pages".into()));
        }
        Ok(pages.remove(0))
    }
}

fn empty_issue_page() -> String {
    json!({"data": {
        "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
        "repository": {
            "nameWithOwner": "octocat/hello",
            "issues": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []}
        }
    }})
    .to_string()
}

/// A pull-request page; `remaining` is the GraphQL quota GitHub reports.
fn pr_page(has_next: bool, cursor: Option<&str>, numbers: &[i64], remaining: i64) -> String {
    let nodes: Vec<_> = numbers
        .iter()
        .map(|n| {
            json!({
                "number": n, "title": format!("PR {n}"), "body": "", "state": "MERGED",
                "isDraft": false, "createdAt": "2026-01-05T18:00:00Z",
                "updatedAt": format!("2026-01-{:02}T09:00:00Z", 5 + n),
                "mergedAt": "2026-01-06T09:00:00Z", "closedAt": "2026-01-06T09:00:00Z",
                "baseRefName": "main", "headRefName": format!("f/{n}"),
                "additions": 1, "deletions": 1, "changedFiles": 0,
                "author": {"login": "octocat", "__typename": "User"},
                "mergedBy": null,
                "reviews": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
                "reviewThreads": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
                "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
                "files": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
                "commits": {"nodes": []}
            })
        })
        .collect();
    json!({"data": {
        "rateLimit": {"limit": 5000, "cost": 1, "remaining": remaining, "resetAt": "2099-01-01T00:00:00Z"},
        "repository": {
            "nameWithOwner": "octocat/hello", "primaryLanguage": null, "isFork": false,
            "isPrivate": false, "defaultBranchRef": {"name": "main"},
            "createdAt": "2020-01-01T00:00:00Z",
            "pullRequests": {"pageInfo": {"hasNextPage": has_next, "endCursor": cursor}, "nodes": nodes}
        }
    }})
    .to_string()
}

/// A server over a file-backed warehouse whose syncs use a gated fake `gh`.
fn gated_server(
    dir: &tempfile::TempDir,
    pages: Vec<String>,
) -> (
    GithubDwServer,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let transport = Arc::new(GatedTransport {
        started: started_tx,
        release: Mutex::new(Some(release_rx)),
        pages: Mutex::new(pages),
    });
    struct Shared(Arc<GatedTransport>);
    impl crate::fetch::GhTransport for Shared {
        fn run(&self, arguments: &[String]) -> crate::Result<String> {
            self.0.run(arguments)
        }
    }
    let mut server = GithubDwServer::new(GithubDW::open(dir.path().join("w.db")).unwrap());
    server.client_factory = Arc::new(move || {
        crate::fetch::GhClient::with_transport(Box::new(Shared(Arc::clone(&transport))))
            .without_sleeping()
    });
    (server, started_rx, release_tx)
}

async fn call_sync(server: &GithubDwServer) -> Result<CallToolResult, McpError> {
    let map = args(json!({ "entity_type": "repo", "name": "octocat/hello", "days": 30 }));
    server.prepare_sync(Some(&map))?.run().await
}

/// Send one JSON-RPC message as a line.
async fn send(writer: &mut (impl tokio::io::AsyncWrite + Unpin), message: serde_json::Value) {
    use tokio::io::AsyncWriteExt;
    writer
        .write_all(format!("{message}\n").as_bytes())
        .await
        .unwrap();
}

/// Read the next JSON-RPC message, failing after five seconds.
async fn receive(
    lines: &mut tokio::io::Lines<impl tokio::io::AsyncBufRead + Unpin>,
) -> serde_json::Value {
    let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("a reply within 5s")
        .unwrap()
        .expect("an open stream");
    serde_json::from_str(&line).unwrap()
}

/// Over the MCP wire: while a sync waits on a slow `gh`, a query sent after
/// it is answered first, and the sync then finishes and stores its data.
/// One worker thread: a sync that blocked a worker would starve the query.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_query_is_answered_while_a_sync_is_blocked_on_gh() {
    use rmcp::ServiceExt;
    use tokio::io::AsyncBufReadExt;

    let dir = tempfile::tempdir().unwrap();
    let (server, started, release) = gated_server(&dir, vec![pr_page(false, None, &[1], 4999)]);
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    tokio::spawn(async move {
        let running = server.serve(server_io).await.unwrap();
        let _ = running.waiting().await;
    });
    let (reader, mut writer) = tokio::io::split(client_io);
    let mut lines = tokio::io::BufReader::new(reader).lines();

    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "test", "version": "0"}
        }}),
    )
    .await;
    assert_eq!(receive(&mut lines).await["id"], 1);
    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    )
    .await;

    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "trigger_sync",
            "arguments": {"entity_type": "repo", "name": "octocat/hello", "days": 30}
        }}),
    )
    .await;
    // Wait until the sync is parked inside the fake transport.
    tokio::task::spawn_blocking(move || {
        started
            .recv_timeout(Duration::from_secs(10))
            .expect("sync started");
    })
    .await
    .unwrap();

    send(
        &mut writer,
        json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "query_pull_requests", "arguments": {"output": "count"}
        }}),
    )
    .await;
    let query = receive(&mut lines).await;
    assert_eq!(
        query["id"], 3,
        "the query is answered before the sync: {query}"
    );
    let count: serde_json::Value =
        serde_json::from_str(query["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(count["count"], 0, "nothing stored yet: {count}");

    release.send(()).unwrap();
    let sync = receive(&mut lines).await;
    assert_eq!(sync["id"], 2);
    let body: serde_json::Value =
        serde_json::from_str(sync["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(body["status"], "Completed", "{body}");
    assert_eq!(body["partial"], false);
    assert_eq!(body["pull_requests_synced"], 1);
}

/// A name that is not owner/name is the caller's mistake, not ours.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_repository_name_is_an_invalid_params_error() {
    let dir = tempfile::tempdir().unwrap();
    let (server, _started, _release) = gated_server(&dir, vec![]);
    let map = args(json!({ "entity_type": "repo", "name": "no-slash" }));
    let error = server
        .prepare_sync(Some(&map))
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        rmcp::model::ErrorCode::INVALID_PARAMS,
        "{error:?}"
    );
    assert!(error.message.contains("owner/name"), "{error:?}");
}

/// Two pull requests against a budget of 2 with more pages behind them: the
/// run stops, says so, and reports what it stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sync_over_budget_reports_partial() {
    let dir = tempfile::tempdir().unwrap();
    let (mut server, _started, release) =
        gated_server(&dir, vec![pr_page(true, Some("C1"), &[10, 9], 4999)]);
    server.sync_item_budget = 2;
    release.send(()).unwrap();
    let body: serde_json::Value =
        serde_json::from_str(&text_of(call_sync(&server).await.unwrap())).unwrap();
    assert_eq!(body["partial"], true);
    assert_eq!(body["status"], "Partial");
    assert_eq!(body["pull_requests_synced"], 2);
}

/// GitHub reports the quota spent with more pages to go: the next request
/// would wait until 2099, so the call returns that time instead of sleeping.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_rate_limit_wait_is_reported_not_slept() {
    let dir = tempfile::tempdir().unwrap();
    let (server, _started, release) = gated_server(&dir, vec![pr_page(true, Some("C1"), &[1], 0)]);
    release.send(()).unwrap();
    let body: serde_json::Value =
        serde_json::from_str(&text_of(call_sync(&server).await.unwrap())).unwrap();
    assert_eq!(body["status"], "RateLimited", "{body}");
    let until: chrono::DateTime<chrono::Utc> = body["rate_limited_until"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let reset: chrono::DateTime<chrono::Utc> = "2099-01-01T00:00:00Z".parse().unwrap();
    assert!((until - reset).num_seconds().abs() < 60, "{until}");
    assert!(
        body["message"].as_str().unwrap().ends_with(", call again"),
        "{body}"
    );
}
