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
    let error = server().trigger_sync(Some(&map)).expect_err("error");
    assert!(error.message.contains("only 'repo'"), "{error:?}");
}
