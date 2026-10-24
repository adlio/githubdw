//! Fixtures and transports shared by the sync tests.

use super::*;

pub(super) fn pr_node(number: i64, created_at: &str, updated_at: &str) -> serde_json::Value {
    json!({
        "number": number,
        "title": format!("PR {number}"),
        "body": "body text",
        "state": "MERGED",
        "isDraft": false,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "mergedAt": "2026-01-06T09:00:00Z",
        "closedAt": "2026-01-06T09:00:00Z",
        "baseRefName": "main",
        "headRefName": format!("feature/{number}"),
        "additions": 10,
        "deletions": 2,
        "changedFiles": 1,
        "author": {"login": "octocat", "__typename": "User"},
        "mergedBy": {"login": "hubot", "__typename": "User"},
        "reviews": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
            "id": format!("REV{number}"), "state": "APPROVED", "body": "",
            "submittedAt": "2026-01-06T08:00:00Z",
            "author": {"login": "hubot", "__typename": "User"}
        }]},
        "reviewThreads": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
        "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
        "files": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
            "path": "src/lib.rs", "changeType": "MODIFIED",
            "additions": 10, "deletions": 2
        }]},
        "commits": {"nodes": []}
    })
}

pub(super) fn page_of(
    has_next: bool,
    cursor: Option<&str>,
    nodes: Vec<serde_json::Value>,
) -> String {
    json!({
        "data": {
            "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
            "repository": {
                "nameWithOwner": "octocat/hello",
                "primaryLanguage": {"name": "Rust"},
                "isFork": false,
                "isPrivate": false,
                "defaultBranchRef": {"name": "main"},
                "createdAt": "2020-01-01T00:00:00Z",
                "pullRequests": {
                    "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                    "nodes": nodes
                }
            }
        }
    })
    .to_string()
}

pub(super) fn page_response(has_next: bool, cursor: Option<&str>, numbers: &[i64]) -> String {
    let nodes = numbers
        .iter()
        .map(|number| {
            pr_node(
                *number,
                "2026-01-05T18:00:00Z",
                &format!("2026-01-{:02}T09:00:00Z", 5 + number),
            )
        })
        .collect();
    page_of(has_next, cursor, nodes)
}

pub(super) fn fast_pr_options() -> SyncOptions {
    SyncOptions {
        skip_diffs: true,
        pull_requests_only: true,
        ..Default::default()
    }
}

pub(super) fn los_angeles_warehouse() -> GithubDW {
    let warehouse = GithubDW::open_in_memory().unwrap();
    warehouse
        .connection()
        .execute(
            "INSERT INTO config (key, value) VALUES ('timezone', 'America/Los_Angeles')
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [],
        )
        .unwrap();
    warehouse
}

// ---- Nested connection continuation --------------------------------

/// Answers page queries from a queue and `node(id:)` continuation queries
/// by (connection field, cursor), so tests do not depend on the order the
/// hydrator walks pending connections.
pub(super) struct RoutingTransport {
    pub(super) pages: std::sync::Mutex<Vec<String>>,
    pub(super) continuations: Vec<(&'static str, &'static str, String)>,
    pub(super) continuation_calls: std::sync::Mutex<Vec<(String, String)>>,
}

impl crate::fetch::GhTransport for RoutingTransport {
    fn run(&self, arguments: &[String]) -> Result<String> {
        let query = arguments
            .iter()
            .find_map(|argument| argument.strip_prefix("query="))
            .unwrap_or_default();
        if !query.contains("node(id: $id)") {
            let mut pages = self.pages.lock().unwrap();
            if pages.is_empty() {
                return Err(crate::Error::GitHubApi("no more pages".into()));
            }
            return Ok(pages.remove(0));
        }
        let cursor = arguments
            .iter()
            .find_map(|argument| argument.strip_prefix("cursor="))
            .unwrap_or_default()
            .to_string();
        let field = query
            .split("... on ")
            .nth(1)
            .and_then(|rest| rest.split('{').nth(1))
            .map(|rest| {
                rest.split('(')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string()
            })
            .unwrap_or_default();
        self.continuation_calls
            .lock()
            .unwrap()
            .push((field.clone(), cursor.clone()));
        self.continuations
            .iter()
            .find(|(f, c, _)| *f == field && *c == cursor)
            .map(|(_, _, body)| Ok(body.clone()))
            .unwrap_or_else(|| {
                Err(crate::Error::GitHubApi(format!(
                    "unexpected continuation {field} after {cursor}"
                )))
            })
    }
}

pub(super) fn routing_client(
    pages: Vec<String>,
    continuations: Vec<(&'static str, &'static str, String)>,
) -> (GhClient, std::sync::Arc<RoutingTransport>) {
    let transport = std::sync::Arc::new(RoutingTransport {
        pages: std::sync::Mutex::new(pages),
        continuations,
        continuation_calls: std::sync::Mutex::new(Vec::new()),
    });
    struct Shared(std::sync::Arc<RoutingTransport>);
    impl crate::fetch::GhTransport for Shared {
        fn run(&self, arguments: &[String]) -> Result<String> {
            self.0.run(arguments)
        }
    }
    let client = GhClient::with_transport(Box::new(Shared(transport.clone()))).without_sleeping();
    (client, transport)
}

pub(super) fn connection_page(
    field: &str,
    has_next: bool,
    cursor: Option<&str>,
    nodes: Vec<serde_json::Value>,
) -> String {
    json!({"data": {"node": {field: {
        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
        "nodes": nodes
    }}}})
    .to_string()
}

pub(super) fn conversation_comment(id: &str) -> serde_json::Value {
    json!({"id": id, "body": format!("comment {id}"), "createdAt": "2026-01-05T19:00:00Z",
           "author": {"login": "hubot", "__typename": "User"}})
}

pub(super) fn review_comment(id: &str) -> serde_json::Value {
    json!({"id": id, "body": "nit", "path": "src/lib.rs", "line": 3,
           "createdAt": "2026-01-05T20:00:00Z",
           "author": {"login": "hubot", "__typename": "User"}, "replyTo": null})
}

pub(super) fn file(path: &str) -> serde_json::Value {
    json!({"path": path, "changeType": "MODIFIED", "additions": 1, "deletions": 0})
}

/// A PR whose comments, files, review threads, and one thread's comments
/// all report a second page.
pub(super) fn heavy_pr_node(number: i64) -> serde_json::Value {
    let mut node = pr_node(number, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["id"] = json!(format!("PR_{number}"));
    node["changedFiles"] = json!(3);
    node["comments"] = json!({
        "pageInfo": {"hasNextPage": true, "endCursor": "COM1"},
        "nodes": [conversation_comment("IC1")]
    });
    node["files"] = json!({
        "pageInfo": {"hasNextPage": true, "endCursor": "FILE1"},
        "nodes": [file("a.rs")]
    });
    node["reviewThreads"] = json!({
        "pageInfo": {"hasNextPage": true, "endCursor": "THR1"},
        "nodes": [{
            "id": "THREAD_A",
            "comments": {
                "pageInfo": {"hasNextPage": true, "endCursor": "TC1"},
                "nodes": [review_comment("RCA1")]
            }
        }]
    });
    node
}

pub(super) fn heavy_pr_continuations() -> Vec<(&'static str, &'static str, String)> {
    vec![
        (
            "comments",
            "COM1",
            connection_page(
                "comments",
                true,
                Some("COM2"),
                vec![conversation_comment("IC2")],
            ),
        ),
        (
            "comments",
            "COM2",
            connection_page("comments", false, None, vec![conversation_comment("IC3")]),
        ),
        (
            "files",
            "FILE1",
            connection_page("files", false, None, vec![file("b.rs"), file("c.rs")]),
        ),
        (
            "reviewThreads",
            "THR1",
            connection_page(
                "reviewThreads",
                false,
                None,
                vec![json!({
                    "id": "THREAD_B",
                    "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null},
                                 "nodes": [review_comment("RCB1")]}
                })],
            ),
        ),
        (
            "comments",
            "TC1",
            connection_page("comments", false, None, vec![review_comment("RCA2")]),
        ),
    ]
}

pub(super) fn assert_heavy_pr_complete(conn: &Connection, pr_key: &str) {
    let conversation: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fact_issue_comments WHERE parent_key = ?1",
            [pr_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        conversation, 3,
        "page-two and page-three conversation comments land"
    );
    let files: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fact_file_diffs WHERE pr_key = ?1",
            [pr_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(files, 3, "page-two files land");
    let review_comments = count(conn, "SELECT COUNT(*) FROM fact_review_comments");
    assert_eq!(
        review_comments, 3,
        "thread page two and a nested thread-comment continuation land"
    );
}

// ---- Resumable repository walks -------------------------------------

/// A transport the test keeps a handle to, so it can assert which page
/// cursor each run actually requested.
pub(super) struct SharedTransport(pub(super) std::sync::Arc<FixtureTransport>);

impl crate::fetch::GhTransport for SharedTransport {
    fn run(&self, arguments: &[String]) -> Result<String> {
        self.0.run(arguments)
    }
}

pub(super) fn shared_client(
    responses: Vec<Result<String>>,
) -> (GhClient, std::sync::Arc<FixtureTransport>) {
    let transport = std::sync::Arc::new(FixtureTransport::new(responses));
    let client =
        GhClient::with_transport(Box::new(SharedTransport(transport.clone()))).without_sleeping();
    (client, transport)
}

pub(super) fn requested_cursors(transport: &FixtureTransport) -> Vec<Option<String>> {
    transport
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|call| {
            call.iter()
                .find_map(|argument| argument.strip_prefix("cursor=").map(str::to_string))
        })
        .collect()
}

pub(super) fn saved_checkpoint(conn: &Connection) -> Option<PullRequestWalkCheckpoint> {
    sync_state::load_checkpoint(conn, REPO_PR_CHECKPOINT, "octocat/hello")
        .unwrap()
        .map(|text| serde_json::from_str(&text).unwrap())
}

pub(super) fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

pub(super) fn pr_cursor(conn: &Connection) -> Option<String> {
    sync_state::last_updated_cursor(conn, "repo", "octocat/hello").unwrap()
}

pub(super) fn issue_page_response(has_next: bool, cursor: Option<&str>, numbers: &[i64]) -> String {
    let nodes: Vec<_> = numbers
        .iter()
        .map(|number| {
            json!({
                "id": format!("ISS{number}"),
                "number": number,
                "title": format!("Issue {number}"),
                "body": "issue body",
                "state": "OPEN",
                "stateReason": null,
                "createdAt": "2026-02-01T12:00:00Z",
                "updatedAt": format!("2026-02-{:02}T12:00:00Z", number),
                "closedAt": null,
                "author": {"login": "octocat", "__typename": "User"},
                "milestone": {
                    "id": "MILE1", "number": 1, "title": "v1.0",
                    "description": null, "state": "OPEN",
                    "dueOn": null, "createdAt": "2026-01-01T00:00:00Z"
                },
                "labels": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                    "id": "LAB1", "name": "bug", "color": "d73a4a", "description": ""
                }]},
                "assignees": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{"login": "hubot", "__typename": "User"}]},
                "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                    "id": format!("ICOM{number}"), "body": "on it",
                    "createdAt": "2026-02-02T08:00:00Z",
                    "author": {"login": "hubot", "__typename": "User"}
                }]}
            })
        })
        .collect();
    json!({
        "data": {
            "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
            "repository": {
                "nameWithOwner": "octocat/hello",
                "issues": {
                    "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                    "nodes": nodes
                }
            }
        }
    })
    .to_string()
}

// ---------------------------------------------------------------- user sync

/// One search-result node in the shape the user query selects: the PR node,
/// plus the repository it lives in.
pub(super) fn search_node(
    number: i64,
    repository: &str,
    created_at: &str,
    updated_at: &str,
) -> serde_json::Value {
    let mut node = pr_node(number, created_at, updated_at);
    node["files"] = json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []});
    node["changedFiles"] = json!(0);
    node["repository"] = json!({
        "nameWithOwner": repository,
        "primaryLanguage": {"name": "Rust"},
        "isFork": false,
        "isPrivate": false,
        "defaultBranchRef": {"name": "main"},
        "createdAt": "2020-01-01T00:00:00Z"
    });
    node
}

pub(super) fn search_response(total: i64, nodes: Vec<serde_json::Value>) -> String {
    json!({
        "data": {
            "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
            "search": {
                "issueCount": total,
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": nodes
            }
        }
    })
    .to_string()
}

pub(super) fn fast_user_options(days: Option<u32>) -> SyncOptions {
    SyncOptions {
        days,
        skip_diffs: true,
        pull_requests_only: true,
        issues_only: false,
    }
}

/// 2026-08-17T03:00:00Z — evening of the 16th in Los Angeles, so the last
/// complete local day is the 15th.
pub(super) fn evening_instant() -> DateTime<Utc> {
    chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 17, 3, 0, 0).unwrap()
}

// ------------------------------------------------------------ windowing

// ----------------------------------------------------------- sync all plan

pub(super) fn source(source_type: &str, identifier: &str, enabled: bool) -> MonitoredSource {
    MonitoredSource {
        source_type: source_type.into(),
        identifier: identifier.into(),
        last_sync_at: None,
        sync_enabled: enabled,
    }
}

// ----- targeted refresh -----

pub(super) fn single_pr_response(node: serde_json::Value) -> String {
    json!({"data": {
        "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
        "repository": {
            "nameWithOwner": "octocat/hello", "primaryLanguage": {"name": "Rust"},
            "isFork": false, "isPrivate": false, "defaultBranchRef": {"name": "main"},
            "createdAt": "2020-01-01T00:00:00Z",
            "pullRequest": node
        }
    }})
    .to_string()
}

pub(super) fn target(kind: TargetKind, reference: &str) -> RefreshTarget {
    RefreshTarget::parse(kind, reference).unwrap()
}

/// Every row of every sync-state and monitoring table, so a test can prove
/// a refresh left all of them unchanged.
pub(super) fn state_snapshot(conn: &Connection) -> Vec<String> {
    let mut rows = Vec::new();
    for table in [
        "sync_metadata",
        "sync_locks",
        "sync_jobs",
        "synced_ranges",
        "monitored_users",
        "monitored_repos",
        "monitored_orgs",
    ] {
        let mut statement = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
        let columns = statement.column_count();
        let mut query = statement.query([]).unwrap();
        while let Some(row) = query.next().unwrap() {
            let values: Vec<String> = (0..columns)
                .map(|index| format!("{:?}", row.get_ref(index).unwrap()))
                .collect();
            rows.push(format!("{table}: {}", values.join("|")));
        }
    }
    rows
}

/// An old, already-synced PR that the incremental walk will never revisit.
pub(super) fn warehouse_with_shipped_pr() -> GithubDW {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let mut node = pr_node(5, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["id"] = json!("PR_5");
    node["comments"] = json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [conversation_comment("IC1")]});
    node["files"] =
        json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [file("a.rs")]});
    let (mut client, _) = routing_client(vec![page_of(false, None, vec![node])], vec![]);
    let summary = Syncer::new(warehouse.connection(), &mut client)
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert!(summary.failed.is_empty());
    warehouse
}

pub(super) fn title_of(conn: &Connection, pr_key: &str) -> String {
    conn.query_row(
        "SELECT title FROM fact_pull_requests WHERE pr_key = ?1",
        [pr_key],
        |row| row.get(0),
    )
    .unwrap()
}

// ----- stale file-set correction -----

/// A PR node listing exactly `paths` in one complete files page.
pub(super) fn pr_with_files(
    number: i64,
    updated_at: &str,
    paths: &[&str],
    changed: Option<i64>,
) -> serde_json::Value {
    let mut node = pr_node(number, "2026-01-05T18:00:00Z", updated_at);
    node["id"] = json!(format!("PR_{number}"));
    match changed {
        Some(total) => node["changedFiles"] = json!(total),
        None => {
            node.as_object_mut().unwrap().remove("changedFiles");
        }
    }
    let files: Vec<_> = paths.iter().map(|path| file(path)).collect();
    node["files"] = json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": files});
    node
}

pub(super) fn rest_patches(paths: &[&str], patch: &str) -> String {
    serde_json::Value::Array(
        paths
            .iter()
            .map(|path| json!({"filename": path, "patch": patch}))
            .collect(),
    )
    .to_string()
}

pub(super) fn stored_paths(conn: &Connection, pr_key: &str) -> Vec<String> {
    let mut statement = conn
        .prepare("SELECT file_path FROM fact_file_diffs WHERE pr_key = ?1 ORDER BY file_path")
        .unwrap();
    statement
        .query_map([pr_key], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<String>>>()
        .unwrap()
}

/// PR 5 stores a.rs, b.rs, c.rs; unrelated PR 6 stores z.rs. Both through
/// the ordinary repository walk.
pub(super) fn warehouse_with_abc_and_unrelated() -> GithubDW {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let (mut client, _) = routing_client(
        vec![page_of(
            false,
            None,
            vec![
                pr_with_files(6, "2026-01-07T09:00:00Z", &["z.rs"], Some(1)),
                pr_with_files(
                    5,
                    "2026-01-06T09:00:00Z",
                    &["a.rs", "b.rs", "c.rs"],
                    Some(3),
                ),
            ],
        )],
        vec![],
    );
    let summary = Syncer::new(warehouse.connection(), &mut client)
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    let conn = warehouse.connection();
    assert_eq!(
        stored_paths(conn, "octocat/hello#5"),
        ["a.rs", "b.rs", "c.rs"]
    );
    warehouse
}

pub(super) fn refresh_pr5(
    conn: &Connection,
    pages: Vec<String>,
    skip_diffs: bool,
) -> RefreshReport {
    let (mut client, _) = routing_client(pages, vec![]);
    Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#5")],
            &RefreshOptions { skip_diffs },
        )
        .unwrap()
}

// ---- git-backed recovery of capped file listings -------------------
