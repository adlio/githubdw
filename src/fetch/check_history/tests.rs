use super::*;
use crate::fetch::test_support::FixtureTransport;
use serde_json::json;

fn client(responses: Vec<Result<String>>) -> GhClient {
    GhClient::with_transport(Box::new(FixtureTransport::new(responses))).without_sleeping()
}

fn run_json(id: &str, sha: &str, name: &str) -> Value {
    json!({"id": 1, "node_id": id, "head_sha": sha, "name": name,
           "status": "completed", "conclusion": "success",
           "started_at": "2026-09-01T00:00:00Z", "completed_at": "2026-09-01T00:01:00Z",
           "pull_requests": []})
}

#[test]
fn pages_filter_all_until_total_count() {
    let first: Vec<Value> = (0..100)
        .map(|i| run_json(&format!("CR_{i}"), "abc", "job"))
        .collect();
    let second: Vec<Value> = (100..130)
        .map(|i| run_json(&format!("CR_{i}"), "abc", "job"))
        .collect();
    let transport = FixtureTransport::new(vec![
        Ok(json!({"total_count": 130, "check_runs": first}).to_string()),
        Ok(json!({"total_count": 130, "check_runs": second}).to_string()),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let CommitCheckRuns::Complete(runs) =
        fetch_commit_check_runs(&mut client, "o/r", "abc").unwrap()
    else {
        panic!("expected complete");
    };
    assert_eq!(runs.len(), 130);
    assert_eq!(runs[0].status, "COMPLETED");
    assert_eq!(runs[0].conclusion.as_deref(), Some("SUCCESS"));
    assert_eq!(runs[129].head_sha, "abc");
}

#[test]
fn requests_filter_all_with_pages() {
    let transport = std::sync::Arc::new(FixtureTransport::new(vec![]));
    // Use a local transport to inspect the call arguments.
    struct Shared(std::sync::Arc<FixtureTransport>);
    impl crate::fetch::GhTransport for Shared {
        fn run(&self, arguments: &[String]) -> Result<String> {
            self.0.calls.lock().unwrap().push(arguments.to_vec());
            Ok(json!({"total_count": 0, "check_runs": []}).to_string())
        }
    }
    let mut client =
        GhClient::with_transport(Box::new(Shared(transport.clone()))).without_sleeping();
    let result = fetch_commit_check_runs(&mut client, "o/r", "abc").unwrap();
    assert_eq!(result, CommitCheckRuns::Complete(vec![]));
    let calls = transport.calls.lock().unwrap();
    assert_eq!(
        calls[0][1],
        "repos/o/r/commits/abc/check-runs?filter=all&per_page=100&page=1"
    );
}

#[test]
fn short_listing_is_an_error_not_a_complete_list() {
    let mut client = client(vec![
        Ok(json!({"total_count": 3, "check_runs": [run_json("CR_1", "abc", "a")]}).to_string()),
        Ok(json!({"total_count": 3, "check_runs": []}).to_string()),
    ]);
    assert!(fetch_commit_check_runs(&mut client, "o/r", "abc").is_err());
}

#[test]
fn duplicate_runs_across_pages_never_count_toward_total() {
    let first: Vec<Value> = (0..100)
        .map(|i| run_json(&format!("CR_{i}"), "abc", "job"))
        .collect();
    let mut client = client(vec![
        Ok(json!({"total_count": 101, "check_runs": first}).to_string()),
        // Provider reshuffled: the second page repeats a first-page run.
        Ok(json!({"total_count": 101, "check_runs": [run_json("CR_5", "abc", "job")]}).to_string()),
        Ok(json!({"total_count": 101, "check_runs": []}).to_string()),
    ]);
    assert!(fetch_commit_check_runs(&mut client, "o/r", "abc").is_err());
}

#[test]
fn run_reporting_another_head_is_refused() {
    let mut client = client(vec![Ok(
        json!({"total_count": 1, "check_runs": [run_json("CR_1", "def", "a")]}).to_string(),
    )]);
    assert!(fetch_commit_check_runs(&mut client, "o/r", "abc").is_err());
}

#[test]
fn missing_commit_is_unavailable_not_empty() {
    let mut client = client(vec![Err(Error::GhCommand {
        exit_code: Some(1),
        stderr: "gh: No commit found for SHA: abc (HTTP 422)".into(),
    })]);
    assert!(matches!(
        fetch_commit_check_runs(&mut client, "o/r", "abc").unwrap(),
        CommitCheckRuns::Unavailable(_)
    ));
}

#[test]
fn lists_pull_request_commits_and_count() {
    let mut client = client(vec![
        Ok(json!({"head": {"sha": "HEAD1"}, "commits": 2}).to_string()),
        Ok(json!([{"sha": "c1"}, {"sha": "HEAD1"}]).to_string()),
    ]);
    let listed = fetch_pull_request_commits(&mut client, "o/r", 5).unwrap();
    assert_eq!(listed.head_sha, "head1");
    assert_eq!(listed.commit_count, 2);
    assert_eq!(listed.commits, vec!["c1", "head1"]);
}

#[test]
fn branch_pull_requests_query_every_state_with_an_encoded_head() {
    let transport = std::sync::Arc::new(FixtureTransport::new(vec![]));
    struct Shared(std::sync::Arc<FixtureTransport>);
    impl crate::fetch::GhTransport for Shared {
        fn run(&self, arguments: &[String]) -> Result<String> {
            self.0.calls.lock().unwrap().push(arguments.to_vec());
            Ok(json!([{"number": 5}, {"number": 7}]).to_string())
        }
    }
    let mut client =
        GhClient::with_transport(Box::new(Shared(transport.clone()))).without_sleeping();
    let owners = fetch_branch_pull_requests(&mut client, "o/r", "feat/a+b#1").unwrap();
    assert_eq!(owners, vec![5, 7]);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(
        calls[0][1],
        "repos/o/r/pulls?state=all&head=o:feat/a%2Bb%231&per_page=100&page=1"
    );
}

#[test]
fn a_full_page_of_commit_pull_requests_reads_the_next_page() {
    let full: Vec<Value> = (1..=100).map(|n| json!({"number": n})).collect();
    let mut client = client(vec![
        Ok(Value::Array(full).to_string()),
        Ok(json!([{"number": 101}]).to_string()),
    ]);
    let owners = fetch_commit_pull_requests(&mut client, "o/r", "abc").unwrap();
    assert_eq!(owners.len(), 101);
}

#[test]
fn an_entry_without_a_number_is_an_error_not_skipped() {
    let mut client = client(vec![Ok(json!([{"title": "x"}]).to_string())]);
    assert!(fetch_commit_pull_requests(&mut client, "o/r", "abc").is_err());
}

/// Trimmed from a real `actions/runs?head_sha=` answer (private-repo run
/// 36975877853, PR #3004's stack branch), field shapes unchanged.
const REAL_WORKFLOW_RUN: &str = r#"{"total_count": 1, "workflow_runs": [{
    "id": 36975877853, "check_suite_id": 100154950283, "event": "pull_request_target",
    "head_branch": "stack/dev/bg-exec-3b-control-tool",
    "head_sha": "02e1ce6092a1d6952b6d502c5872eee921036a5d",
    "created_at": "2026-10-02T06:55:07Z", "status": "completed",
    "repository": {"full_name": "acme/widget"},
    "head_repository": {"full_name": "acme/widget"},
    "pull_requests": [{"url": "https://api.github.com/repos/acme/widget/pulls/3004",
        "id": 4609242526, "number": 3004,
        "head": {"ref": "stack/dev/bg-exec-3b-control-tool",
                 "sha": "bf7484b94fa33262df789403c3ecb505c8c35c02",
                 "repo": {"id": 1000000001, "url": "https://api.github.com/repos/acme/widget", "name": "widget"}},
        "base": {"ref": "stack/dev/bg-exec-3a-execute-bash-start",
                 "sha": "410f72d678ab32f402f6c3ca6d4ae6c921d0c9d4",
                 "repo": {"id": 1000000001, "url": "https://api.github.com/repos/acme/widget", "name": "widget"}}}]}]}"#;

#[test]
fn a_real_workflow_run_parses_with_its_suite_and_pull_request_identity() {
    let mut client = client(vec![Ok(REAL_WORKFLOW_RUN.to_string())]);
    let runs = fetch_commit_workflow_runs(
        &mut client,
        "acme/widget",
        "02e1ce6092a1d6952b6d502c5872eee921036a5d",
    )
    .unwrap();
    assert_eq!(runs.len(), 1);
    let run = &runs[0];
    assert_eq!(run.check_suite_id, Some(100154950283));
    assert_eq!(run.event.as_deref(), Some("pull_request_target"));
    assert_eq!(run.head_repository.as_deref(), Some("acme/widget"));
    let pull = &run.pull_requests[0];
    assert_eq!(pull.number, 3004);
    assert!(api_url_names(
        pull.url.as_deref().unwrap(),
        "acme/widget",
        "/pulls/3004"
    ));
    assert!(api_url_names(
        pull.base_repo_url.as_deref().unwrap(),
        "acme/widget",
        ""
    ));
    assert!(!api_url_names(
        pull.url.as_deref().unwrap(),
        "forker/widget",
        "/pulls/3004"
    ));
    assert!(!api_url_names("repos/acme/widget", "acme/widget", ""));
}

#[test]
fn a_workflow_run_on_another_commit_is_an_error() {
    let mut client = client(vec![Ok(REAL_WORKFLOW_RUN.to_string())]);
    assert!(fetch_commit_workflow_runs(&mut client, "acme/widget", "abc").is_err());
}

#[test]
fn force_pushes_page_and_keep_unresolved_before_commits() {
    let page = |nodes: Value, next: bool, cursor: Option<&str>| {
        json!({"data": {"repository": {"pullRequest": {"timelineItems": {
            "filteredCount": 2,
            "pageInfo": {"hasNextPage": next, "endCursor": cursor},
            "nodes": nodes}}}}})
        .to_string()
    };
    let mut client = client(vec![
        Ok(page(
            json!([{"__typename": "HeadRefForcePushedEvent",
                    "beforeCommit": {"oid": "AAA"}, "afterCommit": {"oid": "BBB"}}]),
            true,
            Some("c1"),
        )),
        Ok(page(
            json!([{"__typename": "HeadRefForcePushedEvent",
                    "beforeCommit": null, "afterCommit": {"oid": "CCC"}}]),
            false,
            None,
        )),
    ]);
    let pushes = fetch_force_pushes(&mut client, "o/r", 5).unwrap();
    assert_eq!(pushes.len(), 2);
    assert_eq!(pushes[0].before.as_deref(), Some("aaa"));
    assert_eq!(pushes[1].before, None);
}

/// The error a fetch returns for a sequence of provider pages.
fn refusal<T: std::fmt::Debug>(
    pages: Vec<Value>,
    fetch: impl FnOnce(&mut GhClient) -> Result<T>,
) -> String {
    let mut client = client(pages.into_iter().map(|page| Ok(page.to_string())).collect());
    fetch(&mut client).unwrap_err().to_string()
}

fn suite_json(id: i64) -> Value {
    json!({"id": id, "head_branch": "feat", "app": {"slug": "github-actions"},
           "repository": {"full_name": "o/r"}})
}

fn workflow_json(id: i64, sha: &str) -> Value {
    json!({"id": id, "check_suite_id": id, "event": "push", "head_sha": sha,
           "pull_requests": []})
}

/// Malformed or inconsistent listings fail closed: a caller must never
/// store a partial or contradictory list of runs, suites, or workflow runs
/// as if it were the complete set.
#[test]
fn inconsistent_listings_are_refused() {
    let runs = |range: std::ops::Range<i64>| -> Vec<Value> {
        range
            .map(|i| run_json(&format!("CR_{i}"), "abc", "job"))
            .collect()
    };
    let suites = |range: std::ops::Range<i64>| -> Vec<Value> { range.map(suite_json).collect() };
    let workflows = |range: std::ops::Range<i64>| -> Vec<Value> {
        range.map(|i| workflow_json(i, "abc")).collect()
    };
    let check_runs = |client: &mut GhClient| fetch_commit_check_runs(client, "o/r", "abc");
    let check_suites = |client: &mut GhClient| fetch_commit_check_suites(client, "o/r", "abc");
    let workflow_runs = |client: &mut GhClient| fetch_commit_workflow_runs(client, "o/r", "abc");

    let cases: Vec<(String, &str)> = vec![
        (
            refusal(vec![json!({"check_runs": []})], check_runs),
            "page 1 has no total_count",
        ),
        (
            refusal(
                vec![
                    json!({"total_count": 101, "check_runs": runs(0..100)}),
                    json!({"total_count": 102, "check_runs": runs(100..101)}),
                ],
                check_runs,
            ),
            "changed while paging (101 then 102)",
        ),
        (
            refusal(vec![json!({"total_count": 1})], check_runs),
            "page 1 has no check_runs",
        ),
        (
            refusal(
                vec![json!({"total_count": 1, "check_runs": runs(0..2)})],
                check_runs,
            ),
            "2 distinct runs exceed total_count 1",
        ),
        (
            refusal(vec![json!({"check_suites": []})], check_suites),
            "check suites for abc page 1 has no total_count",
        ),
        (
            refusal(
                vec![
                    json!({"total_count": 101, "check_suites": suites(0..100)}),
                    json!({"total_count": 102, "check_suites": suites(100..101)}),
                ],
                check_suites,
            ),
            "check suites for abc changed while paging",
        ),
        (
            refusal(vec![json!({"total_count": 1})], check_suites),
            "check suites for abc page 1 has no list",
        ),
        (
            refusal(
                vec![
                    json!({"total_count": 1, "check_suites": [suite_json(1), suite_json(1), suite_json(2)]}),
                ],
                check_suites,
            ),
            "check suites for abc: 2 distinct exceed total_count 1",
        ),
        (
            refusal(
                vec![
                    json!({"total_count": 3, "check_suites": suites(0..1)}),
                    json!({"total_count": 3, "check_suites": []}),
                ],
                check_suites,
            ),
            "check suites for abc: provider stopped at 1 of 3",
        ),
        (
            refusal(vec![json!({"workflow_runs": []})], workflow_runs),
            "workflow runs for abc page 1 has no total_count",
        ),
        (
            refusal(
                vec![
                    json!({"total_count": 101, "workflow_runs": workflows(0..100)}),
                    json!({"total_count": 102, "workflow_runs": workflows(100..101)}),
                ],
                workflow_runs,
            ),
            "workflow runs for abc changed while paging",
        ),
        (
            refusal(vec![json!({"total_count": 1})], workflow_runs),
            "workflow runs for abc page 1 has no list",
        ),
        (
            refusal(
                vec![json!({"total_count": 1, "workflow_runs": workflows(0..2)})],
                workflow_runs,
            ),
            "workflow runs for abc: 2 distinct exceed total_count 1",
        ),
        (
            refusal(
                vec![
                    json!({"total_count": 3, "workflow_runs": workflows(0..1)}),
                    json!({"total_count": 3, "workflow_runs": []}),
                ],
                workflow_runs,
            ),
            "workflow runs for abc: provider stopped at 1 of 3",
        ),
    ];
    for (error, expected) in cases {
        assert!(error.contains(expected), "{error:?} lacks {expected:?}");
    }
}

#[test]
fn malformed_commit_listings_are_refused() {
    let pull = json!({"head": {"sha": "abc", "ref": "feat", "repo": {"full_name": "o/r"}},
                      "base": {"repo": {"full_name": "o/r"}}, "commits": 1});
    let commits = |client: &mut GhClient| fetch_pull_request_commits(client, "o/r", 5);
    let error = refusal(vec![pull.clone(), json!({"sha": "abc"})], commits);
    assert!(
        error.contains("o/r#5 commits page 1 is not a list"),
        "{error}"
    );
    let error = refusal(vec![pull, json!([{"oid": "abc"}])], commits);
    assert!(error.contains("o/r#5 commit has no sha"), "{error}");
}

/// With the base repository missing from the response, the head
/// repository is compared against the repository being read.
#[test]
fn a_missing_base_repository_compares_the_head_with_the_repository_read() {
    for (head_repo, cross) in [("O/R", false), ("fork/r", true)] {
        let mut client = client(vec![
            Ok(
                json!({"head": {"sha": "ABC", "repo": {"full_name": head_repo}},
                      "commits": 1})
                .to_string(),
            ),
            Ok(json!([{"sha": "ABC"}]).to_string()),
        ]);
        let listed = fetch_pull_request_commits(&mut client, "o/r", 5).unwrap();
        assert_eq!(listed.cross_repository, cross, "{head_repo}");
        assert_eq!(listed.commits, ["abc"]);
        assert_eq!(listed.head_ref, None);
    }
}

#[test]
fn a_workflow_run_pull_request_without_a_number_is_refused() {
    let mut run = workflow_json(1, "abc");
    run["pull_requests"] = json!([{"url": "https://api.github.com/repos/o/r/pulls/5"}]);
    let error = refusal(
        vec![json!({"total_count": 1, "workflow_runs": [run]})],
        |client| fetch_commit_workflow_runs(client, "o/r", "abc"),
    );
    assert!(
        error.contains("workflow run 1: pull request has no number"),
        "{error}"
    );
}

fn timeline(items: Value) -> Value {
    json!({"data": {"repository": {"pullRequest": {"timelineItems": items}}}})
}

/// A timeline that is missing, unpaged, or disagrees with its own count
/// fails closed, so a force-pushed head is never silently left unread.
#[test]
fn inconsistent_force_push_timelines_are_refused() {
    let push = json!({"__typename": "HeadRefForcePushedEvent",
                      "beforeCommit": {"oid": "A"}, "afterCommit": {"oid": "B"}});
    let page = |next: bool, cursor: Value| json!({"hasNextPage": next, "endCursor": cursor});
    let cases = [
        (
            vec![json!({"data": {"repository": {"pullRequest": {"timelineItems": null}}}})],
            "o/r#5 timeline is missing",
        ),
        (
            vec![timeline(json!({"pageInfo": page(false, Value::Null)}))],
            "o/r#5 timeline has no nodes",
        ),
        (
            vec![timeline(json!({"nodes": []}))],
            "o/r#5 timeline has no page status",
        ),
        (
            vec![timeline(json!({"filteredCount": 2, "nodes": [push],
                                 "pageInfo": page(false, Value::Null)}))],
            "o/r#5: timeline reports 2 force pushes, served 1",
        ),
        (
            vec![timeline(
                json!({"nodes": [], "pageInfo": page(true, Value::Null)}),
            )],
            "o/r#5 timeline has no endCursor",
        ),
        (
            vec![
                timeline(json!({"nodes": [], "pageInfo": page(true, json!("c1"))})),
                timeline(json!({"nodes": [], "pageInfo": page(true, json!("c1"))})),
            ],
            "o/r#5 timeline cursor repeated",
        ),
    ];
    for (pages, expected) in cases {
        let error = refusal(pages, |client| fetch_force_pushes(client, "o/r", 5));
        assert!(error.contains(expected), "{error:?} lacks {expected:?}");
    }
    let error = refusal(vec![], |client| fetch_force_pushes(client, "not-a-repo", 5));
    assert!(error.contains("not-a-repo"), "{error}");
}

#[test]
fn other_timeline_items_are_not_force_pushes() {
    let mut client = client(vec![Ok(timeline(json!({
        "filteredCount": 1,
        "pageInfo": {"hasNextPage": false, "endCursor": null},
        "nodes": [
            {"__typename": "HeadRefRestoredEvent"},
            {"__typename": "HeadRefForcePushedEvent", "beforeCommit": null,
             "afterCommit": {"oid": "BEEF"}}
        ]
    }))
    .to_string())]);
    let pushes = fetch_force_pushes(&mut client, "o/r", 5).unwrap();
    assert_eq!(pushes.len(), 1);
    assert_eq!(pushes[0].before, None);
    assert_eq!(pushes[0].after.as_deref(), Some("beef"));
}

#[test]
fn rest_quota_reads_core_remaining_and_reset() {
    let mut ok = client(vec![Ok(json!({"resources": {"core": {
        "remaining": 4349, "reset": 1_800_000_000_i64
    }}})
    .to_string())]);
    assert_eq!(fetch_rest_quota(&mut ok).unwrap(), (4349, 1_800_000_000));

    let error = refusal(vec![json!({"resources": {}})], fetch_rest_quota);
    assert!(
        error.contains("rate_limit has no resources.core"),
        "{error}"
    );
    let error = refusal(
        vec![json!({"resources": {"core": {"reset": 1}}})],
        fetch_rest_quota,
    );
    assert!(
        error.contains("rate_limit core has no remaining"),
        "{error}"
    );
}
