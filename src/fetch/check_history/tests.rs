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
