use super::*;
use crate::fetch::test_support::FixtureTransport;
use crate::storage::schema;
use serde_json::{Value, json};

const H1: &str = "1111111111111111111111111111111111111111";
const H2: &str = "2222222222222222222222222222222222222222";
const C3: &str = "3333333333333333333333333333333333333333";

fn warehouse() -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    schema::init(&mut conn).unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
             head_ref, created_at, created_date_key, created_time_key)
         VALUES ('o/r#5', 5, 'o/r', 'user:a', 'MERGED', 'feat/five', '2026-09-01T00:00:00Z', '2026-09-01', '00:00'),
                ('o/r#9', 9, 'o/r', 'user:a', 'MERGED', 'feat/nine', '2026-09-01T00:00:00Z', '2026-09-01', '00:00');",
    )
    .unwrap();
    conn
}

fn store(conn: &Connection, key: &str, pr: &str, sha: &str, status: &str) {
    repository::upsert_check_run(conn, key, pr, sha, "job", status, None, None, None).unwrap();
}

/// A run in suite 1 (created by branch `feat/five`, i.e. PR #5's branch).
fn run(id: &str, sha: &str) -> Value {
    run_in(id, sha, 1)
}

fn run_in(id: &str, sha: &str, suite: i64) -> Value {
    json!({"id": 1, "node_id": id, "head_sha": sha, "name": "job", "status": "completed",
           "conclusion": "success", "started_at": "2026-09-01T00:00:00Z",
           "completed_at": "2026-09-01T00:01:00Z", "pull_requests": [],
           "check_suite": {"id": suite}, "app": {"slug": "github-actions"}})
}

/// The suites every fixture commit carries: 1 = feat/five, 2 = feat/nine,
/// 3 = refs/pull/5/head, 4 = refs/pull/9/head, 5 = null branch, 6 = unknown app.
fn suites() -> Result<String> {
    let suite = |id: i64, branch: Value, app: Value| {
        json!({"id": id, "head_branch": branch, "app": app,
               "repository": {"full_name": "o/r"}})
    };
    let app = json!({"slug": "github-actions"});
    let list = vec![
        suite(1, json!("feat/five"), app.clone()),
        suite(2, json!("feat/nine"), app.clone()),
        suite(3, json!("refs/pull/5/head"), app.clone()),
        suite(4, json!("refs/pull/9/head"), app.clone()),
        suite(5, Value::Null, app.clone()),
        suite(6, json!("feat/five"), Value::Null),
    ];
    Ok(json!({"total_count": list.len(), "check_suites": list}).to_string())
}

fn runs(list: Vec<Value>) -> Result<String> {
    Ok(json!({"total_count": list.len(), "check_runs": list}).to_string())
}

fn pull_json(head: &str, count: usize, head_repo: &str) -> String {
    json!({"head": {"sha": head, "ref": "feat/five", "repo": {"full_name": head_repo}},
           "base": {"repo": {"full_name": "o/r"}}, "commits": count,
           "created_at": "2026-08-15T00:00:00Z", "closed_at": null})
    .to_string()
}

fn pull(head: &str, commits: &[&str]) -> Vec<Result<String>> {
    vec![
        Ok(pull_json(head, commits.len(), "o/r")),
        Ok(Value::Array(commits.iter().map(|sha| json!({"sha": sha})).collect()).to_string()),
    ]
}

fn no_force_pushes() -> Result<String> {
    Ok(
        json!({"data": {"repository": {"pullRequest": {"timelineItems": {
        "filteredCount": 0, "pageInfo": {"hasNextPage": false, "endCursor": null},
        "nodes": []}}}}})
        .to_string(),
    )
}

/// `commits/{sha}/pulls`: the provider's owners of a commit.
fn owners(numbers: &[i64]) -> Result<String> {
    Ok(Value::Array(numbers.iter().map(|n| json!({"number": n})).collect()).to_string())
}

// `actions/runs?head_sha=` answers, by commit, overriding the default.
thread_local! {
    static WORKFLOWS: std::cell::RefCell<std::collections::BTreeMap<String, Vec<Value>>> =
        const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}

fn set_workflows(sha: &str, list: Vec<Value>) {
    WORKFLOWS.with(|map| map.borrow_mut().insert(sha.to_string(), list));
}

fn pr_entry(number: i64, head_ref: &str, head_repo: &str) -> Value {
    json!({"url": format!("https://api.github.com/repos/o/r/pulls/{number}"),
           "id": number * 1000, "number": number,
           "head": {"ref": head_ref, "sha": "ffff",
                    "repo": {"url": format!("https://api.github.com/repos/{head_repo}")}},
           "base": {"ref": "main", "sha": "eeee",
                    "repo": {"url": "https://api.github.com/repos/o/r"}}})
}

/// One workflow run as `actions/runs` reports it (real field shape).
fn workflow(
    suite: i64,
    sha: &str,
    event: &str,
    branch: Value,
    head_repo: &str,
    prs: Vec<Value>,
) -> Value {
    json!({"id": 10_000 + suite, "check_suite_id": suite, "event": event,
           "head_branch": branch, "head_sha": sha, "created_at": "2026-09-01T00:00:00Z",
           "repository": {"full_name": "o/r"}, "head_repository": {"full_name": head_repo},
           "pull_requests": prs})
}

/// Workflow runs matching [`suites`]: 1 = PR #5 on feat/five, 2 = PR #9 on
/// feat/nine, 3/4 = refs/pull/5|9/head, 5 = a branchless push.
fn default_workflows(sha: &str) -> Vec<Value> {
    vec![
        workflow(
            1,
            sha,
            "pull_request",
            json!("feat/five"),
            "o/r",
            vec![pr_entry(5, "feat/five", "o/r")],
        ),
        workflow(
            2,
            sha,
            "pull_request",
            json!("feat/nine"),
            "o/r",
            vec![pr_entry(9, "feat/nine", "o/r")],
        ),
        workflow(3, sha, "dynamic", json!("refs/pull/5/head"), "o/r", vec![]),
        workflow(4, sha, "dynamic", json!("refs/pull/9/head"), "o/r", vec![]),
        workflow(5, sha, "push", Value::Null, "o/r", vec![]),
    ]
}

// Remaining quota reported on routed responses when `--include` is asked.
thread_local! {
    static ROUTED_REMAINING: std::cell::Cell<i64> = const { std::cell::Cell::new(4000) };
}

fn with_rate_headers(resource: &str, remaining: i64, body: &str) -> String {
    format!(
        "HTTP/2.0 200 OK\r\nX-Ratelimit-Remaining: {remaining}\r\nX-Ratelimit-Reset: 1\r\n\
         X-Ratelimit-Resource: {resource}\r\n\r\n{body}"
    )
}

/// Serves workflow-run lookups from [`WORKFLOWS`] / [`default_workflows`] and
/// everything else from the queued fixture responses, recording every call.
struct Routed(std::sync::Arc<FixtureTransport>);

impl crate::fetch::GhTransport for Routed {
    fn run(&self, arguments: &[String]) -> Result<String> {
        let path = arguments.iter().find(|a| a.starts_with("repos/")).cloned();
        if let Some(path) = path
            && let Some(rest) = path.split("/actions/runs?head_sha=").nth(1)
        {
            self.0.calls.lock().unwrap().push(arguments.to_vec());
            let sha = rest.split('&').next().unwrap().to_string();
            let list = WORKFLOWS
                .with(|map| map.borrow().get(&sha).cloned())
                .unwrap_or_else(|| default_workflows(&sha));
            let body = json!({"total_count": list.len(), "workflow_runs": list}).to_string();
            if arguments.iter().any(|a| a == "--include") {
                let remaining = ROUTED_REMAINING.with(|r| r.get());
                return Ok(with_rate_headers("core", remaining, &body));
            }
            return Ok(body);
        }
        self.0.run(arguments)
    }
}

fn client(responses: Vec<Result<String>>) -> GhClient {
    shared_client(responses).0
}

fn target() -> RefreshTarget {
    RefreshTarget::parse(TargetKind::PullRequest, "o/r#5").unwrap()
}

fn heads_of(conn: &Connection, pr: &str) -> Vec<(String, String)> {
    let mut statement = conn
        .prepare("SELECT check_run_key, head_sha FROM fact_check_runs WHERE pr_key = ?1 ORDER BY 1")
        .unwrap();
    statement
        .query_map([pr], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}

/// H1 was observed earlier with one run; upstream also has a re-run on H1
/// and a run on the current head H2.
fn two_head_responses() -> Vec<Result<String>> {
    let mut responses = pull(H2, &[H1, H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_a", H1), run_in("CR_b", H1, 3)]));
    responses.push(suites());
    // Every commit's owners are asked, stored heads included; an empty
    // answer (closed PR, commit not on the default branch) is no proof
    // either way -- the suite evidence decides.
    responses.push(owners(&[]));
    // Workflow runs for each commit are served by the routed transport.
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    responses
}

#[test]
fn stores_each_run_under_the_commit_the_provider_reports() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(
        outcome.status,
        HistoryStatus::Complete,
        "{:?}",
        outcome.caveats
    );
    assert_eq!(
        (outcome.check_runs_before, outcome.check_runs_after),
        (1, 3)
    );
    assert_eq!(
        heads_of(&conn, "o/r#5"),
        vec![
            ("CR_a".into(), H1.into()),
            ("CR_b".into(), H1.into()),
            ("CR_c".into(), H2.into())
        ]
    );
    let h1 = outcome.heads.iter().find(|head| head.sha == H1).unwrap();
    assert_eq!(
        h1.sources,
        vec![HeadSource::StoredHead, HeadSource::CommitList]
    );
    assert_eq!(
        (h1.upstream_runs, h1.stored_before, h1.stored_after),
        (Some(2), 1, 2)
    );
    assert_eq!((h1.inserted, h1.updated), (1, 1));
}

#[test]
fn repeated_runs_are_idempotent() {
    let conn = warehouse();
    let options = CheckHistoryOptions::default();
    let mut first = client(two_head_responses());
    repair_check_history(&conn, &mut first, &[target()], &options).unwrap();
    let mut second = client(two_head_responses());
    let report = repair_check_history(&conn, &mut second, &[target()], &options).unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(
        outcome.status,
        HistoryStatus::Complete,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        (outcome.check_runs_before, outcome.check_runs_after),
        (3, 3)
    );
    assert!(
        outcome
            .heads
            .iter()
            .all(|head| head.inserted == 0 && head.updated == 0)
    );
}

#[test]
fn a_fetch_failure_leaves_stored_rows_unchanged() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    let mut responses = pull(H2, &[H1, H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_a", H1), run("CR_b", H1)]));
    responses.push(suites());
    responses.push(owners(&[]));
    responses.push(Err(Error::GitHubApi("boom".into())));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_a".into(), H1.into())]);
    let status: String = conn
        .query_row(
            "SELECT status FROM fact_check_runs WHERE check_run_key = 'CR_a'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        status, "IN_PROGRESS",
        "the earlier head's update must roll back too"
    );
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 0);
}

#[test]
fn a_run_stored_under_another_pull_request_is_never_moved() {
    // Like the eight c590f74a rows held under #2916/#2905 while the
    // provider's workflow runs name #3004: proof for us, a conflict, unmoved.
    let conn = warehouse();
    store(&conn, "CR_c", "o/r#9", H2, "COMPLETED");
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(heads_of(&conn, "o/r#9"), vec![("CR_c".into(), H2.into())]);
    let h2 = outcome.heads.iter().find(|head| head.sha == H2).unwrap();
    assert!(h2.shared_head);
    assert_eq!(
        h2.ownership_conflicts.get("CR_c").map(String::as_str),
        Some("o/r#9")
    );
    assert!(
        outcome.caveats.iter().any(|c| c.starts_with("CONFLICT")),
        "{:?}",
        outcome.caveats
    );
}

#[test]
fn a_stored_run_on_a_different_commit_is_reported_not_rewritten() {
    let conn = warehouse();
    store(&conn, "CR_b", "o/r#5", H2, "COMPLETED");
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Incomplete);
    let sha: String = conn
        .query_row(
            "SELECT head_sha FROM fact_check_runs WHERE check_run_key = 'CR_b'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sha, H2);
}

#[test]
fn a_listed_commit_the_provider_attributes_elsewhere_is_skipped() {
    let conn = warehouse();
    let mut responses = pull(H2, &[C3, H2]);
    responses.push(no_force_pushes());
    // BTreeMap order: H2 ("222...") before C3 ("333...").
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[5]));
    responses.push(runs(vec![run("CR_x", C3)]));
    responses.push(suites());
    responses.push(owners(&[9]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Complete);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_c".into(), H2.into())]);
    let c3 = outcome.heads.iter().find(|head| head.sha == C3).unwrap();
    assert_eq!(c3.attributed_elsewhere, 1);
    assert!(c3.skipped.is_some());
}

#[test]
fn an_unavailable_stored_head_makes_the_result_incomplete() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(Err(Error::GhCommand {
        exit_code: Some(1),
        stderr: "No commit found for SHA (HTTP 422)".into(),
    }));
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(
        outcome.check_runs_after, 2,
        "the stored run is kept, never pruned"
    );
}

#[test]
fn a_capped_commit_list_is_never_reported_complete() {
    let conn = warehouse();
    let mut responses = vec![
        Ok(pull_json(H2, 300, "o/r")),
        Ok(Value::Array(vec![json!({"sha": H2})]).to_string()),
    ];
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Incomplete);
}

#[test]
fn observed_only_reads_stored_and_current_heads_and_is_not_complete() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut responses = pull(H2, &[H1, H2]);
    responses.push(runs(vec![run("CR_a", H1), run("CR_b", H1)]));
    responses.push(suites());
    responses.push(owners(&[]));
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions {
            observed_only: true,
            rest_reserve: None,
            max_requests: None,
        },
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(outcome.check_runs_after, 3);
}

#[test]
fn a_running_sync_fails_the_target_and_keeps_its_lock() {
    let conn = warehouse();
    sync_state::acquire_lock(&conn, "repo:o/r").unwrap();
    let mut client = client(Vec::new());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 1);
}

#[test]
fn a_pull_request_missing_from_the_warehouse_fails_without_network() {
    let conn = warehouse();
    let mut client = client(Vec::new());
    let other = RefreshTarget::parse(TargetKind::PullRequest, "o/r#77").unwrap();
    let report = repair_check_history(
        &conn,
        &mut client,
        &[other],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
}

#[test]
fn candidates_rank_pull_requests_by_stored_heads() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    store(&conn, "CR_b", "o/r#5", H2, "COMPLETED");
    store(&conn, "CR_c", "o/r#9", H1, "COMPLETED");
    let candidates = history_candidates(&conn, Some("O/R"), 2, 10).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].target.reference(), "o/r#5");
    assert_eq!(candidates[0].stored_heads, 2);
}

#[test]
fn the_guarded_upsert_never_rewrites_another_pull_requests_row() {
    let conn = warehouse();
    store(&conn, "CR_c", "o/r#9", H2, "IN_PROGRESS");
    store(&conn, "CR_c", "o/r#5", H2, "COMPLETED");
    let (pr, status): (String, String) = conn
        .query_row(
            "SELECT pr_key, status FROM fact_check_runs WHERE check_run_key = 'CR_c'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((pr.as_str(), status.as_str()), ("o/r#9", "IN_PROGRESS"));
}

#[test]
fn check_run_continuations_keep_every_rerun() {
    let query = crate::fetch::continuation::continuation_query("CheckSuite", "checkRuns", "id");
    assert!(query.contains("filterBy: {checkType: ALL}"));
    let other = crate::fetch::continuation::continuation_query("PullRequest", "files", "path");
    assert!(!other.contains("filterBy"));
}

fn repair(conn: &Connection, responses: Vec<Result<String>>) -> HistoryOutcome {
    let mut client = client(responses);
    let mut report = repair_check_history(
        conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    report.outcomes.remove(0)
}

fn head<'a>(outcome: &'a HistoryOutcome, sha: &str) -> &'a HeadOutcome {
    outcome.heads.iter().find(|head| head.sha == sha).unwrap()
}

/// Current head H2 only, with the given runs; the provider lists no owner
/// for the commit. Workflow runs come from the routed transport.
fn current_head_only(list: Vec<Value>) -> Vec<Result<String>> {
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(list));
    responses.push(suites());
    responses.push(owners(&[]));
    responses
}

#[test]
fn a_shared_head_is_resolved_only_by_workflow_run_identity() {
    // Like c590f74a: several pull requests share one head. The suite's
    // branch names whichever pushed first; only the workflow run keyed by the
    // suite says which pull request triggered a run.
    let conn = warehouse();
    store(&conn, "CR_other", "o/r#9", H2, "COMPLETED");
    let outcome = repair(
        &conn,
        current_head_only(vec![
            run_in("CR_q", H2, 1),
            run_in("CR_r", H2, 3),
            run_in("CR_s", H2, 2),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert!(h2.shared_head);
    assert!(
        h2.ambiguous.get("CR_r").unwrap().contains("shared"),
        "a bare pull ref on a shared commit is not proof: {h2:?}"
    );
    assert_eq!(h2.attributed_elsewhere, 1);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_q".into(), H2.into())]);
    assert_eq!(
        heads_of(&conn, "o/r#9"),
        vec![("CR_other".into(), H2.into())]
    );
}

#[test]
fn a_shared_head_without_workflow_identity_is_ambiguous() {
    // Fork pull requests: the provider's workflow runs name no pull request,
    // and a unique-looking branch is not proof.
    let conn = warehouse();
    store(&conn, "CR_other", "o/r#9", H2, "COMPLETED");
    set_workflows(
        H2,
        vec![workflow(
            1,
            H2,
            "pull_request",
            json!("feat/five"),
            "o/r",
            vec![],
        )],
    );
    let outcome = repair(&conn, current_head_only(vec![run_in("CR_q", H2, 1)]));
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_q").unwrap();
    assert!(reason.contains("names no pull request"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn provider_pull_request_tags_are_neither_proof_nor_exclusion() {
    let conn = warehouse();
    // A check-run tag contradicting the workflow run's identity is a
    // conflict; an absent tag excludes nothing.
    let mut tagged = run_in("CR_t", H2, 1);
    tagged["pull_requests"] = json!([{"number": 9}]);
    let outcome = repair(
        &conn,
        current_head_only(vec![tagged, run_in("CR_u", H2, 1)]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert!(head(&outcome, H2).ambiguous.contains_key("CR_t"));
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_u".into(), H2.into())]);
}

#[test]
fn absent_tags_with_our_branch_on_an_unshared_head_are_complete() {
    let conn = warehouse();
    let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
    assert_eq!(
        outcome.status,
        HistoryStatus::Complete,
        "{:?}",
        outcome.caveats
    );
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_v".into(), H2.into())]);
}

#[test]
fn a_stack_parent_commit_is_never_imported() {
    let conn = warehouse();
    let mut responses = pull(H2, &[C3, H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    responses.push(runs(vec![run_in("CR_p", C3, 2)]));
    responses.push(suites());
    responses.push(owners(&[])); // closed parents give no owner answer
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(head(&outcome, C3).attributed_elsewhere, 1);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_c".into(), H2.into())]);
}

#[test]
fn a_reused_branch_name_neither_proves_nor_blocks_workflow_identity() {
    let conn = warehouse();
    conn.execute_batch(
        "INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
             head_ref, created_at, created_date_key, created_time_key)
         VALUES ('o/r#7', 7, 'o/r', 'user:a', 'CLOSED', 'feat/five', '2026-08-01T00:00:00Z',
                 '2026-08-01', '00:00');",
    )
    .unwrap();
    // Suite 1's workflow run names #7 (the earlier user of the branch).
    set_workflows(
        H2,
        vec![
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(7, "feat/five", "o/r")],
            ),
            workflow(3, H2, "dynamic", json!("refs/pull/5/head"), "o/r", vec![]),
        ],
    );
    let outcome = repair(
        &conn,
        current_head_only(vec![run("CR_w", H2), run_in("CR_x", H2, 3)]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(head(&outcome, H2).attributed_elsewhere, 1);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_x".into(), H2.into())]);
}

#[test]
fn a_fork_branch_name_is_ambiguous() {
    let conn = warehouse();
    let mut responses = vec![
        Ok(pull_json(H2, 1, "someone/r")),
        Ok(json!([{"sha": H2}]).to_string()),
    ];
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_f", H2), run_in("CR_g", H2, 3)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert!(head(&outcome, H2).ambiguous.contains_key("CR_f"));
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_g".into(), H2.into())]);
}

#[test]
fn unknown_app_and_branchless_suites_are_ambiguous() {
    let conn = warehouse();
    let mut appless = run_in("CR_app", H2, 6);
    appless.as_object_mut().unwrap().remove("app");
    let outcome = repair(
        &conn,
        current_head_only(vec![
            appless,
            run_in("CR_nob", H2, 5),
            run_in("CR_nos", H2, 99),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert_eq!(h2.ambiguous.len(), 3, "{h2:?}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn imported_rows_are_revalidated_on_every_repeat() {
    let conn = warehouse();
    let first = repair(&conn, current_head_only(vec![run("CR_i", H2)]));
    assert_eq!(first.status, HistoryStatus::Complete);
    // Second run: the stored row makes H2 a stored head, which must not
    // stand in for proof. The suite evidence no longer names our branch.
    let second = repair(&conn, current_head_only(vec![run_in("CR_i", H2, 5)]));
    assert_eq!(second.status, HistoryStatus::Incomplete);
    let h2 = head(&second, H2);
    assert!(h2.sources.contains(&HeadSource::StoredHead));
    assert_eq!(h2.stored_unconfirmed, vec!["CR_i".to_string()]);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_i".into(), H2.into())]);
}

#[test]
fn a_write_error_rolls_back_and_reports_rows_unchanged() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    conn.execute_batch(
        "CREATE TRIGGER refuse_c BEFORE INSERT ON fact_check_runs
         WHEN NEW.check_run_key = 'CR_c' BEGIN SELECT RAISE(ABORT, 'refused'); END;",
    )
    .unwrap();
    let outcome = repair(&conn, two_head_responses());
    assert_eq!(outcome.status, HistoryStatus::Failed);
    assert_eq!(outcome.rows_unchanged, Some(true));
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_a".into(), H1.into())]);
    let status: String = conn
        .query_row(
            "SELECT status FROM fact_check_runs WHERE check_run_key = 'CR_a'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "IN_PROGRESS");
}

fn lock_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap()
}

/// A client whose transport stays inspectable after the call.
fn shared_client(responses: Vec<Result<String>>) -> (GhClient, std::sync::Arc<FixtureTransport>) {
    let transport = std::sync::Arc::new(FixtureTransport::new(responses));
    let client = GhClient::with_transport(Box::new(Routed(transport.clone()))).without_sleeping();
    (client, transport)
}

fn nine() -> RefreshTarget {
    RefreshTarget::parse(TargetKind::PullRequest, "o/r#9").unwrap()
}

fn guarded(reserve: i64) -> CheckHistoryOptions {
    CheckHistoryOptions {
        observed_only: false,
        rest_reserve: Some(reserve),
        max_requests: None,
    }
}

/// A queued fixture response carrying `x-ratelimit-*` headers.
fn hdr(remaining: i64, body: Result<String>) -> Result<String> {
    body.map(|body| with_rate_headers("core", remaining, &body))
}

fn gql(remaining: i64, body: Result<String>) -> Result<String> {
    body.map(|body| with_rate_headers("graphql", remaining, &body))
}

#[test]
fn low_observed_quota_stops_the_batch_with_resumable_not_attempted_targets() {
    // The first data response says the bucket is nearly spent: the target
    // writes nothing, the next is not attempted, no lock is left.
    let conn = warehouse();
    let (mut client, transport) = shared_client(vec![
        hdr(104, Ok(pull_json(H2, 1, "o/r"))),
        hdr(103, Ok(json!([{"sha": H2}]).to_string())),
        gql(4000, no_force_pushes()),
    ]);
    let report =
        repair_check_history(&conn, &mut client, &[target(), nine()], &guarded(100)).unwrap();
    assert!(report.stopped.is_some());
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert_eq!(report.outcomes[0].rows_unchanged, Some(true));
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .unwrap()
            .contains("headroom")
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert_eq!(report.not_complete(), 2);
    assert_eq!(transport.calls.lock().unwrap().len(), 3);
    assert_eq!(lock_count(&conn), 0);
}

#[test]
fn a_data_response_without_rate_limit_headers_fails_closed() {
    let conn = warehouse();
    let (mut client, transport) = shared_client(vec![Ok(pull_json(H2, 1, "o/r"))]);
    let report =
        repair_check_history(&conn, &mut client, &[target(), nine()], &guarded(100)).unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .unwrap()
            .contains("no rate-limit headers"),
        "{:?}",
        report.outcomes[0]
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
    assert!(transport.calls.lock().unwrap()[0].contains(&"--include".to_string()));
}

#[test]
fn a_first_call_403_stops_the_batch_without_touching_rows_or_the_lock() {
    // Before any quota is observed the first call is a single probe: a 403
    // is not retried, stops the whole batch, and leaves rows and lock alone.
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    let mut responses = Vec::new();
    for _ in 0..7 {
        responses.push(Err(Error::GhCommand {
            exit_code: Some(1),
            stderr: "gh: Resource not accessible (HTTP 403)".into(),
        }));
    }
    let (mut client, transport) = shared_client(responses);
    let report =
        repair_check_history(&conn, &mut client, &[target(), nine()], &guarded(100)).unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed);
    assert_eq!(first.rows_unchanged, Some(true));
    assert!(
        first.error.as_deref().unwrap().contains("HTTP 403"),
        "{first:?}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("quota/permission")
    );
    assert_eq!(
        transport.calls.lock().unwrap().len(),
        1,
        "no call after the stop"
    );
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_a".into(), H1.into())]);
    assert_eq!(lock_count(&conn), 0);
}

#[test]
fn a_rate_limit_403_after_observation_spends_retries_against_the_reserve() {
    // Observed 103 with reserve 100: three attempts are left, retries
    // included; the fourth is refused locally and never sent.
    let conn = warehouse();
    let mut responses = vec![hdr(103, Ok(pull_json(H2, 1, "o/r")))];
    for _ in 0..7 {
        responses.push(Err(Error::GhCommand {
            exit_code: Some(1),
            stderr: "gh: API rate limit exceeded (HTTP 403)".into(),
        }));
    }
    let (mut client, transport) = shared_client(responses);
    let report = repair_check_history(&conn, &mut client, &[target()], &guarded(100)).unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .unwrap()
            .contains("reserve"),
        "{:?}",
        report.outcomes[0]
    );
    assert_eq!(transport.calls.lock().unwrap().len(), 4);
}

#[test]
fn the_reserve_follows_each_response_not_a_separate_rate_limit_read() {
    // Real probe: rate_limit said 4999 while data headers said 4349. Here
    // the headers drop under the walk (another consumer spending) and the
    // walk stops on the observed value, before the reserve.
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    ROUTED_REMAINING.with(|r| r.set(103));
    let responses = vec![
        hdr(140, Ok(pull_json(H2, 2, "o/r"))),
        hdr(139, Ok(json!([{"sha": H1}, {"sha": H2}]).to_string())),
        gql(4000, no_force_pushes()),
        hdr(138, runs(vec![run("CR_a", H1)])),
        hdr(137, suites()),
        // Another consumer spent 33 between two of our responses.
        hdr(104, owners(&[])),
    ];
    let (mut client, transport) = shared_client(responses);
    let report = repair_check_history(&conn, &mut client, &[target()], &guarded(100)).unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Failed, "{outcome:?}");
    assert_eq!(outcome.rows_unchanged, Some(true));
    assert!(
        outcome.error.as_deref().unwrap().contains("reserve"),
        "{outcome:?}"
    );
    // H1's workflow runs answered 103: H2 is never started.
    assert_eq!(transport.calls.lock().unwrap().len(), 7);
    assert_eq!(status_of(&conn, "CR_a"), "IN_PROGRESS");
    assert_eq!(lock_count(&conn), 0);
}

#[test]
fn incomplete_alone_counts_as_not_complete() {
    let conn = warehouse();
    let mut client = client(current_head_only(vec![run_in("CR_nob", H2, 5)]));
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.count(HistoryStatus::Incomplete), 1);
    assert_eq!(report.not_complete(), 1);
}

#[test]
fn a_stale_lock_is_taken_over_and_only_our_lease_is_released() {
    let conn = warehouse();
    conn.execute(
        "INSERT INTO sync_locks (entity_key, started_at) VALUES ('repo:o/r', ?1)",
        [(chrono::Utc::now() - chrono::Duration::minutes(45)).to_rfc3339()],
    )
    .unwrap();
    let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
    assert_eq!(outcome.status, HistoryStatus::Complete);
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 0);
}

#[test]
fn a_lease_lost_to_another_owner_is_never_released_or_renewed() {
    let conn = warehouse();
    let token = sync_state::acquire_owned_lock(&conn, "repo:o/r").unwrap();
    // A fresh lock refuses a second owner.
    assert!(matches!(
        sync_state::acquire_owned_lock(&conn, "repo:o/r"),
        Err(Error::SyncInProgress(_))
    ));
    let renewed = sync_state::refresh_owned_lock(&conn, "repo:o/r", &token)
        .unwrap()
        .unwrap();
    // Another process takes the lock over (e.g. after a stale window).
    conn.execute(
        "UPDATE sync_locks SET started_at = 'other-owner' WHERE entity_key = 'repo:o/r'",
        [],
    )
    .unwrap();
    assert_eq!(
        sync_state::refresh_owned_lock(&conn, "repo:o/r", &renewed).unwrap(),
        None
    );
    assert!(!sync_state::release_owned_lock(&conn, "repo:o/r", &renewed).unwrap());
    let holder: String = conn
        .query_row("SELECT started_at FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(holder, "other-owner");
}

// ---- ownership evidence: provider facts only ----

/// Suites as [`suites`], plus 7 = our branch from a per-commit app (code
/// scanning), 8 = our branch with no repository, 9 = PR #2916's branch from
/// code scanning.
fn suites_extended() -> Result<String> {
    let app = |slug: &str| json!({"slug": slug});
    let mut list: Vec<Value> =
        serde_json::from_str::<Value>(&suites().unwrap()).unwrap()["check_suites"]
            .as_array()
            .unwrap()
            .clone();
    list.push(
        json!({"id": 7, "head_branch": "feat/five", "app": app("github-advanced-security"),
                     "repository": {"full_name": "o/r"}}),
    );
    list.push(json!({"id": 8, "head_branch": "feat/five", "app": app("github-actions")}));
    list.push(
        json!({"id": 9, "head_branch": "dev/bg-exec-3b-control-tool",
                     "app": app("github-advanced-security"),
                     "repository": {"full_name": "o/r"}}),
    );
    Ok(json!({"total_count": list.len(), "check_suites": list}).to_string())
}

fn run_app(id: &str, sha: &str, suite: i64, app: &str) -> Value {
    let mut value = run_in(id, sha, suite);
    value["app"] = json!({"slug": app});
    value
}

fn current_head_extended(list: Vec<Value>) -> Vec<Result<String>> {
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(list));
    responses.push(suites_extended());
    responses.push(owners(&[]));
    responses
}

#[test]
fn a_workflow_run_naming_several_pull_requests_is_ambiguous() {
    let conn = warehouse();
    set_workflows(
        H2,
        vec![workflow(
            1,
            H2,
            "pull_request",
            json!("feat/five"),
            "o/r",
            vec![
                pr_entry(5, "feat/five", "o/r"),
                pr_entry(3, "feat/five", "o/r"),
            ],
        )],
    );
    let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_v").unwrap();
    assert!(reason.contains("several pull requests"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn unvalidated_or_mismatched_workflow_identity_is_ambiguous() {
    // Each case breaks exactly one link of the identity chain.
    let cases: Vec<(&str, Value)> = vec![
        ("another repository's pull URL", {
            let mut pr = pr_entry(5, "feat/five", "o/r");
            pr["url"] = json!("https://api.github.com/repos/x/r/pulls/5");
            workflow(1, H2, "pull_request", json!("feat/five"), "o/r", vec![pr])
        }),
        ("a base in another repository", {
            let mut pr = pr_entry(5, "feat/five", "o/r");
            pr["base"]["repo"]["url"] = json!("https://api.github.com/repos/x/r");
            workflow(1, H2, "pull_request", json!("feat/five"), "o/r", vec![pr])
        }),
        ("an arbitrary body URL", {
            let mut pr = pr_entry(5, "feat/five", "o/r");
            pr["url"] = json!("see https://api.github.com/repos/o/r/pulls/5");
            workflow(1, H2, "pull_request", json!("feat/five"), "o/r", vec![pr])
        }),
        ("a same-named branch in a fork", {
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "someone/r",
                vec![pr_entry(5, "feat/five", "someone/r")],
            )
        }),
        ("a push event", {
            workflow(
                1,
                H2,
                "push",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            )
        }),
        ("a run before the pull request opened", {
            let mut wf = workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            );
            wf["created_at"] = json!("2026-08-01T00:00:00Z");
            wf
        }),
        ("a workflow branch differing from the suite", {
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/other"),
                "o/r",
                vec![pr_entry(5, "feat/other", "o/r")],
            )
        }),
    ];
    for (what, wf) in cases {
        let conn = warehouse();
        set_workflows(H2, vec![wf]);
        let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
        assert_eq!(outcome.status, HistoryStatus::Incomplete, "{what}");
        assert!(
            head(&outcome, H2).ambiguous.contains_key("CR_v"),
            "{what}: {:?}",
            head(&outcome, H2)
        );
        assert!(heads_of(&conn, "o/r#5").is_empty(), "{what}");
    }
    // No workflow run keyed by the suite, or two claiming it.
    for list in [
        vec![],
        vec![
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            ),
            {
                let mut twin = workflow(
                    1,
                    H2,
                    "pull_request",
                    json!("feat/five"),
                    "o/r",
                    vec![pr_entry(5, "feat/five", "o/r")],
                );
                twin["id"] = json!(99);
                twin
            },
        ],
    ] {
        let conn = warehouse();
        set_workflows(H2, list);
        let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
        assert!(head(&outcome, H2).ambiguous.contains_key("CR_v"));
        assert!(heads_of(&conn, "o/r#5").is_empty());
    }
}

#[test]
fn a_per_commit_app_pull_ref_is_never_proof() {
    // Reviewer finding: a refs/pull/N suite from a per-commit app named its
    // first creator; the early return accepted it before the app check.
    let conn = warehouse();
    let suites = || {
        Ok(json!({"total_count": 1, "check_suites": [
            {"id": 3, "head_branch": "refs/pull/5/head",
             "app": {"slug": "github-advanced-security"},
             "repository": {"full_name": "o/r"}}]})
        .to_string())
    };
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run_app(
        "CR_cs",
        H2,
        3,
        "github-advanced-security",
    )]));
    responses.push(suites());
    responses.push(owners(&[9]));
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_cs").unwrap();
    assert!(reason.contains("one suite per commit"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn a_per_commit_app_branch_is_never_proof_even_when_it_is_ours() {
    // Code scanning keeps one suite per commit: its head branch is the
    // first push, as in the CodeQL run whose suite named #2916's branch while
    // its output referred to #3004 and the warehouse held it under #2905.
    let conn = warehouse();
    let (mut client, transport) = shared_client(current_head_extended(vec![
        run_app("CR_cs", H2, 7, "github-advanced-security"),
        run_app("CR_cq", H2, 9, "github-advanced-security"),
    ]));
    let outcome = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap()
    .outcomes
    .remove(0);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert!(h2.ambiguous.contains_key("CR_cs"), "{h2:?}");
    assert!(
        h2.ambiguous.contains_key("CR_cq"),
        "another PR's branch from a per-commit app is not proof of elsewhere either"
    );
    assert_eq!(h2.attributed_elsewhere, 0);
    assert!(heads_of(&conn, "o/r#5").is_empty());
    let calls = transport.calls.lock().unwrap();
    assert!(
        calls
            .iter()
            .all(|call| !call[1].contains("pulls?state=all")),
        "branch owners are not read when no run could rest on them"
    );
}

#[test]
fn a_suite_without_a_repository_is_ambiguous() {
    let conn = warehouse();
    let outcome = repair(&conn, current_head_extended(vec![run_in("CR_nr", H2, 8)]));
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_nr").unwrap();
    assert!(reason.contains("repository"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn a_run_whose_app_differs_from_its_suite_is_ambiguous_even_on_a_pull_ref() {
    let conn = warehouse();
    let outcome = repair(
        &conn,
        current_head_only(vec![
            run_app("CR_m1", H2, 1, "some-other-app"),
            run_app("CR_m3", H2, 3, "some-other-app"),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert_eq!(h2.ambiguous.len(), 2, "{h2:?}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn a_stored_head_the_provider_shares_with_another_pull_request_is_not_ours_by_branch() {
    // H1 is a stored head of #5 (discovery only). The provider says #9 also
    // contains it, so our branch's suite cannot prove the run is #5's.
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_a", H1), run_in("CR_r", H1, 3)]));
    responses.push(suites());
    responses.push(owners(&[5, 9]));
    responses.push(runs(vec![run_in("CR_c", H2, 3)]));
    responses.push(suites());
    responses.push(owners(&[5]));
    // The workflow run behind suite 1 names no pull request (fork-like).
    set_workflows(
        H1,
        vec![
            workflow(1, H1, "pull_request", json!("feat/five"), "o/r", vec![]),
            workflow(3, H1, "dynamic", json!("refs/pull/5/head"), "o/r", vec![]),
        ],
    );
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h1 = head(&outcome, H1);
    assert!(h1.shared_head);
    assert!(h1.ambiguous.contains_key("CR_a"));
    assert!(
        h1.ambiguous.contains_key("CR_r"),
        "a bare pull ref on a shared commit is not proof: {h1:?}"
    );
    assert_eq!(h1.stored_unconfirmed, vec!["CR_a".to_string()]);
    // The unshared pull-ref run is proof; the stored row is left alone.
    assert_eq!(
        heads_of(&conn, "o/r#5"),
        vec![("CR_a".into(), H1.into()), ("CR_c".into(), H2.into())]
    );
}

// ---- repair-level fault injection ----

fn status_of(conn: &Connection, key: &str) -> String {
    conn.query_row(
        "SELECT status FROM fact_check_runs WHERE check_run_key = ?1",
        [key],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn an_unconfirmed_commit_stops_the_batch_and_reports_rows_unknown() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    arm_fault(Fault::Release);
    let (mut client, transport) = shared_client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed);
    assert_eq!(
        first.rows_unchanged, None,
        "a failed commit is never 'unchanged'"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("could not be confirmed")
    );
    let used = transport.calls.lock().unwrap().len();
    assert_eq!(used, 11, "the second target made no request");
    assert!(conn.is_autocommit(), "no savepoint left open");
    assert_eq!(lock_count(&conn), 0);
    // The JSON a caller emits carries every outcome and the stop.
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["outcomes"].as_array().unwrap().len(), 2);
    assert!(json["outcomes"][0]["rows_unchanged"].is_null());
    assert_eq!(json["outcomes"][1]["status"], "not_attempted");
    assert!(json["stopped"].is_string());
}

#[test]
fn a_failed_rollback_stops_the_batch_and_never_claims_rows_unchanged() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    conn.execute_batch(
        "CREATE TRIGGER refuse_c BEFORE INSERT ON fact_check_runs
         WHEN NEW.check_run_key = 'CR_c' BEGIN SELECT RAISE(ABORT, 'refused'); END;",
    )
    .unwrap();
    arm_fault(Fault::Rollback);
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed);
    assert_eq!(first.rows_unchanged, None);
    assert!(
        first.lock_release.is_some(),
        "an open transaction cannot release the lock: {first:?}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(report.stopped.is_some());
    // Leave the connection clean for the drop.
    conn.execute_batch(
        "ROLLBACK TO SAVEPOINT githubdw_check_history;
         RELEASE SAVEPOINT githubdw_check_history",
    )
    .unwrap();
    assert_eq!(status_of(&conn, "CR_a"), "IN_PROGRESS");
}

#[test]
fn a_lock_release_failure_keeps_the_outcome_and_stops_the_batch() {
    let conn = warehouse();
    arm_fault(Fault::LockRelease);
    let mut client = client(current_head_only(vec![run("CR_v", H2)]));
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(
        first.status,
        HistoryStatus::Incomplete,
        "a lease left behind is never complete: {first:?}"
    );
    assert_eq!((first.check_runs_before, first.check_runs_after), (0, 1));
    assert!(
        first
            .caveats
            .iter()
            .any(|c| c.contains("lease not released"))
    );
    assert!(
        first
            .lock_release
            .as_deref()
            .unwrap()
            .contains("could not be released")
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("could not be released")
    );
    assert_eq!(report.not_complete(), 2, "the CLI exits nonzero on this");
    // The lease is left behind for the stale window, never silently dropped.
    assert_eq!(lock_count(&conn), 1);
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["outcomes"][0]["status"], "incomplete");
    assert!(json["outcomes"][0]["lock_release"].is_string());
}

// ---- request cap ----

#[test]
fn the_request_cap_stops_a_long_page_walk_without_calling_the_provider() {
    let conn = warehouse();
    let page = || {
        let list: Vec<Value> = (0..100)
            .map(|i| run(&format!("CR_{i}_{}", rand_suffix()), H2))
            .collect();
        Ok(json!({"total_count": 1000, "check_runs": list}).to_string())
    };
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    for _ in 0..5 {
        responses.push(page());
    }
    let (mut client, transport) = shared_client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions {
            observed_only: false,
            rest_reserve: None,
            max_requests: Some(8),
        },
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Failed);
    assert!(
        outcome.error.as_deref().unwrap().contains("allowance"),
        "the sixth page is refused locally: {outcome:?}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert_eq!(transport.calls.lock().unwrap().len(), 8);
    assert!(heads_of(&conn, "o/r#5").is_empty());
    assert_eq!(lock_count(&conn), 0);
}

fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

// ---- a real SQLITE_BUSY on the commit ----

#[test]
fn a_reader_lock_that_makes_the_real_release_busy_never_claims_rows_unchanged() {
    // File-backed, rollback-journal mode: a second connection holding a
    // SHARED lock (an open read transaction) makes the outermost RELEASE
    // (the commit) fail with SQLITE_BUSY. Nothing is injected; the hook only
    // times the reader to start after the writes and before the commit.
    let dir = std::env::temp_dir().join(format!(
        "githubdw-busy-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("warehouse.db");
    let mut conn = Connection::open(&path).unwrap();
    schema::init(&mut conn).unwrap();
    // The warehouse defaults to WAL; force a rollback journal so a reader's
    // SHARED lock blocks the writer's commit.
    let _: String = conn
        .query_row("PRAGMA journal_mode = DELETE", [], |r| r.get(0))
        .unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "delete");
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
             head_ref, created_at, created_date_key, created_time_key)
         VALUES ('o/r#5', 5, 'o/r', 'user:a', 'MERGED', 'feat/five', '2026-09-01T00:00:00Z',
                 '2026-09-01', '00:00'),
                ('o/r#9', 9, 'o/r', 'user:a', 'MERGED', 'feat/nine', '2026-09-01T00:00:00Z',
                 '2026-09-01', '00:00');",
    )
    .unwrap();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");

    let reader = std::rc::Rc::new(Connection::open(&path).unwrap());
    let held = reader.clone();
    before_release(Box::new(move || {
        held.execute_batch("BEGIN").unwrap();
        let _: i64 = held
            .query_row("SELECT COUNT(*) FROM fact_check_runs", [], |r| r.get(0))
            .unwrap();
    }));
    let (mut client, transport) = shared_client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed, "{first:?}");
    assert_eq!(
        first.rows_unchanged, None,
        "a commit that hit SQLITE_BUSY is never reported unchanged"
    );
    let error = first.error.as_deref().unwrap().to_lowercase();
    assert!(
        error.contains("locked") || error.contains("busy"),
        "{error}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("could not be confirmed")
    );
    let used = transport.calls.lock().unwrap().len();
    assert_eq!(used, 11, "the second target made no request");
    // The full report still serializes for the caller.
    let json = serde_json::to_value(&report).unwrap();
    assert!(json["outcomes"][0]["rows_unchanged"].is_null());

    // Let the reader go, then show what actually happened on disk.
    reader.execute_batch("COMMIT").unwrap();
    if !conn.is_autocommit() {
        conn.execute_batch(
            "ROLLBACK TO SAVEPOINT githubdw_check_history;
             RELEASE SAVEPOINT githubdw_check_history",
        )
        .unwrap();
    }
    assert_eq!(status_of(&conn, "CR_a"), "IN_PROGRESS", "the rollback held");
    drop(conn);
    drop(reader);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- the c590f74a shape: one head shared by three pull requests ----

#[test]
fn stack_branch_runs_on_a_shared_head_import_only_with_workflow_identity() {
    // Same commit is the head of #5 (same-repo stack branch) and of a fork's
    // #9; suite 1 is our branch with the workflow run naming #5, suite 2 is a
    // fork pull_request_target run on a same-named branch with no pull
    // request listed, suite 3 is refs/pull/5/head, suite 7 is code scanning.
    let conn = warehouse();
    store(&conn, "CR_wrong", "o/r#9", H2, "COMPLETED");
    set_workflows(
        H2,
        vec![
            workflow(
                1,
                H2,
                "pull_request_target",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            ),
            workflow(
                2,
                H2,
                "pull_request_target",
                json!("feat/nine"),
                "fork/r",
                vec![],
            ),
            workflow(3, H2, "dynamic", json!("refs/pull/5/head"), "o/r", vec![]),
        ],
    );
    let outcome = repair(
        &conn,
        current_head_extended(vec![
            run_in("CR_stack", H2, 1),
            run_in("CR_wrong", H2, 1),
            run_in("CR_fork", H2, 2),
            run_in("CR_dyn", H2, 3),
            run_app("CR_codeql", H2, 7, "github-advanced-security"),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert!(h2.shared_head);
    assert!(h2.ambiguous.contains_key("CR_fork"), "{h2:?}");
    assert!(h2.ambiguous.contains_key("CR_dyn"), "{h2:?}");
    assert!(h2.ambiguous.contains_key("CR_codeql"), "{h2:?}");
    assert_eq!(
        h2.ownership_conflicts.get("CR_wrong").map(String::as_str),
        Some("o/r#9"),
        "the existing wrong owner is reported, never moved"
    );
    assert_eq!(
        heads_of(&conn, "o/r#5"),
        vec![("CR_stack".into(), H2.into())]
    );
    assert_eq!(
        heads_of(&conn, "o/r#9"),
        vec![("CR_wrong".into(), H2.into())]
    );
}

// ---- CODE BLOCKER reproduction: normal collection (not this repair) ----

#[test]
fn known_defect_normal_collection_stores_a_shared_head_run_under_whichever_pr_is_visited() {
    // sync.rs stores every check run on a pull request's CURRENT head under
    // that pull request via upsert_check_run, with no suite/app/workflow
    // attribution (fetch/pull_requests.rs parse_head_commit flattens all
    // suites). When two pull requests share the head, the first one visited
    // takes the row and the guarded upsert then keeps it there forever. This
    // characterizes today's behavior so a fix must flip it deliberately; it
    // is the likely origin of the eight c590f74a rows held by #2916/#2905
    // that workflow-run evidence attributes to #3004.
    let conn = warehouse();
    // Sync visits #9 first: the run triggered for #5 lands under #9.
    repository::upsert_check_run(
        &conn,
        "CR_for5",
        "o/r#9",
        H2,
        "job",
        "COMPLETED",
        None,
        None,
        None,
    )
    .unwrap();
    // Visiting #5 later cannot correct it.
    repository::upsert_check_run(
        &conn,
        "CR_for5",
        "o/r#5",
        H2,
        "job",
        "COMPLETED",
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        heads_of(&conn, "o/r#9"),
        vec![("CR_for5".into(), H2.into())]
    );
    assert!(heads_of(&conn, "o/r#5").is_empty());
}
