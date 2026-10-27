//! Fixtures and transports shared by these tests.

use super::*;

pub(super) const H1: &str = "1111111111111111111111111111111111111111";
pub(super) const H2: &str = "2222222222222222222222222222222222222222";
pub(super) const C3: &str = "3333333333333333333333333333333333333333";

pub(super) fn warehouse() -> Connection {
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

pub(super) fn store(conn: &Connection, key: &str, pr: &str, sha: &str, status: &str) {
    repository::upsert_check_run(conn, key, pr, sha, "job", status, None, None, None).unwrap();
}

/// A run in suite 1 (created by branch `feat/five`, i.e. PR #5's branch).
pub(super) fn run(id: &str, sha: &str) -> Value {
    run_in(id, sha, 1)
}

pub(super) fn run_in(id: &str, sha: &str, suite: i64) -> Value {
    json!({"id": 1, "node_id": id, "head_sha": sha, "name": "job", "status": "completed",
           "conclusion": "success", "started_at": "2026-09-01T00:00:00Z",
           "completed_at": "2026-09-01T00:01:00Z", "pull_requests": [],
           "check_suite": {"id": suite}, "app": {"slug": "github-actions"}})
}

/// The suites every fixture commit carries: 1 = feat/five, 2 = feat/nine,
/// 3 = refs/pull/5/head, 4 = refs/pull/9/head, 5 = null branch, 6 = unknown app.
pub(super) fn suites() -> Result<String> {
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

pub(super) fn runs(list: Vec<Value>) -> Result<String> {
    Ok(json!({"total_count": list.len(), "check_runs": list}).to_string())
}

pub(super) fn pull_json(head: &str, count: usize, head_repo: &str) -> String {
    json!({"head": {"sha": head, "ref": "feat/five", "repo": {"full_name": head_repo}},
           "base": {"repo": {"full_name": "o/r"}}, "commits": count,
           "created_at": "2026-08-15T00:00:00Z", "closed_at": null})
    .to_string()
}

pub(super) fn pull(head: &str, commits: &[&str]) -> Vec<Result<String>> {
    vec![
        Ok(pull_json(head, commits.len(), "o/r")),
        Ok(Value::Array(commits.iter().map(|sha| json!({"sha": sha})).collect()).to_string()),
    ]
}

pub(super) fn no_force_pushes() -> Result<String> {
    Ok(
        json!({"data": {"repository": {"pullRequest": {"timelineItems": {
        "filteredCount": 0, "pageInfo": {"hasNextPage": false, "endCursor": null},
        "nodes": []}}}}})
        .to_string(),
    )
}

/// `commits/{sha}/pulls`: the provider's owners of a commit.
pub(super) fn owners(numbers: &[i64]) -> Result<String> {
    Ok(Value::Array(numbers.iter().map(|n| json!({"number": n})).collect()).to_string())
}

// `actions/runs?head_sha=` answers, by commit, overriding the default.
thread_local! {
    pub(super) static WORKFLOWS: std::cell::RefCell<std::collections::BTreeMap<String, Vec<Value>>> =
        const { std::cell::RefCell::new(std::collections::BTreeMap::new()) };
}

pub(super) fn set_workflows(sha: &str, list: Vec<Value>) {
    WORKFLOWS.with(|map| map.borrow_mut().insert(sha.to_string(), list));
}

pub(super) fn pr_entry(number: i64, head_ref: &str, head_repo: &str) -> Value {
    json!({"url": format!("https://api.github.com/repos/o/r/pulls/{number}"),
           "id": number * 1000, "number": number,
           "head": {"ref": head_ref, "sha": "ffff",
                    "repo": {"url": format!("https://api.github.com/repos/{head_repo}")}},
           "base": {"ref": "main", "sha": "eeee",
                    "repo": {"url": "https://api.github.com/repos/o/r"}}})
}

/// One workflow run as `actions/runs` reports it (real field shape).
pub(super) fn workflow(
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
pub(super) fn default_workflows(sha: &str) -> Vec<Value> {
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
    pub(super) static ROUTED_REMAINING: std::cell::Cell<i64> = const { std::cell::Cell::new(4000) };
}

pub(super) fn with_rate_headers(resource: &str, remaining: i64, body: &str) -> String {
    format!(
        "HTTP/2.0 200 OK\r\nX-Ratelimit-Remaining: {remaining}\r\nX-Ratelimit-Reset: 1\r\n\
         X-Ratelimit-Resource: {resource}\r\n\r\n{body}"
    )
}

/// Serves workflow-run lookups from [`WORKFLOWS`] / [`default_workflows`] and
/// everything else from the queued fixture responses, recording every call.
pub(super) struct Routed(pub(super) std::sync::Arc<FixtureTransport>);

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

pub(super) fn client(responses: Vec<Result<String>>) -> GhClient {
    shared_client(responses).0
}

pub(super) fn target() -> RefreshTarget {
    RefreshTarget::parse(TargetKind::PullRequest, "o/r#5").unwrap()
}

pub(super) fn heads_of(conn: &Connection, pr: &str) -> Vec<(String, String)> {
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
pub(super) fn two_head_responses() -> Vec<Result<String>> {
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

pub(super) fn repair(conn: &Connection, responses: Vec<Result<String>>) -> HistoryOutcome {
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

pub(super) fn head<'a>(outcome: &'a HistoryOutcome, sha: &str) -> &'a HeadOutcome {
    outcome.heads.iter().find(|head| head.sha == sha).unwrap()
}

/// Current head H2 only, with the given runs; the provider lists no owner
/// for the commit. Workflow runs come from the routed transport.
pub(super) fn current_head_only(list: Vec<Value>) -> Vec<Result<String>> {
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(list));
    responses.push(suites());
    responses.push(owners(&[]));
    responses
}

pub(super) fn lock_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap()
}

/// A client whose transport stays inspectable after the call.
pub(super) fn shared_client(
    responses: Vec<Result<String>>,
) -> (GhClient, std::sync::Arc<FixtureTransport>) {
    let transport = std::sync::Arc::new(FixtureTransport::new(responses));
    let client = GhClient::with_transport(Box::new(Routed(transport.clone()))).without_sleeping();
    (client, transport)
}

pub(super) fn nine() -> RefreshTarget {
    RefreshTarget::parse(TargetKind::PullRequest, "o/r#9").unwrap()
}

pub(super) fn guarded(reserve: i64) -> CheckHistoryOptions {
    CheckHistoryOptions {
        observed_only: false,
        rest_reserve: Some(reserve),
        max_requests: None,
    }
}

/// Suites as [`suites`], plus 7 = our branch from a per-commit app (code
/// scanning), 8 = our branch with no repository, 9 = PR #2916's branch from
/// code scanning.
pub(super) fn suites_extended() -> Result<String> {
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

pub(super) fn run_app(id: &str, sha: &str, suite: i64, app: &str) -> Value {
    let mut value = run_in(id, sha, suite);
    value["app"] = json!({"slug": app});
    value
}

pub(super) fn current_head_extended(list: Vec<Value>) -> Vec<Result<String>> {
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(list));
    responses.push(suites_extended());
    responses.push(owners(&[]));
    responses
}

pub(super) fn status_of(conn: &Connection, key: &str) -> String {
    conn.query_row(
        "SELECT status FROM fact_check_runs WHERE check_run_key = ?1",
        [key],
        |r| r.get(0),
    )
    .unwrap()
}
