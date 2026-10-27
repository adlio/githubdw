//! `githubdw sync ...` against a stand-in `gh` serving fixture responses.
#![cfg(unix)]

mod support;

use serde_json::{Value, json};
use support::{
    Cli, REPO, comment, empty_search, issue, issue_page, pr, pr_page, single_issue, single_pr,
};

#[test]
fn repo_sync_reports_what_it_stored_then_reports_up_to_date() {
    let cli = Cli::new();
    let pulls = || pr_page(&[pr(2, "second change"), pr(1, "first feature")]);
    cli.queue(pulls());
    cli.queue(issue_page(&[issue(7, "crash on startup")]));
    let first = cli.run(&["sync", "repo", REPO, "--skip-diffs"]).ok();
    assert_eq!(
        first.stdout.trim(),
        "octocat/hello: synced 2 pull requests, 1 issues (2 pages, 0 failed)"
    );

    // Nothing changed since the last run: the newest pull request the
    // provider returns is older than the stored cursor.
    cli.queue(pr_page(&[pr(1, "first feature")]));
    let second = cli
        .run(&["sync", "repo", REPO, "--skip-diffs", "--pull-requests-only"])
        .ok();
    assert_eq!(second.stdout.trim(), "octocat/hello: already up to date");
}

#[test]
fn sync_refuses_to_start_without_an_authenticated_gh() {
    let cli = Cli::new();
    cli.fail_auth();
    for arguments in [
        vec!["sync", "repo", REPO],
        vec!["sync", "user", "octocat"],
        vec!["sync", "all"],
        vec!["sync", "targets", "--pr", "octocat/hello#1"],
        vec!["sync", "check-history", "--pr", "octocat/hello#1"],
    ] {
        let run = cli.run(&arguments).failed();
        assert!(
            run.stderr.contains("gh auth login"),
            "{arguments:?}: {}",
            run.stderr
        );
    }
}

#[test]
fn a_provider_error_fails_the_repo_sync() {
    let cli = Cli::new();
    let run = cli
        .run(&["sync", "repo", REPO, "--pull-requests-only"])
        .failed();
    assert!(run.stderr.contains("no fixture"), "{}", run.stderr);
}

#[test]
fn user_sync_with_nothing_new_is_up_to_date() {
    let cli = Cli::new();
    cli.route("search(", empty_search());
    let run = cli
        .run(&["sync", "user", "octocat", "--days", "2", "--skip-diffs"])
        .ok();
    assert_eq!(run.stdout.trim(), "octocat: already up to date");
}

#[test]
fn sync_all_walks_every_monitored_source_and_reports_failures_per_source() {
    let cli = Cli::new();
    cli.run(&["monitor", "add-org", "acme"]).ok();
    cli.run(&["monitor", "add-user", "octocat"]).ok();
    cli.run(&["monitor", "add-repo", REPO]).ok();
    cli.run(&["monitor", "add-repo", "octocat/broken"]).ok();
    cli.route("search(", empty_search());
    cli.route("issues(first", issue_page(&[]));
    cli.route(
        "name=hello",
        pr_page(&[pr(1, "first feature"), pr(2, "second change")]),
    );

    let run = cli.run(&["sync", "all"]).ok();
    assert!(
        run.stdout.contains("skipping org 'acme'"),
        "orgs are announced as unsupported:\n{}",
        run.stdout
    );
    assert!(
        run.stdout.contains("octocat/hello: 2 PRs, 0 issues"),
        "{}",
        run.stdout
    );
    assert!(
        run.stdout.contains("user octocat: 0 PRs, 0 issues"),
        "{}",
        run.stdout
    );
    // One failing source does not stop the others or fail the run.
    assert!(run.stderr.contains("octocat/broken: "), "{}", run.stderr);
    assert!(
        cli.run(&["monitor", "list"])
            .ok()
            .stdout
            .lines()
            .any(|line| line.contains("octocat/hello") && !line.ends_with("never")),
        "the synced repository records its last sync"
    );
}

#[test]
fn targets_refresh_pull_requests_and_issues() {
    let cli = Cli::seeded();
    let mut refreshed = pr(1, "first feature");
    refreshed["comments"]["nodes"] = json!([comment("C1"), comment("C1b")]);
    cli.route("pullRequest(number", single_pr(refreshed));
    cli.route("issue(number", single_issue(issue(7, "crash on startup")));

    let run = cli
        .run(&[
            "sync",
            "targets",
            "--pr",
            "https://github.com/octocat/hello/pull/1",
            "--issue",
            "octocat/hello#7",
            "--skip-diffs",
        ])
        .ok();
    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(
        lines,
        [
            "octocat/hello#1: refreshed (comments 1->2, reviews 1->1, review comments 0->0, \
             files 1->1, labels 0->0, assignees 0->0)",
            "octocat/hello#7: refreshed (comments 1->1, reviews 0->0, review comments 0->0, \
             files 0->0, labels 1->1, assignees 1->1)",
        ]
    );
}

#[test]
fn targets_report_caveats_as_incomplete() {
    let cli = Cli::seeded();
    cli.route("pullRequest(number", single_pr(pr(1, "first feature")));
    // Patch text comes from REST, which this provider refuses.
    let run = cli
        .run(&["sync", "targets", "--pr", "octocat/hello#1"])
        .ok();
    assert!(
        run.stdout
            .starts_with("octocat/hello#1: refreshed INCOMPLETE ("),
        "{}",
        run.stdout
    );
    assert!(
        run.stdout
            .contains("  caveat: PR #1: patch text not retrieved"),
        "{}",
        run.stdout
    );
}

#[test]
fn targets_json_reports_before_and_after_counts() {
    let cli = Cli::seeded();
    cli.route("pullRequest(number", single_pr(pr(1, "first feature")));
    let run = cli
        .run(&[
            "sync",
            "targets",
            "--pr",
            "octocat/hello#1",
            "--json",
            "--skip-diffs",
        ])
        .ok();
    let report: Value = serde_json::from_str(&run.stdout).unwrap();
    let outcome = &report["outcomes"][0];
    assert_eq!(outcome["status"], "refreshed");
    assert_eq!(outcome["before"]["comments"], 1);
    assert_eq!(outcome["after"]["reviews"], 1);
}

#[test]
fn a_failed_target_fails_the_run_and_names_the_target() {
    let cli = Cli::seeded();
    let run = cli
        .run(&["sync", "targets", "--pr", "octocat/hello#1", "--skip-diffs"])
        .failed();
    assert!(
        run.stderr
            .contains("octocat/hello#1: FAILED, stored rows unchanged:"),
        "{}",
        run.stderr
    );
    assert!(
        run.stderr.contains("1 of 1 target(s) failed"),
        "{}",
        run.stderr
    );
}

#[test]
fn bad_target_references_fail_before_any_network_call() {
    let cli = Cli::new();
    // No authenticated gh: a reference error must come first.
    cli.fail_auth();
    let run = cli.run(&["sync", "targets", "--pr", "nonsense"]).failed();
    assert!(
        run.stderr
            .contains("invalid pull request reference 'nonsense'"),
        "{}",
        run.stderr
    );
    let run = cli
        .run(&["sync", "targets", "--issue", "octocat/hello"])
        .failed();
    assert!(
        run.stderr.contains("invalid issue reference"),
        "{}",
        run.stderr
    );
    let run = cli.run(&["sync", "check-history", "--pr", "x#y"]).failed();
    assert!(
        run.stderr.contains("invalid pull request reference"),
        "{}",
        run.stderr
    );
    let run = cli.run(&["sync", "targets"]).failed();
    assert!(run.stderr.starts_with("error: "), "{}", run.stderr);
}

/// A provider response with the rate-limit headers `gh api --include` shows.
fn with_headers(resource: &str, body: Value) -> String {
    format!(
        "HTTP/2.0 200 OK\r\nX-Ratelimit-Remaining: 4000\r\nX-Ratelimit-Reset: 1\r\n\
         X-Ratelimit-Resource: {resource}\r\n\r\n{body}"
    )
}

/// The REST and GraphQL answers for re-reading PR #1's check history: one
/// commit (the stored head) carrying the stored run plus a re-run.
fn route_check_history(cli: &Cli) {
    let head = format!("{:0>40}", 1);
    let run = |id: &str, run_id: i64| {
        json!({"id": run_id, "node_id": id, "head_sha": head, "name": "ci",
               "status": "completed", "conclusion": "success",
               "started_at": "2026-01-05T18:01:00Z", "completed_at": "2026-01-05T18:05:00Z",
               "pull_requests": [], "check_suite": {"id": 1},
               "app": {"slug": "github-actions"}})
    };
    let workflow = json!({
        "id": 10_001, "check_suite_id": 1, "event": "pull_request",
        "head_branch": "feature/1", "head_sha": head, "created_at": "2026-01-05T18:00:00Z",
        "repository": {"full_name": REPO}, "head_repository": {"full_name": REPO},
        "pull_requests": [{
            "url": "https://api.github.com/repos/octocat/hello/pulls/1",
            "id": 1000, "number": 1,
            "head": {"ref": "feature/1", "sha": head,
                     "repo": {"url": "https://api.github.com/repos/octocat/hello"}},
            "base": {"ref": "main", "sha": "eeee",
                     "repo": {"url": "https://api.github.com/repos/octocat/hello"}}
        }]
    });
    cli.route(
        "/actions/runs?",
        with_headers(
            "core",
            json!({"total_count": 1, "workflow_runs": [workflow]}),
        ),
    );
    cli.route(
        "/check-runs?",
        with_headers(
            "core",
            json!({"total_count": 2, "check_runs": [run("RUN_1", 1), run("RUN_1b", 2)]}),
        ),
    );
    cli.route(
        "/check-suites?",
        with_headers(
            "core",
            json!({"total_count": 1, "check_suites": [{
                "id": 1, "head_branch": "feature/1", "app": {"slug": "github-actions"},
                "repository": {"full_name": REPO}
            }]}),
        ),
    );
    cli.route(
        &format!("/commits/{head}/pulls?"),
        with_headers("core", json!([{"number": 1}])),
    );
    cli.route(
        "/pulls/1/commits?",
        with_headers("core", json!([{"sha": head}])),
    );
    cli.route(
        "timelineItems",
        with_headers(
            "graphql",
            json!({"data": {"repository": {"pullRequest": {"timelineItems": {
                "filteredCount": 0,
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": []
            }}}}}),
        ),
    );
    cli.route(
        "repos/octocat/hello/pulls/1",
        with_headers(
            "core",
            json!({"head": {"sha": head, "ref": "feature/1", "repo": {"full_name": REPO}},
                   "base": {"repo": {"full_name": REPO}}, "commits": 1,
                   "created_at": "2026-01-05T18:00:00Z", "closed_at": "2026-01-06T09:00:00Z"}),
        ),
    );
}

#[test]
fn check_history_rereads_every_head() {
    let cli = Cli::seeded();
    route_check_history(&cli);
    let run = cli
        .run(&["sync", "check-history", "--pr", "octocat/hello#1"])
        .ok();
    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(
        lines,
        [
            "octocat/hello#1: complete (check runs 1->2, 1 commits read)",
            "  000000000000 upstream 2 stored 1->2 (+1 ~0)",
        ]
    );

    // Re-running the same target resumes idempotently: nothing new to store.
    let json: Value = serde_json::from_str(
        &cli.run(&["sync", "check-history", "--pr", "octocat/hello#1", "--json"])
            .ok()
            .stdout,
    )
    .unwrap();
    let outcome = &json["outcomes"][0];
    assert_eq!(outcome["status"], "complete");
    assert_eq!(outcome["check_runs_before"], 2);
    assert_eq!(outcome["check_runs_after"], 2);
}

#[test]
fn observed_only_check_history_is_never_complete() {
    let cli = Cli::seeded();
    route_check_history(&cli);
    let run = cli
        .run(&[
            "sync",
            "check-history",
            "--pr",
            "octocat/hello#1",
            "--observed-only",
        ])
        .failed();
    assert!(
        run.stdout.starts_with("octocat/hello#1: INCOMPLETE ("),
        "{}",
        run.stdout
    );
    assert!(run.stdout.contains("  caveat: "), "{}", run.stdout);
    assert!(
        run.stderr
            .contains("1 incomplete, 0 failed, 0 not attempted of 1 target(s)"),
        "{}",
        run.stderr
    );
}

#[test]
fn check_history_reports_a_spent_request_budget() {
    let cli = Cli::seeded();
    let run = cli
        .run(&[
            "sync",
            "check-history",
            "--pr",
            "octocat/hello#1",
            "--max-requests",
            "0",
        ])
        .failed();
    assert!(
        run.stderr
            .contains("octocat/hello#1: FAILED, stored rows unchanged:"),
        "{}",
        run.stderr
    );
    assert!(run.stderr.contains("batch stopped after"), "{}", run.stderr);
    assert!(
        run.stderr
            .contains("0 incomplete, 1 failed, 0 not attempted of 1 target(s)"),
        "{}",
        run.stderr
    );
}

#[test]
fn check_history_json_still_fails_the_run_when_incomplete() {
    let cli = Cli::seeded();
    let run = cli
        .run(&[
            "sync",
            "check-history",
            "--pr",
            "octocat/hello#1",
            "--pr",
            "octocat/hello#2",
            "--json",
            "--max-requests",
            "0",
        ])
        .failed();
    let report: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(report["outcomes"][0]["status"], "failed");
    assert_eq!(report["outcomes"][1]["status"], "not_attempted");
    assert!(report["stopped"].as_str().unwrap().contains("rerun"));
}

#[test]
fn repair_plan_lists_first_slice_truncation() {
    let cli = Cli::new();
    let mut crowded = pr(3, "busy discussion");
    crowded["comments"]["nodes"] = Value::Array(
        (0..50)
            .map(|index| comment(&format!("C3_{index}")))
            .collect(),
    );
    cli.queue(pr_page(&[crowded, pr(4, "quiet")]));
    cli.run(&["sync", "repo", REPO, "--skip-diffs", "--pull-requests-only"])
        .ok();

    let text = cli.run(&["sync", "repair-plan"]).ok().stdout;
    assert_eq!(
        text.trim(),
        "--pr octocat/hello#3\t# 50 comments (first-slice size)"
    );

    let json: Value =
        serde_json::from_str(&cli.run(&["sync", "repair-plan", "--json"]).ok().stdout).unwrap();
    assert_eq!(json[0]["target"]["number"], 3);

    assert_eq!(
        cli.run(&["sync", "repair-plan", "--repo", "other/repo"])
            .ok()
            .stdout
            .trim(),
        "no repair candidates"
    );
}

#[test]
fn check_history_plan_lists_pull_requests_by_stored_heads() {
    let cli = Cli::seeded();
    assert_eq!(
        cli.run(&["sync", "check-history-plan"]).ok().stdout.trim(),
        "no check-history candidates"
    );
    let text = cli
        .run(&[
            "sync",
            "check-history-plan",
            "--min-heads",
            "1",
            "--limit",
            "1",
        ])
        .ok()
        .stdout;
    assert_eq!(
        text.trim(),
        "--pr octocat/hello#1\t# 1 stored heads, 1 stored runs"
    );
    let json: Value = serde_json::from_str(
        &cli.run(&["sync", "check-history-plan", "--min-heads", "1", "--json"])
            .ok()
            .stdout,
    )
    .unwrap();
    assert_eq!(json.as_array().unwrap().len(), 2);
}

#[test]
fn plans_need_an_existing_warehouse() {
    let cli = Cli::new();
    // Plans open read-only and never create a database.
    let run = cli.run(&["sync", "repair-plan"]).failed();
    assert!(run.stderr.starts_with("error: "), "{}", run.stderr);
}

#[test]
fn watch_shows_recent_jobs_and_live_progress() {
    let cli = Cli::seeded();
    let idle = cli.run(&["sync", "watch"]).ok().stdout;
    let mut lines = idle.lines();
    assert_eq!(lines.next(), Some("no sync in progress"));
    let job = lines.next().unwrap();
    assert!(
        job.starts_with("repo:octocat/hello: completed (started "),
        "{job}"
    );
    assert!(job.ends_with(", 3 synced)"), "{job}");

    // Another process holds the repository's lock mid-walk.
    {
        let warehouse = githubdw::GithubDW::open(cli.db()).unwrap();
        let connection = warehouse.connection();
        githubdw::storage::sync_state_repository::acquire_lock(connection, "repo:octocat/hello")
            .unwrap();
        githubdw::storage::sync_state_repository::update_lock_progress(
            connection,
            "repo:octocat/hello",
            4,
            "octocat/hello#4",
            3,
            1,
            0,
        )
        .unwrap();
    }
    let busy = cli.run(&["sync", "watch"]).ok().stdout;
    assert!(
        busy.starts_with("repo:octocat/hello: item 4 (octocat/hello#4) since "),
        "{busy}"
    );
    assert!(busy.contains("3 synced, 1 skipped, 0 failed"), "{busy}");
}
