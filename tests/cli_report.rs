//! `githubdw query`, `metrics`, and `search` against a warehouse seeded
//! through the CLI's own `sync repo` path.
#![cfg(unix)]

mod support;

use serde_json::Value;
use support::{Cli, REPO, issue, issue_page, pr, pr_page};

#[test]
fn query_table_lists_pull_requests() {
    let cli = Cli::seeded();
    let run = cli.run(&["query"]).ok();
    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(
        lines,
        [
            "octocat/hello#1\tMERGED\toctocat\t2026-01-05T18:00:00Z\tfirst feature",
            "octocat/hello#2\tMERGED\toctocat\t2026-01-05T18:00:00Z\tsecond change",
        ]
    );
}

#[test]
fn query_table_truncates_after_25_rows() {
    let cli = Cli::new();
    let nodes: Vec<Value> = (1..=27).map(|n| pr(n, &format!("change {n}"))).collect();
    cli.queue(pr_page(&nodes));
    cli.run(&["sync", "repo", REPO, "--skip-diffs", "--pull-requests-only"])
        .ok();

    let run = cli.run(&["query"]).ok();
    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(lines.len(), 26, "{}", run.stdout);
    assert_eq!(lines[25], "… and 2 more");
}

#[test]
fn query_json_and_csv_carry_the_same_rows() {
    let cli = Cli::seeded();

    let json: Value =
        serde_json::from_str(&cli.run(&["query", "--output", "json"]).ok().stdout).unwrap();
    let keys: Vec<&str> = json
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["pr_key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["octocat/hello#1", "octocat/hello#2"]);
    assert_eq!(json[0]["review_count"], 1);

    let csv = cli.run(&["query", "--output", "csv"]).ok().stdout;
    let mut lines = csv.lines();
    assert!(
        lines
            .next()
            .unwrap()
            .starts_with("pr_key,number,repo,author")
    );
    assert_eq!(lines.count(), 2);
}

#[test]
fn query_counts_and_groupings() {
    let cli = Cli::seeded();
    let output = |format: &str, extra: &[&str]| {
        let mut arguments = vec!["query", "--output", format];
        arguments.extend_from_slice(extra);
        cli.run(&arguments).ok().stdout
    };
    assert_eq!(output("count", &[]).trim(), "2");
    assert_eq!(output("count-by-author", &[]).trim(), "octocat\t2");
    assert_eq!(output("count-by-repo", &[]).trim(), "octocat/hello\t2");
    assert_eq!(output("count-by-state", &[]).trim(), "MERGED\t2");
    assert!(
        output("count-by-period", &[]).trim().ends_with("\t2"),
        "{}",
        output("count-by-period", &[])
    );
}

#[test]
fn query_filters_narrow_the_rows() {
    let cli = Cli::seeded();
    let count = |extra: &[&str]| {
        let mut arguments = vec!["query", "--output", "count"];
        arguments.extend_from_slice(extra);
        cli.run(&arguments).ok().stdout.trim().to_string()
    };
    assert_eq!(count(&["--author", "octocat", "--merged"]), "2");
    assert_eq!(count(&["--reviewer", "hubot", "--repo", REPO]), "2");
    assert_eq!(count(&["--org", "octocat", "--state", "merged"]), "2");
    assert_eq!(count(&["--state", "open"]), "0");
    assert_eq!(count(&["--author", "nobody"]), "0");
    assert_eq!(
        count(&["--since", "2026-01-01", "--until", "2026-01-31"]),
        "2"
    );
    assert_eq!(count(&["--period", "2025-12"]), "0");
    assert_eq!(count(&["--label", "bug"]), "0");

    let page = cli
        .run(&["query", "--limit", "1", "--offset", "1"])
        .ok()
        .stdout;
    assert_eq!(page.lines().count(), 1, "{page}");
    assert!(page.starts_with("octocat/hello#2\t"), "{page}");
}

#[test]
fn query_rejects_bad_arguments() {
    let cli = Cli::seeded();
    for (arguments, needle) in [
        (vec!["query", "--state", "sideways"], "sideways"),
        (vec!["query", "--since", "January"], "bad date 'January'"),
        (
            vec!["query", "--until", "2026-13-01"],
            "bad date '2026-13-01'",
        ),
        (
            vec!["query", "--output", "yaml"],
            "unknown output format 'yaml'",
        ),
    ] {
        let run = cli.run(&arguments).failed();
        assert!(run.stderr.contains(needle), "{arguments:?}: {}", run.stderr);
    }
}

#[test]
fn user_metrics_text_and_json() {
    let cli = Cli::seeded();
    let text = cli
        .run(&["metrics", "user", "octocat", "--period", "2026-01"])
        .ok()
        .stdout;
    assert!(
        text.starts_with("User octocat — 2026-01 (vs 2025-12)"),
        "{text}"
    );
    assert!(text.contains("PRs merged:       2 (+2)"), "{text}");
    assert!(text.contains("Top reviewers:"), "{text}");
    assert!(text.contains("hubot — 2"), "{text}");

    let json: Value = serde_json::from_str(
        &cli.run(&[
            "metrics", "user", "octocat", "--period", "2026-01", "--top", "1", "--json",
        ])
        .ok()
        .stdout,
    )
    .unwrap();
    assert_eq!(json["metrics"]["period_key"], "2026-01");
    assert_eq!(
        json["aggregations"]["top_reviewers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn reviewer_metrics_leave_empty_leaderboards_out() {
    let cli = Cli::seeded();
    let text = cli
        .run(&["metrics", "user", "hubot", "--period", "2026-01"])
        .ok()
        .stdout;
    assert!(text.contains("Reviews given:    2 (+2)"), "{text}");
    assert!(!text.contains("Top reviewers:"), "{text}");
}

#[test]
fn repo_metrics_text_and_json() {
    let cli = Cli::seeded();
    let text = cli
        .run(&["metrics", "repo", REPO, "--period", "2026-01"])
        .ok()
        .stdout;
    assert!(text.starts_with("Repo octocat/hello — 2026-01"), "{text}");
    assert!(text.contains("Check failure rate: 0.0%"), "{text}");
    assert!(text.contains("Top commenters:"), "{text}");

    let json: Value = serde_json::from_str(
        &cli.run(&["metrics", "repo", REPO, "--period", "2026-01", "--json"])
            .ok()
            .stdout,
    )
    .unwrap();
    assert_eq!(json["metrics"]["repository"], REPO);
}

#[test]
fn group_metrics_for_user_and_repo_groups() {
    let cli = Cli::seeded();
    cli.run(&["group", "user", "create", "team"]).ok();
    cli.run(&["group", "user", "add", "team", "octocat"]).ok();
    cli.run(&["group", "repo", "create", "apps"]).ok();
    cli.run(&["group", "repo", "add", "apps", REPO]).ok();

    let users = cli
        .run(&["metrics", "group", "team", "--period", "2026-01"])
        .ok()
        .stdout;
    assert!(
        users.starts_with("Group team (user group, 1 members)"),
        "{users}"
    );
    assert!(users.contains("PRs merged: 2 (+2)"), "{users}");
    assert!(users.contains("Reviews given (to non-members):"), "{users}");

    let repos = cli
        .run(&["metrics", "group", "apps", "--period", "2026-01"])
        .ok()
        .stdout;
    assert!(repos.contains("Reviews: 2 (+2)"), "{repos}");
    assert!(repos.contains("Comments: 2 (+2)"), "{repos}");

    let json: Value = serde_json::from_str(
        &cli.run(&["metrics", "group", "apps", "--period", "2026-01", "--json"])
            .ok()
            .stdout,
    )
    .unwrap();
    assert_eq!(json["group_name"], "apps");
}

#[test]
fn metrics_reject_a_bad_period() {
    let cli = Cli::seeded();
    let run = cli
        .run(&["metrics", "user", "octocat", "--period", "someday"])
        .failed();
    assert!(run.stderr.starts_with("error: "), "{}", run.stderr);
}

#[test]
fn search_finds_pull_requests_issues_and_comments() {
    let cli = Cli::seeded();

    let comments = cli.run(&["search", "widget", "--comments"]).ok().stdout;
    assert_eq!(comments.lines().count(), 2, "{comments}");
    assert!(
        comments.contains("octocat/hello#1\tissue_comment"),
        "{comments}"
    );

    let issues = cli.run(&["search", "crash", "--issues"]).ok().stdout;
    assert!(
        issues.starts_with("octocat/hello#7\tissue\tcrash on startup\t"),
        "{issues}"
    );

    let prs = cli
        .run(&[
            "search",
            "second",
            "--pull-requests",
            "--repo",
            "Octocat/Hello",
        ])
        .ok()
        .stdout;
    assert!(prs.starts_with("octocat/hello#2\t"), "{prs}");

    let json: Value = serde_json::from_str(
        &cli.run(&["search", "widget", "--json", "--limit", "1"])
            .ok()
            .stdout,
    )
    .unwrap();
    assert_eq!(json.as_array().unwrap().len(), 1);
}

#[test]
fn search_reports_no_matches_and_rejects_two_scopes() {
    let cli = Cli::seeded();
    assert_eq!(
        cli.run(&["search", "zebra"]).ok().stdout.trim(),
        "no matches"
    );
    let run = cli
        .run(&["search", "widget", "--issues", "--comments"])
        .failed();
    assert!(run.stderr.contains("pick at most one"), "{}", run.stderr);
}

#[test]
fn issues_sync_alongside_pull_requests() {
    let cli = Cli::new();
    cli.queue(issue_page(&[issue(3, "flaky test"), issue(4, "docs typo")]));
    let run = cli.run(&["sync", "repo", REPO, "--issues-only"]).ok();
    assert_eq!(
        run.stdout.trim(),
        "octocat/hello: synced 0 pull requests, 2 issues (1 pages, 0 failed)"
    );
    assert!(
        cli.run(&["search", "typo", "--issues"])
            .ok()
            .stdout
            .starts_with("octocat/hello#4\t")
    );
}
