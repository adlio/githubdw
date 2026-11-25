//! `githubdw notes`: printing and writing change notes through the binary.

mod support;

use support::{Cli, REPO};

#[cfg(unix)]
#[test]
fn show_prints_each_pull_request_without_a_note_as_such() {
    let cli = Cli::seeded();
    let key = format!("{REPO}#1");
    let run = cli.run(&["notes", "show", &key]).ok();
    assert_eq!(run.stdout.trim(), format!("{key}\t(no note)"));
}

#[test]
fn summarize_needs_pull_requests_to_work_on() {
    let cli = Cli::new();
    let run = cli.run(&["notes", "summarize"]).failed();
    assert!(
        run.stderr
            .contains("name pull requests, or pass --author or --repo"),
        "{}",
        run.stderr
    );
}

#[cfg(unix)]
#[test]
fn summarize_without_a_model_says_how_to_set_one() {
    let cli = Cli::seeded();
    let run = cli.run(&["notes", "summarize", "--repo", REPO]).failed();
    let expected = if cfg!(feature = "summaries") {
        "githubdw config set summaries.model"
    } else {
        "cargo install githubdw --features summaries"
    };
    assert!(run.stderr.contains(expected), "{}", run.stderr);
}

/// With no AWS credentials anywhere (HOME is a temporary directory) every
/// model request fails: the run still reports what it did and exits 0, and
/// the pull requests are left for the next run.
#[cfg(all(unix, feature = "summaries"))]
#[test]
fn summarize_reports_failed_requests_and_writes_nothing() {
    let cli = Cli::seeded();
    cli.run(&["config", "set", "summaries.model", "us.example.model-v1:0"])
        .ok();
    cli.run(&["config", "set", "summaries.region", "us-west-2"])
        .ok();
    let run = cli
        .run(&[
            "notes",
            "summarize",
            "--author",
            "octocat",
            "--since",
            "2000-01-01",
            "--aws-profile",
            "no-such-profile",
            "--no-diffs",
        ])
        .ok();
    assert!(run.stdout.starts_with("0 written"), "{}", run.stdout);
    assert!(run.stderr.contains("change_notes"), "{}", run.stderr);
}
