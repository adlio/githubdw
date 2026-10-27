//! Shared harness for the command-line tests: a temporary warehouse, the
//! real `githubdw` binary, and a stand-in `gh` on `PATH` that serves
//! fixture responses instead of talking to GitHub.

#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

use serde_json::{Value, json};

/// The repository every fixture lives in.
pub const REPO: &str = "octocat/hello";

/// A temporary warehouse plus a fake `gh` the binary finds first on `PATH`.
pub struct Cli {
    root: tempfile::TempDir,
    queued: std::cell::Cell<usize>,
}

/// Captured result of one `githubdw` run.
pub struct Run {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

impl From<Output> for Run {
    fn from(output: Output) -> Self {
        Self {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }
}

impl Run {
    /// Panic with both streams unless the run exited 0.
    pub fn ok(self) -> Self {
        assert!(
            self.success,
            "expected success\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        self
    }

    /// Panic with both streams unless the run exited non-zero.
    pub fn failed(self) -> Self {
        assert!(
            !self.success,
            "expected failure\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        );
        self
    }
}

/// The fake `gh`. `auth status` succeeds unless `auth_fail` exists. Any
/// other call is answered by the first `routes/*` file whose first line
/// occurs in the arguments, else by the oldest `queue/*` file (consumed),
/// else by `default`, else it fails like a non-retryable API error.
#[cfg(unix)]
const FAKE_GH: &str = r#"#!/bin/sh
dir="$FAKE_GH_DIR"
if [ "$1" = "auth" ]; then
  if [ -e "$dir/auth_fail" ]; then echo "not logged in" >&2; exit 1; fi
  exit 0
fi
args="$*"
for route in "$dir"/routes/*; do
  [ -e "$route" ] || continue
  pattern=$(head -n 1 "$route")
  case "$args" in
    *"$pattern"*) tail -n +2 "$route"; exit 0 ;;
  esac
done
next=$(ls "$dir/queue" 2>/dev/null | sort | head -n 1)
if [ -n "$next" ]; then
  cat "$dir/queue/$next"
  rm -f "$dir/queue/$next"
  exit 0
fi
if [ -e "$dir/default" ]; then cat "$dir/default"; exit 0; fi
echo "fake gh: no fixture for this call" >&2
exit 1
"#;

impl Cli {
    pub fn new() -> Self {
        let cli = Self {
            root: tempfile::tempdir().unwrap(),
            queued: std::cell::Cell::new(0),
        };
        fs::create_dir_all(cli.gh_dir().join("queue")).unwrap();
        fs::create_dir_all(cli.gh_dir().join("routes")).unwrap();
        fs::create_dir_all(cli.bin_dir()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let gh = cli.bin_dir().join("gh");
            fs::write(&gh, FAKE_GH).unwrap();
            fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
        }
        cli
    }

    pub fn db(&self) -> PathBuf {
        self.root.path().join("warehouse").join("githubdw.db")
    }

    fn gh_dir(&self) -> PathBuf {
        self.root.path().join("gh")
    }

    fn bin_dir(&self) -> PathBuf {
        self.root.path().join("bin")
    }

    /// Run `githubdw --db <temp> <arguments>` with the fake `gh` on `PATH`.
    pub fn run(&self, arguments: &[&str]) -> Run {
        fs::create_dir_all(self.db().parent().unwrap()).unwrap();
        let path = std::env::var_os("PATH").unwrap_or_default();
        let mut paths = vec![self.bin_dir()];
        paths.extend(std::env::split_paths(&path));
        Command::new(env!("CARGO_BIN_EXE_githubdw"))
            .arg("--db")
            .arg(self.db())
            .args(arguments)
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("FAKE_GH_DIR", self.gh_dir())
            .env("HOME", self.root.path())
            .output()
            .unwrap()
            .into()
    }

    /// Queue one response; queued responses are served oldest first.
    pub fn queue(&self, body: impl AsRef<str>) {
        let index = self.queued.get();
        self.queued.set(index + 1);
        let path = self.gh_dir().join("queue").join(format!("{index:04}"));
        fs::write(path, body.as_ref()).unwrap();
    }

    /// Answer every call whose arguments contain `pattern` with `body`.
    pub fn route(&self, pattern: &str, body: impl AsRef<str>) {
        let routes = self.gh_dir().join("routes");
        let index = fs::read_dir(&routes).unwrap().count();
        fs::write(
            routes.join(format!("{index:04}")),
            format!("{pattern}\n{}", body.as_ref()),
        )
        .unwrap();
    }

    /// Answer any otherwise unmatched call with `body`.
    pub fn default_response(&self, body: impl AsRef<str>) {
        fs::write(self.gh_dir().join("default"), body.as_ref()).unwrap();
    }

    /// Make `gh auth status` fail.
    pub fn fail_auth(&self) {
        fs::write(self.gh_dir().join("auth_fail"), "").unwrap();
    }

    /// Seed the warehouse by syncing the fixture repository through the CLI.
    #[cfg(unix)]
    pub fn seeded() -> Self {
        let cli = Self::new();
        cli.queue(pr_page(&[pr(1, "first feature"), pr(2, "second change")]));
        cli.queue(issue_page(&[issue(7, "crash on startup")]));
        cli.run(&["sync", "repo", REPO, "--skip-diffs"]).ok();
        cli
    }
}

impl Default for Cli {
    fn default() -> Self {
        Self::new()
    }
}

fn page_info() -> Value {
    json!({"hasNextPage": false, "endCursor": null})
}

fn rate_limit() -> Value {
    json!({"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"})
}

fn repository_header() -> Value {
    json!({
        "nameWithOwner": REPO,
        "primaryLanguage": {"name": "Rust"},
        "isFork": false,
        "isPrivate": false,
        "defaultBranchRef": {"name": "main"},
        "createdAt": "2020-01-01T00:00:00Z"
    })
}

/// A merged pull request by `octocat`, reviewed by `hubot`, with one file,
/// one conversation comment, and one check run on its head commit.
pub fn pr(number: i64, title: &str) -> Value {
    json!({
        "id": format!("PR_{number}"),
        "number": number,
        "title": title,
        "body": format!("body of {title}"),
        "state": "MERGED",
        "isDraft": false,
        "createdAt": "2026-01-05T18:00:00Z",
        "updatedAt": format!("2026-01-10T09:{number:02}:00Z"),
        "mergedAt": "2026-01-06T09:00:00Z",
        "closedAt": "2026-01-06T09:00:00Z",
        "baseRefName": "main",
        "headRefName": format!("feature/{number}"),
        "additions": 10,
        "deletions": 2,
        "changedFiles": 1,
        "author": {"login": "octocat", "__typename": "User"},
        "mergedBy": {"login": "hubot", "__typename": "User"},
        "reviews": {"pageInfo": page_info(), "nodes": [{
            "id": format!("REV{number}"), "state": "APPROVED", "body": "looks good",
            "submittedAt": "2026-01-06T08:00:00Z",
            "author": {"login": "hubot", "__typename": "User"}
        }]},
        "reviewThreads": {"pageInfo": page_info(), "nodes": []},
        "comments": {"pageInfo": page_info(), "nodes": [comment(&format!("C{number}"))]},
        "files": {"pageInfo": page_info(), "nodes": [{
            "path": "src/lib.rs", "changeType": "MODIFIED", "additions": 10, "deletions": 2
        }]},
        "commits": {"nodes": [head_commit(number)]}
    })
}

/// The head commit of fixture PR `number`, carrying one successful check run.
fn head_commit(number: i64) -> Value {
    let run = json!({
        "id": format!("RUN_{number}"), "name": "ci", "status": "COMPLETED",
        "conclusion": "SUCCESS", "startedAt": "2026-01-05T18:01:00Z",
        "completedAt": "2026-01-05T18:05:00Z"
    });
    json!({"commit": {
        "id": format!("COMMIT_{number}"),
        "oid": format!("{number:0>40}"),
        "checkSuites": {"pageInfo": page_info(), "nodes": [{
            "id": format!("SUITE_{number}"),
            "checkRuns": {"pageInfo": page_info(), "nodes": [run]}
        }]}
    }})
}

pub fn comment(id: &str) -> Value {
    json!({"id": id, "body": format!("comment {id} about the widget"),
           "createdAt": "2026-01-05T19:00:00Z",
           "author": {"login": "hubot", "__typename": "User"}})
}

/// A repository page of pull requests (the walk's GraphQL response).
pub fn pr_page(nodes: &[Value]) -> String {
    let mut repository = repository_header();
    repository["pullRequests"] = json!({"pageInfo": page_info(), "nodes": nodes});
    json!({"data": {"rateLimit": rate_limit(), "repository": repository}}).to_string()
}

/// A single-pull-request response (the targeted refresh's GraphQL response).
pub fn single_pr(node: Value) -> String {
    let mut repository = repository_header();
    repository["pullRequest"] = node;
    json!({"data": {"rateLimit": rate_limit(), "repository": repository}}).to_string()
}

/// An open issue by `octocat` with one label, assignee, and comment.
pub fn issue(number: i64, title: &str) -> Value {
    json!({
        "id": format!("ISS{number}"),
        "number": number,
        "title": title,
        "body": format!("details of {title}"),
        "state": "OPEN",
        "stateReason": null,
        "createdAt": "2026-02-01T12:00:00Z",
        "updatedAt": format!("2026-02-{number:02}T12:00:00Z"),
        "closedAt": null,
        "author": {"login": "octocat", "__typename": "User"},
        "milestone": null,
        "labels": {"pageInfo": page_info(), "nodes": [{
            "id": "LAB1", "name": "bug", "color": "d73a4a", "description": ""
        }]},
        "assignees": {"pageInfo": page_info(), "nodes": [{"login": "hubot", "__typename": "User"}]},
        "comments": {"pageInfo": page_info(), "nodes": [{
            "id": format!("ICOM{number}"), "body": "on it",
            "createdAt": "2026-02-02T08:00:00Z",
            "author": {"login": "hubot", "__typename": "User"}
        }]}
    })
}

pub fn issue_page(nodes: &[Value]) -> String {
    json!({"data": {"rateLimit": rate_limit(), "repository": {
        "nameWithOwner": REPO,
        "issues": {"pageInfo": page_info(), "nodes": nodes}
    }}})
    .to_string()
}

/// A single-issue response (the targeted refresh's GraphQL response).
pub fn single_issue(node: Value) -> String {
    json!({"data": {"rateLimit": rate_limit(), "repository": {
        "nameWithOwner": REPO,
        "issue": node
    }}})
    .to_string()
}

/// An empty search result (the user walk's GraphQL response).
pub fn empty_search() -> String {
    json!({"data": {"rateLimit": rate_limit(), "search": {
        "issueCount": 0, "pageInfo": page_info(), "nodes": []
    }}})
    .to_string()
}
