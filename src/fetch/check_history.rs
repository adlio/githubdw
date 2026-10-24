//! Check runs for a pull request's historical head commits.
//!
//! The page queries read check runs from the pull request's CURRENT head
//! commit only, so every run recorded against an earlier head is whatever an
//! earlier sync happened to see while that commit was the head. A push between
//! two syncs, or a re-run after the head moved on, never lands. This module
//! reads the provider's per-commit check-run list (REST
//! `commits/{sha}/check-runs?filter=all`, which also returns superseded
//! re-runs) for an explicit set of commits, with strict pagination, and
//! discovers the commits a pull request has had: its commit list and its
//! force-push history.
//!
//! Every fetch either returns the provider's complete answer or fails; a
//! commit the provider no longer serves (garbage-collected after a force push)
//! is reported as unavailable rather than as an empty list.

use std::collections::HashSet;

use serde_json::Value;

use super::GhClient;
use crate::error::{Error, Result};

/// REST page size (the provider maximum).
const PAGE_SIZE: usize = 100;
/// Page bound for one commit's check runs (10,000 runs).
const MAX_CHECK_RUN_PAGES: usize = 100;
/// GitHub lists at most this many commits for one pull request.
pub const PR_COMMIT_LIST_LIMIT: usize = 250;
/// Page bound for the force-push timeline walk.
const MAX_TIMELINE_PAGES: usize = 50;

/// One check run as the provider reports it for a specific commit.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitCheckRun {
    /// GraphQL node id (`CR_...`), the warehouse's `check_run_key`.
    pub node_id: String,
    /// The commit the run belongs to, as the provider states it.
    pub head_sha: String,
    pub name: String,
    /// Upper-cased to match the GraphQL enum the page queries store.
    pub status: String,
    pub conclusion: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    /// Open pull requests the provider associates with the run. Empty for
    /// closed and merged pull requests and for forks, so an empty list says
    /// nothing; a non-empty list that omits a pull request excludes it.
    pub pull_requests: Vec<i64>,
    /// The check suite the run belongs to.
    pub suite_id: Option<i64>,
    /// The GitHub App that created the run.
    pub app_slug: Option<String>,
}

/// One check suite on a commit. A suite is created for an app and a commit;
/// `head_branch` is the branch of the push or pull request ref that created
/// it, so on a commit shared by several branches it names only the first.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckSuite {
    pub id: i64,
    pub head_branch: Option<String>,
    pub app_slug: Option<String>,
    /// `owner/name` of the repository the suite belongs to, lower-cased.
    pub repository: Option<String>,
}

/// The provider's answer for one commit.
#[derive(Debug, Clone, PartialEq)]
pub enum CommitCheckRuns {
    /// Every run the provider lists for the commit (`total_count` matched).
    Complete(Vec<CommitCheckRun>),
    /// The provider no longer serves the commit (404/422).
    Unavailable(String),
}

/// Pull request metadata the history walk needs.
#[derive(Debug, Clone, PartialEq)]
pub struct PullRequestCommits {
    pub head_sha: String,
    /// Commit count the provider reports for the pull request.
    pub commit_count: i64,
    /// The pull request's head branch name (`head.ref`).
    pub head_ref: Option<String>,
    /// True when the head repository differs from the base repository (a
    /// fork), so a branch name alone cannot identify the pull request.
    pub cross_repository: bool,
    /// Commits the provider listed (at most [`PR_COMMIT_LIST_LIMIT`]).
    pub commits: Vec<String>,
    /// `owner/name` of the head repository, lower-cased; `None` if deleted.
    pub head_repository: Option<String>,
    /// When the pull request was opened and (if so) closed, RFC 3339.
    pub created_at: Option<String>,
    pub closed_at: Option<String>,
}

/// One pull request a workflow run names, as the provider reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct RunPullRequest {
    pub number: i64,
    /// API URL of the pull request (`.../repos/{owner}/{name}/pulls/{n}`).
    pub url: Option<String>,
    pub head_ref: Option<String>,
    /// API URL of the head and base repositories (`.../repos/{owner}/{name}`).
    pub head_repo_url: Option<String>,
    pub base_repo_url: Option<String>,
}

/// One GitHub Actions workflow run on a commit (`actions/runs?head_sha=`).
/// Each run owns exactly one check suite (`check_suite_id`), and unlike the
/// suite it records the triggering event and head repository, which is what
/// separates a fork's branch from a same-named branch in the base repository.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowRun {
    pub id: i64,
    pub check_suite_id: Option<i64>,
    pub event: Option<String>,
    pub head_branch: Option<String>,
    /// Lower-cased `owner/name`.
    pub repository: Option<String>,
    pub head_repository: Option<String>,
    pub created_at: Option<String>,
    pub pull_requests: Vec<RunPullRequest>,
}

/// One force push recorded on the pull request timeline.
#[derive(Debug, Clone, PartialEq)]
pub struct ForcePush {
    /// `None` when the provider no longer resolves the replaced commit.
    pub before: Option<String>,
    pub after: Option<String>,
}

fn is_unavailable(error: &Error) -> bool {
    match error {
        Error::GhCommand { stderr, .. } => {
            let lowered = stderr.to_lowercase();
            lowered.contains("http 404") || lowered.contains("http 422")
        }
        _ => false,
    }
}

fn upper(value: Option<&str>) -> Option<String> {
    value
        .filter(|text| !text.is_empty())
        .map(str::to_ascii_uppercase)
}

fn parse_check_run(run: &Value, requested_sha: &str) -> Result<CommitCheckRun> {
    let text = |field: &str| run.get(field).and_then(Value::as_str);
    let node_id = text("node_id")
        .filter(|id| !id.is_empty())
        .ok_or_else(|| Error::GitHubApi(format!("check run on {requested_sha} has no node_id")))?
        .to_string();
    let head_sha = text("head_sha")
        .ok_or_else(|| Error::GitHubApi(format!("check run {node_id} has no head_sha")))?
        .to_string();
    if !head_sha.eq_ignore_ascii_case(requested_sha) {
        return Err(Error::GitHubApi(format!(
            "check run {node_id} listed for {requested_sha} reports head {head_sha}"
        )));
    }
    let status = upper(text("status"))
        .ok_or_else(|| Error::GitHubApi(format!("check run {node_id} has no status")))?;
    let pull_requests = run
        .get("pull_requests")
        .and_then(Value::as_array)
        .map(|prs| {
            prs.iter()
                .filter_map(|pr| pr.get("number").and_then(Value::as_i64))
                .collect()
        })
        .unwrap_or_default();
    let suite_id = run
        .get("check_suite")
        .and_then(|suite| suite.get("id"))
        .and_then(Value::as_i64);
    let app_slug = run
        .get("app")
        .and_then(|app| app.get("slug"))
        .and_then(Value::as_str)
        .filter(|slug| !slug.is_empty())
        .map(str::to_string);
    Ok(CommitCheckRun {
        suite_id,
        app_slug,
        name: text("name").unwrap_or_default().to_string(),
        status,
        conclusion: upper(text("conclusion")),
        started_at: text("started_at").map(str::to_string),
        completed_at: text("completed_at").map(str::to_string),
        head_sha: requested_sha.to_ascii_lowercase(),
        node_id,
        pull_requests,
    })
}

/// Every check run on `sha`, including superseded re-runs (`filter=all`).
///
/// Pages until the distinct runs collected equal the provider's
/// `total_count`. A page that comes back short before that, a page bound
/// overrun, or a count that disagrees with the collected set is an error, so
/// a caller never stores a partial list as if it were complete.
pub fn fetch_commit_check_runs(
    client: &mut GhClient,
    repository: &str,
    sha: &str,
) -> Result<CommitCheckRuns> {
    let mut runs: Vec<CommitCheckRun> = Vec::new();
    let mut seen = HashSet::new();
    let mut expected: Option<usize> = None;
    for page in 1..=MAX_CHECK_RUN_PAGES {
        let path = format!(
            "repos/{repository}/commits/{sha}/check-runs?filter=all&per_page={PAGE_SIZE}&page={page}"
        );
        let body = match client.rest(&path) {
            Ok(body) => body,
            Err(error) if page == 1 && is_unavailable(&error) => {
                return Ok(CommitCheckRuns::Unavailable(error.to_string()));
            }
            Err(error) => return Err(error),
        };
        let total = body
            .get("total_count")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "check runs for {sha} page {page} has no total_count"
                ))
            })? as usize;
        if *expected.get_or_insert(total) != total {
            return Err(Error::GitHubApi(format!(
                "check runs for {sha} changed while paging ({} then {total}); retry",
                expected.unwrap_or_default()
            )));
        }
        let nodes = body
            .get("check_runs")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "check runs for {sha} page {page} has no check_runs"
                ))
            })?;
        for node in nodes {
            let run = parse_check_run(node, sha)?;
            if seen.insert(run.node_id.clone()) {
                runs.push(run);
            }
        }
        if runs.len() >= total {
            if runs.len() > total {
                return Err(Error::GitHubApi(format!(
                    "check runs for {sha}: {} distinct runs exceed total_count {total}",
                    runs.len()
                )));
            }
            return Ok(CommitCheckRuns::Complete(runs));
        }
        if nodes.is_empty() {
            return Err(Error::GitHubApi(format!(
                "check runs for {sha}: provider stopped at {} of {total}",
                runs.len()
            )));
        }
    }
    Err(Error::GitHubApi(format!(
        "check runs for {sha} exceeded {MAX_CHECK_RUN_PAGES} pages"
    )))
}

/// Every check suite on `sha`, paged strictly to `total_count`.
pub fn fetch_commit_check_suites(
    client: &mut GhClient,
    repository: &str,
    sha: &str,
) -> Result<Vec<CheckSuite>> {
    let mut suites: Vec<CheckSuite> = Vec::new();
    let mut seen = HashSet::new();
    let mut expected: Option<usize> = None;
    for page in 1..=MAX_CHECK_RUN_PAGES {
        let body = client.rest(&format!(
            "repos/{repository}/commits/{sha}/check-suites?per_page={PAGE_SIZE}&page={page}"
        ))?;
        let total = body
            .get("total_count")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "check suites for {sha} page {page} has no total_count"
                ))
            })? as usize;
        if *expected.get_or_insert(total) != total {
            return Err(Error::GitHubApi(format!(
                "check suites for {sha} changed while paging; retry"
            )));
        }
        let nodes = body
            .get("check_suites")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::GitHubApi(format!("check suites for {sha} page {page} has no list"))
            })?;
        for node in nodes {
            let id = node
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| Error::GitHubApi(format!("check suite on {sha} has no id")))?;
            if seen.insert(id) {
                suites.push(CheckSuite {
                    id,
                    head_branch: node
                        .get("head_branch")
                        .and_then(Value::as_str)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string),
                    app_slug: node
                        .pointer("/app/slug")
                        .and_then(Value::as_str)
                        .filter(|slug| !slug.is_empty())
                        .map(str::to_string),
                    repository: node
                        .pointer("/repository/full_name")
                        .and_then(Value::as_str)
                        .map(str::to_ascii_lowercase),
                });
            }
        }
        if suites.len() >= total {
            if suites.len() > total {
                return Err(Error::GitHubApi(format!(
                    "check suites for {sha}: {} distinct exceed total_count {total}",
                    suites.len()
                )));
            }
            return Ok(suites);
        }
        if nodes.is_empty() {
            return Err(Error::GitHubApi(format!(
                "check suites for {sha}: provider stopped at {} of {total}",
                suites.len()
            )));
        }
    }
    Err(Error::GitHubApi(format!(
        "check suites for {sha} exceeded {MAX_CHECK_RUN_PAGES} pages"
    )))
}

/// Remaining REST core quota and its reset epoch (`rate_limit`).
///
/// Not used for budgeting: on a real token this endpoint reported 4999
/// remaining while data responses' `x-ratelimit-*` headers reported 4349 with
/// a different reset. Budgets read the data responses' headers instead
/// ([`GhClient::enforce_quota_reserve`]).
pub fn fetch_rest_quota(client: &mut GhClient) -> Result<(i64, i64)> {
    let body = client.rest("rate_limit")?;
    let core = body
        .pointer("/resources/core")
        .ok_or_else(|| Error::GitHubApi("rate_limit has no resources.core".into()))?;
    let remaining = core
        .get("remaining")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::GitHubApi("rate_limit core has no remaining".into()))?;
    let reset = core.get("reset").and_then(Value::as_i64).unwrap_or(0);
    Ok((remaining, reset))
}

/// The pull request's current head, commit count, and listed commits.
pub fn fetch_pull_request_commits(
    client: &mut GhClient,
    repository: &str,
    number: i64,
) -> Result<PullRequestCommits> {
    let pull = client.rest(&format!("repos/{repository}/pulls/{number}"))?;
    let head_sha = pull
        .get("head")
        .and_then(|head| head.get("sha"))
        .and_then(Value::as_str)
        .ok_or_else(|| Error::GitHubApi(format!("{repository}#{number} has no head.sha")))?
        .to_ascii_lowercase();
    let head_ref = pull
        .pointer("/head/ref")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    let repo_name = |side: &str| {
        pull.pointer(&format!("/{side}/repo/full_name"))
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
    };
    // A deleted head repository reads as null: treat it as cross-repository,
    // since nothing then ties the branch name to this repository.
    let cross_repository = match (repo_name("head"), repo_name("base")) {
        (Some(head), Some(base)) => head != base,
        (None, _) => true,
        (Some(head), None) => head != repository.to_ascii_lowercase(),
    };
    let commit_count = pull
        .get("commits")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::GitHubApi(format!("{repository}#{number} has no commit count")))?;
    let mut commits = Vec::new();
    let mut seen = HashSet::new();
    let pages = PR_COMMIT_LIST_LIMIT.div_ceil(PAGE_SIZE);
    for page in 1..=pages {
        let body = client.rest(&format!(
            "repos/{repository}/pulls/{number}/commits?per_page={PAGE_SIZE}&page={page}"
        ))?;
        let nodes = body.as_array().ok_or_else(|| {
            Error::GitHubApi(format!(
                "{repository}#{number} commits page {page} is not a list"
            ))
        })?;
        for node in nodes {
            let sha = node
                .get("sha")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::GitHubApi(format!("{repository}#{number} commit has no sha"))
                })?
                .to_ascii_lowercase();
            if seen.insert(sha.clone()) {
                commits.push(sha);
            }
        }
        if nodes.len() < PAGE_SIZE {
            break;
        }
    }
    Ok(PullRequestCommits {
        head_sha,
        commit_count,
        head_ref,
        cross_repository,
        commits,
        head_repository: repo_name("head"),
        created_at: pull
            .get("created_at")
            .and_then(Value::as_str)
            .map(str::to_string),
        closed_at: pull
            .get("closed_at")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// Every GitHub Actions workflow run on `sha`, paged strictly to
/// `total_count`. A run reporting another commit, or an entry in its
/// `pull_requests` without a number, is an error rather than skipped.
pub fn fetch_commit_workflow_runs(
    client: &mut GhClient,
    repository: &str,
    sha: &str,
) -> Result<Vec<WorkflowRun>> {
    let mut runs: Vec<WorkflowRun> = Vec::new();
    let mut seen = HashSet::new();
    let mut expected: Option<usize> = None;
    let lower = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_ascii_lowercase)
    };
    let text = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    };
    for page in 1..=MAX_CHECK_RUN_PAGES {
        let body = client.rest(&format!(
            "repos/{repository}/actions/runs?head_sha={sha}&per_page={PAGE_SIZE}&page={page}"
        ))?;
        let total = body
            .get("total_count")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "workflow runs for {sha} page {page} has no total_count"
                ))
            })? as usize;
        if *expected.get_or_insert(total) != total {
            return Err(Error::GitHubApi(format!(
                "workflow runs for {sha} changed while paging; retry"
            )));
        }
        let nodes = body
            .get("workflow_runs")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::GitHubApi(format!("workflow runs for {sha} page {page} has no list"))
            })?;
        for node in nodes {
            let id = node
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| Error::GitHubApi(format!("workflow run on {sha} has no id")))?;
            let head = node.get("head_sha").and_then(Value::as_str);
            if !head.is_some_and(|head| head.eq_ignore_ascii_case(sha)) {
                return Err(Error::GitHubApi(format!(
                    "workflow run {id} listed for {sha} reports head {head:?}"
                )));
            }
            let mut pull_requests = Vec::new();
            for pull in node
                .get("pull_requests")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let number = pull.get("number").and_then(Value::as_i64).ok_or_else(|| {
                    Error::GitHubApi(format!("workflow run {id}: pull request has no number"))
                })?;
                pull_requests.push(RunPullRequest {
                    number,
                    url: text(pull.get("url")),
                    head_ref: text(pull.pointer("/head/ref")),
                    head_repo_url: lower(pull.pointer("/head/repo/url")),
                    base_repo_url: lower(pull.pointer("/base/repo/url")),
                });
            }
            if seen.insert(id) {
                runs.push(WorkflowRun {
                    id,
                    check_suite_id: node.get("check_suite_id").and_then(Value::as_i64),
                    event: text(node.get("event")),
                    head_branch: text(node.get("head_branch")),
                    repository: lower(node.pointer("/repository/full_name")),
                    head_repository: lower(node.pointer("/head_repository/full_name")),
                    created_at: text(node.get("created_at")),
                    pull_requests,
                });
            }
        }
        if runs.len() >= total {
            if runs.len() > total {
                return Err(Error::GitHubApi(format!(
                    "workflow runs for {sha}: {} distinct exceed total_count {total}",
                    runs.len()
                )));
            }
            return Ok(runs);
        }
        if nodes.is_empty() {
            return Err(Error::GitHubApi(format!(
                "workflow runs for {sha}: provider stopped at {} of {total}",
                runs.len()
            )));
        }
    }
    Err(Error::GitHubApi(format!(
        "workflow runs for {sha} exceeded {MAX_CHECK_RUN_PAGES} pages"
    )))
}

/// True when an API URL names exactly `repos/{repository}` plus `suffix`
/// (any host, so GitHub Enterprise works too).
pub fn api_url_names(url: &str, repository: &str, suffix: &str) -> bool {
    let url = url.to_ascii_lowercase();
    let wanted = format!("/repos/{}{suffix}", repository.to_ascii_lowercase());
    url.ends_with(&wanted)
        && (url.starts_with("https://") || url.starts_with("http://"))
        && url[..url.len() - wanted.len()]
            .split("://")
            .nth(1)
            .is_some_and(|host| !host.is_empty())
}

const FORCE_PUSH_QUERY: &str = r#"
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      timelineItems(first: 100, after: $cursor, itemTypes: [HEAD_REF_FORCE_PUSHED_EVENT]) {
        filteredCount
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on HeadRefForcePushedEvent { beforeCommit { oid } afterCommit { oid } }
        }
      }
    }
  }
}
"#;

/// Every force push on the pull request's timeline, in provider order.
pub fn fetch_force_pushes(
    client: &mut GhClient,
    repository: &str,
    number: i64,
) -> Result<Vec<ForcePush>> {
    let (owner, name) = repository
        .split_once('/')
        .ok_or_else(|| Error::InvalidArgument(repository.to_string()))?;
    let number_text = number.to_string();
    let mut pushes = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    for _ in 0..MAX_TIMELINE_PAGES {
        let mut variables = vec![
            ("owner", owner),
            ("name", name),
            ("number", number_text.as_str()),
        ];
        if let Some(cursor) = cursor.as_deref() {
            variables.push(("cursor", cursor));
        }
        let data = client.graphql(FORCE_PUSH_QUERY, &variables)?;
        let timeline = data
            .pointer("/repository/pullRequest/timelineItems")
            .filter(|value| !value.is_null())
            .ok_or_else(|| {
                Error::GitHubApi(format!("{repository}#{number} timeline is missing"))
            })?;
        let filtered = timeline.get("filteredCount").and_then(Value::as_i64);
        let nodes = timeline
            .get("nodes")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::GitHubApi(format!("{repository}#{number} timeline has no nodes"))
            })?;
        for node in nodes {
            if node.get("__typename").and_then(Value::as_str) != Some("HeadRefForcePushedEvent") {
                continue;
            }
            let oid = |field: &str| {
                node.get(field)
                    .and_then(|commit| commit.get("oid"))
                    .and_then(Value::as_str)
                    .map(str::to_ascii_lowercase)
            };
            pushes.push(ForcePush {
                before: oid("beforeCommit"),
                after: oid("afterCommit"),
            });
        }
        let page_info = timeline.get("pageInfo");
        let has_next = page_info
            .and_then(|info| info.get("hasNextPage"))
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                Error::GitHubApi(format!("{repository}#{number} timeline has no page status"))
            })?;
        if !has_next {
            if let Some(filtered) = filtered
                && filtered != pushes.len() as i64
            {
                return Err(Error::GitHubApi(format!(
                    "{repository}#{number}: timeline reports {filtered} force pushes, served {}",
                    pushes.len()
                )));
            }
            return Ok(pushes);
        }
        let next = page_info
            .and_then(|info| info.get("endCursor"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::GitHubApi(format!("{repository}#{number} timeline has no endCursor"))
            })?
            .to_string();
        if !seen_cursors.insert(next.clone()) {
            return Err(Error::GitHubApi(format!(
                "{repository}#{number} timeline cursor repeated"
            )));
        }
        cursor = Some(next);
    }
    Err(Error::GitHubApi(format!(
        "{repository}#{number} timeline exceeded {MAX_TIMELINE_PAGES} pages"
    )))
}

/// Pull requests the provider associates with a commit
/// (`commits/{sha}/pulls`): the merged pull request that introduced it to the
/// default branch, or open pull requests containing it. Paged until a short
/// page, so a commit shared by more than one page of pull requests is never
/// read as owned by the first hundred. Not exhaustive for closed pull
/// requests, so an empty or partial answer proves nothing about them.
pub fn fetch_commit_pull_requests(
    client: &mut GhClient,
    repository: &str,
    sha: &str,
) -> Result<Vec<i64>> {
    paged_numbers(
        client,
        &format!("repos/{repository}/commits/{sha}/pulls?per_page={PAGE_SIZE}"),
        &format!("pulls for commit {sha}"),
    )
}

/// The GitHub App whose check suites are created per workflow run, so a
/// suite's `head_branch` is the branch that run was triggered for. Every
/// other app gets one suite per commit, whose `head_branch` names only the
/// first branch to push the commit and says nothing about which pull request
/// a run served.
pub const ACTIONS_APP: &str = "github-actions";

/// Every pull request, open or closed, whose head is `owner:branch`
/// (`pulls?state=all&head=`). The provider's answer, not the warehouse's
/// inventory, decides whether a branch name identifies exactly one pull
/// request: the warehouse never saw every closed pull request.
pub fn fetch_branch_pull_requests(
    client: &mut GhClient,
    repository: &str,
    branch: &str,
) -> Result<Vec<i64>> {
    let (owner, _) = repository
        .split_once('/')
        .ok_or_else(|| Error::InvalidArgument(repository.to_string()))?;
    let head = encode_query(&format!("{owner}:{branch}"));
    paged_numbers(
        client,
        &format!("repos/{repository}/pulls?state=all&head={head}&per_page={PAGE_SIZE}"),
        &format!("pulls for branch {branch}"),
    )
}

/// Percent-encode a query value, keeping unreserved characters and the `/`
/// and `:` that branch labels carry.
fn encode_query(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b':' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `number` of every element of a paged REST list, until a short page.
fn paged_numbers(client: &mut GhClient, base: &str, what: &str) -> Result<Vec<i64>> {
    let mut numbers = Vec::new();
    let mut seen = HashSet::new();
    for page in 1..=MAX_TIMELINE_PAGES {
        let body = client.rest(&format!("{base}&page={page}"))?;
        let nodes = body
            .as_array()
            .ok_or_else(|| Error::GitHubApi(format!("{what} page {page} is not a list")))?;
        for node in nodes {
            let number = node
                .get("number")
                .and_then(Value::as_i64)
                .ok_or_else(|| Error::GitHubApi(format!("{what}: entry has no number")))?;
            if seen.insert(number) {
                numbers.push(number);
            }
        }
        if nodes.len() < PAGE_SIZE {
            return Ok(numbers);
        }
    }
    Err(Error::GitHubApi(format!(
        "{what} exceeded {MAX_TIMELINE_PAGES} pages"
    )))
}

#[cfg(test)]
mod tests {
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
            Ok(
                json!({"total_count": 101, "check_runs": [run_json("CR_5", "abc", "job")]})
                    .to_string(),
            ),
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
}
