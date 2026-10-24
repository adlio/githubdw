//! Targeted repair of a pull request's historical check runs.
//!
//! Normal collection reads check runs from the CURRENT head commit only, so
//! runs on earlier heads exist in the warehouse only if a sync happened to run
//! while that commit was the head (and then only the latest run per name).
//! This repair re-reads, for each named pull request, every commit it can
//! attribute to that pull request and stores the provider's complete per-commit
//! run list, each run under the commit the provider reports for it.
//!
//! Commits considered, strongest attribution first:
//! - heads already stored for the pull request (observed by an earlier sync),
//! - the current head,
//! - both sides of every force push on the pull request timeline,
//! - every other commit the provider lists for the pull request (at most 250).
//!
//! Guarantees:
//! - Everything is fetched before anything is written; any fetch failure
//!   leaves the pull request's stored rows exactly as they were.
//! - Existing rows are never deleted and never moved to another pull request
//!   or commit; a conflict is reported, not resolved.
//! - Re-running is idempotent, so a partly completed batch resumes by running
//!   the same targets again.
//! - "complete" is claimed only when every attributable commit was read in
//!   full and nothing the provider withheld or the schema cannot represent
//!   was skipped.
//! - A commit or rollback that cannot be confirmed, a lock that cannot be
//!   released, or a quota refusal stops the batch: no later target is written,
//!   and the report is still returned in full.
//!
//! Ownership evidence (see `attribute`). A run is written for the pull
//! request only when provider evidence names it. The run must be a GitHub
//! Actions run (run app == suite app == `github-actions`, suite in this
//! repository), and exactly one workflow run (`actions/runs?head_sha=`) must
//! be keyed by its `check_suite_id`, in this repository, on the suite's
//! branch. Then either:
//! - the ref is `refs/pull/<N>/head|merge` for this number, the workflow run's
//!   head repository is this repository, and the commit is not known to be
//!   shared (unless the workflow run names only this pull request), or
//! - the workflow run was triggered by a `pull_request(_target)` event and
//!   names exactly one pull request, validated as this repository's (its API
//!   URL and base), that is this one, with its head branch and head
//!   repository equal to the pull request's, created within the pull
//!   request's open lifetime.
//!
//! Nothing else is proof: not a branch name being unique (the provider's
//! `pulls?head=` and `commits/{sha}/pulls` are not exhaustive, and a fork and
//! the base repository can carry same-named branches), not a stored head
//! (discovery only), not a per-commit app's suite (code scanning: one suite
//! per commit, naming its first push, whatever ref it names), not the
//! warehouse's own pull request inventory, not a run's `pull_requests` tags,
//! and not text or URLs in its output. Fork pull requests, whose workflow
//! runs name no pull request, stay ambiguous.

use std::collections::{BTreeMap, HashSet};

use rusqlite::Connection;
use serde::Serialize;

use crate::error::{Error, Result};
use crate::fetch::GhClient;
use crate::fetch::check_history::{
    self, CheckSuite, CommitCheckRun, CommitCheckRuns, RunPullRequest, WorkflowRun,
};
use crate::refresh::{self, RefreshTarget, TargetKind};
use crate::storage::repository::{self, CheckRunWrite};
use crate::storage::sync_state_repository as sync_state;

/// Options for one repair run.
#[derive(Debug, Clone, Default)]
pub struct CheckHistoryOptions {
    /// Read only the heads already stored plus the current head; skip commit
    /// list and force-push discovery. Faster, never reported complete.
    pub observed_only: bool,
    /// When set, never let a request spend into this reserve: the client
    /// reads the provider's `x-ratelimit-*` headers from every data response
    /// (core and GraphQL buckets separately; every attempt, retry and page
    /// counts) and refuses locally before the reserve. A missing or
    /// malformed header fails closed. A refusal writes nothing for the
    /// current target and stops the batch (remaining targets
    /// `not_attempted`). Best-effort: other consumers of the same token can
    /// spend between two responses. `None` skips the guard (library callers
    /// that budget quota themselves, and tests).
    pub rest_reserve: Option<i64>,
    /// Hard cap on requests (every attempt) for the whole batch. Reaching it
    /// is a quota refusal: nothing written for the current target, batch
    /// stopped. `None` = no cap.
    pub max_requests: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryStatus {
    /// Every run on every attributable commit was proven to belong to this
    /// pull request or provably to another, read in full, and stored.
    Complete,
    /// Some history is unavailable, ambiguous, or unrepresentable (caveats).
    /// Only proven runs were written.
    Incomplete,
    /// Nothing was written (see `rows_unchanged`).
    Failed,
    /// Not started: the batch stopped first (quota). Rerun to resume.
    NotAttempted,
}

/// Why a commit is attributed to the pull request. Discovery only: no source
/// (including `stored_head`) is ownership proof for a run; every run is
/// re-attributed from provider suite evidence on every repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HeadSource {
    StoredHead,
    CurrentHead,
    ForcePush,
    CommitList,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HeadOutcome {
    pub sha: String,
    pub sources: Vec<HeadSource>,
    /// Runs the provider lists for the commit (`filter=all`).
    pub upstream_runs: Option<usize>,
    /// Why the provider could not serve the commit.
    pub unavailable: Option<String>,
    /// Set when the commit's runs were not stored for this pull request.
    pub skipped: Option<String>,
    /// True when other pull requests are known to share this commit, so a
    /// suite's head branch cannot identify which one triggered a run.
    pub shared_head: bool,
    pub stored_before: i64,
    pub stored_after: i64,
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    /// Runs whose suite names another pull request's ref or branch.
    pub attributed_elsewhere: usize,
    /// Runs whose owner cannot be proven: node id -> reason. Never written.
    pub ambiguous: BTreeMap<String, String>,
    /// Runs already stored under THIS pull request that the provider evidence
    /// does not confirm. Left unchanged, reported for investigation.
    pub stored_unconfirmed: Vec<String>,
    /// Runs already stored under another pull request: key -> pr_key.
    pub owned_by_other_pr: BTreeMap<String, String>,
    /// Runs stored for this pull request against a different commit.
    pub head_mismatch: BTreeMap<String, String>,
    /// Runs the provider evidence proves belong to THIS pull request but
    /// that are stored under another: node id -> stored pr_key. Never moved;
    /// a conflict for review.
    pub ownership_conflicts: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryOutcome {
    pub target: RefreshTarget,
    pub status: HistoryStatus,
    pub error: Option<String>,
    pub caveats: Vec<String>,
    /// For `failed`/`not_attempted`: whether stored rows are known unchanged.
    /// `None` means a commit or rollback could not be confirmed.
    pub rows_unchanged: Option<bool>,
    /// Set when the repository lease could not be released as this run's:
    /// the lock may remain until it goes stale, or another process holds it.
    pub lock_release: Option<String>,
    /// `-1` when the stored count could not be read.
    pub check_runs_before: i64,
    pub check_runs_after: i64,
    pub heads: Vec<HeadOutcome>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CheckHistoryReport {
    pub outcomes: Vec<HistoryOutcome>,
    /// Why the batch stopped early, when it did.
    pub stopped: Option<String>,
}

impl CheckHistoryReport {
    pub fn count(&self, status: HistoryStatus) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.status == status)
            .count()
    }

    /// Targets that did not finish complete (incomplete, failed, or not
    /// attempted). Any nonzero value must be reported as a failure.
    pub fn not_complete(&self) -> usize {
        self.outcomes.len() - self.count(HistoryStatus::Complete)
    }
}

fn stored_runs(conn: &Connection, pr_key: &str, sha: Option<&str>) -> Result<i64> {
    Ok(match sha {
        Some(sha) => conn.query_row(
            "SELECT COUNT(*) FROM fact_check_runs WHERE pr_key = ?1 AND lower(head_sha) = ?2",
            [pr_key, sha],
            |row| row.get(0),
        )?,
        None => conn.query_row(
            "SELECT COUNT(*) FROM fact_check_runs WHERE pr_key = ?1",
            [pr_key],
            |row| row.get(0),
        )?,
    })
}

fn stored_heads(conn: &Connection, pr_key: &str) -> Result<Vec<String>> {
    let mut statement = conn.prepare(
        "SELECT DISTINCT lower(head_sha) FROM fact_check_runs WHERE pr_key = ?1 ORDER BY 1",
    )?;
    let heads = statement
        .query_map([pr_key], |row| row.get(0))?
        .collect::<std::result::Result<Vec<String>, _>>()?;
    Ok(heads)
}

/// Longest a single target may hold its lease before it is abandoned
/// (nothing written). Keeps a lease bounded even though it is refreshed.
const MAX_TARGET_SECONDS: i64 = 20 * 60;

/// Requests a target needs before it can learn anything: the pull request,
/// its first commit page, the force-push timeline, and the current head's
/// runs, suites, commit owners and workflow runs.
const MIN_TARGET_REQUESTS: i64 = 8;

/// Requests one more commit needs at least (runs, suites, owners, workflow
/// runs).
const MIN_COMMIT_REQUESTS: u64 = 4;

fn quota_refusal(error: &Error) -> bool {
    if matches!(error, Error::QuotaReserve(_)) {
        return true;
    }
    let text = error.to_string().to_lowercase();
    text.contains("rate limit")
        || text.contains("http 403")
        || text.contains("http 429")
        || text.contains("secondary rate")
}

/// Test-only fault points for the write and lock-release phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
enum Fault {
    /// `RELEASE SAVEPOINT` (the commit) fails.
    Release,
    /// `ROLLBACK TO SAVEPOINT` after a write error fails.
    Rollback,
    /// Releasing the repository lease fails.
    LockRelease,
}

#[cfg(test)]
thread_local! {
    static FAULTS: std::cell::RefCell<Vec<Fault>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
fn arm_fault(fault: Fault) {
    FAULTS.with(|faults| faults.borrow_mut().push(fault));
}

#[cfg(test)]
thread_local! {
    /// Runs right before the real `RELEASE SAVEPOINT`, so a test can make a
    /// second connection take a lock that makes the actual commit fail.
    static BEFORE_RELEASE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn before_release(hook: Box<dyn FnOnce()>) {
    BEFORE_RELEASE.with(|slot| *slot.borrow_mut() = Some(hook));
}

fn run_before_release_hook() {
    #[cfg(test)]
    if let Some(hook) = BEFORE_RELEASE.with(|slot| slot.borrow_mut().take()) {
        hook();
    }
}

fn inject(point: Fault) -> Result<()> {
    #[cfg(test)]
    {
        let hit = FAULTS.with(|faults| {
            let mut faults = faults.borrow_mut();
            let found = faults.iter().position(|fault| *fault == point);
            found.map(|index| faults.remove(index)).is_some()
        });
        if hit {
            return Err(Error::Io(std::io::Error::other(format!(
                "injected {point:?} fault"
            ))));
        }
    }
    let _ = point;
    Ok(())
}

fn sql(conn: &Connection, point: Fault, statements: &str) -> Result<()> {
    inject(point)?;
    conn.execute_batch(statements)?;
    Ok(())
}

fn not_attempted(target: &RefreshTarget, reason: &str, before: i64) -> HistoryOutcome {
    HistoryOutcome {
        target: target.clone(),
        status: HistoryStatus::NotAttempted,
        error: Some(reason.to_string()),
        caveats: Vec::new(),
        rows_unchanged: Some(true),
        lock_release: None,
        check_runs_before: before,
        check_runs_after: before,
        heads: Vec::new(),
    }
}

/// Request cap for one target, derived from quota observed in response
/// headers.
struct QuotaBudget;

impl QuotaBudget {
    /// Refuse (nothing written) when the observed headroom of either bucket
    /// this walk draws from is already below `needed`. An unobserved bucket
    /// passes: the client's first call to it is a single probe.
    fn check(&self, client: &GhClient, needed: i64) -> Result<()> {
        for resource in ["core", "graphql"] {
            if let Some(headroom) = client.quota_headroom(resource)
                && headroom < needed
            {
                let (remaining, reset) = client.observed_quota(resource).unwrap_or_default();
                return Err(Error::QuotaReserve(format!(
                    "{resource} quota {remaining} leaves {headroom} above the reserve, {needed} \
                     needed; resets at epoch {reset}"
                )));
            }
        }
        Ok(())
    }
}

/// Repair the named pull requests (1-64). Each target takes its own
/// repository lease, refreshed while it works and released only if still
/// owned, so an active sync is never overlapped and never unlocked.
///
/// Returns `Err` only for invalid arguments, before anything is read. Every
/// later failure, including a database or lock error, is recorded in the
/// report, which is always returned whole so the caller can emit it.
pub fn repair_check_history(
    conn: &Connection,
    client: &mut GhClient,
    targets: &[RefreshTarget],
    options: &CheckHistoryOptions,
) -> Result<CheckHistoryReport> {
    if let Some(issue) = targets.iter().find(|t| t.kind != TargetKind::PullRequest) {
        return Err(Error::InvalidArgument(format!(
            "{} is an issue; check history applies to pull requests",
            issue.reference()
        )));
    }
    let targets = refresh::validate_targets(targets.to_vec())?;
    let mut report = CheckHistoryReport::default();
    if let Some(reserve) = options.rest_reserve {
        client.enforce_quota_reserve(reserve);
    }
    if options.max_requests.is_some() {
        client.set_request_allowance(options.max_requests);
    }
    for target in &targets {
        let outcome = repair_target(conn, client, target, options, &mut report.stopped);
        report.outcomes.push(outcome);
    }
    if options.max_requests.is_some() {
        client.set_request_allowance(None);
    }
    Ok(report)
}

/// One target of the batch. Sets `stopped` when no later target may run.
fn repair_target(
    conn: &Connection,
    client: &mut GhClient,
    target: &RefreshTarget,
    options: &CheckHistoryOptions,
    stopped: &mut Option<String>,
) -> HistoryOutcome {
    let pr_key = target.reference();
    let before = match stored_runs(conn, &pr_key, None) {
        Ok(before) => before,
        Err(error) => {
            let reason = format!("stored check runs could not be read: {error}");
            stopped.get_or_insert_with(|| format!("stopped: {reason}"));
            return failed(target, reason, -1, Some(true));
        }
    };
    if let Some(reason) = stopped.clone() {
        return not_attempted(target, &reason, before);
    }
    let budget = options.rest_reserve.map(|_| QuotaBudget);
    if let Some(budget) = &budget
        && let Err(error) = budget.check(client, MIN_TARGET_REQUESTS)
    {
        let reason = format!("stopped: {error}. Rerun the same targets to resume");
        *stopped = Some(reason.clone());
        return not_attempted(target, &reason, before);
    }
    let entity_key = format!("repo:{}", target.repository);
    let token = match sync_state::acquire_owned_lock(conn, &entity_key) {
        Ok(token) => token,
        Err(Error::SyncInProgress(_)) => {
            return failed(
                target,
                format!(
                    "a sync of {} is in progress; retry after it finishes",
                    target.repository
                ),
                before,
                Some(true),
            );
        }
        Err(error) => {
            let reason = format!("lock could not be taken: {error}");
            *stopped = Some(format!("stopped: {reason}"));
            return failed(target, reason, before, Some(true));
        }
    };
    let mut lease = Lease {
        entity_key: &entity_key,
        token,
        started: chrono::Utc::now(),
    };
    let mut outcome = match repair_one(conn, client, target, options, budget.as_ref(), &mut lease) {
        Ok(outcome) => outcome,
        Err(RepairError {
            error,
            rows_unchanged,
        }) => {
            if rows_unchanged.is_none() {
                *stopped = Some(format!(
                    "stopped: {pr_key}'s writes could not be confirmed committed or rolled \
                     back; no further target was written"
                ));
            } else if quota_refusal(&error) {
                *stopped = Some(format!(
                    "stopped after a provider quota/permission refusal on {pr_key} ({error}); \
                     rerun the remaining targets to resume"
                ));
            }
            failed(target, error.to_string(), before, rows_unchanged)
        }
    };
    // Never delete a lock this run no longer owns, and never let a release
    // failure discard the outcome already earned.
    let release = if conn.is_autocommit() {
        inject(Fault::LockRelease)
            .and_then(|()| sync_state::release_owned_lock(conn, &entity_key, &lease.token))
    } else {
        Err(Error::GitHubApi(
            "the connection is still inside an unfinished transaction".into(),
        ))
    };
    let note = match release {
        Ok(true) => None,
        Ok(false) => Some(format!(
            "lease on {entity_key} was no longer this run's at release; left to its holder"
        )),
        Err(error) => Some(format!(
            "lease on {entity_key} could not be released ({error}); it remains until stale"
        )),
    };
    if let Some(note) = note {
        stopped.get_or_insert_with(|| format!("stopped: {note}"));
        // A lease left behind is a failure of this run, never a footnote to
        // a complete one: the rows earned stay reported, the status is not
        // complete, and the batch's not-complete count includes it.
        if outcome.status == HistoryStatus::Complete {
            outcome.status = HistoryStatus::Incomplete;
        }
        outcome
            .caveats
            .push(format!("repository lease not released: {note}"));
        outcome.lock_release = Some(note);
    }
    outcome
}

fn failed(
    target: &RefreshTarget,
    error: String,
    before: i64,
    rows_unchanged: Option<bool>,
) -> HistoryOutcome {
    HistoryOutcome {
        target: target.clone(),
        status: HistoryStatus::Failed,
        error: Some(error),
        caveats: Vec::new(),
        rows_unchanged,
        lock_release: None,
        check_runs_before: before,
        check_runs_after: before,
        heads: Vec::new(),
    }
}

struct Lease<'a> {
    entity_key: &'a str,
    token: String,
    started: chrono::DateTime<chrono::Utc>,
}

impl Lease<'_> {
    /// Renew the lease; error (nothing written yet) if it was lost or the
    /// per-target bound is spent.
    fn renew(&mut self, conn: &Connection) -> Result<()> {
        if (chrono::Utc::now() - self.started).num_seconds() > MAX_TARGET_SECONDS {
            return Err(Error::GitHubApi(format!(
                "target exceeded its {MAX_TARGET_SECONDS}s lease bound; nothing written"
            )));
        }
        match sync_state::refresh_owned_lock(conn, self.entity_key, &self.token)? {
            Some(next) => {
                self.token = next;
                Ok(())
            }
            None => Err(Error::SyncInProgress(format!(
                "{} lease lost to another process; nothing written",
                self.entity_key
            ))),
        }
    }
}

struct RepairError {
    error: Error,
    /// Some(true): rolled back or never written. None: unknown.
    rows_unchanged: Option<bool>,
}

impl From<Error> for RepairError {
    fn from(error: Error) -> Self {
        RepairError {
            error,
            rows_unchanged: Some(true),
        }
    }
}

impl From<rusqlite::Error> for RepairError {
    fn from(error: rusqlite::Error) -> Self {
        Error::from(error).into()
    }
}

/// One commit's fetched answer, held until the write phase.
struct Fetched {
    head: HeadOutcome,
    runs: Vec<CommitCheckRun>,
}

/// Per-run attribution from provider suite evidence.
#[derive(Debug, PartialEq)]
enum Attribution {
    Own,
    Elsewhere(String),
    Ambiguous(String),
}

struct Context<'a> {
    repository: &'a str,
    number: i64,
    head_ref: Option<&'a str>,
    /// Lower-cased `owner/name` of the pull request's head repository.
    head_repository: Option<&'a str>,
    created_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    closed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    /// The pull request is closed but the provider gave no close time.
    closed_unknown: bool,
}

/// Events whose workflow runs a pull request triggers.
const PULL_REQUEST_EVENTS: [&str; 2] = ["pull_request", "pull_request_target"];

fn parse_time(value: Option<&str>) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    value.and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
}

/// The single pull request a workflow run names, validated against this
/// repository: its API URL is this repository's `pulls/{n}` and its base is
/// this repository. `Err` with the reason when the identity is missing,
/// conflicting, or does not validate.
fn workflow_pull_request<'r>(
    workflow: &'r WorkflowRun,
    repository: &str,
) -> std::result::Result<&'r RunPullRequest, String> {
    let numbers: std::collections::BTreeSet<i64> =
        workflow.pull_requests.iter().map(|pr| pr.number).collect();
    let pull = match (numbers.len(), workflow.pull_requests.first()) {
        (0, _) | (_, None) => {
            return Err(
                "workflow run names no pull request (fork or closed pull requests are \
                        not listed), and a branch name is not proof"
                    .into(),
            );
        }
        (1, Some(pull)) => pull,
        _ => {
            return Err(format!(
                "workflow run names several pull requests: {numbers:?}"
            ));
        }
    };
    for entry in &workflow.pull_requests {
        let suffix = format!("/pulls/{}", entry.number);
        if !entry
            .url
            .as_deref()
            .is_some_and(|url| check_history::api_url_names(url, repository, &suffix))
        {
            return Err(format!(
                "workflow run's pull request #{} is not identified as this repository's",
                entry.number
            ));
        }
        if !entry
            .base_repo_url
            .as_deref()
            .is_some_and(|url| check_history::api_url_names(url, repository, ""))
        {
            return Err(format!(
                "workflow run's pull request #{} does not name this repository as its base",
                entry.number
            ));
        }
    }
    Ok(pull)
}

/// Attribute one run from provider evidence only (see the module docs).
/// Anything short of proof is `Ambiguous`; `Elsewhere` is reported only when
/// validated evidence names something other than this pull request.
fn attribute(
    run: &CommitCheckRun,
    suites: &BTreeMap<i64, CheckSuite>,
    workflows: &[WorkflowRun],
    context: &Context,
    shared_head: bool,
) -> Attribution {
    let Some(suite) = run.suite_id.and_then(|id| suites.get(&id)) else {
        return Attribution::Ambiguous("run's check suite is not listed for the commit".into());
    };
    let app = match (run.app_slug.as_deref(), suite.app_slug.as_deref()) {
        (Some(run_app), Some(suite_app)) if run_app == suite_app => run_app,
        (None, _) | (_, None) => return Attribution::Ambiguous("unknown app".into()),
        (Some(run_app), Some(suite_app)) => {
            return Attribution::Ambiguous(format!(
                "run app {run_app} differs from its suite's app {suite_app}"
            ));
        }
    };
    match suite.repository.as_deref() {
        Some(name) if name.eq_ignore_ascii_case(context.repository) => {}
        Some(_) => return Attribution::Ambiguous("suite belongs to another repository".into()),
        None => return Attribution::Ambiguous("suite does not name its repository".into()),
    }
    let Some(branch) = suite.head_branch.as_deref() else {
        return Attribution::Ambiguous("suite has no head branch".into());
    };
    // Only a GitHub Actions suite belongs to one workflow run. Every other
    // app keeps one suite per commit, whose ref -- branch OR refs/pull/N --
    // names only the first push, so nothing about it proves an owner.
    if app != check_history::ACTIONS_APP {
        return Attribution::Ambiguous(format!(
            "{app} keeps one suite per commit; its ref {branch} names the first push, not the \
             pull request a run served"
        ));
    }
    let suite_id = suite.id;
    let mut keyed = workflows
        .iter()
        .filter(|workflow| workflow.check_suite_id == Some(suite_id));
    let workflow = match (keyed.next(), keyed.next()) {
        (Some(workflow), None) => workflow,
        (None, _) => {
            return Attribution::Ambiguous(format!("no workflow run is keyed by suite {suite_id}"));
        }
        (Some(_), Some(_)) => {
            return Attribution::Ambiguous(format!("several workflow runs claim suite {suite_id}"));
        }
    };
    if !workflow
        .repository
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case(context.repository))
    {
        return Attribution::Ambiguous("workflow run does not name this repository".into());
    }
    if workflow.head_branch.as_deref() != Some(branch) {
        return Attribution::Ambiguous(format!(
            "workflow run branch {:?} differs from its suite's {branch}",
            workflow.head_branch
        ));
    }
    // Check-run tags that contradict the workflow run's identity.
    let named: std::collections::BTreeSet<i64> =
        workflow.pull_requests.iter().map(|pr| pr.number).collect();
    if !run.pull_requests.is_empty()
        && run
            .pull_requests
            .iter()
            .any(|number| !named.contains(number))
    {
        return Attribution::Ambiguous(format!(
            "check run tags {:?} conflict with its workflow run's pull requests {named:?}",
            run.pull_requests
        ));
    }
    if let Some(rest) = branch.strip_prefix("refs/pull/") {
        let number = rest
            .strip_suffix("/head")
            .or_else(|| rest.strip_suffix("/merge"))
            .and_then(|n| n.parse::<i64>().ok());
        let Some(number) = number else {
            return Attribution::Ambiguous(format!("unrecognized pull ref {branch}"));
        };
        if !workflow
            .head_repository
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(context.repository))
        {
            return Attribution::Ambiguous(format!(
                "workflow run on {branch} does not name this repository as its head"
            ));
        }
        if number != context.number {
            return Attribution::Elsewhere(format!("workflow run ref names #{number}"));
        }
        if !named.is_empty() && named.iter().any(|n| *n != context.number) {
            return Attribution::Ambiguous(format!(
                "workflow run on {branch} also names {named:?}"
            ));
        }
        if shared_head && named.is_empty() {
            return Attribution::Ambiguous(format!(
                "commit is shared with other pull request(s) and the workflow run on {branch} \
                 names no pull request"
            ));
        }
        return Attribution::Own;
    }
    let event = workflow.event.as_deref().unwrap_or("unknown");
    if !PULL_REQUEST_EVENTS.contains(&event) {
        return Attribution::Ambiguous(format!(
            "workflow run event {event} was not triggered by a pull request"
        ));
    }
    let pull = match workflow_pull_request(workflow, context.repository) {
        Ok(pull) => pull,
        Err(reason) => return Attribution::Ambiguous(reason),
    };
    if pull.number != context.number {
        return Attribution::Elsewhere(format!("workflow run names #{}", pull.number));
    }
    // The named pull request is ours: every identity field must agree.
    let Some(head_ref) = context.head_ref else {
        return Attribution::Ambiguous("pull request head branch unknown".into());
    };
    if pull.head_ref.as_deref() != Some(branch) || branch != head_ref {
        return Attribution::Ambiguous(format!(
            "workflow run branch {branch} does not match the pull request's head {head_ref}"
        ));
    }
    let Some(head_repository) = context.head_repository else {
        return Attribution::Ambiguous("pull request head repository unknown".into());
    };
    if !workflow
        .head_repository
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case(head_repository))
        || !pull
            .head_repo_url
            .as_deref()
            .is_some_and(|url| check_history::api_url_names(url, head_repository, ""))
    {
        return Attribution::Ambiguous(
            "workflow run head repository does not match the pull request's".into(),
        );
    }
    // `pull_requests` reflects open pull requests matching the run, not
    // necessarily the trigger; a run outside the pull request's lifetime
    // could not have been triggered by it.
    let Some(created) = parse_time(workflow.created_at.as_deref()) else {
        return Attribution::Ambiguous("workflow run has no creation time".into());
    };
    let Some(opened) = context.created_at else {
        return Attribution::Ambiguous("pull request creation time unknown".into());
    };
    if created < opened
        || context.closed_unknown
        || context.closed_at.is_some_and(|closed| created > closed)
    {
        return Attribution::Ambiguous(
            "workflow run falls outside the pull request's open lifetime".into(),
        );
    }
    Attribution::Own
}

/// True when observed headroom could not cover one more commit.
fn client_allowance_low(client: &GhClient) -> bool {
    client
        .request_allowance()
        .is_some_and(|left| left < MIN_COMMIT_REQUESTS)
        || ["core", "graphql"].iter().any(|resource| {
            client
                .quota_headroom(resource)
                .is_some_and(|left| left < MIN_COMMIT_REQUESTS as i64)
        })
}

fn existing_owner(conn: &Connection, key: &str) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;
    Ok(conn
        .query_row(
            "SELECT pr_key FROM fact_check_runs WHERE check_run_key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()?)
}

fn repair_one(
    conn: &Connection,
    client: &mut GhClient,
    target: &RefreshTarget,
    options: &CheckHistoryOptions,
    budget: Option<&QuotaBudget>,
    lease: &mut Lease,
) -> std::result::Result<HistoryOutcome, RepairError> {
    let pr_key = target.reference();
    let repository = target.repository.as_str();
    let present: i64 = conn.query_row(
        "SELECT COUNT(*) FROM fact_pull_requests WHERE pr_key = ?1",
        [&pr_key],
        |row| row.get(0),
    )?;
    if present == 0 {
        return Err(Error::InvalidArgument(format!(
            "{pr_key} is not in the warehouse; run `sync targets --pr {pr_key}` first"
        ))
        .into());
    }
    let before = stored_runs(conn, &pr_key, None)?;
    let mut caveats = Vec::new();

    // ---- discover commits (network only) ----
    let mut sources: BTreeMap<String, Vec<HeadSource>> = BTreeMap::new();
    let mut add = |sha: &str, source: HeadSource| {
        let entry = sources.entry(sha.to_ascii_lowercase()).or_default();
        if !entry.contains(&source) {
            entry.push(source);
        }
    };
    for sha in stored_heads(conn, &pr_key)? {
        add(&sha, HeadSource::StoredHead);
    }
    let listed = check_history::fetch_pull_request_commits(client, repository, target.number)?;
    add(&listed.head_sha, HeadSource::CurrentHead);
    if options.observed_only {
        caveats
            .push("observed-only: commit list and force-push history were not walked".to_string());
    } else {
        if (listed.commits.len() as i64) < listed.commit_count {
            caveats.push(format!(
                "provider listed {} of {} commits (GitHub lists at most {}); runs on unlisted \
                 commits that were never a stored head are not recoverable here",
                listed.commits.len(),
                listed.commit_count,
                check_history::PR_COMMIT_LIST_LIMIT
            ));
        }
        for sha in &listed.commits {
            add(sha, HeadSource::CommitList);
        }
        let pushes = check_history::fetch_force_pushes(client, repository, target.number)?;
        let unresolved = pushes.iter().filter(|push| push.before.is_none()).count();
        if unresolved > 0 {
            caveats.push(format!(
                "{unresolved} force push(es) replaced a commit the provider no longer resolves; \
                 runs on those heads are unavailable unless already stored"
            ));
        }
        for push in &pushes {
            for sha in [&push.before, &push.after].into_iter().flatten() {
                add(sha, HeadSource::ForcePush);
            }
        }
    }
    let closed_at = parse_time(listed.closed_at.as_deref());
    let context = Context {
        repository,
        number: target.number,
        head_ref: listed.head_ref.as_deref(),
        head_repository: listed.head_repository.as_deref(),
        created_at: parse_time(listed.created_at.as_deref()),
        closed_at,
        closed_unknown: listed.closed_at.is_some() && closed_at.is_none(),
    };

    // ---- fetch runs and suites per commit (network only) ----
    let mut fetched: Vec<Fetched> = Vec::new();
    for (sha, mut head_sources) in sources {
        lease.renew(conn)?;
        // Every newly discovered commit costs requests: stop before one the
        // observed quota (or the batch cap) could not cover.
        if budget.is_some() && client_allowance_low(client) {
            return Err(Error::QuotaReserve(
                "quota headroom or request cap cannot cover another commit".into(),
            )
            .into());
        }
        head_sources.sort();
        let strong = head_sources
            .iter()
            .any(|source| *source != HeadSource::CommitList);
        let mut head = HeadOutcome {
            sha: sha.clone(),
            sources: head_sources,
            stored_before: stored_runs(conn, &pr_key, Some(&sha))?,
            ..Default::default()
        };
        let mut runs = match check_history::fetch_commit_check_runs(client, repository, &sha)? {
            CommitCheckRuns::Complete(runs) => runs,
            CommitCheckRuns::Unavailable(reason) => {
                head.unavailable = Some(reason);
                fetched.push(Fetched {
                    head,
                    runs: Vec::new(),
                });
                continue;
            }
        };
        head.upstream_runs = Some(runs.len());
        if runs.is_empty() {
            fetched.push(Fetched { head, runs });
            continue;
        }
        let suites: BTreeMap<i64, CheckSuite> =
            check_history::fetch_commit_check_suites(client, repository, &sha)?
                .into_iter()
                .map(|suite| (suite.id, suite))
                .collect();
        let others_stored: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT pr_key) FROM fact_check_runs
             WHERE lower(head_sha) = ?1 AND pr_key <> ?2",
            [&sha, &pr_key],
            |row| row.get(0),
        )?;
        let mut shared = others_stored > 0
            || runs
                .iter()
                .any(|run| run.pull_requests.iter().any(|n| *n != target.number));
        // Pull requests the provider says contain the commit. Asked for every
        // commit, stored heads included: a stored head is discovery, not
        // proof, so it never exempts a commit from the shared-commit check.
        let owners = check_history::fetch_commit_pull_requests(client, repository, &sha)?;
        if !strong && !owners.is_empty() && !owners.contains(&target.number) {
            // A listed commit can belong to another pull request (a stacked
            // or merged branch). A positive provider answer naming only other
            // pull requests is enough to leave it out.
            head.skipped = Some(format!(
                "commit belongs to pull request(s) {owners:?} per the provider"
            ));
            head.attributed_elsewhere = runs.len();
            runs.clear();
            fetched.push(Fetched { head, runs });
            continue;
        }
        shared |= owners.iter().any(|n| *n != target.number);
        // Workflow runs carry the identity a suite lacks (event, head
        // repository, pull requests), keyed by suite id. Read only when a
        // run here is a GitHub Actions run of this repository.
        let workflows = if runs.iter().any(|run| {
            run.app_slug.as_deref() == Some(check_history::ACTIONS_APP)
                && run
                    .suite_id
                    .and_then(|id| suites.get(&id))
                    .is_some_and(|suite| {
                        suite.app_slug.as_deref() == Some(check_history::ACTIONS_APP)
                    })
        }) {
            check_history::fetch_commit_workflow_runs(client, repository, &sha)?
        } else {
            Vec::new()
        };
        head.shared_head = shared;
        let mut own = Vec::new();
        for run in runs {
            let attribution = attribute(&run, &suites, &workflows, &context, shared);
            if attribution == Attribution::Own {
                own.push(run);
                continue;
            }
            match existing_owner(conn, &run.node_id)? {
                Some(owner) if owner == pr_key => head.stored_unconfirmed.push(run.node_id.clone()),
                Some(owner) => {
                    head.owned_by_other_pr.insert(run.node_id.clone(), owner);
                }
                None => {}
            }
            match attribution {
                Attribution::Elsewhere(_) => head.attributed_elsewhere += 1,
                Attribution::Ambiguous(reason) => {
                    head.ambiguous.insert(run.node_id.clone(), reason);
                }
                Attribution::Own => unreachable!(),
            }
        }
        fetched.push(Fetched { head, runs: own });
    }
    lease.renew(conn)?;

    // ---- write everything together; final counts read before commit ----
    conn.execute_batch("SAVEPOINT githubdw_check_history")?;
    let written = (|| -> Result<i64> {
        for item in &mut fetched {
            for run in &item.runs {
                let write = repository::record_check_run(
                    conn,
                    &run.node_id,
                    &pr_key,
                    &run.head_sha,
                    &run.name,
                    &run.status,
                    run.conclusion.as_deref(),
                    run.started_at.as_deref(),
                    run.completed_at.as_deref(),
                )?;
                match write {
                    CheckRunWrite::Inserted => item.head.inserted += 1,
                    CheckRunWrite::Updated => item.head.updated += 1,
                    CheckRunWrite::Unchanged => item.head.unchanged += 1,
                    CheckRunWrite::OwnedByOtherPullRequest(owner) => {
                        // Provider evidence proves this pull request, the
                        // row says another: a conflict, never moved here.
                        item.head
                            .ownership_conflicts
                            .insert(run.node_id.clone(), owner);
                    }
                    CheckRunWrite::HeadMismatch(stored) => {
                        item.head.head_mismatch.insert(run.node_id.clone(), stored);
                    }
                }
            }
            item.head.stored_after = stored_runs(conn, &pr_key, Some(&item.head.sha))?;
        }
        stored_runs(conn, &pr_key, None)
    })();
    let after = match written {
        Ok(after) => match (
            run_before_release_hook(),
            sql(
                conn,
                Fault::Release,
                "RELEASE SAVEPOINT githubdw_check_history",
            ),
        ) {
            ((), Ok(())) => after,
            ((), Err(error)) => {
                // The commit is unconfirmed. Try not to leave the savepoint
                // open; whether rows changed stays unknown either way.
                let _ = conn.execute_batch(
                    "ROLLBACK TO SAVEPOINT githubdw_check_history;
                     RELEASE SAVEPOINT githubdw_check_history",
                );
                return Err(RepairError {
                    error,
                    rows_unchanged: None,
                });
            }
        },
        Err(error) => {
            let rolled_back = sql(
                conn,
                Fault::Rollback,
                "ROLLBACK TO SAVEPOINT githubdw_check_history;
                 RELEASE SAVEPOINT githubdw_check_history",
            )
            .is_ok();
            return Err(RepairError {
                error,
                rows_unchanged: rolled_back.then_some(true),
            });
        }
    };

    // ---- account for what could not be completed ----
    let heads: Vec<HeadOutcome> = fetched.into_iter().map(|item| item.head).collect();
    let unavailable: Vec<&str> = heads
        .iter()
        .filter(|head| head.unavailable.is_some())
        .map(|head| head.sha.as_str())
        .collect();
    if !unavailable.is_empty() {
        caveats.push(format!(
            "provider no longer serves {} commit(s): {}",
            unavailable.len(),
            unavailable.join(", ")
        ));
    }
    let ambiguous: usize = heads.iter().map(|head| head.ambiguous.len()).sum();
    if ambiguous > 0 {
        caveats.push(format!(
            "{ambiguous} run(s) could not be attributed to this pull request from provider \
             suite evidence; not written"
        ));
    }
    let elsewhere: usize = heads
        .iter()
        .filter(|head| head.skipped.is_none())
        .map(|head| head.attributed_elsewhere)
        .sum();
    if elsewhere > 0 {
        caveats.push(format!(
            "{elsewhere} run(s) on this pull request's commits are attributed by their suite to \
             another pull request; a shared commit cannot prove they are not this one's"
        ));
    }
    let unconfirmed: usize = heads.iter().map(|head| head.stored_unconfirmed.len()).sum();
    if unconfirmed > 0 {
        caveats.push(format!(
            "{unconfirmed} run(s) stored under this pull request are not confirmed by provider \
             evidence; left unchanged for investigation"
        ));
    }
    let shared: usize = heads.iter().map(|head| head.owned_by_other_pr.len()).sum();
    if shared > 0 {
        caveats.push(format!(
            "{shared} run(s) are already stored under another pull request; a check run row \
             holds one pull request, so they were left there"
        ));
    }
    let conflicts: usize = heads
        .iter()
        .map(|head| head.ownership_conflicts.len())
        .sum();
    if conflicts > 0 {
        caveats.push(format!(
            "CONFLICT: {conflicts} run(s) the provider's workflow-run evidence attributes to this \
             pull request are stored under another pull request; not moved, needs review"
        ));
    }
    let mismatched: usize = heads.iter().map(|head| head.head_mismatch.len()).sum();
    if mismatched > 0 {
        caveats.push(format!(
            "{mismatched} stored run(s) record a different commit than the provider; left \
             unchanged for investigation"
        ));
    }
    let status = if caveats.is_empty() {
        HistoryStatus::Complete
    } else {
        HistoryStatus::Incomplete
    };
    Ok(HistoryOutcome {
        target: target.clone(),
        status,
        error: None,
        caveats,
        rows_unchanged: None,
        lock_release: None,
        check_runs_before: before,
        check_runs_after: after,
        heads,
    })
}

/// A stored pull request worth a history repair. Read-only.
#[derive(Debug, Clone, Serialize)]
pub struct HistoryCandidate {
    pub target: RefreshTarget,
    pub stored_heads: i64,
    pub stored_runs: i64,
}

/// Pull requests with at least `min_heads` distinct stored check-run heads,
/// most heads first. More than one stored head is the signature of a pull
/// request whose history was only partly observed. Read-only, no network.
pub fn history_candidates(
    conn: &Connection,
    repository: Option<&str>,
    min_heads: i64,
    limit: usize,
) -> Result<Vec<HistoryCandidate>> {
    let repository = repository.map(str::to_lowercase);
    let mut statement = conn.prepare(
        "SELECT p.repo_key, p.number, COUNT(DISTINCT lower(c.head_sha)), COUNT(*)
         FROM fact_check_runs c JOIN fact_pull_requests p ON p.pr_key = c.pr_key
         WHERE (?1 IS NULL OR p.repo_key = ?1)
         GROUP BY p.pr_key
         HAVING COUNT(DISTINCT lower(c.head_sha)) >= ?2
         ORDER BY 3 DESC, p.pr_key
         LIMIT ?3",
    )?;
    let rows = statement.query_map(
        rusqlite::params![repository, min_heads, limit as i64],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    )?;
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for row in rows {
        let (repo_key, number, stored_heads, stored_runs) = row?;
        let target = RefreshTarget {
            kind: TargetKind::PullRequest,
            repository: repo_key.to_lowercase(),
            number,
        };
        if seen.insert(target.clone()) {
            candidates.push(HistoryCandidate {
                target,
                stored_heads,
                stored_runs,
            });
        }
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests;
