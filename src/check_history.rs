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

mod attribution;
mod repair;

use attribution::*;
use repair::*;

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
