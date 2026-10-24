//! Sync orchestration: fetch pages via `gh`, upsert into the warehouse,
//! stream progress, and record incremental coverage.

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use rusqlite::Connection;

use crate::error::Result;
use crate::fetch::GhClient;
use crate::fetch::git_files::{self, GitRecoveryConfig, RecoveredFileSet};
use crate::fetch::issues;
use crate::fetch::pull_requests::{self, ActorReference, PullRequestData};
use crate::fetch::user_search::{self, UserRole};
use crate::refresh::{
    self, RefreshOptions, RefreshReport, RefreshTarget, TargetKind, TargetOutcome, TargetStatus,
};
use crate::storage::issue_repository;
use crate::storage::monitor_repository::{self, MonitoredSource};
use crate::storage::repository::{self, PullRequestRow};
use crate::storage::sync_state_repository as sync_state;
use crate::storage::time_dimension;

mod checkpoint;
mod issue_walk;
mod plan;
mod pr_walk;
mod targets;
#[cfg(test)]
mod tests;
mod user_walk;

pub use checkpoint::REPO_PR_CHECKPOINT;
pub use plan::{SyncPlan, SyncSource, plan_all};

use checkpoint::*;
use pr_walk::*;

/// Options controlling one sync run.
#[derive(Debug, Clone, Default)]
pub struct SyncOptions {
    /// Start the recorded coverage window N days back instead of the default.
    ///
    /// This sets the *coverage claim* only. It does not bound the fetch: the
    /// PR and issue loops page until the source is exhausted or the
    /// incremental cursor stops them.
    pub days: Option<u32>,
    /// Skip fetching per-file patch text via REST (fast mode).
    pub skip_diffs: bool,
    /// Sync only pull requests (skip issues).
    pub pull_requests_only: bool,
    /// Sync only issues (skip pull requests).
    pub issues_only: bool,
}

/// Result summary of one sync run.
#[derive(Debug, Default)]
pub struct SyncSummary {
    pub pull_requests_synced: i64,
    pub issues_synced: i64,
    pub skipped: i64,
    pub failed: Vec<(String, String)>,
    pub pages_fetched: i64,
    /// True when the incremental cursor made the run a no-op.
    pub up_to_date: bool,
    /// Caveats the caller must surface: coverage this run could not reach, and
    /// why. Empty means the run served everything it claimed.
    pub notes: Vec<String>,
    /// True when the run stopped at its work budget with more to fetch. The
    /// position is checkpointed and the next run resumes from it; no coverage
    /// range or high-water cursor was claimed for the unfinished walk.
    pub partial: bool,
}

/// Default initial window when an entity has never been synced.
const INITIAL_WINDOW_DAYS: i64 = 90;

/// Page size for search-backed walks. Search nodes carry the same nested
/// collections as repository nodes, so the same conservative page applies.
const SEARCH_PAGE_SIZE: u32 = 25;

/// How far a created-date window may be bisected before giving up on the
/// 1,000-result cap. A month halved eight times is under four hours, well past
/// the point where a further split can help.
const MAX_WINDOW_SPLIT_DEPTH: u32 = 8;

/// Orchestrates syncing one entity at a time.
pub struct Syncer<'a> {
    connection: &'a Connection,
    client: &'a mut GhClient,
    /// The instant this run is anchored to. Injectable so coverage-window
    /// arithmetic is deterministic under test.
    now: DateTime<Utc>,
    /// Optional cap on pull requests processed per repository walk. When the
    /// cap is reached at a page boundary the walk checkpoints and yields, so
    /// one large repository cannot consume a whole scheduled run.
    item_budget: Option<i64>,
    /// When set, a pull request whose API file listing is capped, short, or
    /// contradictory is completed from git (see [`git_files`]).
    git_recovery: Option<GitRecoveryConfig>,
}

fn search_timestamp(seconds: i64) -> Result<String> {
    DateTime::from_timestamp(seconds, 0)
        .map(|instant| instant.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .ok_or_else(|| {
            crate::Error::InvalidArgument(format!("search window bound {seconds} is out of range"))
        })
}

/// GitHub's message for a connection cursor it no longer accepts.
fn is_invalid_cursor_error(error: &crate::Error) -> bool {
    matches!(error, crate::Error::GitHubApi(message)
        if message.to_lowercase().contains("valid cursor"))
}

impl<'a> Syncer<'a> {
    pub fn new(connection: &'a Connection, client: &'a mut GhClient) -> Self {
        Self {
            connection,
            client,
            now: Utc::now(),
            item_budget: None,
            git_recovery: None,
        }
    }

    /// Complete capped or contradictory pull-request file listings from git.
    pub fn with_git_recovery(mut self, config: GitRecoveryConfig) -> Self {
        self.git_recovery = Some(config);
        self
    }

    /// Cap the pull requests one repository walk processes before it
    /// checkpoints and yields. The remainder resumes on the next run.
    pub fn with_item_budget(mut self, items: i64) -> Self {
        self.item_budget = Some(items.max(1));
        self
    }

    /// Anchor this run to a specific instant instead of the wall clock.
    pub fn as_of(mut self, instant: DateTime<Utc>) -> Self {
        self.now = instant;
        self
    }

    /// Sync a repository ("owner/name"): PRs (M1) and issues (M2).
    pub fn sync_repository(
        &mut self,
        repository_name: &str,
        options: &SyncOptions,
    ) -> Result<SyncSummary> {
        let repository_name = repository_name.to_lowercase();
        let entity_key = format!("repo:{repository_name}");

        sync_state::acquire_lock(self.connection, &entity_key)?;
        sync_state::start_job(self.connection, &entity_key)?;

        let outcome = self.sync_repository_locked(&repository_name, &entity_key, options);

        match &outcome {
            Ok(summary) => {
                sync_state::complete_job_outcome(
                    self.connection,
                    &entity_key,
                    summary.pull_requests_synced + summary.issues_synced,
                    summary.skipped,
                    &summary.failed,
                    summary.partial,
                )?;
            }
            Err(error) => {
                sync_state::fail_job(self.connection, &entity_key, &error.to_string())?;
            }
        }
        sync_state::release_lock(self.connection, &entity_key)?;
        outcome
    }

    fn sync_repository_locked(
        &mut self,
        repository_name: &str,
        entity_key: &str,
        options: &SyncOptions,
    ) -> Result<SyncSummary> {
        let (owner, name) = repository_name.split_once('/').ok_or_else(|| {
            crate::Error::InvalidArgument(format!("expected owner/name, got '{repository_name}'"))
        })?;

        let mut summary = SyncSummary::default();
        let cursor_before =
            sync_state::last_updated_cursor(self.connection, "repo", repository_name)?;

        // Determine the work window from coverage + --days, in the warehouse's
        // own calendar: these strings live alongside `*_date_key` values, which
        // are built in the configured zone.
        let today = time_dimension::today_as_of(self.connection, self.now)?;
        // Never claim the day that is still in progress locally. An item
        // created later today would otherwise fall inside a range recorded as
        // fully covered and be skipped by any reader that trusts the claim.
        let coverage_end = today - Duration::days(1);
        let window_start = match options.days {
            Some(days) => today - Duration::days(days as i64),
            None => match sync_state::coverage_extent_as_of(self.connection, entity_key, self.now)?
            {
                // Incremental: the cursor governs what is fetched; the range
                // only needs to extend coverage to the last complete day.
                Some(_) => coverage_end,
                None => today - Duration::days(INITIAL_WINDOW_DAYS),
            },
        }
        .min(coverage_end);

        if !options.issues_only {
            self.sync_pull_requests(
                owner,
                name,
                repository_name,
                entity_key,
                cursor_before.as_deref(),
                options,
                &mut summary,
            )?;
        }

        if summary.partial {
            // The PR walk yielded with its position checkpointed. Claiming
            // coverage or freshness now would overstate what the warehouse
            // holds, and spending the rest of the run on issues would defeat
            // the budget that keeps scheduled runs fair across sources.
            return Ok(summary);
        }

        if !options.pull_requests_only {
            let issue_cursor =
                sync_state::last_updated_cursor(self.connection, "repo_issues", repository_name)?;
            self.sync_issues(
                owner,
                name,
                repository_name,
                entity_key,
                issue_cursor.as_deref(),
                &mut summary,
            )?;
        }

        if !summary.failed.is_empty() {
            summary.partial = true;
            summary.notes.push(
                "Some records failed; coverage remains incomplete and they will be retried.".into(),
            );
            return Ok(summary);
        }
        // Record coverage through the last complete local day and advance the
        // cursor.
        sync_state::record_range_as_of(
            self.connection,
            entity_key,
            &window_start.format("%Y-%m-%d").to_string(),
            &coverage_end.format("%Y-%m-%d").to_string(),
            summary.pull_requests_synced + summary.issues_synced,
            self.now,
        )?;
        monitor_repository::touch_repo(self.connection, repository_name)?;
        Ok(summary)
    }

    fn resolve_actor(&self, actor: Option<&ActorReference>, bot_suffix: &str) -> Result<String> {
        match actor {
            Some(actor) => repository::ensure_entity(self.connection, actor, bot_suffix),
            None => repository::ensure_ghost_entity(self.connection),
        }
    }
}

/// Run `body` inside a SQLite savepoint: commit on success, roll every write
/// back on failure, so a target that fails part-way leaves no partial rows.
fn within_savepoint<T>(conn: &Connection, body: impl FnOnce() -> Result<T>) -> Result<T> {
    conn.execute_batch("SAVEPOINT githubdw_refresh_target")?;
    match body() {
        Ok(value) => {
            conn.execute_batch("RELEASE SAVEPOINT githubdw_refresh_target")?;
            Ok(value)
        }
        Err(error) => {
            conn.execute_batch(
                "ROLLBACK TO SAVEPOINT githubdw_refresh_target;
                 RELEASE SAVEPOINT githubdw_refresh_target",
            )?;
            Err(error)
        }
    }
}

/// Partition `[start, end]` into calendar-month windows.
///
/// Search caps at 1,000 results per query, so a backfill cannot ask for a
/// person's whole history in one call. Months are the natural first cut: they
/// keep the query count low for a normal contributor, and a month that still
/// overflows is bisected further by the walk itself.
pub fn month_windows(start: NaiveDate, end: NaiveDate) -> Vec<(NaiveDate, NaiveDate)> {
    if start > end {
        return Vec::new();
    }
    let mut windows = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let month_end = last_day_of_month(cursor);
        let window_end = month_end.min(end);
        windows.push((cursor, window_end));
        cursor = window_end + Duration::days(1);
    }
    windows
}

fn last_day_of_month(date: NaiveDate) -> NaiveDate {
    let (year, month) = (date.year(), date.month());
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .map(|first| first - Duration::days(1))
        // Only unreachable for a year at the edge of the calendar; falling back
        // to the input keeps the window non-empty rather than panicking.
        .unwrap_or(date)
}
