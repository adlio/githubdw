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

/// Outcome of git-backed recovery for one pull request: `None` when it did
/// not run, `Some(Err(reason))` when it ran and could not prove a set.
type Recovery = Option<std::result::Result<RecoveredFileSet, String>>;

/// Run git-backed recovery when the API listing cannot be trusted. All
/// network and git work happens here, before any savepoint opens.
fn recover_files(
    client: &mut GhClient,
    config: Option<&GitRecoveryConfig>,
    repo_key: &str,
    pull_request: &PullRequestData,
    patches: Option<&Result<pull_requests::PatchListing>>,
    want_patches: bool,
) -> Recovery {
    let config = config?;
    let listing = patches.and_then(|listing| listing.as_ref().ok());
    if !git_files::needs_recovery(pull_request, listing) {
        return None;
    }
    // The REST walk carries GitHub's rename pairs; fetch it if fast mode
    // skipped it. It is used for validation only, never stored here.
    let fetched;
    let listing = match patches {
        Some(Ok(listing)) => listing,
        Some(Err(error)) => return Some(Err(format!("REST file listing failed: {error}"))),
        None => {
            fetched = pull_requests::fetch_file_patches(client, repo_key, pull_request.number);
            match &fetched {
                Ok(listing) => listing,
                Err(error) => return Some(Err(format!("REST file listing failed: {error}"))),
            }
        }
    };
    Some(
        git_files::recover_pull_request_files(
            client,
            config,
            repo_key,
            pull_request,
            listing,
            want_patches,
        )
        .map_err(|error| error.to_string()),
    )
}

/// Drop the hydrator's truncation note once git proved the complete set.
fn without_superseded_notes(note: Option<String>, recovered: &Recovery) -> Vec<String> {
    let proven = matches!(recovered, Some(Ok(_)));
    note.map(|note| {
        note.split("; ")
            // The hydrator's truncation caveat itself contains "; ", so both
            // halves are recognised.
            .filter(|part| {
                !(proven
                    && (*part == "file coverage is truncated"
                        || (part.contains("provider listed") && part.ends_with("changed files"))))
            })
            .map(str::to_string)
            .collect()
    })
    .unwrap_or_default()
}

/// `sync_metadata.source_type` under which an unfinished repository PR walk
/// keeps its resume position. Distinct from `repo`, so the real high-water
/// cursor is never touched until a walk completes.
pub const REPO_PR_CHECKPOINT: &str = "repo_pr_checkpoint";

/// What a newest-first walk may safely commit as its high-water mark.
///
/// A walk serves items `updated_at` DESC and later runs stop at the first item
/// strictly older than the committed cursor. So the committable value is the
/// newest landed item, capped at the OLDEST failed item: anything below that
/// either landed or sat below the previous cursor, and the failed item itself
/// (equal to the cap) is re-read because the stop is strict.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WalkWatermark {
    max_landed: Option<String>,
    min_failed: Option<String>,
    /// A failure whose `updated_at` is unknown cannot be bounded, so it pins
    /// the cursor where it was.
    #[serde(default)]
    unbounded_failure: bool,
}

impl WalkWatermark {
    fn landed(&mut self, updated: Option<&str>) {
        if let Some(updated) = updated
            && self.max_landed.as_deref().is_none_or(|max| updated > max)
        {
            self.max_landed = Some(updated.to_string());
        }
    }

    fn failed(&mut self, updated: Option<&str>) {
        match updated {
            Some(updated) => {
                if self.min_failed.as_deref().is_none_or(|min| updated < min) {
                    self.min_failed = Some(updated.to_string());
                }
            }
            None => self.unbounded_failure = true,
        }
    }

    /// The cursor this walk may commit, or `None` to leave it unchanged.
    /// `advance_cursor` never lowers a stored cursor, so this is an upper
    /// bound on the move, never a rewind.
    fn committable(&self) -> Option<String> {
        if self.unbounded_failure {
            return None;
        }
        match (&self.max_landed, &self.min_failed) {
            (Some(landed), Some(failed)) => Some(landed.min(failed).clone()),
            (Some(landed), None) => Some(landed.clone()),
            (None, Some(failed)) => Some(failed.clone()),
            (None, None) => None,
        }
    }
}

/// Durable resume position of an unfinished repository PR walk.
///
/// Bound to the high-water cursor the walk started from (`base`): a checkpoint
/// whose base no longer matches the stored cursor belongs to a different walk
/// and is discarded rather than trusted.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PullRequestWalkCheckpoint {
    version: u32,
    base: Option<String>,
    /// GraphQL `endCursor` of the last page whose items were all processed.
    after: String,
    watermark: WalkWatermark,
    #[serde(default)]
    pages: i64,
    #[serde(default)]
    items: i64,
    /// Times this walk restarted from the newest page after GitHub rejected
    /// the saved cursor.
    #[serde(default)]
    restarts: u32,
}

const CHECKPOINT_VERSION: u32 = 1;

/// Search offsets are mutable. Persist completed time windows instead, splitting
/// until every leaf fits in one response (no offset can skip a moved record).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct UserUpdateCheckpoint {
    version: u32,
    base: String,
    upper: i64,
    pending: Vec<(i64, i64)>,
}

fn search_timestamp(seconds: i64) -> String {
    DateTime::from_timestamp(seconds, 0)
        .expect("timestamp originated from a parsed UTC date")
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
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

    /// Sync one monitored user: pull requests they authored and reviews they
    /// gave, across every repository GitHub will show us.
    ///
    /// The repository path can walk one repo's `pullRequests` connection to
    /// exhaustion. A user's work has no such connection — it is spread across
    /// repositories, so search is the only endpoint that can enumerate it. Two
    /// searches run per user (`author:` and `reviewed-by:`), each with its own
    /// watermark, and both feed the same entity/fact pipeline the repository
    /// path uses.
    ///
    /// Repositories discovered this way get their dimension row so the facts
    /// have somewhere to hang, but they are *not* added to `monitored_repos`:
    /// syncing a person is a statement about that person, not an instruction to
    /// start tracking every repo they contributed to.
    pub fn sync_user(&mut self, login: &str, options: &SyncOptions) -> Result<SyncSummary> {
        let login = crate::query::to_bare_login(login);
        let entity_key = format!("user:{login}");

        sync_state::acquire_lock(self.connection, &entity_key)?;
        sync_state::start_job(self.connection, &entity_key)?;

        let outcome = self.sync_user_locked(&login, &entity_key, options);

        match &outcome {
            Ok(summary) => {
                sync_state::complete_job_outcome(
                    self.connection,
                    &entity_key,
                    summary.pull_requests_synced,
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

    fn sync_user_locked(
        &mut self,
        login: &str,
        entity_key: &str,
        options: &SyncOptions,
    ) -> Result<SyncSummary> {
        let mut summary = SyncSummary::default();
        if options.issues_only {
            // Issues are fetched per repository; there is no user-scoped issue
            // search in this release. Saying so beats returning zero silently.
            summary.notes.push(format!(
                "{login}: --issues-only has no user-scoped equivalent; user sync covers pull \
                 requests and reviews"
            ));
            return Ok(summary);
        }

        let today = time_dimension::today_as_of(self.connection, self.now)?;
        // Same rule as the repository path: never claim the day still running
        // in the warehouse's own calendar.
        let coverage_end = today - Duration::days(1);
        let coverage_before =
            sync_state::coverage_extent_as_of(self.connection, entity_key, self.now)?;
        let window_start = match options.days {
            Some(days) => today - Duration::days(days as i64),
            None => match coverage_before.as_ref() {
                Some((_, end)) => {
                    NaiveDate::parse_from_str(end, "%Y-%m-%d").map_err(|e| {
                        crate::error::Error::InvalidArgument(format!("bad coverage date: {e}"))
                    })? + Duration::days(1)
                }
                None => today - Duration::days(INITIAL_WINDOW_DAYS),
            },
        }
        .min(coverage_end);

        for role in UserRole::all() {
            let cursor =
                sync_state::last_updated_cursor(self.connection, role.cursor_source_type(), login)?;
            let mut watermark = WalkWatermark::default();

            // Incremental when there is both prior coverage and a watermark to
            // stop on. Otherwise this is a backfill, and a backfill must be
            // windowed: `sort:updated-desc` plus a watermark stop cannot reach
            // an old PR whose `updated_at` already sits below the cursor.
            let incremental = options.days.is_none() && coverage_before.is_some();
            if incremental && cursor.is_some() {
                self.walk_user_search(
                    login,
                    role,
                    None,
                    cursor.as_deref(),
                    entity_key,
                    options,
                    &mut summary,
                    &mut watermark,
                    0,
                )?;
            } else {
                for (start, end) in month_windows(window_start, coverage_end) {
                    self.walk_user_search(
                        login,
                        role,
                        Some((start, end)),
                        None,
                        entity_key,
                        options,
                        &mut summary,
                        &mut watermark,
                        0,
                    )?;
                }
            }

            if !summary.partial {
                sync_state::advance_cursor(
                    self.connection,
                    role.cursor_source_type(),
                    login,
                    watermark.committable().as_deref(),
                )?;
            }
        }

        summary.partial |= !summary.failed.is_empty();
        if summary.partial {
            return Ok(summary);
        }
        summary.up_to_date = summary.pull_requests_synced == 0 && summary.failed.is_empty();
        sync_state::record_range_as_of(
            self.connection,
            entity_key,
            &window_start.format("%Y-%m-%d").to_string(),
            &coverage_end.format("%Y-%m-%d").to_string(),
            summary.pull_requests_synced,
            self.now,
        )?;
        monitor_repository::touch_user(self.connection, login)?;
        Ok(summary)
    }

    /// Walk one search query to exhaustion, splitting the created-date window
    /// when GitHub reports more matches than search will ever serve.
    #[allow(clippy::too_many_arguments)]
    fn walk_user_search(
        &mut self,
        login: &str,
        role: UserRole,
        window: Option<(NaiveDate, NaiveDate)>,
        cursor_stop: Option<&str>,
        entity_key: &str,
        options: &SyncOptions,
        summary: &mut SyncSummary,
        watermark: &mut WalkWatermark,
        depth: u32,
    ) -> Result<()> {
        if let Some(base) = cursor_stop
            && sync_state::load_checkpoint(
                self.connection,
                "user_update_checkpoint",
                &format!("{}:{login}", role.qualifier()),
            )?
            .is_some()
        {
            return self.walk_user_update_windows(login, role, base, entity_key, options, summary);
        }
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);

        let window_text = window.map(|(start, end)| {
            (
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            )
        });
        let expression = user_search::user_search_expression(
            login,
            role,
            window_text
                .as_ref()
                .map(|(start, end)| (start.as_str(), end.as_str())),
        );

        let mut page_cursor: Option<String> = None;
        let mut page_size = SEARCH_PAGE_SIZE;
        let mut first_page = true;
        let mut reported_total: i64 = 0;
        let mut stopped_at_watermark = false;

        'pages: loop {
            let query = user_search::user_pull_requests_query(page_size);
            let mut variables: Vec<(&str, &str)> = vec![("q", expression.as_str())];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = match self.client.graphql_search(&query, &variables) {
                Ok(data) => data,
                Err(error) if page_size > 1 => {
                    page_size = (page_size / 2).max(1);
                    let _ = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let page = user_search::parse_user_search_page(&data)?;
            summary.pages_fetched += 1;

            if first_page {
                reported_total = page.total_count;
                if let Some(base) = cursor_stop
                    && page.exceeds_result_cap()
                    && page.items.first().is_none_or(|item| {
                        item.pull_request
                            .updated_at
                            .as_deref()
                            .is_none_or(|at| at >= base)
                    })
                {
                    return self
                        .walk_user_update_windows(login, role, base, entity_key, options, summary);
                }
                // A windowed walk intends to paginate to exhaustion, so a match
                // count over the cap is decided here — before any of this
                // window's work is spent on a query that cannot serve it all.
                //
                // An incremental walk is bounded by its watermark instead, and a
                // user with thousands of lifetime matches is the normal case
                // there. Whether the cap actually cost anything is only knowable
                // once the walk ends, so it is checked after the loop.
                if cursor_stop.is_none() && page.exceeds_result_cap() {
                    if let Some((start, end)) = window
                        && start < end
                        && depth < MAX_WINDOW_SPLIT_DEPTH
                    {
                        let midpoint = start + Duration::days((end - start).num_days() / 2);
                        for half in [(start, midpoint), (midpoint + Duration::days(1), end)] {
                            self.walk_user_search(
                                login,
                                role,
                                Some(half),
                                cursor_stop,
                                entity_key,
                                options,
                                summary,
                                watermark,
                                depth + 1,
                            )?;
                        }
                        return Ok(());
                    }
                    // Nothing left to narrow: report the shortfall instead of
                    // recording coverage that was never fetched. Keep both the
                    // date range and the role cursor open for a future backfill.
                    summary.partial = true;
                    watermark.unbounded_failure = true;
                    summary.notes.push(format!(
                        "{login} ({}): '{expression}' matches {} items but GitHub search serves \
                         at most {}; results beyond the cap in this window were not fetched",
                        role.qualifier(),
                        page.total_count,
                        user_search::SEARCH_RESULT_CAP
                    ));
                }
            }
            first_page = false;

            let page_is_empty = page.items.is_empty();
            for item in &page.items {
                // Strict comparison, exactly as the repository walk does it: an
                // item updated in the same second as the cursor may have missed
                // the previous run's page, and re-ingesting it is a free
                // idempotent upsert where skipping it is permanent.
                if let (Some(cursor), Some(updated)) =
                    (cursor_stop, item.pull_request.updated_at.as_deref())
                    && updated < cursor
                {
                    stopped_at_watermark = true;
                    break 'pages;
                }

                let repo_key = repository::upsert_repository(
                    self.connection,
                    &item.repository.name_with_owner,
                    item.repository.primary_language.as_deref(),
                    item.repository.is_fork,
                    item.repository.is_private,
                    item.repository.default_branch.as_deref(),
                    item.repository.created_at.as_deref(),
                )?;
                let pr_key = format!("{repo_key}#{}", item.pull_request.number);
                match self.upsert_one_pull_request(
                    &repo_key,
                    &pr_key,
                    &item.pull_request,
                    timezone,
                    core_hours,
                    &bot_suffix,
                    options,
                ) {
                    Ok(note) => {
                        summary.notes.extend(note);
                        summary.pull_requests_synced += 1;
                        watermark.landed(item.pull_request.updated_at.as_deref());
                    }
                    Err(error) => {
                        // Caps the committable watermark at this item, so a
                        // failure in the middle of the walk is retried rather
                        // than stranded below a newer item's timestamp.
                        watermark.failed(item.pull_request.updated_at.as_deref());
                        summary.failed.push((pr_key.clone(), error.to_string()));
                    }
                }
                sync_state::update_lock_progress(
                    self.connection,
                    entity_key,
                    summary.pull_requests_synced + summary.failed.len() as i64,
                    &pr_key,
                    summary.pull_requests_synced,
                    summary.skipped,
                    summary.failed.len() as i64,
                )?;
            }

            if !page.has_next_page || page_is_empty {
                break;
            }
            page_cursor = page.end_cursor;
            if page_cursor.is_none() {
                break;
            }
        }

        // An incremental walk that ran out of pages *without* reaching its
        // watermark did not converge: search stopped serving results before the
        // gap closed, so items between the cap and the watermark are still
        // missing. A walk that reached the watermark is complete no matter how
        // many lifetime matches the query reports, which is why this cannot be
        // decided from the match count alone.
        if cursor_stop.is_some()
            && !stopped_at_watermark
            && reported_total > user_search::SEARCH_RESULT_CAP
        {
            summary.partial = true;
            watermark.unbounded_failure = true;
            summary.notes.push(format!(
                "{login} ({}): incremental walk ran out of search results before reaching its \
                 watermark ({} matches, {} served per query); backfill the gap with `githubdw \
                 sync user {login} --days N`",
                role.qualifier(),
                reported_total,
                user_search::SEARCH_RESULT_CAP
            ));
        }
        Ok(())
    }

    /// Resume a large incremental user search without persisting unstable search
    /// offsets. A leaf is committed only when its complete result fits one page.
    #[allow(clippy::too_many_arguments)]
    fn walk_user_update_windows(
        &mut self,
        login: &str,
        role: UserRole,
        base: &str,
        entity_key: &str,
        options: &SyncOptions,
        summary: &mut SyncSummary,
    ) -> Result<()> {
        const KIND: &str = "user_update_checkpoint";
        let key = format!("{}:{login}", role.qualifier());
        let lower = DateTime::parse_from_rfc3339(base)
            .map_err(|e| crate::error::Error::InvalidArgument(format!("bad user cursor: {e}")))?
            .timestamp();
        let upper = self.now.timestamp().max(lower);
        let mut saved = sync_state::load_checkpoint(self.connection, KIND, &key)?
            .and_then(|text| serde_json::from_str::<UserUpdateCheckpoint>(&text).ok())
            .filter(|c| {
                c.version == CHECKPOINT_VERSION
                    && c.base == base
                    && c.upper >= lower
                    && c.upper <= upper
                    && c.pending.first().is_none_or(|(_, end)| *end == c.upper)
                    && c.pending
                        .windows(2)
                        .all(|pair| pair[1].1.checked_add(1) == Some(pair[0].0))
                    && c.pending
                        .iter()
                        .all(|(a, b)| lower <= *a && a <= b && *b <= c.upper)
            })
            .unwrap_or_else(|| UserUpdateCheckpoint {
                version: CHECKPOINT_VERSION,
                base: base.to_string(),
                upper,
                pending: vec![(lower, upper)],
            });
        sync_state::save_checkpoint(self.connection, KIND, &key, &serde_json::to_string(&saved)?)?;
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);
        let mut page_size = SEARCH_PAGE_SIZE;

        while let Some(&(start, end)) = saved.pending.last() {
            if summary.pull_requests_synced + summary.failed.len() as i64
                >= self.item_budget.unwrap_or(500)
            {
                summary.partial = true;
                summary.notes.push(format!(
                    "{login} ({}): catch-up paused with {} update windows remaining",
                    role.qualifier(),
                    saved.pending.len()
                ));
                return Ok(());
            }
            let expression = format!(
                "is:pr {}:{login} updated:{}..{} sort:updated-desc",
                role.qualifier(),
                search_timestamp(start),
                search_timestamp(end),
            );
            let data = loop {
                match self.client.graphql_search(
                    &user_search::user_pull_requests_query(page_size),
                    &[("q", expression.as_str())],
                ) {
                    Ok(data) => break data,
                    Err(_) if page_size > 1 => page_size = (page_size / 2).max(1),
                    Err(error) => return Err(error),
                }
            };
            let page = user_search::parse_user_search_page(&data)?;
            summary.pages_fetched += 1;
            if page.total_count > i64::from(page_size) || page.has_next_page {
                if start == end {
                    summary.partial = true;
                    summary.notes.push(format!("{login} ({}): {} matches share one second; cannot safely complete this update window", role.qualifier(), page.total_count));
                    return Ok(());
                }
                let middle = start + (end - start) / 2;
                saved.pending.pop();
                // Stack: process oldest first, with disjoint inclusive seconds.
                saved.pending.push((middle + 1, end));
                saved.pending.push((start, middle));
            } else {
                // A sparse or malformed response must not close a coverage gap.
                // GitHub's search-index date and the returned PR updatedAt can
                // differ. Ingest every returned node from a complete page, even
                // if newer than the query window; only the frozen search bound
                // is committed, so later updates remain eligible next time.
                if page.total_count != page.items.len() as i64
                    || page.items.iter().any(|item| {
                        item.pull_request
                            .updated_at
                            .as_deref()
                            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                            .is_none()
                    })
                {
                    summary.partial = true;
                    summary.notes.push(format!(
                        "{login}: incomplete or malformed search response; window retained"
                    ));
                    return Ok(());
                }
                let failures_before = summary.failed.len();
                for item in &page.items {
                    let repo_key = repository::upsert_repository(
                        self.connection,
                        &item.repository.name_with_owner,
                        item.repository.primary_language.as_deref(),
                        item.repository.is_fork,
                        item.repository.is_private,
                        item.repository.default_branch.as_deref(),
                        item.repository.created_at.as_deref(),
                    )?;
                    let pr_key = format!("{repo_key}#{}", item.pull_request.number);
                    match self.upsert_one_pull_request(
                        &repo_key,
                        &pr_key,
                        &item.pull_request,
                        timezone,
                        core_hours,
                        &bot_suffix,
                        options,
                    ) {
                        Ok(note) => {
                            summary.notes.extend(note);
                            summary.pull_requests_synced += 1;
                        }
                        Err(error) => summary.failed.push((pr_key.clone(), error.to_string())),
                    }
                    sync_state::update_lock_progress(
                        self.connection,
                        entity_key,
                        summary.pull_requests_synced + summary.failed.len() as i64,
                        &pr_key,
                        summary.pull_requests_synced,
                        summary.skipped,
                        summary.failed.len() as i64,
                    )?;
                }
                if summary.failed.len() != failures_before {
                    summary.partial = true;
                    return Ok(());
                }
                saved.pending.pop();
            }
            sync_state::save_checkpoint(
                self.connection,
                KIND,
                &key,
                &serde_json::to_string(&saved)?,
            )?;
        }
        // The frozen bound, not the newest observed item, closes this walk.
        // Future updates are fetched by the next incremental walk. Keep the
        // freshness stamp at that bound when resuming a walk from a prior run.
        let boundary = search_timestamp(saved.upper);
        let transaction = self.connection.unchecked_transaction()?;
        sync_state::advance_cursor(
            &transaction,
            role.cursor_source_type(),
            login,
            Some(&boundary),
        )?;
        transaction.execute(
            "UPDATE sync_metadata SET last_sync_at=?3 WHERE source_type=?1 AND source_id=?2",
            rusqlite::params![role.cursor_source_type(), login, boundary],
        )?;
        sync_state::clear_checkpoint(&transaction, KIND, &key)?;
        transaction.commit()?;
        if saved.upper < self.now.timestamp() {
            summary.partial = true;
            summary.notes.push(format!(
                "{login} ({}): caught up through {boundary}; newer updates remain for the next run",
                role.qualifier()
            ));
        }
        Ok(())
    }

    /// Walk a repository's pull requests newest-first, resumably.
    ///
    /// The walk is checkpointed after every fully processed page, so a run
    /// that is killed, fails mid-walk, or yields at its item budget leaves a
    /// durable position and the next run continues from it instead of
    /// re-walking from the newest page. The real high-water cursor moves only
    /// once the walk reaches its stop, and then only as far as
    /// [`WalkWatermark::committable`] allows.
    #[allow(clippy::too_many_arguments)]
    fn sync_pull_requests(
        &mut self,
        owner: &str,
        name: &str,
        repository_name: &str,
        entity_key: &str,
        updated_cursor: Option<&str>,
        options: &SyncOptions,
        summary: &mut SyncSummary,
    ) -> Result<()> {
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);

        // Resume only a checkpoint that belongs to this walk: same format,
        // started from the cursor that is still stored. Anything else (corrupt
        // text, a walk begun from an older cursor) is dropped — it can only
        // cost re-reading pages, never skipping one.
        let checkpoint: Option<PullRequestWalkCheckpoint> =
            sync_state::load_checkpoint(self.connection, REPO_PR_CHECKPOINT, repository_name)?
                .and_then(|text| serde_json::from_str::<PullRequestWalkCheckpoint>(&text).ok())
                .filter(|saved| {
                    saved.version == CHECKPOINT_VERSION
                        && saved.base.as_deref() == updated_cursor
                        && !saved.after.is_empty()
                });
        if checkpoint.is_none() {
            sync_state::clear_checkpoint(self.connection, REPO_PR_CHECKPOINT, repository_name)?;
        }
        let mut watermark = checkpoint
            .as_ref()
            .map(|saved| saved.watermark.clone())
            .unwrap_or_default();
        let mut page_cursor: Option<String> = checkpoint.as_ref().map(|saved| saved.after.clone());
        let mut walk_pages = checkpoint.as_ref().map_or(0, |saved| saved.pages);
        let mut walk_items = checkpoint.as_ref().map_or(0, |saved| saved.items);
        let mut restarts = checkpoint.as_ref().map_or(0, |saved| saved.restarts);
        if let Some(saved) = &checkpoint {
            summary.notes.push(format!(
                "{repository_name}: resumed pull request walk after {} page(s), {} item(s)",
                saved.pages, saved.items
            ));
        }
        let mut restarted_this_run = false;
        // Page cursors served this run: a provider that repeats one is cycling.
        let mut seen_page_cursors: std::collections::HashSet<String> =
            page_cursor.iter().cloned().collect();

        let mut item_index: i64 = 0;
        let mut processed_this_run: i64 = 0;
        // Adaptive page size: heavy PRs (many files/comments/checks) can
        // overflow GitHub's response stream; halve and retry on failure.
        let mut page_size: u32 = 25;

        'pages: loop {
            let query = pull_requests::repository_pull_requests_query(page_size);
            let mut variables: Vec<(&str, &str)> = vec![("owner", owner), ("name", name)];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = match self.client.graphql(&query, &variables) {
                Ok(data) => data,
                // A saved cursor GitHub no longer accepts: restart this walk
                // from the newest page, keeping its base and watermark. At most
                // once per run, so a persistent rejection cannot loop.
                Err(error)
                    if is_invalid_cursor_error(&error)
                        && page_cursor.is_some()
                        && !restarted_this_run =>
                {
                    restarted_this_run = true;
                    restarts += 1;
                    page_cursor = None;
                    summary.notes.push(format!(
                        "{repository_name}: saved page cursor was rejected; restarted the \
                         walk from the newest page"
                    ));
                    continue;
                }
                Err(error) if page_size > 1 => {
                    // Degrade and retry the same page with a smaller window.
                    page_size = (page_size / 2).max(1);
                    let _ = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let page = pull_requests::parse_pull_request_page(&data)?;
            summary.pages_fetched += 1;

            // Upsert the repository dimension from the page header.
            let repo_key = repository::upsert_repository(
                self.connection,
                &page.repository.name_with_owner,
                page.repository.primary_language.as_deref(),
                page.repository.is_fork,
                page.repository.is_private,
                page.repository.default_branch.as_deref(),
                page.repository.created_at.as_deref(),
            )?;

            let page_is_empty = page.pull_requests.is_empty();
            for pull_request in &page.pull_requests {
                // Incremental stop: results are updated-desc, so once an item
                // is *strictly* older than the cursor everything after it is
                // already ingested.
                //
                // The comparison must be strict. `updated_at` is
                // second-granular, so an item updated in the same second as
                // the cursor — after the previous run had already served its
                // page — sits exactly at the boundary. Stopping there would
                // skip it forever, since only a further update (which moves
                // its timestamp) could bring it back. Re-ingesting the
                // boundary second instead is free: the upserts are
                // `ON CONFLICT` idempotent.
                if let (Some(cursor), Some(updated)) =
                    (updated_cursor, pull_request.updated_at.as_deref())
                    && updated < cursor
                {
                    summary.up_to_date = summary.pull_requests_synced == 0;
                    break 'pages;
                }

                item_index += 1;
                processed_this_run += 1;
                let pr_key = format!("{repo_key}#{}", pull_request.number);
                match self.upsert_one_pull_request(
                    &repo_key,
                    &pr_key,
                    pull_request,
                    timezone,
                    core_hours,
                    &bot_suffix,
                    options,
                ) {
                    Ok(note) => {
                        summary.notes.extend(note);
                        summary.pull_requests_synced += 1;
                        watermark.landed(pull_request.updated_at.as_deref());
                    }
                    Err(error) => {
                        // A failed item caps the committable watermark at its
                        // own timestamp, wherever it sits in the walk, so the
                        // next walk reaches it again.
                        watermark.failed(pull_request.updated_at.as_deref());
                        summary.failed.push((pr_key.clone(), error.to_string()));
                    }
                }
                sync_state::update_lock_progress(
                    self.connection,
                    entity_key,
                    item_index,
                    &pr_key,
                    summary.pull_requests_synced,
                    summary.skipped,
                    summary.failed.len() as i64,
                )?;
            }
            walk_pages += 1;
            walk_items += page.pull_requests.len() as i64;

            if !page.has_next_page {
                break;
            }
            if page_is_empty {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: pull request page reports more results but holds none"
                )));
            }
            if let Some(next) = page.end_cursor.as_deref()
                && !seen_page_cursors.insert(next.to_string())
            {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: pull request page cursor repeated"
                )));
            }
            page_cursor = page.end_cursor;

            // Every item on this page is now processed (landed or recorded as
            // failed in the watermark), so the position after it is durable.
            let saved = PullRequestWalkCheckpoint {
                version: CHECKPOINT_VERSION,
                base: updated_cursor.map(str::to_string),
                after: page_cursor.clone().unwrap_or_default(),
                watermark: watermark.clone(),
                pages: walk_pages,
                items: walk_items,
                restarts,
            };
            sync_state::save_checkpoint(
                self.connection,
                REPO_PR_CHECKPOINT,
                repository_name,
                &serde_json::to_string(&saved)?,
            )?;

            if let Some(budget) = self.item_budget
                && processed_this_run >= budget
            {
                summary.partial = true;
                summary.up_to_date = false;
                summary.notes.push(format!(
                    "{repository_name}: pull request walk paused at its budget of {budget} \
                     item(s); the next run resumes from page {}",
                    walk_pages + 1
                ));
                return Ok(());
            }
        }

        sync_state::complete_walk(
            self.connection,
            "repo",
            REPO_PR_CHECKPOINT,
            repository_name,
            watermark.committable().as_deref().or(updated_cursor),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_one_pull_request(
        &mut self,
        repo_key: &str,
        pr_key: &str,
        pull_request: &PullRequestData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
        options: &SyncOptions,
    ) -> Result<Option<String>> {
        let mut pull_request = pull_request.clone();
        let note = pull_requests::hydrate_pull_request(self.client, &mut pull_request)?;
        // Fetch patch text BEFORE any write, then land every write for this
        // item in one savepoint: a failure part-way (including a patch write)
        // leaves the previously stored rows exactly as they were.
        let patches = (!options.skip_diffs && !pull_request.files.is_empty())
            .then(|| pull_requests::fetch_file_patches(self.client, repo_key, pull_request.number));
        let recovered = recover_files(
            self.client,
            self.git_recovery.as_ref(),
            repo_key,
            &pull_request,
            patches.as_ref(),
            !options.skip_diffs,
        );
        let connection = self.connection;
        let mut caveats = without_superseded_notes(note, &recovered);
        caveats.extend(within_savepoint(connection, || {
            self.store_pull_request(
                repo_key,
                pr_key,
                &pull_request,
                timezone,
                core_hours,
                bot_suffix,
                patches,
                recovered,
            )
        })?);
        Ok((!caveats.is_empty()).then(|| caveats.join("; ")))
    }

    /// Write one hydrated pull request and everything nested under it.
    ///
    /// Performs no network calls; callers wrap it in a savepoint. When the
    /// incoming file list is provably complete
    /// ([`pull_requests::authoritative_file_paths`]) it is authoritative and
    /// stored file rows absent from it are deleted; otherwise every stored row
    /// is kept and a caveat says so. Patch text, when supplied, is written
    /// after the file set settles. Returns the caveats.
    #[allow(clippy::too_many_arguments)]
    fn store_pull_request(
        &self,
        repo_key: &str,
        pr_key: &str,
        pull_request: &PullRequestData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
        patches: Option<Result<pull_requests::PatchListing>>,
        recovered: Recovery,
    ) -> Result<Vec<String>> {
        let conn = self.connection;
        let (changed_files, additions, deletions) = match &recovered {
            Some(Ok(set)) => (set.changed_files, set.additions, set.deletions),
            Some(Err(reason)) => return Err(crate::Error::GitRecovery(reason.clone())),
            None => (
                pull_request.changed_files,
                pull_request.additions,
                pull_request.deletions,
            ),
        };

        let author_key = self.resolve_actor(pull_request.author.as_ref(), bot_suffix)?;
        let merged_by_key = match pull_request.merged_by.as_ref() {
            Some(actor) => Some(repository::ensure_entity(conn, actor, bot_suffix)?),
            None => None,
        };

        let created = time_dimension::ensure_keys_for_timestamp(
            conn,
            &pull_request.created_at,
            timezone,
            core_hours,
        )?;
        let updated = pull_request
            .updated_at
            .as_deref()
            .map(|ts| time_dimension::ensure_keys_for_timestamp(conn, ts, timezone, core_hours))
            .transpose()?;
        let merged = pull_request
            .merged_at
            .as_deref()
            .map(|ts| time_dimension::ensure_keys_for_timestamp(conn, ts, timezone, core_hours))
            .transpose()?;

        let comment_count =
            (pull_request.conversation_comments.len() + pull_request.review_comments.len()) as i64;

        repository::upsert_pull_request(
            conn,
            &PullRequestRow {
                pr_key,
                number: pull_request.number,
                repo_key,
                author_key: &author_key,
                state: &pull_request.state,
                is_draft: pull_request.is_draft,
                title: pull_request.title.as_deref(),
                body: pull_request.body.as_deref(),
                base_ref: pull_request.base_ref.as_deref(),
                head_ref: pull_request.head_ref.as_deref(),
                created_at: &pull_request.created_at,
                updated_at: pull_request.updated_at.as_deref(),
                merged_at: pull_request.merged_at.as_deref(),
                closed_at: pull_request.closed_at.as_deref(),
                merged_by_key: merged_by_key.as_deref(),
                created_date_key: &created.date_key,
                created_time_key: &created.time_key,
                updated_date_key: updated.as_ref().map(|keys| keys.date_key.as_str()),
                updated_time_key: updated.as_ref().map(|keys| keys.time_key.as_str()),
                merged_date_key: merged.as_ref().map(|keys| keys.date_key.as_str()),
                comment_count,
                review_count: pull_request.reviews.len() as i64,
                changed_files,
                additions,
                deletions,
            },
        )?;

        for review in &pull_request.reviews {
            let Some(submitted_at) = review.submitted_at.as_deref() else {
                continue; // pending reviews have no timestamp
            };
            let reviewer_key = self.resolve_actor(review.author.as_ref(), bot_suffix)?;
            let submitted = time_dimension::ensure_keys_for_timestamp(
                conn,
                submitted_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_review(
                conn,
                &review.id,
                pr_key,
                &reviewer_key,
                &review.state,
                review.body.as_deref(),
                submitted_at,
                &submitted.date_key,
                &submitted.time_key,
            )?;
        }

        for comment in &pull_request.review_comments {
            let author = self.resolve_actor(comment.author.as_ref(), bot_suffix)?;
            let created_keys = time_dimension::ensure_keys_for_timestamp(
                conn,
                &comment.created_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_review_comment(
                conn,
                &comment.id,
                pr_key,
                &author,
                comment.in_reply_to.as_deref(),
                comment.path.as_deref(),
                comment.line,
                comment.body.as_deref(),
                &comment.created_at,
                &created_keys.date_key,
                &created_keys.time_key,
            )?;
        }

        for comment in &pull_request.conversation_comments {
            let author = self.resolve_actor(comment.author.as_ref(), bot_suffix)?;
            let created_keys = time_dimension::ensure_keys_for_timestamp(
                conn,
                &comment.created_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_issue_comment(
                conn,
                &comment.id,
                "pull_request",
                pr_key,
                &author,
                None,
                comment.body.as_deref(),
                &comment.created_at,
                &created_keys.date_key,
                &created_keys.time_key,
            )?;
        }

        if let Some(head_sha) = pull_request.head_sha.as_deref() {
            for check in &pull_request.check_runs {
                repository::upsert_check_run(
                    conn,
                    &check.id,
                    pr_key,
                    head_sha,
                    &check.name,
                    &check.status,
                    check.conclusion.as_deref(),
                    check.started_at.as_deref(),
                    check.completed_at.as_deref(),
                )?;
            }
        }

        match recovered {
            Some(Ok(set)) => {
                return Self::store_recovered_files(
                    conn,
                    repo_key,
                    pr_key,
                    pull_request.number,
                    &set,
                    patches.is_some(),
                );
            }
            Some(Err(reason)) => return Err(crate::Error::GitRecovery(reason)),
            None => {}
        }

        for file in &pull_request.files {
            repository::upsert_file_diff(
                conn,
                pr_key,
                repo_key,
                &file.path,
                None,
                &file.change_type,
                None,
                file.additions,
                file.deletions,
            )?;
        }

        let mut caveats = Vec::new();
        match pull_requests::authoritative_file_paths(pull_request) {
            Ok(current) => {
                let stale = repository::file_diffs_absent_from(conn, pr_key, &current)?;
                repository::delete_file_diffs(conn, pr_key, &stale)?;
            }
            Err(gap) => {
                let kept = repository::file_diffs_absent_from(
                    conn,
                    pr_key,
                    &pull_request
                        .files
                        .iter()
                        .map(|file| file.path.as_str())
                        .collect(),
                )?;
                if !kept.is_empty() || gap.anomalous {
                    caveats.push(format!(
                        "PR #{}: file list is not provably complete ({}); kept {} stored \
                         file row(s) absent from this response",
                        pull_request.number,
                        gap.reason,
                        kept.len()
                    ));
                }
            }
        }

        // Patch text backfill via REST (absent in fast mode).
        if let Some(listing) = patches {
            caveats.extend(Self::store_patch_listing(
                conn,
                pr_key,
                pull_request,
                listing,
            )?);
        }
        Ok(caveats)
    }

    /// Land a git-proven complete file set: every record (with GitHub's served
    /// rename pairs and patches, git's tail), then prune rows outside the set.
    /// Runs inside the caller's savepoint, so any failure rolls everything back.
    fn store_recovered_files(
        conn: &Connection,
        repo_key: &str,
        pr_key: &str,
        number: i64,
        set: &RecoveredFileSet,
        write_patches: bool,
    ) -> Result<Vec<String>> {
        for file in &set.files {
            repository::upsert_file_diff(
                conn,
                pr_key,
                repo_key,
                &file.path,
                file.previous_path.as_deref(),
                &file.change_type,
                None,
                file.additions,
                file.deletions,
            )?;
            if write_patches {
                repository::set_file_diff_patch(
                    conn,
                    pr_key,
                    &file.path,
                    file.previous_path.as_deref(),
                    file.patch.as_deref(),
                )?;
            }
        }
        let keep: std::collections::HashSet<&str> = set.keep.iter().map(String::as_str).collect();
        let stale = repository::file_diffs_absent_from(conn, pr_key, &keep)?;
        repository::delete_file_diffs(conn, pr_key, &stale)?;
        let records: std::collections::HashSet<&str> =
            set.files.iter().map(|file| file.path.as_str()).collect();
        let empty = std::collections::HashSet::new();
        let stored = repository::file_diffs_absent_from(conn, pr_key, &empty)?;
        let landed = stored
            .iter()
            .filter(|path| records.contains(path.as_str()))
            .count();
        if landed != set.files.len()
            || stored.len() != set.files.len()
            || set.files.len() as i64 != set.changed_files
        {
            return Err(crate::Error::GitRecovery(format!(
                "PR #{number}: {landed} of {} recovered records landed for {} changed files",
                set.files.len(),
                set.changed_files
            )));
        }
        let mut caveats = Vec::new();
        if write_patches && set.oversized_patches > 0 {
            caveats.push(format!(
                "PR #{number}: {} recovered patch(es) exceeded the size bound and were not stored",
                set.oversized_patches
            ));
        }
        Ok(caveats)
    }

    /// Store patch text for every file the REST files endpoint served.
    ///
    /// The endpoint pages at 100 files; every page is walked, and a failed or
    /// capped walk is returned as a caveat rather than passed off as the
    /// complete listing. Only UPDATEs existing file rows.
    fn store_patch_listing(
        conn: &Connection,
        pr_key: &str,
        pull_request: &PullRequestData,
        listing: Result<pull_requests::PatchListing>,
    ) -> Result<Vec<String>> {
        let mut caveats = Vec::new();
        match listing {
            Ok(listing) => {
                for (file_path, previous_path, patch) in &listing.files {
                    repository::set_file_diff_patch(
                        conn,
                        pr_key,
                        file_path,
                        previous_path.as_deref(),
                        patch.as_deref(),
                    )?;
                }
                let served = listing.files.len();
                if listing.provider_capped
                    && pull_request.changed_files > pull_requests::REST_FILE_LIMIT as i64
                {
                    caveats.push(format!(
                        "PR #{}: provider serves patches for at most {} of {} changed files",
                        pull_request.number,
                        pull_requests::REST_FILE_LIMIT,
                        pull_request.changed_files
                    ));
                } else if served < pull_request.files.len() {
                    caveats.push(format!(
                        "PR #{}: provider served patches for {served} of {} listed files",
                        pull_request.number,
                        pull_request.files.len()
                    ));
                }
            }
            Err(error) => caveats.push(format!(
                "PR #{}: patch text not retrieved: {error}",
                pull_request.number
            )),
        }
        Ok(caveats)
    }

    #[allow(clippy::too_many_arguments)]
    fn sync_issues(
        &mut self,
        owner: &str,
        name: &str,
        repository_name: &str,
        entity_key: &str,
        updated_cursor: Option<&str>,
        summary: &mut SyncSummary,
    ) -> Result<()> {
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);

        // Ensure the repo dimension exists even in --issues-only mode.
        let repo_key = repository::upsert_repository(
            self.connection,
            repository_name,
            None,
            false,
            false,
            None,
            None,
        )?;

        let mut page_cursor: Option<String> = None;
        let mut watermark = WalkWatermark::default();
        let mut item_index: i64 = 0;

        'pages: loop {
            let mut variables: Vec<(&str, &str)> = vec![("owner", owner), ("name", name)];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = self
                .client
                .graphql(&issues::repository_issues_query(), &variables)?;
            let page = issues::parse_issue_page(&data)?;
            summary.pages_fetched += 1;

            let page_is_empty = page.issues.is_empty();
            for issue in &page.issues {
                // Strict comparison, so an item updated in the same second as
                // the cursor is re-ingested rather than skipped forever. See
                // the equivalent stop in `sync_pull_requests`.
                if let Some(cursor) = updated_cursor
                    && issue.updated_at.as_str() < cursor
                {
                    break 'pages;
                }

                item_index += 1;
                let item_id = format!("{repo_key}#issue-{}", issue.number);
                match self.upsert_one_issue(&repo_key, issue, timezone, core_hours, &bot_suffix) {
                    Ok(()) => {
                        summary.issues_synced += 1;
                        watermark.landed(Some(issue.updated_at.as_str()));
                    }
                    Err(error) => {
                        watermark.failed(Some(issue.updated_at.as_str()));
                        summary.failed.push((item_id.clone(), error.to_string()));
                    }
                }
                sync_state::update_lock_progress(
                    self.connection,
                    entity_key,
                    item_index,
                    &item_id,
                    summary.pull_requests_synced + summary.issues_synced,
                    summary.skipped,
                    summary.failed.len() as i64,
                )?;
            }

            if !page.has_next_page {
                break;
            }
            if page_is_empty {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: issue page reports more results but holds none"
                )));
            }
            if page.end_cursor == page_cursor {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: issue page cursor did not advance"
                )));
            }
            page_cursor = page.end_cursor;
        }

        sync_state::advance_cursor(
            self.connection,
            "repo_issues",
            repository_name,
            watermark.committable().as_deref(),
        )?;
        Ok(())
    }

    fn upsert_one_issue(
        &mut self,
        repo_key: &str,
        issue: &issues::IssueData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
    ) -> Result<()> {
        let mut issue = issue.clone();
        issues::hydrate_issue(self.client, &mut issue)?;
        let issue = &issue;
        let conn = self.connection;

        let author_key = match issue.author.as_ref() {
            Some(actor) => Some(repository::ensure_entity(conn, actor, bot_suffix)?),
            None => Some(repository::ensure_ghost_entity(conn)?),
        };
        let created_keys = time_dimension::ensure_keys_for_timestamp(
            conn,
            &issue.created_at,
            timezone,
            core_hours,
        )?;

        if let Some(milestone) = issue.milestone.as_ref() {
            issue_repository::upsert_milestone(conn, repo_key, milestone)?;
        }

        issue_repository::upsert_issue(
            conn,
            repo_key,
            issue,
            author_key.as_deref(),
            Some(&created_keys.date_key),
        )?;

        let mut label_ids = Vec::new();
        for label in &issue.labels {
            issue_repository::upsert_label(conn, repo_key, label)?;
            label_ids.push(label.id.clone());
        }
        issue_repository::replace_issue_labels(conn, &issue.id, &label_ids)?;

        let mut assignee_keys = Vec::new();
        for assignee in &issue.assignees {
            assignee_keys.push(repository::ensure_entity(conn, assignee, bot_suffix)?);
        }
        issue_repository::replace_issue_assignees(conn, &issue.id, &assignee_keys)?;

        for comment in &issue.comments {
            let comment_author = match comment.author.as_ref() {
                Some(actor) => repository::ensure_entity(conn, actor, bot_suffix)?,
                None => repository::ensure_ghost_entity(conn)?,
            };
            let comment_keys = time_dimension::ensure_keys_for_timestamp(
                conn,
                &comment.created_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_issue_comment(
                conn,
                &comment.id,
                "issue",
                &issue.id,
                &comment_author,
                None,
                comment.body.as_deref(),
                &comment.created_at,
                &comment_keys.date_key,
                &comment_keys.time_key,
            )?;
        }
        Ok(())
    }

    /// Re-fetch a bounded set of exact pull requests and issues and store them
    /// through the same hydrate and upsert path the walks use.
    ///
    /// Targets are validated (1-64, deduplicated, no number named as both
    /// kinds) and grouped by repository. Each repository's ordinary sync lock is
    /// taken for the duration of its targets; when another sync holds it, those
    /// targets fail with that reason and the other run's lock is left alone.
    ///
    /// Nothing outside the items changes: no sync job is started or finished,
    /// no cursor, checkpoint, or coverage range moves, and nothing is enrolled
    /// for monitoring. Each target lands inside its own savepoint, so a target
    /// that fails part-way leaves its previously stored rows exactly as they
    /// were.
    pub fn refresh_targets(
        &mut self,
        targets: &[RefreshTarget],
        options: &RefreshOptions,
    ) -> Result<RefreshReport> {
        let targets = refresh::validate_targets(targets.to_vec())?;
        let mut repositories: Vec<String> = Vec::new();
        for target in &targets {
            if !repositories.contains(&target.repository) {
                repositories.push(target.repository.clone());
            }
        }
        let mut report = RefreshReport::default();
        for repository_name in repositories {
            let group: Vec<&RefreshTarget> = targets
                .iter()
                .filter(|target| target.repository == repository_name)
                .collect();
            let entity_key = format!("repo:{repository_name}");
            match sync_state::acquire_lock(self.connection, &entity_key) {
                Ok(()) => {}
                Err(crate::Error::SyncInProgress(_)) => {
                    for target in group {
                        let counts = refresh::stored_counts(self.connection, target)?;
                        report.outcomes.push(TargetOutcome {
                            target: target.clone(),
                            status: TargetStatus::Failed,
                            error: Some(format!(
                                "a sync of {repository_name} is in progress; retry after it finishes"
                            )),
                            caveats: Vec::new(),
                            before: counts.clone(),
                            after: counts,
                        });
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
            let outcome = self.refresh_group(&group, &entity_key, options, &mut report);
            sync_state::release_lock(self.connection, &entity_key)?;
            outcome?;
        }
        Ok(report)
    }

    fn refresh_group(
        &mut self,
        group: &[&RefreshTarget],
        entity_key: &str,
        options: &RefreshOptions,
        report: &mut RefreshReport,
    ) -> Result<()> {
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);
        let mut failed = 0_i64;
        for (index, target) in group.iter().enumerate() {
            let before = refresh::stored_counts(self.connection, target)?;
            let landed = self.refresh_one(target, timezone, core_hours, &bot_suffix, options);
            let (status, error, caveats) = match landed {
                Ok(caveats) => (TargetStatus::Refreshed, None, caveats),
                Err(error) => {
                    failed += 1;
                    (TargetStatus::Failed, Some(error.to_string()), Vec::new())
                }
            };
            let after = refresh::stored_counts(self.connection, target)?;
            sync_state::update_lock_progress(
                self.connection,
                entity_key,
                index as i64 + 1,
                &target.reference(),
                index as i64 + 1 - failed,
                0,
                failed,
            )?;
            report.outcomes.push(TargetOutcome {
                target: (*target).clone(),
                status,
                error,
                caveats,
                before,
                after,
            });
        }
        Ok(())
    }

    /// Fetch, hydrate, and store one target. Returns the provider caveats.
    fn refresh_one(
        &mut self,
        target: &RefreshTarget,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
        options: &RefreshOptions,
    ) -> Result<Vec<String>> {
        let (owner, name) = target
            .repository
            .split_once('/')
            .ok_or_else(|| crate::Error::InvalidArgument(target.reference()))?;
        let number = target.number.to_string();
        let variables = [
            ("owner", owner),
            ("name", name),
            ("number", number.as_str()),
        ];
        let check_identity = |metadata: &pull_requests::RepositoryMetadata, found: i64| {
            let resolved = metadata.name_with_owner.to_lowercase();
            if resolved != target.repository {
                return Err(crate::Error::GitHubApi(format!(
                    "{} now resolves to {resolved}; refresh {resolved}#{} instead",
                    target.repository, target.number
                )));
            }
            if found != target.number {
                return Err(crate::Error::GitHubApi(format!(
                    "asked for #{} but the provider returned #{found}",
                    target.number
                )));
            }
            Ok(())
        };
        let upsert_repo = |conn: &Connection, metadata: &pull_requests::RepositoryMetadata| {
            repository::upsert_repository(
                conn,
                &metadata.name_with_owner,
                metadata.primary_language.as_deref(),
                metadata.is_fork,
                metadata.is_private,
                metadata.default_branch.as_deref(),
                metadata.created_at.as_deref(),
            )
        };
        // Everything is fetched and hydrated BEFORE the savepoint opens, so the
        // write lock is never held across network calls, and a fetch failure
        // writes nothing at all.
        match target.kind {
            TargetKind::PullRequest => {
                let data = self
                    .client
                    .graphql(&pull_requests::single_pull_request_query(), &variables)?;
                let (metadata, mut pull_request) = pull_requests::parse_single_pull_request(&data)?;
                check_identity(&metadata, pull_request.number)?;
                let note = pull_requests::hydrate_pull_request(self.client, &mut pull_request)?;
                let repo_key = metadata.name_with_owner.to_lowercase();
                let pr_key = format!("{repo_key}#{}", pull_request.number);
                // Fetch patches before the savepoint, then land all database
                // writes together. A patch-write failure rolls back the PR,
                // its file-set prune, and the repository row too.
                let patches = (!options.skip_diffs && !pull_request.files.is_empty()).then(|| {
                    pull_requests::fetch_file_patches(self.client, &repo_key, pull_request.number)
                });
                let recovered = recover_files(
                    self.client,
                    self.git_recovery.as_ref(),
                    &repo_key,
                    &pull_request,
                    patches.as_ref(),
                    !options.skip_diffs,
                );
                let connection = self.connection;
                within_savepoint(connection, || {
                    upsert_repo(connection, &metadata)?;
                    let mut caveats = without_superseded_notes(note, &recovered);
                    caveats.extend(self.store_pull_request(
                        &repo_key,
                        &pr_key,
                        &pull_request,
                        timezone,
                        core_hours,
                        bot_suffix,
                        patches,
                        recovered,
                    )?);
                    Ok(caveats)
                })
            }
            TargetKind::Issue => {
                let data = self
                    .client
                    .graphql(&issues::single_issue_query(), &variables)?;
                let (metadata, mut issue) = issues::parse_single_issue(&data)?;
                check_identity(&metadata, issue.number)?;
                issues::hydrate_issue(self.client, &mut issue)?;
                let repo_key = metadata.name_with_owner.to_lowercase();
                let connection = self.connection;
                within_savepoint(connection, || {
                    upsert_repo(connection, &metadata)?;
                    self.upsert_one_issue(&repo_key, &issue, timezone, core_hours, bot_suffix)
                })?;
                Ok(Vec::new())
            }
        }
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

/// What a `sync all` run will do with the monitored source list.
///
/// The plan exists so no enabled source can be dropped without a word: every
/// source is either in a bucket this build can serve, or named in `notices`.
#[derive(Debug, Default, PartialEq)]
pub struct SyncPlan {
    /// Repositories to sync, in list order.
    pub repos: Vec<String>,
    /// Monitored users to sync, in list order.
    pub users: Vec<String>,
    /// Lines the caller must print: sources this run will not serve, and why.
    pub notices: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SyncSource {
    User(String),
    Repository(String),
}

impl SyncSource {
    fn key(&self) -> String {
        match self {
            Self::User(login) => format!("user:{login}"),
            Self::Repository(repo) => format!("repo:{repo}"),
        }
    }
}

impl SyncPlan {
    /// Interleave users and repositories, resuming after the last source
    /// attempted. The caller records the next source BEFORE starting work, so
    /// an interrupted expensive source cannot starve every subsequent run.
    pub fn fair_sources(&self, conn: &Connection) -> Result<Vec<SyncSource>> {
        let mut sources = Vec::new();
        for index in 0..self.users.len().max(self.repos.len()) {
            if let Some(login) = self.users.get(index) {
                sources.push(SyncSource::User(login.clone()));
            }
            if let Some(repo) = self.repos.get(index) {
                sources.push(SyncSource::Repository(repo.clone()));
            }
        }
        if let Some(next) = sync_state::load_checkpoint(conn, "sync_all", "next_source")?
            && let Some(position) = sources.iter().position(|source| source.key() == next)
        {
            sources.rotate_left(position);
        }
        Ok(sources)
    }

    pub fn checkpoint_next_source(conn: &Connection, next: &SyncSource) -> Result<()> {
        sync_state::save_checkpoint(conn, "sync_all", "next_source", &next.key())
    }

    /// True when there is nothing at all to do.
    pub fn is_empty(&self) -> bool {
        self.repos.is_empty() && self.users.is_empty()
    }
}

/// Build the plan for `sync all` from the monitored source list.
pub fn plan_all(sources: &[MonitoredSource]) -> SyncPlan {
    let mut plan = SyncPlan::default();
    let mut unsupported: Vec<(String, String)> = Vec::new();
    let mut disabled: Vec<String> = Vec::new();

    for source in sources {
        if !source.sync_enabled {
            disabled.push(source.identifier.clone());
            continue;
        }
        match source.source_type.as_str() {
            "repo" => plan.repos.push(source.identifier.clone()),
            "user" => plan.users.push(source.identifier.clone()),
            other => unsupported.push((other.to_string(), source.identifier.clone())),
        }
    }

    for (source_type, identifier) in &unsupported {
        plan.notices.push(format!(
            "skipping {source_type} '{identifier}': {source_type}-scoped sync is not implemented \
             — add the repositories or users you want covered"
        ));
    }
    if !disabled.is_empty() {
        plan.notices.push(format!(
            "skipping {} disabled source(s): {}",
            disabled.len(),
            disabled.join(", ")
        ));
    }
    if plan.is_empty() {
        plan.notices.push(
            "nothing enabled to sync — use `githubdw monitor add-repo` or `monitor add-user`"
                .to_string(),
        );
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;
    use crate::fetch::test_support::FixtureTransport;
    use serde_json::json;

    #[test]
    fn interrupted_sync_all_moves_on_and_eventually_revisits_every_source() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let plan = SyncPlan {
            users: vec!["one".into(), "two".into()],
            repos: vec!["owner/a".into(), "owner/b".into()],
            notices: vec![],
        };
        let initial = plan.fair_sources(conn).unwrap();
        assert_eq!(
            initial.iter().map(SyncSource::key).collect::<Vec<_>>(),
            vec!["user:one", "repo:owner/a", "user:two", "repo:owner/b"]
        );
        let mut visited = Vec::new();
        for _ in 0..initial.len() {
            let run = plan.fair_sources(conn).unwrap();
            visited.push(run[0].key());
            SyncPlan::checkpoint_next_source(conn, &run[1]).unwrap();
            // Simulate interruption during the first source, before it finishes.
        }
        assert_eq!(
            visited,
            initial.iter().map(SyncSource::key).collect::<Vec<_>>()
        );
        assert_eq!(plan.fair_sources(conn).unwrap(), initial);
        SyncPlan::checkpoint_next_source(conn, &SyncSource::User("removed".into())).unwrap();
        assert_eq!(
            plan.fair_sources(conn).unwrap(),
            initial,
            "removed sources do not break scheduling"
        );
        assert!(SyncPlan::default().fair_sources(conn).unwrap().is_empty());
    }

    #[test]
    fn budget_yield_is_persisted_as_partial_not_completed() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        sync_state::start_job(conn, "repo:octocat/hello").unwrap();
        sync_state::complete_job_outcome(conn, "repo:octocat/hello", 500, 0, &[], true).unwrap();
        let (status, count, note): (String, i64, String) = conn
            .query_row(
                "SELECT status,synced,error FROM sync_jobs WHERE entity_key='repo:octocat/hello'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "partial");
        assert_eq!(count, 500);
        assert!(note.contains("paused"));
        sync_state::complete_job(conn, "repo:octocat/hello", 25, 0, &[]).unwrap();
        let status: String = conn
            .query_row(
                "SELECT status FROM sync_jobs WHERE entity_key='repo:octocat/hello'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    fn pr_node(number: i64, created_at: &str, updated_at: &str) -> serde_json::Value {
        json!({
            "number": number,
            "title": format!("PR {number}"),
            "body": "body text",
            "state": "MERGED",
            "isDraft": false,
            "createdAt": created_at,
            "updatedAt": updated_at,
            "mergedAt": "2026-01-06T09:00:00Z",
            "closedAt": "2026-01-06T09:00:00Z",
            "baseRefName": "main",
            "headRefName": format!("feature/{number}"),
            "additions": 10,
            "deletions": 2,
            "changedFiles": 1,
            "author": {"login": "octocat", "__typename": "User"},
            "mergedBy": {"login": "hubot", "__typename": "User"},
            "reviews": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                "id": format!("REV{number}"), "state": "APPROVED", "body": "",
                "submittedAt": "2026-01-06T08:00:00Z",
                "author": {"login": "hubot", "__typename": "User"}
            }]},
            "reviewThreads": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
            "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
            "files": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                "path": "src/lib.rs", "changeType": "MODIFIED",
                "additions": 10, "deletions": 2
            }]},
            "commits": {"nodes": []}
        })
    }

    fn page_of(has_next: bool, cursor: Option<&str>, nodes: Vec<serde_json::Value>) -> String {
        json!({
            "data": {
                "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
                "repository": {
                    "nameWithOwner": "octocat/hello",
                    "primaryLanguage": {"name": "Rust"},
                    "isFork": false,
                    "isPrivate": false,
                    "defaultBranchRef": {"name": "main"},
                    "createdAt": "2020-01-01T00:00:00Z",
                    "pullRequests": {
                        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                        "nodes": nodes
                    }
                }
            }
        })
        .to_string()
    }

    fn page_response(has_next: bool, cursor: Option<&str>, numbers: &[i64]) -> String {
        let nodes = numbers
            .iter()
            .map(|number| {
                pr_node(
                    *number,
                    "2026-01-05T18:00:00Z",
                    &format!("2026-01-{:02}T09:00:00Z", 5 + number),
                )
            })
            .collect();
        page_of(has_next, cursor, nodes)
    }

    fn fast_pr_options() -> SyncOptions {
        SyncOptions {
            skip_diffs: true,
            pull_requests_only: true,
            ..Default::default()
        }
    }

    fn los_angeles_warehouse() -> GithubDW {
        let warehouse = GithubDW::open_in_memory().unwrap();
        warehouse
            .connection()
            .execute(
                "INSERT INTO config (key, value) VALUES ('timezone', 'America/Los_Angeles')
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                [],
            )
            .unwrap();
        warehouse
    }

    #[test]
    fn syncs_two_pages_and_records_state() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let transport = FixtureTransport::new(vec![
            Ok(page_response(true, Some("C1"), &[1, 2])),
            Ok(page_response(false, None, &[3])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = fast_pr_options();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

        assert_eq!(summary.pull_requests_synced, 3);
        assert_eq!(summary.pages_fetched, 2);
        assert!(summary.failed.is_empty());

        let conn = warehouse.connection();
        let pr_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_pull_requests", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pr_count, 3);
        let review_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_reviews", [], |r| r.get(0))
            .unwrap();
        assert_eq!(review_count, 3);
        let diff_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_file_diffs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(diff_count, 3);

        // Job completed, lock released, range recorded, cursor advanced.
        let status: String = conn
            .query_row(
                "SELECT status FROM sync_jobs WHERE entity_key = 'repo:octocat/hello'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
        let locks: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(locks, 0);
        let cursor = sync_state::last_updated_cursor(conn, "repo", "octocat/hello").unwrap();
        assert_eq!(cursor.as_deref(), Some("2026-01-08T09:00:00Z"));
    }

    #[test]
    fn incremental_rerun_stops_at_cursor() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let transport = FixtureTransport::new(vec![
            Ok(page_response(false, None, &[1, 2])),
            // Second run returns the same (unchanged) PRs.
            Ok(page_response(false, None, &[1, 2])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = fast_pr_options();

        {
            let mut syncer = Syncer::new(warehouse.connection(), &mut client);
            let first = syncer.sync_repository("octocat/hello", &options).unwrap();
            assert_eq!(first.pull_requests_synced, 2);
        }
        {
            let mut syncer = Syncer::new(warehouse.connection(), &mut client);
            let second = syncer.sync_repository("octocat/hello", &options).unwrap();
            assert_eq!(second.pull_requests_synced, 0, "no-op on unchanged data");
            assert!(second.up_to_date);
        }
    }

    /// An item updated in the *same second* as the cursor was late to the
    /// previous run's page. The stop must be strict so the boundary second is
    /// re-read, otherwise that item is skipped for good: nothing short of
    /// another update (which moves its timestamp) would ever surface it.
    #[test]
    fn same_second_late_update_is_not_lost() {
        let boundary = "2026-01-10T10:00:00Z";
        let older = "2026-01-09T10:00:00Z";
        let warehouse = GithubDW::open_in_memory().unwrap();
        let transport = FixtureTransport::new(vec![
            // Run 1 sees only PR 1; the cursor lands exactly on `boundary`.
            Ok(page_of(
                false,
                None,
                vec![pr_node(1, "2026-01-05T18:00:00Z", boundary)],
            )),
            // Run 2: PR 2 was updated in the same second but missed page 1,
            // followed by the already-ingested PR 1 and a strictly older PR 3.
            Ok(page_of(
                false,
                None,
                vec![
                    pr_node(2, "2026-01-05T18:00:00Z", boundary),
                    pr_node(1, "2026-01-05T18:00:00Z", boundary),
                    pr_node(3, "2026-01-05T18:00:00Z", older),
                ],
            )),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = fast_pr_options();

        {
            let mut syncer = Syncer::new(warehouse.connection(), &mut client);
            syncer.sync_repository("octocat/hello", &options).unwrap();
        }
        {
            let mut syncer = Syncer::new(warehouse.connection(), &mut client);
            syncer.sync_repository("octocat/hello", &options).unwrap();
        }

        let conn = warehouse.connection();
        let numbers: Vec<i64> = conn
            .prepare("SELECT number FROM fact_pull_requests ORDER BY number")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            numbers,
            vec![1, 2],
            "the same-second item is picked up, and the strictly older one \
             still terminates the walk"
        );
    }

    /// The cursor must not move past an item that failed to persist. If it
    /// does, the next run's stop condition fires before reaching that item and
    /// the gap never heals.
    #[test]
    fn cursor_does_not_advance_past_a_failed_upsert() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        // Newest-first, as GitHub serves it. PR 9 has an unparseable
        // `createdAt`, so its upsert fails; PR 8 is fine.
        let transport = FixtureTransport::new(vec![Ok(page_of(
            false,
            None,
            vec![
                pr_node(9, "not-a-timestamp", "2026-01-20T10:00:00Z"),
                pr_node(8, "2026-01-05T18:00:00Z", "2026-01-19T10:00:00Z"),
            ],
        ))]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = fast_pr_options();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

        assert_eq!(summary.pull_requests_synced, 1);
        assert_eq!(summary.failed.len(), 1, "PR 9 is reported as failed");

        let cursor =
            sync_state::last_updated_cursor(warehouse.connection(), "repo", "octocat/hello")
                .unwrap();
        assert_eq!(
            cursor.as_deref(),
            Some("2026-01-19T10:00:00Z"),
            "the watermark stays below the failed item so it is retried"
        );
    }

    // ---- Nested connection continuation --------------------------------

    /// Answers page queries from a queue and `node(id:)` continuation queries
    /// by (connection field, cursor), so tests do not depend on the order the
    /// hydrator walks pending connections.
    struct RoutingTransport {
        pages: std::sync::Mutex<Vec<String>>,
        continuations: Vec<(&'static str, &'static str, String)>,
        continuation_calls: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl crate::fetch::GhTransport for RoutingTransport {
        fn run(&self, arguments: &[String]) -> Result<String> {
            let query = arguments
                .iter()
                .find_map(|argument| argument.strip_prefix("query="))
                .unwrap_or_default();
            if !query.contains("node(id: $id)") {
                let mut pages = self.pages.lock().unwrap();
                if pages.is_empty() {
                    return Err(crate::Error::GitHubApi("no more pages".into()));
                }
                return Ok(pages.remove(0));
            }
            let cursor = arguments
                .iter()
                .find_map(|argument| argument.strip_prefix("cursor="))
                .unwrap_or_default()
                .to_string();
            let field = query
                .split("... on ")
                .nth(1)
                .and_then(|rest| rest.split('{').nth(1))
                .map(|rest| {
                    rest.split('(')
                        .next()
                        .unwrap_or_default()
                        .trim()
                        .to_string()
                })
                .unwrap_or_default();
            self.continuation_calls
                .lock()
                .unwrap()
                .push((field.clone(), cursor.clone()));
            self.continuations
                .iter()
                .find(|(f, c, _)| *f == field && *c == cursor)
                .map(|(_, _, body)| Ok(body.clone()))
                .unwrap_or_else(|| {
                    Err(crate::Error::GitHubApi(format!(
                        "unexpected continuation {field} after {cursor}"
                    )))
                })
        }
    }

    fn routing_client(
        pages: Vec<String>,
        continuations: Vec<(&'static str, &'static str, String)>,
    ) -> (GhClient, std::sync::Arc<RoutingTransport>) {
        let transport = std::sync::Arc::new(RoutingTransport {
            pages: std::sync::Mutex::new(pages),
            continuations,
            continuation_calls: std::sync::Mutex::new(Vec::new()),
        });
        struct Shared(std::sync::Arc<RoutingTransport>);
        impl crate::fetch::GhTransport for Shared {
            fn run(&self, arguments: &[String]) -> Result<String> {
                self.0.run(arguments)
            }
        }
        let client =
            GhClient::with_transport(Box::new(Shared(transport.clone()))).without_sleeping();
        (client, transport)
    }

    fn connection_page(
        field: &str,
        has_next: bool,
        cursor: Option<&str>,
        nodes: Vec<serde_json::Value>,
    ) -> String {
        json!({"data": {"node": {field: {
            "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
            "nodes": nodes
        }}}})
        .to_string()
    }

    fn conversation_comment(id: &str) -> serde_json::Value {
        json!({"id": id, "body": format!("comment {id}"), "createdAt": "2026-01-05T19:00:00Z",
               "author": {"login": "hubot", "__typename": "User"}})
    }

    fn review_comment(id: &str) -> serde_json::Value {
        json!({"id": id, "body": "nit", "path": "src/lib.rs", "line": 3,
               "createdAt": "2026-01-05T20:00:00Z",
               "author": {"login": "hubot", "__typename": "User"}, "replyTo": null})
    }

    fn file(path: &str) -> serde_json::Value {
        json!({"path": path, "changeType": "MODIFIED", "additions": 1, "deletions": 0})
    }

    /// A PR whose comments, files, review threads, and one thread's comments
    /// all report a second page.
    fn heavy_pr_node(number: i64) -> serde_json::Value {
        let mut node = pr_node(number, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        node["id"] = json!(format!("PR_{number}"));
        node["changedFiles"] = json!(3);
        node["comments"] = json!({
            "pageInfo": {"hasNextPage": true, "endCursor": "COM1"},
            "nodes": [conversation_comment("IC1")]
        });
        node["files"] = json!({
            "pageInfo": {"hasNextPage": true, "endCursor": "FILE1"},
            "nodes": [file("a.rs")]
        });
        node["reviewThreads"] = json!({
            "pageInfo": {"hasNextPage": true, "endCursor": "THR1"},
            "nodes": [{
                "id": "THREAD_A",
                "comments": {
                    "pageInfo": {"hasNextPage": true, "endCursor": "TC1"},
                    "nodes": [review_comment("RCA1")]
                }
            }]
        });
        node
    }

    fn heavy_pr_continuations() -> Vec<(&'static str, &'static str, String)> {
        vec![
            (
                "comments",
                "COM1",
                connection_page(
                    "comments",
                    true,
                    Some("COM2"),
                    vec![conversation_comment("IC2")],
                ),
            ),
            (
                "comments",
                "COM2",
                connection_page("comments", false, None, vec![conversation_comment("IC3")]),
            ),
            (
                "files",
                "FILE1",
                connection_page("files", false, None, vec![file("b.rs"), file("c.rs")]),
            ),
            (
                "reviewThreads",
                "THR1",
                connection_page(
                    "reviewThreads",
                    false,
                    None,
                    vec![json!({
                        "id": "THREAD_B",
                        "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null},
                                     "nodes": [review_comment("RCB1")]}
                    })],
                ),
            ),
            (
                "comments",
                "TC1",
                connection_page("comments", false, None, vec![review_comment("RCA2")]),
            ),
        ]
    }

    fn assert_heavy_pr_complete(conn: &Connection, pr_key: &str) {
        let conversation: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fact_issue_comments WHERE parent_key = ?1",
                [pr_key],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            conversation, 3,
            "page-two and page-three conversation comments land"
        );
        let files: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fact_file_diffs WHERE pr_key = ?1",
                [pr_key],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(files, 3, "page-two files land");
        let review_comments = count(conn, "SELECT COUNT(*) FROM fact_review_comments");
        assert_eq!(
            review_comments, 3,
            "thread page two and a nested thread-comment continuation land"
        );
    }

    #[test]
    fn repository_path_follows_nested_connection_cursors() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let (mut client, transport) = routing_client(
            vec![page_of(false, None, vec![heavy_pr_node(5)])],
            heavy_pr_continuations(),
        );
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        assert!(summary.notes.is_empty(), "{:?}", summary.notes);
        assert_eq!(transport.continuation_calls.lock().unwrap().len(), 5);
        let pr_key: String = warehouse
            .connection()
            .query_row("SELECT pr_key FROM fact_pull_requests", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_heavy_pr_complete(warehouse.connection(), &pr_key);
    }

    #[test]
    fn user_path_follows_nested_connection_cursors() {
        let warehouse = los_angeles_warehouse();
        monitor_repository::add_user(warehouse.connection(), "octocat").unwrap();
        let mut node = heavy_pr_node(5);
        node["updatedAt"] = json!("2026-08-15T09:00:00Z");
        node["createdAt"] = json!("2026-08-14T18:00:00Z");
        node["repository"] = json!({
            "nameWithOwner": "acme/widgets", "primaryLanguage": {"name": "Rust"},
            "isFork": false, "isPrivate": false, "defaultBranchRef": {"name": "main"},
            "createdAt": "2020-01-01T00:00:00Z"
        });
        let (mut client, _transport) = routing_client(
            vec![search_response(1, vec![node]), search_response(0, vec![])],
            heavy_pr_continuations(),
        );
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let summary = syncer
            .sync_user("octocat", &fast_user_options(Some(1)))
            .unwrap();
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        assert_eq!(summary.pull_requests_synced, 1);
        let pr_key: String = warehouse
            .connection()
            .query_row("SELECT pr_key FROM fact_pull_requests", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_heavy_pr_complete(warehouse.connection(), &pr_key);
    }

    /// A page that claims more nodes but carries no cursor cannot be completed;
    /// the PR is recorded as failed (and retried) rather than stored short.
    #[test]
    fn continuation_without_cursor_fails_the_record_instead_of_truncating() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let mut node = pr_node(6, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        node["id"] = json!("PR_6");
        node["comments"] = json!({
            "pageInfo": {"hasNextPage": true, "endCursor": null},
            "nodes": [conversation_comment("IC1")]
        });
        let (mut client, transport) =
            routing_client(vec![page_of(false, None, vec![node])], vec![]);
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert_eq!(summary.failed.len(), 1);
        assert!(
            summary.failed[0].1.contains("no endCursor"),
            "{:?}",
            summary.failed
        );
        assert!(transport.continuation_calls.lock().unwrap().is_empty());
        let conn = warehouse.connection();
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 0);
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_issue_comments"), 0);
    }

    /// A continuation request that errors fails the record; nothing partial is stored.
    #[test]
    fn failed_continuation_page_fails_the_record() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let mut continuations = heavy_pr_continuations();
        continuations.retain(|(field, cursor, _)| !(*field == "comments" && *cursor == "COM2"));
        let (mut client, _transport) = routing_client(
            vec![page_of(false, None, vec![heavy_pr_node(7)])],
            continuations,
        );
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert_eq!(summary.failed.len(), 1, "{:?}", summary.failed);
        let conn = warehouse.connection();
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 0);
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_issue_comments"), 0);
    }

    /// GitHub stops listing files at a cap; the shortfall is surfaced as a note
    /// rather than the list being treated as complete.
    #[test]
    fn provider_file_cap_is_reported_as_truncation() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let mut node = pr_node(8, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        node["id"] = json!("PR_8");
        node["changedFiles"] = json!(4443);
        let (mut client, _transport) =
            routing_client(vec![page_of(false, None, vec![node])], vec![]);
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert!(summary.failed.is_empty());
        assert_eq!(summary.notes.len(), 1, "{:?}", summary.notes);
        assert!(
            summary.notes[0].contains("1 of 4443"),
            "{}",
            summary.notes[0]
        );
    }

    #[test]
    fn issue_comments_follow_their_cursor() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let mut page: serde_json::Value =
            serde_json::from_str(&issue_page_response(false, None, &[1])).unwrap();
        page["data"]["repository"]["issues"]["nodes"][0]["comments"]["pageInfo"] =
            json!({"hasNextPage": true, "endCursor": "IC_1"});
        let (mut client, transport) = routing_client(
            vec![page.to_string()],
            vec![(
                "comments",
                "IC_1",
                connection_page(
                    "comments",
                    false,
                    None,
                    vec![conversation_comment("ICOM_X")],
                ),
            )],
        );
        let options = SyncOptions {
            issues_only: true,
            ..Default::default()
        };
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer.sync_repository("octocat/hello", &options).unwrap();
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        assert_eq!(
            transport.continuation_calls.lock().unwrap().as_slice(),
            &[("comments".to_string(), "IC_1".to_string())]
        );
        assert_eq!(
            count(
                warehouse.connection(),
                "SELECT COUNT(*) FROM fact_issue_comments"
            ),
            2
        );
    }

    // ---- Resumable repository walks -------------------------------------

    /// A transport the test keeps a handle to, so it can assert which page
    /// cursor each run actually requested.
    struct SharedTransport(std::sync::Arc<FixtureTransport>);

    impl crate::fetch::GhTransport for SharedTransport {
        fn run(&self, arguments: &[String]) -> Result<String> {
            self.0.run(arguments)
        }
    }

    fn shared_client(
        responses: Vec<Result<String>>,
    ) -> (GhClient, std::sync::Arc<FixtureTransport>) {
        let transport = std::sync::Arc::new(FixtureTransport::new(responses));
        let client = GhClient::with_transport(Box::new(SharedTransport(transport.clone())))
            .without_sleeping();
        (client, transport)
    }

    fn requested_cursors(transport: &FixtureTransport) -> Vec<Option<String>> {
        transport
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| {
                call.iter()
                    .find_map(|argument| argument.strip_prefix("cursor=").map(str::to_string))
            })
            .collect()
    }

    fn saved_checkpoint(conn: &Connection) -> Option<PullRequestWalkCheckpoint> {
        sync_state::load_checkpoint(conn, REPO_PR_CHECKPOINT, "octocat/hello")
            .unwrap()
            .map(|text| serde_json::from_str(&text).unwrap())
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn pr_cursor(conn: &Connection) -> Option<String> {
        sync_state::last_updated_cursor(conn, "repo", "octocat/hello").unwrap()
    }

    /// The reported failure: a run dies on page 3 (here the transport fails
    /// at every page size; in production the scheduler kills it). The next
    /// run must resume at page 3 from the durable checkpoint, not re-walk
    /// pages 1-2, and the high-water cursor must not move until the walk is
    /// complete.
    #[test]
    fn failed_page_three_resumes_durably_instead_of_restarting() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let options = fast_pr_options();

        // Run 1: two good pages, then page 3 fails at every page size.
        let (mut client, _) = shared_client(vec![
            Ok(page_response(true, Some("C1"), &[10, 9])),
            Ok(page_response(true, Some("C2"), &[8, 7])),
        ]);
        let error = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap_err();
        assert!(error.to_string().contains("no more fixture responses"));
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 4);
        assert_eq!(
            pr_cursor(conn),
            None,
            "no high-water claim for a broken walk"
        );
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM synced_ranges"),
            0,
            "no coverage claim for a broken walk"
        );
        let checkpoint = saved_checkpoint(conn).expect("position is durable");
        assert_eq!(checkpoint.after, "C2");
        assert_eq!(checkpoint.pages, 2);
        assert_eq!(checkpoint.base, None);

        // Run 2: resumes after C2 and finishes.
        let (mut client, transport) = shared_client(vec![Ok(page_response(false, None, &[6]))]);
        let summary = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(
            requested_cursors(&transport),
            vec![Some("C2".to_string())],
            "resumes at page 3, not page 1"
        );
        assert_eq!(summary.pull_requests_synced, 1);
        assert!(!summary.partial);
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 5);
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM fact_reviews"),
            5,
            "review data from the interrupted run is preserved"
        );
        assert_eq!(
            pr_cursor(conn).as_deref(),
            Some("2026-01-15T09:00:00Z"),
            "the completed walk commits its newest landed item (PR 10)"
        );
        assert!(
            saved_checkpoint(conn).is_none(),
            "checkpoint cleared on completion"
        );
        assert_eq!(count(conn, "SELECT COUNT(*) FROM synced_ranges"), 1);
    }

    /// A failure in the MIDDLE of an updated-desc walk must cap the committed
    /// cursor at the failed item. Taking the newest landed item instead would
    /// put the cursor above the failure and the next run would stop before it.
    #[test]
    fn failed_middle_item_caps_the_committed_cursor() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let (mut client, _) = shared_client(vec![
            Ok(page_of(
                false,
                None,
                vec![
                    pr_node(9, "2026-01-05T18:00:00Z", "2026-01-20T10:00:00Z"),
                    pr_node(8, "not-a-timestamp", "2026-01-19T10:00:00Z"),
                    pr_node(7, "2026-01-05T18:00:00Z", "2026-01-18T10:00:00Z"),
                ],
            )),
            // Run 2: PR 8 is fixed and still at its old timestamp.
            Ok(page_of(
                false,
                None,
                vec![
                    pr_node(9, "2026-01-05T18:00:00Z", "2026-01-20T10:00:00Z"),
                    pr_node(8, "2026-01-05T18:00:00Z", "2026-01-19T10:00:00Z"),
                    pr_node(7, "2026-01-05T18:00:00Z", "2026-01-18T10:00:00Z"),
                ],
            )),
        ]);
        let options = fast_pr_options();
        let first = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(first.failed.len(), 1);
        assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-19T10:00:00Z"));

        Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(
            count(
                conn,
                "SELECT COUNT(*) FROM fact_pull_requests WHERE number = 8"
            ),
            1,
            "the failed middle item is retried and lands"
        );
        assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-20T10:00:00Z"));
    }

    /// A budgeted run yields at a page boundary without claiming coverage or
    /// moving the cursor; the next run continues. An item updated between the
    /// runs jumps to the front of the updated-desc order, past the resume
    /// position — the committed watermark stays at or below its new
    /// timestamp, so the following walk picks it up.
    #[test]
    fn budgeted_walk_yields_resumes_and_catches_items_that_moved() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let options = fast_pr_options();

        let (mut client, _) = shared_client(vec![Ok(page_response(true, Some("C1"), &[10, 9]))]);
        let first = Syncer::new(conn, &mut client)
            .with_item_budget(2)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert!(first.partial);
        assert!(!first.up_to_date);
        assert_eq!(pr_cursor(conn), None);
        assert_eq!(count(conn, "SELECT COUNT(*) FROM synced_ranges"), 0);
        let status: String = conn
            .query_row(
                "SELECT status FROM sync_jobs WHERE entity_key = 'repo:octocat/hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "partial");
        assert_eq!(count(conn, "SELECT COUNT(*) FROM sync_locks"), 0);

        // Between runs PR 7 is edited and moves to the front, so the resumed
        // page no longer contains it.
        let (mut client, transport) = shared_client(vec![Ok(page_response(false, None, &[8]))]);
        let second = Syncer::new(conn, &mut client)
            .with_item_budget(2)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(requested_cursors(&transport), vec![Some("C1".to_string())]);
        assert!(!second.partial);
        assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-15T09:00:00Z"));
        assert!(saved_checkpoint(conn).is_none());

        // Run 3: PR 7 at its new time leads the walk; PR 10 sits exactly on
        // the cursor (same second, re-read); PR 9 is strictly older and stops.
        let moved = "2026-01-21T09:00:00Z";
        let (mut client, _) = shared_client(vec![Ok(page_of(
            false,
            None,
            vec![
                pr_node(7, "2026-01-05T18:00:00Z", moved),
                pr_node(10, "2026-01-05T18:00:00Z", "2026-01-15T09:00:00Z"),
                pr_node(9, "2026-01-05T18:00:00Z", "2026-01-14T09:00:00Z"),
            ],
        ))]);
        let third = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(third.pull_requests_synced, 2);
        let stored: String = conn
            .query_row(
                "SELECT updated_at FROM fact_pull_requests WHERE number = 7",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, moved);
        assert_eq!(pr_cursor(conn).as_deref(), Some(moved));
    }

    /// A saved page cursor GitHub rejects restarts the walk from the newest
    /// page once, keeping its base and watermark, rather than failing every
    /// future run on the same dead cursor.
    #[test]
    fn rejected_saved_cursor_restarts_the_walk_once() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let options = fast_pr_options();
        let (mut client, _) = shared_client(vec![Ok(page_response(true, Some("C1"), &[10, 9]))]);
        Syncer::new(conn, &mut client)
            .with_item_budget(1)
            .sync_repository("octocat/hello", &options)
            .unwrap();

        let invalid = json!({"data": null, "errors": [
            {"message": "`C1` does not appear to be a valid cursor."}
        ]})
        .to_string();
        let (mut client, transport) = shared_client(vec![
            Ok(invalid),
            Ok(page_response(false, None, &[10, 9, 8])),
        ]);
        let summary = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(
            requested_cursors(&transport),
            vec![Some("C1".to_string()), None],
            "one rejected resume, then one restart from the newest page"
        );
        assert!(summary.notes.iter().any(|note| note.contains("rejected")));
        assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-15T09:00:00Z"));
        assert!(saved_checkpoint(conn).is_none());
    }

    /// A checkpoint that is corrupt, or was taken from a different starting
    /// cursor, is not trusted: the walk starts from the newest page.
    #[test]
    fn foreign_or_corrupt_checkpoint_is_discarded() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let options = fast_pr_options();
        for stale in [
            "not json".to_string(),
            serde_json::to_string(&PullRequestWalkCheckpoint {
                version: CHECKPOINT_VERSION,
                base: Some("2020-01-01T00:00:00Z".into()),
                after: "OLD".into(),
                watermark: WalkWatermark::default(),
                pages: 9,
                items: 9,
                restarts: 0,
            })
            .unwrap(),
        ] {
            sync_state::save_checkpoint(conn, REPO_PR_CHECKPOINT, "octocat/hello", &stale).unwrap();
            let (mut client, transport) =
                shared_client(vec![Ok(page_response(false, None, &[10]))]);
            Syncer::new(conn, &mut client)
                .sync_repository("octocat/hello", &options)
                .unwrap();
            assert_eq!(requested_cursors(&transport), vec![None]);
            assert!(saved_checkpoint(conn).is_none());
        }
    }

    #[test]
    fn watermark_commits_below_the_oldest_failure() {
        let mut watermark = WalkWatermark::default();
        assert_eq!(watermark.committable(), None);
        watermark.landed(Some("2026-01-03T00:00:00Z"));
        watermark.failed(Some("2026-01-02T00:00:00Z"));
        watermark.landed(Some("2026-01-01T00:00:00Z"));
        watermark.failed(Some("2026-01-02T12:00:00Z"));
        assert_eq!(
            watermark.committable().as_deref(),
            Some("2026-01-02T00:00:00Z")
        );
        watermark.failed(None);
        assert_eq!(
            watermark.committable(),
            None,
            "an unbounded failure pins the cursor"
        );
    }

    /// Coverage is recorded in the warehouse's own calendar and stops at the
    /// last completed day there. The injected instant is evening in Los
    /// Angeles, which is already the next day in UTC — so a UTC-derived
    /// "today" would seal two days that are not finished locally.
    #[test]
    fn recorded_coverage_stops_at_the_last_complete_local_day() {
        let warehouse = los_angeles_warehouse();
        let transport = FixtureTransport::new(vec![Ok(page_response(false, None, &[1]))]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = fast_pr_options();

        // 2026-08-16 20:00 PDT.
        let instant = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 17, 3, 0, 0).unwrap();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(instant);
        syncer.sync_repository("octocat/hello", &options).unwrap();

        let (start, end): (String, String) = warehouse
            .connection()
            .query_row(
                "SELECT start_date, end_date FROM synced_ranges
                 WHERE entity_key = 'repo:octocat/hello'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            end, "2026-08-15",
            "neither the running local day (08-16) nor the UTC day (08-17)"
        );
        // Initial window: 90 days back from the local day, 2026-08-16.
        assert_eq!(start, "2026-05-18");
    }

    fn issue_page_response(has_next: bool, cursor: Option<&str>, numbers: &[i64]) -> String {
        let nodes: Vec<_> = numbers
            .iter()
            .map(|number| {
                json!({
                    "id": format!("ISS{number}"),
                    "number": number,
                    "title": format!("Issue {number}"),
                    "body": "issue body",
                    "state": "OPEN",
                    "stateReason": null,
                    "createdAt": "2026-02-01T12:00:00Z",
                    "updatedAt": format!("2026-02-{:02}T12:00:00Z", number),
                    "closedAt": null,
                    "author": {"login": "octocat", "__typename": "User"},
                    "milestone": {
                        "id": "MILE1", "number": 1, "title": "v1.0",
                        "description": null, "state": "OPEN",
                        "dueOn": null, "createdAt": "2026-01-01T00:00:00Z"
                    },
                    "labels": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                        "id": "LAB1", "name": "bug", "color": "d73a4a", "description": ""
                    }]},
                    "assignees": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{"login": "hubot", "__typename": "User"}]},
                    "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                        "id": format!("ICOM{number}"), "body": "on it",
                        "createdAt": "2026-02-02T08:00:00Z",
                        "author": {"login": "hubot", "__typename": "User"}
                    }]}
                })
            })
            .collect();
        json!({
            "data": {
                "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
                "repository": {
                    "nameWithOwner": "octocat/hello",
                    "issues": {
                        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                        "nodes": nodes
                    }
                }
            }
        })
        .to_string()
    }

    #[test]
    fn syncs_issues_with_labels_and_comments() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let transport = FixtureTransport::new(vec![
            Ok(issue_page_response(true, Some("IC1"), &[1, 2])),
            Ok(issue_page_response(false, None, &[3])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = SyncOptions {
            issues_only: true,
            ..Default::default()
        };
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

        assert_eq!(summary.issues_synced, 3);
        assert_eq!(summary.pages_fetched, 2);

        let conn = warehouse.connection();
        let issue_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM issues", [], |r| r.get(0))
            .unwrap();
        assert_eq!(issue_count, 3);
        let label_links: i64 = conn
            .query_row("SELECT COUNT(*) FROM issue_labels", [], |r| r.get(0))
            .unwrap();
        assert_eq!(label_links, 3);
        let assignees: i64 = conn
            .query_row("SELECT COUNT(*) FROM issue_assignees", [], |r| r.get(0))
            .unwrap();
        assert_eq!(assignees, 3);
        let comments: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fact_issue_comments WHERE parent_type = 'issue'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(comments, 3);
        let milestones: i64 = conn
            .query_row("SELECT COUNT(*) FROM milestones", [], |r| r.get(0))
            .unwrap();
        assert_eq!(milestones, 1);
        let fts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM issues_fts WHERE issues_fts MATCH 'ssue'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(fts, 3, "issues_fts populated via trigger");
    }

    // ---------------------------------------------------------------- user sync

    /// One search-result node in the shape the user query selects: the PR node,
    /// plus the repository it lives in.
    fn search_node(
        number: i64,
        repository: &str,
        created_at: &str,
        updated_at: &str,
    ) -> serde_json::Value {
        let mut node = pr_node(number, created_at, updated_at);
        node["files"] = json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []});
        node["changedFiles"] = json!(0);
        node["repository"] = json!({
            "nameWithOwner": repository,
            "primaryLanguage": {"name": "Rust"},
            "isFork": false,
            "isPrivate": false,
            "defaultBranchRef": {"name": "main"},
            "createdAt": "2020-01-01T00:00:00Z"
        });
        node
    }

    fn search_response(total: i64, nodes: Vec<serde_json::Value>) -> String {
        json!({
            "data": {
                "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
                "search": {
                    "issueCount": total,
                    "pageInfo": {"hasNextPage": false, "endCursor": null},
                    "nodes": nodes
                }
            }
        })
        .to_string()
    }

    /// A page of `count` PRs, all in one repo, numbered from `first`.
    fn search_page(repository: &str, numbers: &[i64]) -> String {
        let nodes = numbers
            .iter()
            .map(|number| {
                search_node(
                    *number,
                    repository,
                    "2026-08-14T18:00:00Z",
                    &format!("2026-08-{:02}T09:00:00Z", 10 + number),
                )
            })
            .collect();
        search_response(numbers.len() as i64, nodes)
    }

    fn fast_user_options(days: Option<u32>) -> SyncOptions {
        SyncOptions {
            days,
            skip_diffs: true,
            pull_requests_only: true,
            issues_only: false,
        }
    }

    /// 2026-08-17T03:00:00Z — evening of the 16th in Los Angeles, so the last
    /// complete local day is the 15th.
    fn evening_instant() -> DateTime<Utc> {
        chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 17, 3, 0, 0).unwrap()
    }

    /// The whole point of the feature: a monitored user's PRs and reviews land
    /// in the warehouse through the same pipeline a repository sync uses, and
    /// the source's `last_sync_at` finally moves.
    #[test]
    fn syncs_a_user_across_repositories() {
        let warehouse = los_angeles_warehouse();
        monitor_repository::add_user(warehouse.connection(), "octocat").unwrap();
        let transport = FixtureTransport::new(vec![
            // author: two PRs in a repo nobody monitors
            Ok(search_page("acme/widgets", &[1, 2])),
            // reviewed-by: one PR in a third-party repo
            Ok(search_page("other/thing", &[3])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let summary = syncer
            .sync_user("octocat", &fast_user_options(Some(1)))
            .unwrap();

        assert_eq!(summary.pull_requests_synced, 3);
        assert_eq!(summary.pages_fetched, 2, "one search per role");
        assert!(summary.failed.is_empty());
        assert!(summary.notes.is_empty());

        let conn = warehouse.connection();
        let prs: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_pull_requests", [], |r| r.get(0))
            .unwrap();
        assert_eq!(prs, 3);
        let reviews: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_reviews", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            reviews, 3,
            "nested reviews ingest exactly as on the repo path"
        );

        // Both repositories exist as dimensions...
        let repos: Vec<String> = conn
            .prepare("SELECT repo_key FROM dim_repositories ORDER BY repo_key")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(repos, vec!["acme/widgets", "other/thing"]);

        // ...and neither was promoted into the monitored set. Syncing a person
        // says nothing about wanting their repositories tracked.
        let monitored: i64 = conn
            .query_row("SELECT COUNT(*) FROM monitored_repos", [], |r| r.get(0))
            .unwrap();
        assert_eq!(monitored, 0);

        // The source stamp advances, which is what `monitor list` reads.
        let last_sync: Option<String> = conn
            .query_row(
                "SELECT last_sync_at FROM monitored_users WHERE login = 'octocat'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(last_sync.is_some(), "last_sync_at no longer stays NULL");

        // Coverage and job state are recorded under the user's own entity key.
        let (start, end): (String, String) = conn
            .query_row(
                "SELECT start_date, end_date FROM synced_ranges WHERE entity_key = 'user:octocat'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((start.as_str(), end.as_str()), ("2026-08-15", "2026-08-15"));
        let status: String = conn
            .query_row(
                "SELECT status FROM sync_jobs WHERE entity_key = 'user:octocat'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
        let locks: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(locks, 0);
    }

    /// Authored and reviewed work are disjoint result sets, so one watermark
    /// cannot serve both: whichever side moved more recently would carry the
    /// cursor past items the other side has not fetched.
    #[test]
    fn each_role_keeps_its_own_watermark() {
        let warehouse = los_angeles_warehouse();
        let transport = FixtureTransport::new(vec![
            Ok(search_response(
                1,
                vec![search_node(
                    1,
                    "acme/widgets",
                    "2026-08-14T18:00:00Z",
                    "2026-08-15T09:00:00Z",
                )],
            )),
            Ok(search_response(
                1,
                vec![search_node(
                    2,
                    "acme/widgets",
                    "2026-08-14T18:00:00Z",
                    "2026-08-12T09:00:00Z",
                )],
            )),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        syncer
            .sync_user("octocat", &fast_user_options(Some(1)))
            .unwrap();

        let conn = warehouse.connection();
        assert_eq!(
            sync_state::last_updated_cursor(conn, "user", "octocat")
                .unwrap()
                .as_deref(),
            Some("2026-08-15T09:00:00Z")
        );
        assert_eq!(
            sync_state::last_updated_cursor(conn, "user_reviews", "octocat")
                .unwrap()
                .as_deref(),
            Some("2026-08-12T09:00:00Z")
        );
    }

    /// Once a user has coverage and a watermark, a re-run walks updated-desc and
    /// stops at the first strictly-older item instead of re-reading history.
    #[test]
    fn user_rerun_stops_at_the_watermark() {
        let warehouse = los_angeles_warehouse();
        // Newest-first within the page, as `sort:updated-desc` serves it.
        let fresh = search_response(
            2,
            vec![
                search_node(
                    2,
                    "acme/widgets",
                    "2026-08-14T18:00:00Z",
                    "2026-08-15T09:00:00Z",
                ),
                search_node(
                    1,
                    "acme/widgets",
                    "2026-08-13T18:00:00Z",
                    "2026-08-14T09:00:00Z",
                ),
            ],
        );
        let transport = FixtureTransport::new(vec![
            Ok(fresh.clone()), // run 1, author
            Ok(fresh.clone()), // run 1, reviewed-by
            Ok(fresh.clone()), // run 2, author — everything at or below cursor
            Ok(fresh.clone()), // run 2, reviewed-by
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();

        {
            let mut syncer =
                Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
            let first = syncer
                .sync_user("octocat", &fast_user_options(Some(1)))
                .unwrap();
            assert_eq!(first.pull_requests_synced, 4, "both roles, both PRs");
        }
        {
            // No --days: coverage exists and both cursors are set, so this run
            // is incremental — one unwindowed search per role.
            let mut syncer =
                Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
            let second = syncer
                .sync_user("octocat", &fast_user_options(None))
                .unwrap();
            assert_eq!(
                second.pull_requests_synced, 2,
                "the boundary-second item is re-read; the strictly older one stops the walk"
            );
            assert_eq!(
                second.pages_fetched, 2,
                "no date windowing on an incremental run"
            );
        }
    }

    /// Same invariant the repository path holds: a watermark that moves past an
    /// item the warehouse failed to store turns a retryable failure into a
    /// permanent hole, because the next run's stop fires before reaching it.
    #[test]
    fn user_cursor_does_not_advance_past_a_failed_upsert() {
        let warehouse = los_angeles_warehouse();
        let transport = FixtureTransport::new(vec![
            Ok(search_response(
                2,
                vec![
                    // Newest, and unstorable: `createdAt` will not parse.
                    search_node(9, "acme/widgets", "not-a-timestamp", "2026-08-15T10:00:00Z"),
                    search_node(
                        8,
                        "acme/widgets",
                        "2026-08-14T18:00:00Z",
                        "2026-08-14T10:00:00Z",
                    ),
                ],
            )),
            Ok(search_response(0, vec![])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let summary = syncer
            .sync_user("octocat", &fast_user_options(Some(1)))
            .unwrap();

        assert_eq!(summary.pull_requests_synced, 1);
        assert_eq!(summary.failed.len(), 1);
        assert_eq!(
            sync_state::last_updated_cursor(warehouse.connection(), "user", "octocat")
                .unwrap()
                .as_deref(),
            Some("2026-08-14T10:00:00Z"),
            "the watermark stays below the failed item so the next run retries it"
        );
    }

    /// Search serves at most 1,000 results per query however many it reports
    /// matching. A window over the cap is bisected until each half fits, so a
    /// prolific contributor's backfill is complete rather than truncated.
    #[test]
    fn a_window_over_the_result_cap_is_split() {
        let warehouse = los_angeles_warehouse();
        let transport = FixtureTransport::new(vec![
            // author, 2026-08-13..2026-08-15: more matches than search serves
            Ok(search_response(
                2500,
                vec![search_node(
                    1,
                    "acme/widgets",
                    "2026-08-14T18:00:00Z",
                    "2026-08-15T09:00:00Z",
                )],
            )),
            // first half (08-13..08-14) and second half (08-15..08-15)
            Ok(search_page("acme/widgets", &[1])),
            Ok(search_page("acme/widgets", &[2])),
            // reviewed-by, whole window, under the cap
            Ok(search_page("acme/widgets", &[3])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let summary = syncer
            .sync_user("octocat", &fast_user_options(Some(3)))
            .unwrap();

        assert_eq!(summary.pages_fetched, 4);
        assert_eq!(
            summary.pull_requests_synced, 3,
            "only the split halves are ingested; the over-cap query is abandoned before work"
        );
        assert!(
            summary.notes.is_empty(),
            "a window that could be narrowed needs no caveat"
        );
    }

    /// When the window is already a single day there is nothing left to narrow.
    /// The shortfall is reported rather than passed off as full coverage.
    #[test]
    fn an_unsplittable_over_cap_window_is_reported() {
        let warehouse = los_angeles_warehouse();
        let transport = FixtureTransport::new(vec![
            Ok(search_response(
                4200,
                vec![search_node(
                    1,
                    "acme/widgets",
                    "2026-08-14T18:00:00Z",
                    "2026-08-15T09:00:00Z",
                )],
            )),
            Ok(search_response(0, vec![])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let summary = syncer
            .sync_user("octocat", &fast_user_options(Some(1)))
            .unwrap();

        assert_eq!(summary.notes.len(), 1);
        let note = &summary.notes[0];
        assert!(note.contains("4200"), "the match count is named: {note}");
        assert!(note.contains("1000"), "so is the cap: {note}");
        assert!(summary.partial, "a truncated day is not complete coverage");
        assert!(
            sync_state::last_updated_cursor(warehouse.connection(), "user", "octocat")
                .unwrap()
                .is_none(),
            "do not advance past records hidden by the cap"
        );
        assert!(
            sync_state::coverage_extent_as_of(
                warehouse.connection(),
                "user:octocat",
                evening_instant(),
            )
            .unwrap()
            .is_none(),
            "do not record a completely covered date range"
        );
    }

    /// `--issues-only` has no user-scoped counterpart. Returning zero quietly
    /// would read as "this user has no issues"; the run says what it skipped.
    #[test]
    fn issues_only_on_a_user_reports_instead_of_no_opping() {
        let warehouse = los_angeles_warehouse();
        let transport = FixtureTransport::new(vec![]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let summary = syncer
            .sync_user(
                "octocat",
                &SyncOptions {
                    issues_only: true,
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(summary.pages_fetched, 0);
        assert_eq!(summary.notes.len(), 1);
        assert!(summary.notes[0].contains("--issues-only"));
    }

    /// A namespaced spelling reaches the same row `monitor add-user` wrote.
    #[test]
    fn user_sync_accepts_a_namespaced_login() {
        let warehouse = los_angeles_warehouse();
        monitor_repository::add_user(warehouse.connection(), "octocat").unwrap();
        let transport = FixtureTransport::new(vec![
            Ok(search_page("acme/widgets", &[1])),
            Ok(search_response(0, vec![])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        syncer
            .sync_user("user:OctoCat", &fast_user_options(Some(1)))
            .unwrap();

        let last_sync: Option<String> = warehouse
            .connection()
            .query_row(
                "SELECT last_sync_at FROM monitored_users WHERE login = 'octocat'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(last_sync.is_some());
    }

    /// A user with thousands of lifetime matches is ordinary. An incremental
    /// walk that reaches its watermark has fetched everything that changed, so
    /// the lifetime match count is not a shortfall and must not be reported as
    /// one — the live case that produced this test was a real login whose
    /// `reviewed-by:` query matches 1,122 items while only five had changed.
    #[test]
    fn an_incremental_walk_that_reaches_its_watermark_reports_nothing() {
        let warehouse = los_angeles_warehouse();
        let seed = search_response(
            1,
            vec![search_node(
                1,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-15T09:00:00Z",
            )],
        );
        // Run 2 reports a huge lifetime total but its first node is already
        // below the watermark, so the walk stops on the first item.
        let over_cap_but_converging = search_response(
            1122,
            vec![search_node(
                1,
                "acme/widgets",
                "2026-08-01T18:00:00Z",
                "2026-08-02T09:00:00Z",
            )],
        );
        let transport = FixtureTransport::new(vec![
            Ok(seed.clone()),
            Ok(seed.clone()),
            Ok(over_cap_but_converging.clone()),
            Ok(over_cap_but_converging.clone()),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();

        {
            let mut syncer =
                Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
            syncer
                .sync_user("octocat", &fast_user_options(Some(1)))
                .unwrap();
        }
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let second = syncer
            .sync_user("octocat", &fast_user_options(None))
            .unwrap();

        assert_eq!(second.pull_requests_synced, 0);
        assert!(
            second.notes.is_empty(),
            "reaching the watermark is convergence, whatever the match count: {:?}",
            second.notes
        );
    }

    /// Large incremental searches switch to bounded time windows before
    /// ingesting the truncated first page. A later process resumes the saved
    /// oldest unfinished window without replaying the completed prefix.
    #[test]
    fn large_user_walk_resumes_time_windows_without_search_offsets() {
        let warehouse = los_angeles_warehouse();
        let conn = warehouse.connection();
        let now = evening_instant();
        let base = search_timestamp(now.timestamp() - 1);
        sync_state::record_range_as_of(conn, "user:octocat", "2026-08-15", "2026-08-15", 0, now)
            .unwrap();
        for role in UserRole::all() {
            sync_state::advance_cursor(conn, role.cursor_source_type(), "octocat", Some(&base))
                .unwrap();
        }
        let leaf = |number, at: &str| {
            search_response(
                1,
                vec![search_node(
                    number,
                    "acme/widgets",
                    "2026-08-14T18:00:00Z",
                    at,
                )],
            )
        };
        let (mut client, transport) = shared_client(vec![
            // Legacy probe, oversized bounded interval, oldest single second.
            Ok(search_response(5000, vec![])),
            Ok(search_response(1001, vec![])),
            Ok(leaf(1, &base)),
            // The other role has nothing newer than its existing cursor.
            Ok(search_response(0, vec![])),
            // Resume must fetch only the remaining second, no legacy probe.
            Ok(leaf(2, &search_timestamp(now.timestamp()))),
            Ok(search_response(0, vec![])),
        ]);
        let first = Syncer::new(conn, &mut client)
            .as_of(now)
            .with_item_budget(1)
            .sync_user("octocat", &fast_user_options(None))
            .unwrap();
        assert!(first.partial);
        assert_eq!(first.pull_requests_synced, 1);
        assert_eq!(
            sync_state::last_updated_cursor(conn, "user", "octocat")
                .unwrap()
                .as_deref(),
            Some(base.as_str())
        );
        let checkpoint: UserUpdateCheckpoint = serde_json::from_str(
            &sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(checkpoint.pending, vec![(now.timestamp(), now.timestamp())]);
        assert_eq!(
            count(
                conn,
                "SELECT count(*) FROM sync_jobs WHERE status='partial'"
            ),
            1
        );
        // Even when the process starts later, the frozen bound stays fixed and
        // freshness does not claim the intervening minute was fetched.
        let second = Syncer::new(conn, &mut client)
            .as_of(now + Duration::minutes(1))
            .with_item_budget(1)
            .sync_user("octocat", &fast_user_options(None))
            .unwrap();
        assert!(second.partial);
        assert_eq!(second.pull_requests_synced, 1);
        assert!(
            sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
            Some(search_timestamp(now.timestamp()))
        );
        let stamp: String = conn.query_row("SELECT last_sync_at FROM sync_metadata WHERE source_type='user' AND source_id='octocat'", [], |r| r.get(0)).unwrap();
        assert_eq!(stamp, search_timestamp(now.timestamp()));
        assert!(requested_cursors(&transport).iter().all(Option::is_none));
        let calls = transport.calls.lock().unwrap();
        let last_author_query = calls[4].iter().find(|a| a.starts_with("q=")).unwrap();
        assert!(last_author_query.contains(&format!(
            "updated:{0}..{0}",
            search_timestamp(now.timestamp())
        )));
    }

    #[test]
    fn user_update_window_failure_retains_checkpoint_and_cursor() {
        let warehouse = los_angeles_warehouse();
        let conn = warehouse.connection();
        let now = evening_instant();
        let base = search_timestamp(now.timestamp());
        sync_state::advance_cursor(conn, "user", "octocat", Some(&base)).unwrap();
        let (mut client, _) = shared_client(vec![Ok(search_response(
            1,
            vec![search_node(1, "acme/widgets", "bad-created-date", &base)],
        ))]);
        let mut summary = SyncSummary::default();
        Syncer::new(conn, &mut client)
            .as_of(now)
            .walk_user_update_windows(
                "octocat",
                UserRole::Author,
                &base,
                "user:octocat",
                &fast_user_options(None),
                &mut summary,
            )
            .unwrap();
        assert!(summary.partial);
        assert_eq!(summary.failed.len(), 1);
        let saved: UserUpdateCheckpoint = serde_json::from_str(
            &sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(saved.pending, vec![(now.timestamp(), now.timestamp())]);
        assert_eq!(
            sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
            Some(base)
        );
    }

    #[test]
    fn user_update_windows_ingest_complete_search_results_with_newer_node_timestamps() {
        let warehouse = los_angeles_warehouse();
        let conn = warehouse.connection();
        let now = evening_instant();
        let base = search_timestamp(now.timestamp() - 60);
        let later = search_timestamp(now.timestamp() + 86400);
        // GitHub can match the older search-index timestamp while returning a
        // newer PR node. This reproduces the live #14430 catch-up blocker.
        let (mut client, _) = shared_client(vec![Ok(search_response(
            2,
            vec![
                search_node(1, "acme/widgets", "2026-08-14T18:00:00Z", &base),
                search_node(2, "acme/widgets", "2026-08-14T18:00:00Z", &later),
            ],
        ))]);
        let mut summary = SyncSummary::default();
        Syncer::new(conn, &mut client)
            .as_of(now)
            .walk_user_update_windows(
                "octocat",
                UserRole::Author,
                &base,
                "user:octocat",
                &fast_user_options(None),
                &mut summary,
            )
            .unwrap();
        assert!(!summary.partial);
        assert_eq!(summary.pull_requests_synced, 2);
        assert_eq!(count(conn, "SELECT count(*) FROM fact_pull_requests"), 2);
        assert_eq!(
            sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
            Some(search_timestamp(now.timestamp())),
            "a newer returned node must not advance coverage beyond the frozen search bound"
        );
        assert!(
            sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn user_update_windows_keep_unsplittable_ties_and_incomplete_responses_open() {
        for response in [search_response(1001, vec![]), search_response(1, vec![])] {
            let warehouse = los_angeles_warehouse();
            let conn = warehouse.connection();
            let now = evening_instant();
            let base = search_timestamp(now.timestamp());
            let (mut client, _) = shared_client(vec![Ok(response)]);
            let mut summary = SyncSummary::default();
            Syncer::new(conn, &mut client)
                .as_of(now)
                .walk_user_update_windows(
                    "octocat",
                    UserRole::Author,
                    &base,
                    "user:octocat",
                    &fast_user_options(None),
                    &mut summary,
                )
                .unwrap();
            assert!(summary.partial);
            assert!(
                sync_state::last_updated_cursor(conn, "user", "octocat")
                    .unwrap()
                    .is_none()
            );
            assert!(
                sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn user_incremental_success_covers_days_missed_between_runs() {
        let warehouse = los_angeles_warehouse();
        let conn = warehouse.connection();
        let now = evening_instant();
        sync_state::record_range_as_of(conn, "user:octocat", "2026-08-01", "2026-08-15", 0, now)
            .unwrap();
        for role in UserRole::all() {
            sync_state::advance_cursor(
                conn,
                role.cursor_source_type(),
                "octocat",
                Some("2026-08-15T09:00:00Z"),
            )
            .unwrap();
        }
        let (mut client, _) = shared_client(vec![
            Ok(search_response(0, vec![])),
            Ok(search_response(0, vec![])),
        ]);
        let summary = Syncer::new(conn, &mut client)
            .as_of(now + Duration::days(3))
            .sync_user("octocat", &fast_user_options(None))
            .unwrap();
        assert!(!summary.partial);
        let extent =
            sync_state::coverage_extent_as_of(conn, "user:octocat", now + Duration::days(3))
                .unwrap()
                .unwrap();
        assert_eq!(extent.1.to_string(), "2026-08-18");
        assert_eq!(
            count(
                conn,
                "SELECT count(*) FROM synced_ranges WHERE entity_key='user:octocat'"
            ),
            1,
            "missed days join the existing range, not an isolated latest day"
        );
    }

    #[test]
    fn user_update_checkpoint_survives_transport_failure_and_restarts_invalid_state() {
        let warehouse = los_angeles_warehouse();
        let conn = warehouse.connection();
        let now = evening_instant();
        let base = search_timestamp(now.timestamp() - 1);
        let saved = UserUpdateCheckpoint {
            version: CHECKPOINT_VERSION,
            base: base.clone(),
            upper: now.timestamp(),
            pending: vec![(now.timestamp(), now.timestamp())],
        };
        sync_state::save_checkpoint(
            conn,
            "user_update_checkpoint",
            "author:octocat",
            &serde_json::to_string(&saved).unwrap(),
        )
        .unwrap();
        let (mut client, _) = shared_client(vec![]);
        let mut summary = SyncSummary::default();
        assert!(
            Syncer::new(conn, &mut client)
                .as_of(now)
                .walk_user_update_windows(
                    "octocat",
                    UserRole::Author,
                    &base,
                    "user:octocat",
                    &fast_user_options(None),
                    &mut summary,
                )
                .is_err()
        );
        let after: UserUpdateCheckpoint = serde_json::from_str(
            &sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(after.pending, saved.pending);
        assert!(
            sync_state::last_updated_cursor(conn, "user", "octocat")
                .unwrap()
                .is_none()
        );
        // Malformed bounds must restart at the real cursor, never panic or skip.
        let bad = UserUpdateCheckpoint {
            pending: vec![(i64::MAX, i64::MAX), (0, i64::MAX)],
            ..saved
        };
        sync_state::save_checkpoint(
            conn,
            "user_update_checkpoint",
            "author:octocat",
            &serde_json::to_string(&bad).unwrap(),
        )
        .unwrap();
        let (mut client, transport) = shared_client(vec![Ok(search_response(0, vec![]))]);
        Syncer::new(conn, &mut client)
            .as_of(now)
            .walk_user_update_windows(
                "octocat",
                UserRole::Author,
                &base,
                "user:octocat",
                &fast_user_options(None),
                &mut summary,
            )
            .unwrap();
        let calls = transport.calls.lock().unwrap();
        assert!(calls[0].iter().any(|a| a.contains(&format!(
            "updated:{base}..{}",
            search_timestamp(now.timestamp())
        ))));
        assert!(
            sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                .unwrap()
                .is_none()
        );
    }

    // ------------------------------------------------------------ windowing

    #[test]
    fn month_windows_clip_both_ends() {
        let day = |text: &str| NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap();
        assert_eq!(
            month_windows(day("2026-01-15"), day("2026-03-10")),
            vec![
                (day("2026-01-15"), day("2026-01-31")),
                (day("2026-02-01"), day("2026-02-28")),
                (day("2026-03-01"), day("2026-03-10")),
            ]
        );
        assert_eq!(
            month_windows(day("2026-06-05"), day("2026-06-05")),
            vec![(day("2026-06-05"), day("2026-06-05"))]
        );
        assert!(month_windows(day("2026-06-05"), day("2026-06-04")).is_empty());
        // A leap February and a year boundary both land on real month ends.
        assert_eq!(
            month_windows(day("2024-02-01"), day("2024-02-29")),
            vec![(day("2024-02-01"), day("2024-02-29"))]
        );
        assert_eq!(
            month_windows(day("2025-12-20"), day("2026-01-05")),
            vec![
                (day("2025-12-20"), day("2025-12-31")),
                (day("2026-01-01"), day("2026-01-05")),
            ]
        );
    }

    // ----------------------------------------------------------- sync all plan

    fn source(source_type: &str, identifier: &str, enabled: bool) -> MonitoredSource {
        MonitoredSource {
            source_type: source_type.into(),
            identifier: identifier.into(),
            last_sync_at: None,
            sync_enabled: enabled,
        }
    }

    /// `sync all` covers users as well as repositories.
    #[test]
    fn the_plan_covers_both_supported_source_types() {
        let plan = plan_all(&[
            source("repo", "acme/widgets", true),
            source("user", "octocat", true),
        ]);
        assert_eq!(plan.repos, vec!["acme/widgets"]);
        assert_eq!(plan.users, vec!["octocat"]);
        assert!(plan.notices.is_empty());
    }

    /// The invariant that keeps the plan honest: every enabled source is either
    /// in a bucket this build serves or named in a notice the caller prints. A
    /// source type nobody handles must never just vanish from the run.
    #[test]
    fn no_enabled_source_is_dropped_without_a_word() {
        let sources = [
            source("repo", "acme/widgets", true),
            source("user", "octocat", true),
            source("org", "acme", true),
            source("repo", "acme/legacy", false),
        ];
        let plan = plan_all(&sources);
        let notices = plan.notices.join("\n");

        for entry in &sources {
            let accounted = plan.repos.contains(&entry.identifier)
                || plan.users.contains(&entry.identifier)
                || notices.contains(&entry.identifier);
            assert!(
                accounted,
                "{} ({}) is neither synced nor mentioned",
                entry.identifier, entry.source_type
            );
        }
        assert!(
            notices.contains("org") && notices.contains("not implemented"),
            "the unsupported type says so: {notices}"
        );
        assert!(
            notices.contains("disabled"),
            "a deliberately disabled source is still accounted for: {notices}"
        );
    }

    #[test]
    fn an_empty_monitored_set_says_what_to_do() {
        let plan = plan_all(&[]);
        assert!(plan.is_empty());
        assert_eq!(plan.notices.len(), 1);
        assert!(plan.notices[0].contains("monitor add-user"));
    }

    // ----- targeted refresh -----

    fn single_pr_response(node: serde_json::Value) -> String {
        json!({"data": {
            "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
            "repository": {
                "nameWithOwner": "octocat/hello", "primaryLanguage": {"name": "Rust"},
                "isFork": false, "isPrivate": false, "defaultBranchRef": {"name": "main"},
                "createdAt": "2020-01-01T00:00:00Z",
                "pullRequest": node
            }
        }})
        .to_string()
    }

    fn single_issue_response(node: serde_json::Value) -> String {
        json!({"data": {
            "repository": {
                "nameWithOwner": "octocat/hello", "primaryLanguage": null,
                "isFork": false, "isPrivate": false, "defaultBranchRef": {"name": "main"},
                "createdAt": "2020-01-01T00:00:00Z",
                "issue": node
            }
        }})
        .to_string()
    }

    fn target(kind: TargetKind, reference: &str) -> RefreshTarget {
        RefreshTarget::parse(kind, reference).unwrap()
    }

    /// Every row of every sync-state and monitoring table, so a test can prove
    /// a refresh left all of them unchanged.
    fn state_snapshot(conn: &Connection) -> Vec<String> {
        let mut rows = Vec::new();
        for table in [
            "sync_metadata",
            "sync_locks",
            "sync_jobs",
            "synced_ranges",
            "monitored_users",
            "monitored_repos",
            "monitored_orgs",
        ] {
            let mut statement = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let columns = statement.column_count();
            let mut query = statement.query([]).unwrap();
            while let Some(row) = query.next().unwrap() {
                let values: Vec<String> = (0..columns)
                    .map(|index| format!("{:?}", row.get_ref(index).unwrap()))
                    .collect();
                rows.push(format!("{table}: {}", values.join("|")));
            }
        }
        rows
    }

    /// An old, already-synced PR that the incremental walk will never revisit.
    fn warehouse_with_shipped_pr() -> GithubDW {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let mut node = pr_node(5, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        node["id"] = json!("PR_5");
        node["comments"] = json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [conversation_comment("IC1")]});
        node["files"] =
            json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [file("a.rs")]});
        let (mut client, _) = routing_client(vec![page_of(false, None, vec![node])], vec![]);
        let summary = Syncer::new(warehouse.connection(), &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert!(summary.failed.is_empty());
        warehouse
    }

    fn title_of(conn: &Connection, pr_key: &str) -> String {
        conn.query_row(
            "SELECT title FROM fact_pull_requests WHERE pr_key = ?1",
            [pr_key],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn refresh_completes_an_old_shipped_pr_without_moving_any_sync_state() {
        let warehouse = warehouse_with_shipped_pr();
        let conn = warehouse.connection();
        assert!(pr_cursor(conn).is_some(), "walk recorded a cursor");

        // The incremental walk stops at the cursor: it cannot repair PR 5.
        let (mut client, transport) = routing_client(
            vec![page_of(
                false,
                None,
                vec![pr_node(5, "2026-01-05T18:00:00Z", "2026-01-06T08:00:00Z")],
            )],
            vec![],
        );
        let summary = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert_eq!(summary.pull_requests_synced, 0);
        assert!(transport.continuation_calls.lock().unwrap().is_empty());
        let state_before = state_snapshot(conn);

        let mut node = heavy_pr_node(5);
        node["title"] = json!("Refreshed title");
        let (mut client, transport) =
            routing_client(vec![single_pr_response(node)], heavy_pr_continuations());
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#5")],
                &RefreshOptions { skip_diffs: true },
            )
            .unwrap();
        assert_eq!(report.failed(), 0, "{report:?}");
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed);
        assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
        assert_eq!(outcome.before.comments, 1);
        assert_eq!(outcome.after.comments, 3);
        assert_eq!(outcome.before.files, 1);
        assert_eq!(outcome.after.files, 3);
        assert_eq!(outcome.after.review_comments, 3);
        assert_eq!(transport.continuation_calls.lock().unwrap().len(), 5);
        assert_heavy_pr_complete(conn, "octocat/hello#5");
        assert_eq!(title_of(conn, "octocat/hello#5"), "Refreshed title");
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 1);
        assert_eq!(
            state_snapshot(conn),
            state_before,
            "no cursor, checkpoint, job, range, lock, or monitor row changed"
        );
    }

    #[test]
    fn failed_refresh_preserves_stored_rows_and_state() {
        let warehouse = warehouse_with_shipped_pr();
        let conn = warehouse.connection();
        let state_before = state_snapshot(conn);

        // A continuation page fails: nothing is fetched completely, nothing written.
        let mut continuations = heavy_pr_continuations();
        continuations.retain(|(field, cursor, _)| !(*field == "comments" && *cursor == "COM2"));
        let (mut client, _) =
            routing_client(vec![single_pr_response(heavy_pr_node(5))], continuations);
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#5")],
                &RefreshOptions { skip_diffs: true },
            )
            .unwrap();
        assert_eq!(report.failed(), 1);
        assert_eq!(report.outcomes[0].before, report.outcomes[0].after);
        assert!(report.outcomes[0].error.is_some());

        // A failure AFTER the PR row was written (a review with an unparsable
        // timestamp) rolls the whole target back, title included.
        let mut node = pr_node(5, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        node["title"] = json!("must not land");
        node["reviews"]["nodes"][0]["submittedAt"] = json!("not a timestamp");
        let (mut client, _) = routing_client(vec![single_pr_response(node)], vec![]);
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#5")],
                &RefreshOptions { skip_diffs: true },
            )
            .unwrap();
        assert_eq!(report.failed(), 1, "{report:?}");
        assert_eq!(title_of(conn, "octocat/hello#5"), "PR 5");
        assert_eq!(state_snapshot(conn), state_before);
    }

    #[test]
    fn refresh_honours_a_running_sync_lock_and_leaves_it_in_place() {
        let warehouse = warehouse_with_shipped_pr();
        let conn = warehouse.connection();
        sync_state::acquire_lock(conn, "repo:octocat/hello").unwrap();
        let state_before = state_snapshot(conn);
        let (mut client, transport) =
            routing_client(vec![single_pr_response(heavy_pr_node(5))], vec![]);
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#5")],
                &RefreshOptions::default(),
            )
            .unwrap();
        assert_eq!(report.failed(), 1);
        let error = report.outcomes[0].error.as_deref().unwrap();
        assert!(error.contains("in progress"), "{error}");
        assert_eq!(
            transport.pages.lock().unwrap().len(),
            1,
            "no request was made"
        );
        assert_eq!(
            state_snapshot(conn),
            state_before,
            "the other run's lock survives"
        );
    }

    #[test]
    fn each_target_fails_or_lands_on_its_own() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let mut capped = pr_node(8, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        capped["id"] = json!("PR_8");
        capped["changedFiles"] = json!(4443);
        let mut renamed: serde_json::Value = serde_json::from_str(&single_pr_response(pr_node(
            9,
            "2026-01-05T18:00:00Z",
            "2026-01-06T09:00:00Z",
        )))
        .unwrap();
        renamed["data"]["repository"]["nameWithOwner"] = json!("octocat/renamed");
        let mut wrong_number = pr_node(11, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        wrong_number["number"] = json!(12);
        let (mut client, _) = routing_client(
            vec![
                single_pr_response(serde_json::Value::Null),
                single_pr_response(capped),
                renamed.to_string(),
                single_pr_response(wrong_number),
            ],
            vec![],
        );
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[
                    target(TargetKind::PullRequest, "octocat/hello#7"),
                    target(TargetKind::PullRequest, "octocat/hello#8"),
                    target(TargetKind::PullRequest, "octocat/hello#9"),
                    target(TargetKind::PullRequest, "octocat/hello#11"),
                    target(TargetKind::PullRequest, "octocat/hello#7"),
                ],
                &RefreshOptions { skip_diffs: true },
            )
            .unwrap();
        assert_eq!(report.outcomes.len(), 4, "duplicate collapsed");
        let by_number = |number: i64| {
            report
                .outcomes
                .iter()
                .find(|outcome| outcome.target.number == number)
                .unwrap()
        };
        assert!(by_number(7).error.as_deref().unwrap().contains("not found"));
        assert_eq!(by_number(8).status, TargetStatus::Refreshed);
        assert!(
            by_number(8).caveats[0].contains("1 of 4443"),
            "{:?}",
            by_number(8).caveats
        );
        assert_eq!(report.incomplete(), 1);
        assert!(
            by_number(9)
                .error
                .as_deref()
                .unwrap()
                .contains("octocat/renamed")
        );
        assert!(
            by_number(11)
                .error
                .as_deref()
                .unwrap()
                .contains("returned #12")
        );
        assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 1);
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM sync_locks"),
            0,
            "lock released"
        );
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM sync_jobs"),
            0,
            "no job record"
        );
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM monitored_repos"),
            0,
            "not enrolled"
        );
    }

    #[test]
    fn transport_failure_fails_only_its_target() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let mut node = pr_node(3, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        node["id"] = json!("PR_3");
        let (mut client, _) = shared_client(vec![
            Err(crate::Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 404: Not Found".into(),
            }),
            Ok(single_pr_response(node)),
        ]);
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[
                    target(TargetKind::PullRequest, "octocat/hello#2"),
                    target(TargetKind::PullRequest, "octocat/hello#3"),
                ],
                &RefreshOptions { skip_diffs: true },
            )
            .unwrap();
        assert_eq!(report.outcomes[0].status, TargetStatus::Failed);
        assert_eq!(report.outcomes[1].status, TargetStatus::Refreshed);
    }

    fn rest_patch_page(range: std::ops::Range<usize>) -> String {
        serde_json::Value::Array(
            range
                .map(|i| json!({"filename": format!("f{i}.rs"), "patch": "@@ -1 +1 @@"}))
                .collect(),
        )
        .to_string()
    }

    fn hundred_and_one_file_pr() -> serde_json::Value {
        let mut node = pr_node(4, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
        node["id"] = json!("PR_4");
        node["changedFiles"] = json!(101);
        let files: Vec<_> = (0..100).map(|i| file(&format!("f{i}.rs"))).collect();
        node["files"] =
            json!({"pageInfo": {"hasNextPage": true, "endCursor": "F1"}, "nodes": files});
        node
    }

    fn second_file_page() -> Vec<(&'static str, &'static str, String)> {
        vec![(
            "files",
            "F1",
            connection_page("files", false, None, vec![file("f100.rs")]),
        )]
    }

    #[test]
    fn refresh_walks_rest_patch_pages_and_reports_a_failed_page() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let (mut client, _) = routing_client(
            vec![
                single_pr_response(hundred_and_one_file_pr()),
                rest_patch_page(0..100),
                rest_patch_page(100..101),
            ],
            second_file_page(),
        );
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#4")],
                &RefreshOptions::default(),
            )
            .unwrap();
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
        assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
        assert_eq!(outcome.after.files, 101);
        assert_eq!(
            outcome.after.files_with_patch, 101,
            "page-two patches landed"
        );

        // The second REST page fails: stored, but explicitly incomplete.
        let (mut client, _) = routing_client(
            vec![
                single_pr_response(hundred_and_one_file_pr()),
                rest_patch_page(0..100),
            ],
            second_file_page(),
        );
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#4")],
                &RefreshOptions::default(),
            )
            .unwrap();
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed);
        assert!(
            outcome.caveats[0].contains("patch text not retrieved"),
            "{:?}",
            outcome.caveats
        );
        assert_eq!(report.incomplete(), 1);
    }

    #[test]
    fn refresh_patch_write_failure_rolls_back_the_entire_target() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let selected = target(TargetKind::PullRequest, "octocat/hello#4");
        let (mut client, _) = routing_client(
            vec![
                single_pr_response(hundred_and_one_file_pr()),
                rest_patch_page(0..100),
                rest_patch_page(100..101),
            ],
            second_file_page(),
        );
        let initial = Syncer::new(conn, &mut client)
            .refresh_targets(std::slice::from_ref(&selected), &RefreshOptions::default())
            .unwrap();
        assert_eq!(initial.failed(), 0);
        let before = refresh::stored_counts(conn, &selected).unwrap();
        let old_title = title_of(conn, "octocat/hello#4");
        let state = state_snapshot(conn);
        conn.execute_batch(
            "CREATE TRIGGER reject_second_patch BEFORE UPDATE OF patch ON fact_file_diffs
            WHEN NEW.file_diff_key = 'octocat/hello#4:f1.rs'
            BEGIN SELECT RAISE(ABORT, 'synthetic patch write failure'); END;",
        )
        .unwrap();
        let mut changed = hundred_and_one_file_pr();
        changed["title"] = json!("must not land");
        let (mut client, _) = routing_client(
            vec![
                single_pr_response(changed),
                rest_patch_page(0..100).replace("@@ -1 +1 @@", "new patch"),
                rest_patch_page(100..101),
            ],
            second_file_page(),
        );
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(&[selected], &RefreshOptions::default())
            .unwrap();
        assert_eq!(report.failed(), 1);
        assert!(
            report.outcomes[0]
                .error
                .as_deref()
                .unwrap()
                .contains("synthetic patch write failure")
        );
        assert_eq!(report.outcomes[0].after, before);
        assert_eq!(title_of(conn, "octocat/hello#4"), old_title);
        let patch: String = conn
            .query_row(
                "SELECT patch FROM fact_file_diffs WHERE file_diff_key='octocat/hello#4:f0.rs'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            patch, "@@ -1 +1 @@",
            "the first patch update also rolls back"
        );
        assert_eq!(state_snapshot(conn), state);
    }

    // ----- stale file-set correction -----

    /// A PR node listing exactly `paths` in one complete files page.
    fn pr_with_files(
        number: i64,
        updated_at: &str,
        paths: &[&str],
        changed: Option<i64>,
    ) -> serde_json::Value {
        let mut node = pr_node(number, "2026-01-05T18:00:00Z", updated_at);
        node["id"] = json!(format!("PR_{number}"));
        match changed {
            Some(total) => node["changedFiles"] = json!(total),
            None => {
                node.as_object_mut().unwrap().remove("changedFiles");
            }
        }
        let files: Vec<_> = paths.iter().map(|path| file(path)).collect();
        node["files"] =
            json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": files});
        node
    }

    fn rest_patches(paths: &[&str], patch: &str) -> String {
        serde_json::Value::Array(
            paths
                .iter()
                .map(|path| json!({"filename": path, "patch": patch}))
                .collect(),
        )
        .to_string()
    }

    fn stored_paths(conn: &Connection, pr_key: &str) -> Vec<String> {
        let mut statement = conn
            .prepare("SELECT file_path FROM fact_file_diffs WHERE pr_key = ?1 ORDER BY file_path")
            .unwrap();
        statement
            .query_map([pr_key], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap()
    }

    /// PR 5 stores a.rs, b.rs, c.rs; unrelated PR 6 stores z.rs. Both through
    /// the ordinary repository walk.
    fn warehouse_with_abc_and_unrelated() -> GithubDW {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let (mut client, _) = routing_client(
            vec![page_of(
                false,
                None,
                vec![
                    pr_with_files(6, "2026-01-07T09:00:00Z", &["z.rs"], Some(1)),
                    pr_with_files(
                        5,
                        "2026-01-06T09:00:00Z",
                        &["a.rs", "b.rs", "c.rs"],
                        Some(3),
                    ),
                ],
            )],
            vec![],
        );
        let summary = Syncer::new(warehouse.connection(), &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        let conn = warehouse.connection();
        assert_eq!(
            stored_paths(conn, "octocat/hello#5"),
            ["a.rs", "b.rs", "c.rs"]
        );
        warehouse
    }

    fn refresh_pr5(conn: &Connection, pages: Vec<String>, skip_diffs: bool) -> RefreshReport {
        let (mut client, _) = routing_client(pages, vec![]);
        Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#5")],
                &RefreshOptions { skip_diffs },
            )
            .unwrap()
    }

    #[test]
    fn complete_refresh_prunes_files_that_left_the_pull_request() {
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        conn.execute(
            "UPDATE fact_file_diffs SET patch = 'kept patch' WHERE file_diff_key = 'octocat/hello#5:a.rs'",
            [],
        )
        .unwrap();
        let report = refresh_pr5(
            conn,
            vec![single_pr_response(pr_with_files(
                5,
                "2026-01-08T09:00:00Z",
                &["a.rs", "b.rs"],
                Some(2),
            ))],
            true,
        );
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
        assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
        assert_eq!(outcome.before.files, 3);
        assert_eq!(outcome.after.files, 2);
        assert_eq!(stored_paths(conn, "octocat/hello#5"), ["a.rs", "b.rs"]);
        let patch: Option<String> = conn
            .query_row(
                "SELECT patch FROM fact_file_diffs WHERE file_diff_key = 'octocat/hello#5:a.rs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            patch.as_deref(),
            Some("kept patch"),
            "skip_diffs keeps a retained file's patch"
        );
        assert_eq!(
            stored_paths(conn, "octocat/hello#6"),
            ["z.rs"],
            "unrelated PR untouched"
        );
    }

    #[test]
    fn complete_walk_prunes_files_that_left_the_pull_request() {
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        let (mut client, _) = routing_client(
            vec![page_of(
                false,
                None,
                vec![pr_with_files(
                    5,
                    "2026-01-09T09:00:00Z",
                    &["a.rs", "d.rs"],
                    Some(2),
                )],
            )],
            vec![],
        );
        let summary = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        assert_eq!(summary.pull_requests_synced, 1);
        assert_eq!(stored_paths(conn, "octocat/hello#5"), ["a.rs", "d.rs"]);
        assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
    }

    #[test]
    fn complete_empty_file_set_prunes_every_stored_file() {
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        let report = refresh_pr5(
            conn,
            vec![single_pr_response(pr_with_files(
                5,
                "2026-01-08T09:00:00Z",
                &[],
                Some(0),
            ))],
            false,
        );
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
        assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
        assert!(stored_paths(conn, "octocat/hello#5").is_empty());
        assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
    }

    #[test]
    fn incomplete_or_capped_listing_keeps_stored_files_with_a_caveat() {
        for (changed, paths) in [
            (Some(5), vec!["a.rs", "b.rs"]),    // short of the reported total
            (Some(4443), vec!["a.rs", "b.rs"]), // provider file cap
            (None, vec!["a.rs", "b.rs"]),       // total not reported at all
            (None, vec![]),                     // missing total is not an empty set
        ] {
            let warehouse = warehouse_with_abc_and_unrelated();
            let conn = warehouse.connection();
            let report = refresh_pr5(
                conn,
                vec![single_pr_response(pr_with_files(
                    5,
                    "2026-01-08T09:00:00Z",
                    &paths,
                    changed,
                ))],
                true,
            );
            let outcome = &report.outcomes[0];
            assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
            assert_eq!(
                stored_paths(conn, "octocat/hello#5"),
                ["a.rs", "b.rs", "c.rs"],
                "{changed:?} {paths:?}"
            );
            let kept = 3 - paths.len();
            assert!(
                outcome
                    .caveats
                    .iter()
                    .any(|caveat| caveat.contains(&format!("kept {kept} stored file row(s)"))),
                "{changed:?}: {:?}",
                outcome.caveats
            );
            assert_eq!(report.incomplete(), 1);
        }
    }

    #[test]
    fn duplicate_or_overcounted_listing_never_prunes_silently() {
        for (changed, paths, expected) in [
            (Some(3), vec!["a.rs", "a.rs", "b.rs"], "more than once"),
            (Some(2), vec!["a.rs", "a.rs", "b.rs"], "more than once"),
            (
                Some(1),
                vec!["a.rs", "b.rs"],
                "2 distinct paths for 1 changed files",
            ),
            (Some(2), vec!["a.rs", ""], "empty path"),
        ] {
            let warehouse = warehouse_with_abc_and_unrelated();
            let conn = warehouse.connection();
            let report = refresh_pr5(
                conn,
                vec![single_pr_response(pr_with_files(
                    5,
                    "2026-01-08T09:00:00Z",
                    &paths,
                    changed,
                ))],
                true,
            );
            let outcome = &report.outcomes[0];
            assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
            let stored = stored_paths(conn, "octocat/hello#5");
            for path in ["a.rs", "b.rs", "c.rs"] {
                assert!(stored.iter().any(|p| p == path), "{paths:?}: {stored:?}");
            }
            assert!(
                outcome
                    .caveats
                    .iter()
                    .any(|caveat| caveat.contains(expected)
                        && caveat.contains("not provably complete")),
                "{paths:?}: {:?}",
                outcome.caveats
            );
        }
    }

    /// A patch write failing after the prune rolls back the prune, the PR
    /// metadata, and every other write for that target.
    fn assert_patch_failure_rolls_back_prune(walk: bool) {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let seed = vec![
            page_of(
                false,
                None,
                vec![
                    pr_with_files(6, "2026-01-07T09:00:00Z", &["z.rs"], Some(1)),
                    pr_with_files(
                        5,
                        "2026-01-06T09:00:00Z",
                        &["a.rs", "b.rs", "c.rs"],
                        Some(3),
                    ),
                ],
            ),
            rest_patches(&["z.rs"], "old"),
            rest_patches(&["a.rs", "b.rs", "c.rs"], "old"),
        ];
        let (mut client, _) = routing_client(seed, vec![]);
        let options = SyncOptions {
            skip_diffs: false,
            pull_requests_only: true,
            ..Default::default()
        };
        let summary = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
        assert_eq!(
            stored_paths(conn, "octocat/hello#5"),
            ["a.rs", "b.rs", "c.rs"]
        );
        let selected = target(TargetKind::PullRequest, "octocat/hello#5");
        let before = refresh::stored_counts(conn, &selected).unwrap();
        let old_title = title_of(conn, "octocat/hello#5");
        let state = state_snapshot(conn);
        conn.execute_batch(
            "CREATE TRIGGER reject_b_patch BEFORE UPDATE OF patch ON fact_file_diffs
             WHEN NEW.file_diff_key = 'octocat/hello#5:b.rs'
             BEGIN SELECT RAISE(ABORT, 'synthetic patch write failure'); END;",
        )
        .unwrap();
        let mut changed = pr_with_files(5, "2026-01-09T09:00:00Z", &["a.rs", "b.rs"], Some(2));
        changed["title"] = json!("must not land");
        if walk {
            let (mut client, _) = routing_client(
                vec![
                    page_of(false, None, vec![changed]),
                    rest_patches(&["a.rs", "b.rs"], "new"),
                ],
                vec![],
            );
            let summary = Syncer::new(conn, &mut client)
                .sync_repository("octocat/hello", &options)
                .unwrap();
            assert_eq!(summary.failed.len(), 1, "{summary:?}");
            assert!(
                summary.failed[0]
                    .1
                    .contains("synthetic patch write failure")
            );
        } else {
            let report = refresh_pr5(
                conn,
                vec![
                    single_pr_response(changed),
                    rest_patches(&["a.rs", "b.rs"], "new"),
                ],
                false,
            );
            assert_eq!(report.failed(), 1, "{report:?}");
            assert_eq!(state_snapshot(conn), state);
        }
        assert_eq!(refresh::stored_counts(conn, &selected).unwrap(), before);
        assert_eq!(
            stored_paths(conn, "octocat/hello#5"),
            ["a.rs", "b.rs", "c.rs"]
        );
        assert_eq!(title_of(conn, "octocat/hello#5"), old_title);
        let patch: String = conn
            .query_row(
                "SELECT patch FROM fact_file_diffs WHERE file_diff_key = 'octocat/hello#5:a.rs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(patch, "old", "the earlier patch update also rolls back");
        let changed_files: i64 = conn
            .query_row(
                "SELECT changed_files FROM fact_pull_requests WHERE pr_key = 'octocat/hello#5'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(changed_files, 3);
        assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
    }

    #[test]
    fn refresh_patch_failure_rolls_back_the_prune_and_metadata() {
        assert_patch_failure_rolls_back_prune(false);
    }

    #[test]
    fn walk_patch_failure_rolls_back_the_prune_and_metadata() {
        assert_patch_failure_rolls_back_prune(true);
    }

    #[test]
    fn repair_plan_flags_stale_excess_file_rows() {
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        assert!(
            refresh::repair_candidates(conn, None, 64)
                .unwrap()
                .is_empty()
        );
        conn.execute(
            "UPDATE fact_pull_requests SET changed_files = 2 WHERE pr_key = 'octocat/hello#5'",
            [],
        )
        .unwrap();
        let candidates = refresh::repair_candidates(conn, None, 64).unwrap();
        assert_eq!(candidates.len(), 1, "{candidates:?}");
        assert_eq!(candidates[0].target.reference(), "octocat/hello#5");
        assert!(
            candidates[0].reasons[0].contains("3 file rows stored for 2 changed files"),
            "{:?}",
            candidates[0].reasons
        );
    }

    #[test]
    fn refresh_completes_an_issue_with_paged_comments_labels_and_assignees() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let node = json!({
            "id": "ISS9", "number": 9, "title": "Heavy issue", "body": "b",
            "state": "OPEN", "stateReason": null,
            "createdAt": "2026-02-01T12:00:00Z", "updatedAt": "2026-02-03T12:00:00Z",
            "closedAt": null, "author": {"login": "octocat", "__typename": "User"},
            "milestone": null,
            "labels": {"pageInfo": {"hasNextPage": true, "endCursor": "L1"},
                       "nodes": [{"id": "LAB1", "name": "bug", "color": "f00", "description": ""}]},
            "assignees": {"pageInfo": {"hasNextPage": true, "endCursor": "A1"},
                          "nodes": [{"login": "hubot", "__typename": "User"}]},
            "comments": {"pageInfo": {"hasNextPage": true, "endCursor": "C1"},
                         "nodes": [conversation_comment("IC1")]}
        });
        let label_two = json!({"id": "LAB2", "name": "p1", "color": "0f0", "description": ""});
        let (mut client, _) = routing_client(
            vec![single_issue_response(node)],
            vec![
                (
                    "labels",
                    "L1",
                    connection_page("labels", false, None, vec![label_two]),
                ),
                (
                    "assignees",
                    "A1",
                    connection_page(
                        "assignees",
                        false,
                        None,
                        vec![json!({"login": "octocat", "__typename": "User"})],
                    ),
                ),
                (
                    "comments",
                    "C1",
                    connection_page("comments", false, None, vec![conversation_comment("IC2")]),
                ),
            ],
        );
        let report = Syncer::new(conn, &mut client)
            .refresh_targets(
                &[target(TargetKind::Issue, "octocat/hello#9")],
                &RefreshOptions::default(),
            )
            .unwrap();
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
        assert!(!outcome.before.present);
        assert_eq!(outcome.after.comments, 2);
        assert_eq!(outcome.after.labels, 2);
        assert_eq!(outcome.after.assignees, 2);
    }

    #[test]
    fn repair_plan_selects_first_slice_shaped_items_from_stored_rows() {
        let warehouse = warehouse_with_shipped_pr();
        let conn = warehouse.connection();
        assert!(
            refresh::repair_candidates(conn, None, 64)
                .unwrap()
                .is_empty()
        );
        conn.execute(
            "UPDATE fact_pull_requests SET changed_files = 4443 WHERE pr_key = 'octocat/hello#5'",
            [],
        )
        .unwrap();
        let candidates = refresh::repair_candidates(conn, Some("OctoCat/Hello"), 64).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].target.reference(), "octocat/hello#5");
        assert!(
            candidates[0].reasons[0].contains("1 of 4443"),
            "{:?}",
            candidates[0].reasons
        );
        assert!(
            refresh::repair_candidates(conn, Some("other/repo"), 64)
                .unwrap()
                .is_empty()
        );
        assert!(
            refresh::repair_candidates(conn, None, 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn repair_plan_flags_items_stopped_at_a_first_slice() {
        let warehouse = warehouse_with_shipped_pr();
        let conn = warehouse.connection();
        let author = repository::ensure_ghost_entity(conn).unwrap();
        let keys = time_dimension::ensure_keys_for_timestamp(
            conn,
            "2026-01-05T19:00:00Z",
            chrono_tz::UTC,
            (9, 17),
        )
        .unwrap();
        for index in 2..=50 {
            repository::upsert_issue_comment(
                conn,
                &format!("EXTRA{index}"),
                "pull_request",
                "octocat/hello#5",
                &author,
                None,
                None,
                "2026-01-05T19:00:00Z",
                &keys.date_key,
                &keys.time_key,
            )
            .unwrap_or_else(|error| panic!("seed comment {index}: {error}"));
        }
        let candidates = refresh::repair_candidates(conn, None, 64).unwrap();
        assert_eq!(candidates.len(), 1, "{candidates:?}");
        assert!(candidates[0].reasons[0].contains("50 comments"));
    }

    #[test]
    fn walk_fails_closed_on_an_empty_page_that_claims_more() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let (mut client, _) = routing_client(vec![page_of(true, Some("C1"), vec![])], vec![]);
        let error = Syncer::new(warehouse.connection(), &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap_err();
        assert!(error.to_string().contains("holds none"), "{error}");
        assert!(pr_cursor(warehouse.connection()).is_none());
    }

    #[test]
    fn walk_fails_closed_on_a_repeated_page_cursor() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let (mut client, _) = routing_client(
            vec![
                page_response(true, Some("C1"), &[3]),
                page_response(true, Some("C2"), &[2]),
                page_response(true, Some("C1"), &[1]),
            ],
            vec![],
        );
        let error = Syncer::new(warehouse.connection(), &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap_err();
        assert!(error.to_string().contains("repeated"), "{error}");
        assert!(pr_cursor(warehouse.connection()).is_none());
    }

    #[test]
    fn walk_fails_closed_on_missing_page_metadata() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let mut page: serde_json::Value =
            serde_json::from_str(&page_response(false, None, &[1])).unwrap();
        page["data"]["repository"]["pullRequests"]
            .as_object_mut()
            .unwrap()
            .remove("pageInfo");
        let (mut client, _) = routing_client(vec![page.to_string(); 6], vec![]);
        let result = Syncer::new(warehouse.connection(), &mut client)
            .sync_repository("octocat/hello", &fast_pr_options());
        assert!(result.is_err());
        assert!(pr_cursor(warehouse.connection()).is_none());
    }

    // ---- git-backed recovery of capped file listings -------------------

    use crate::fetch::git_files::{GitFile, test_repo};

    /// PR 5 as GitHub serves a capped pull request: the first 3,000 git
    /// records through GraphQL and the REST walk, totals for all of them.
    fn capped_pages(
        git: &[GitFile],
        rest_additions_offset: i64,
        fixture: &test_repo::FixtureRepo,
    ) -> Vec<String> {
        let lines = |file: &GitFile| file.lines.unwrap_or((0, 0));
        let change = |file: &GitFile| match file.status {
            'A' => "ADDED",
            'D' => "DELETED",
            'R' => "RENAMED",
            _ => "MODIFIED",
        };
        let additions: i64 = git.iter().map(|f| lines(f).0).sum();
        let deletions: i64 = git.iter().map(|f| lines(f).1).sum();
        let mut node = pr_node(5, "2026-01-05T18:00:00Z", "2026-01-08T09:00:00Z");
        node["id"] = json!("PR_5");
        node["title"] = json!("refreshed title");
        node["changedFiles"] = json!(git.len());
        node["additions"] = json!(additions);
        node["deletions"] = json!(deletions);
        let served: Vec<_> = git[..3000]
            .iter()
            .map(|f| {
                json!({"path": f.path, "changeType": change(f),
                            "additions": lines(f).0, "deletions": lines(f).1})
            })
            .collect();
        node["files"] =
            json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": served});
        let mut pages = vec![single_pr_response(node)];
        for chunk in git[..3000].chunks(100) {
            pages.push(
                serde_json::Value::Array(
                    chunk
                        .iter()
                        .map(|f| {
                            json!({"filename": f.path, "previous_filename": f.previous_path,
                                        "patch": f.patch})
                        })
                        .collect(),
                )
                .to_string(),
            );
        }
        pages.push(
            json!({"base": {"sha": fixture.base}, "head": {"sha": fixture.head},
                   "changed_files": git.len(), "additions": additions + rest_additions_offset,
                   "deletions": deletions})
            .to_string(),
        );
        pages.push(json!({"merge_base_commit": {"sha": fixture.base}}).to_string());
        pages
    }

    fn refresh_pr5_recovering(
        conn: &Connection,
        pages: Vec<String>,
        fixture: &test_repo::FixtureRepo,
    ) -> RefreshReport {
        let (mut client, _) = routing_client(pages, vec![]);
        Syncer::new(conn, &mut client)
            .with_git_recovery(fixture.config.clone())
            .refresh_targets(
                &[target(TargetKind::PullRequest, "octocat/hello#5")],
                &RefreshOptions { skip_diffs: false },
            )
            .unwrap()
    }

    #[test]
    fn capped_listing_is_completed_from_git_and_stale_rows_pruned_atomically() {
        let fixture = test_repo::build();
        let git = fixture.records();
        assert!(git.len() > 3000);
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        // A prior revision stored the rename source as its own file. It must
        // disappear as a record when the complete set contains only the target.
        repository::upsert_file_diff(
            conn,
            "octocat/hello#5",
            "octocat/hello",
            "old name/basename.rs",
            None,
            "MODIFIED",
            None,
            1,
            0,
        )
        .unwrap();
        let report = refresh_pr5_recovering(conn, capped_pages(&git, 0, &fixture), &fixture);
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
        assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
        let stored = stored_paths(conn, "octocat/hello#5");
        assert_eq!(
            stored.len(),
            git.len(),
            "every record, and the stale a/b/c pruned"
        );
        assert!(!stored.iter().any(|p| p == "a.rs"));
        assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
        let (changed, previous, tail_patch): (i64, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT pr.changed_files,
                        (SELECT previous_path FROM fact_file_diffs WHERE file_diff_key = 'octocat/hello#5:new dir/basename.rs'),
                        (SELECT patch FROM fact_file_diffs WHERE file_diff_key = ?1)
                 FROM fact_pull_requests pr WHERE pr.pr_key = 'octocat/hello#5'",
                [format!("octocat/hello#5:{}", git.last().unwrap().path)],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(changed, git.len() as i64);
        assert_eq!(previous.as_deref(), Some("old name/basename.rs"));
        assert_eq!(tail_patch, git.last().unwrap().patch, "tail patch from git");
        assert_eq!(title_of(conn, "octocat/hello#5"), "refreshed title");
    }

    #[test]
    fn unprovable_recovery_fails_closed_and_keeps_stored_rows() {
        let fixture = test_repo::build();
        let git = fixture.records();
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        let before = stored_paths(conn, "octocat/hello#5");
        let title_before = title_of(conn, "octocat/hello#5");
        // REST totals disagree with GraphQL: a stale snapshot, nothing proven.
        let report = refresh_pr5_recovering(conn, capped_pages(&git, 7, &fixture), &fixture);
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Failed, "{outcome:?}");
        assert!(outcome.error.as_deref().unwrap().contains("stale snapshot"));
        assert_eq!(stored_paths(conn, "octocat/hello#5"), before);
        assert_eq!(title_of(conn, "octocat/hello#5"), title_before);
        assert_eq!(outcome.before, outcome.after);
        assert_eq!(report.failed(), 1);
    }

    #[test]
    fn recovered_write_failure_rolls_back_the_whole_target() {
        let fixture = test_repo::build();
        let git = fixture.records();
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        let tail = git.last().unwrap().path.clone();
        conn.execute_batch(&format!(
            "CREATE TRIGGER refuse_tail BEFORE INSERT ON fact_file_diffs
             WHEN NEW.file_path = '{}' BEGIN SELECT RAISE(ABORT, 'injected'); END;",
            tail.replace('\'', "''")
        ))
        .unwrap();
        let title_before = title_of(conn, "octocat/hello#5");
        let report = refresh_pr5_recovering(conn, capped_pages(&git, 0, &fixture), &fixture);
        assert_eq!(
            report.outcomes[0].status,
            TargetStatus::Failed,
            "{:?}",
            report.outcomes[0]
        );
        assert!(
            report.outcomes[0]
                .error
                .as_deref()
                .is_some_and(|e| e.contains("injected")),
            "{:?}",
            report.outcomes[0]
        );
        assert_eq!(
            stored_paths(conn, "octocat/hello#5"),
            ["a.rs", "b.rs", "c.rs"]
        );
        assert_eq!(title_of(conn, "octocat/hello#5"), title_before);
    }
}
