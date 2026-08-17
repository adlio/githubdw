//! Sync orchestration: fetch pages via `gh`, upsert into the warehouse,
//! stream progress, and record incremental coverage.

use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use rusqlite::Connection;

use crate::error::Result;
use crate::fetch::GhClient;
use crate::fetch::issues::{self, REPOSITORY_ISSUES_QUERY};
use crate::fetch::pull_requests::{self, ActorReference, PullRequestData};
use crate::fetch::user_search::{self, UserRole};
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
}

impl<'a> Syncer<'a> {
    pub fn new(connection: &'a Connection, client: &'a mut GhClient) -> Self {
        Self {
            connection,
            client,
            now: Utc::now(),
        }
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
                sync_state::complete_job(
                    self.connection,
                    &entity_key,
                    summary.pull_requests_synced + summary.issues_synced,
                    summary.skipped,
                    &summary.failed,
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
                sync_state::complete_job(
                    self.connection,
                    &entity_key,
                    summary.pull_requests_synced,
                    summary.skipped,
                    &summary.failed,
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
            None => match coverage_before {
                Some(_) => coverage_end,
                None => today - Duration::days(INITIAL_WINDOW_DAYS),
            },
        }
        .min(coverage_end);

        for role in UserRole::all() {
            let cursor =
                sync_state::last_updated_cursor(self.connection, role.cursor_source_type(), login)?;
            let mut max_updated_at = cursor.clone();

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
                    &mut max_updated_at,
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
                        &mut max_updated_at,
                        0,
                    )?;
                }
            }

            sync_state::advance_cursor(
                self.connection,
                role.cursor_source_type(),
                login,
                max_updated_at.as_deref(),
            )?;
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
        max_updated_at: &mut Option<String>,
        depth: u32,
    ) -> Result<()> {
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
                                max_updated_at,
                                depth + 1,
                            )?;
                        }
                        return Ok(());
                    }
                    // Nothing left to narrow: report the shortfall instead of
                    // recording coverage that was never fetched.
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
                    Ok(()) => {
                        summary.pull_requests_synced += 1;
                        // The watermark only ever moves past items that landed.
                        if let Some(updated) = item.pull_request.updated_at.as_deref()
                            && max_updated_at.as_deref().is_none_or(|max| updated > max)
                        {
                            *max_updated_at = Some(updated.to_string());
                        }
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

        let mut page_cursor: Option<String> = None;
        let mut max_updated_at: Option<String> = updated_cursor.map(str::to_string);
        let mut item_index: i64 = 0;
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
                    Ok(()) => {
                        summary.pull_requests_synced += 1;
                        // Only advance the cursor past items that actually
                        // landed. Folding a failed item's timestamp in would
                        // move the watermark past data the warehouse does not
                        // hold, and the next run's stop condition would break
                        // before reaching it again — a permanent hole with no
                        // self-heal. Because the page is updated-desc, the
                        // surviving maximum stays below any failed item, so the
                        // failure is retried on the next run.
                        if let Some(updated) = pull_request.updated_at.as_deref()
                            && max_updated_at.as_deref().is_none_or(|max| updated > max)
                        {
                            max_updated_at = Some(updated.to_string());
                        }
                    }
                    Err(error) => summary.failed.push((pr_key.clone(), error.to_string())),
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

            if !page.has_next_page || page_is_empty {
                break;
            }
            page_cursor = page.end_cursor;
            if page_cursor.is_none() {
                break;
            }
        }

        sync_state::advance_cursor(
            self.connection,
            "repo",
            repository_name,
            max_updated_at.as_deref(),
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
    ) -> Result<()> {
        let conn = self.connection;

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
                changed_files: pull_request.changed_files,
                additions: pull_request.additions,
                deletions: pull_request.deletions,
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

        // Patch text backfill via REST (skipped in fast mode).
        if !options.skip_diffs && !pull_request.files.is_empty() {
            let path = format!(
                "repos/{repo_key}/pulls/{}/files?per_page=100",
                pull_request.number
            );
            if let Ok(response) = self.client.rest(&path) {
                for (file_path, previous_path, patch) in
                    pull_requests::parse_rest_file_patches(&response)
                {
                    repository::set_file_diff_patch(
                        conn,
                        pr_key,
                        &file_path,
                        previous_path.as_deref(),
                        patch.as_deref(),
                    )?;
                }
            }
        }

        Ok(())
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
        let mut max_updated_at: Option<String> = updated_cursor.map(str::to_string);
        let mut item_index: i64 = 0;

        'pages: loop {
            let mut variables: Vec<(&str, &str)> = vec![("owner", owner), ("name", name)];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = self.client.graphql(REPOSITORY_ISSUES_QUERY, &variables)?;
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
                        // Cursor advances only past items that persisted.
                        if max_updated_at
                            .as_deref()
                            .is_none_or(|max| issue.updated_at.as_str() > max)
                        {
                            max_updated_at = Some(issue.updated_at.clone());
                        }
                    }
                    Err(error) => summary.failed.push((item_id.clone(), error.to_string())),
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

            if !page.has_next_page || page_is_empty {
                break;
            }
            page_cursor = page.end_cursor;
            if page_cursor.is_none() {
                break;
            }
        }

        sync_state::advance_cursor(
            self.connection,
            "repo_issues",
            repository_name,
            max_updated_at.as_deref(),
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

    fn resolve_actor(&self, actor: Option<&ActorReference>, bot_suffix: &str) -> Result<String> {
        match actor {
            Some(actor) => repository::ensure_entity(self.connection, actor, bot_suffix),
            None => repository::ensure_ghost_entity(self.connection),
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

impl SyncPlan {
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
            "reviews": {"nodes": [{
                "id": format!("REV{number}"), "state": "APPROVED", "body": "",
                "submittedAt": "2026-01-06T08:00:00Z",
                "author": {"login": "hubot", "__typename": "User"}
            }]},
            "reviewThreads": {"nodes": []},
            "comments": {"nodes": []},
            "files": {"nodes": [{
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
                    "labels": {"nodes": [{
                        "id": "LAB1", "name": "bug", "color": "d73a4a", "description": ""
                    }]},
                    "assignees": {"nodes": [{"login": "hubot", "__typename": "User"}]},
                    "comments": {"nodes": [{
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
        node["files"] = json!({"nodes": []});
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

    /// The case that *is* a shortfall: an incremental walk exhausts the pages
    /// search will serve without ever reaching its watermark, so the items
    /// between the cap and the watermark are still missing. The run says so and
    /// names the command that closes the gap.
    #[test]
    fn an_incremental_walk_that_runs_out_before_its_watermark_reports_the_gap() {
        let warehouse = los_angeles_warehouse();
        let seed = search_response(
            1,
            vec![search_node(
                1,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-10T09:00:00Z",
            )],
        );
        // Everything on run 2 is newer than the watermark, and the page reports
        // more matches than search serves: pagination ends first.
        let never_reaches_watermark = search_response(
            5000,
            vec![search_node(
                2,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-15T09:00:00Z",
            )],
        );
        let transport = FixtureTransport::new(vec![
            Ok(seed.clone()),
            Ok(seed.clone()),
            Ok(never_reaches_watermark.clone()),
            Ok(never_reaches_watermark.clone()),
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

        assert_eq!(second.notes.len(), 2, "one per role");
        assert!(
            second.notes[0].contains("--days"),
            "the note names the fix: {}",
            second.notes[0]
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
}
