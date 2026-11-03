//! Counts-first collection: day windows, coverage, and on-demand detail.
//!
//! The dashboard renders org and person charts from three facts per person
//! per warehouse-calendar day: pull requests authored (by `created_date_key`),
//! pull requests merged (by `merged_date_key`), and reviews submitted (by
//! `submitted_date_key`). This module collects exactly those facts with a
//! *light* search node (identifying metadata, plus the person's own reviews
//! with no bodies) and records per-day coverage, so the dashboard can collect
//! today for every member before going further back for anyone.
//!
//! Detail (review bodies, comments, files) and diff text are separate stages:
//! [`fetch_pull_request_detail`] stores everything a full fetch returns except
//! new patch text unless the policy allows retention, and
//! [`fetch_pull_request_diff_live`] returns diff text in memory only.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, SecondsFormat, TimeZone, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::fetch::GhClient;
use crate::fetch::pull_requests::{self, ActorReference, PullRequestData};
use crate::fetch::user_search::SEARCH_RESULT_CAP;
use crate::retention::{DiffRetention, PolicySource};
use crate::storage::repository::{self, CounterSource, PullRequestRow};
use crate::storage::time_dimension;

mod light;
#[cfg(test)]
mod tests;

pub use light::{LightPullRequest, light_search_query};

/// Which activity a day window counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CountRole {
    /// `is:pr author:L created:<day>`; counted by `created_date_key`.
    Authored,
    /// `is:pr author:L merged:<day>`; counted by `merged_date_key`.
    Merged,
    /// `is:pr reviewed-by:L updated:<range>`; reviews counted by
    /// `submitted_date_key`. See [`collect_day`] for why this needs a range.
    Reviewed,
}

impl CountRole {
    pub fn as_str(self) -> &'static str {
        match self {
            CountRole::Authored => "authored",
            CountRole::Merged => "merged",
            CountRole::Reviewed => "reviewed",
        }
    }

    pub fn all() -> [CountRole; 3] {
        [CountRole::Authored, CountRole::Merged, CountRole::Reviewed]
    }
}

/// Caller controls for one collection operation.
#[derive(Debug, Clone, Default)]
pub struct CollectionControl {
    /// Set to stop at the next request or wait. Nothing partial is claimed.
    pub cancel: Option<Arc<AtomicBool>>,
    /// A rate-limit wait longer than this is not slept; the window reports
    /// `RateLimited` with the wake time. `None` sleeps as the client always has.
    pub defer_waits_over: Option<Duration>,
}

/// How one role of one day window ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WindowStatus {
    /// Every match was served (inaccessible items counted, not stored).
    Complete,
    /// The provider would not serve every match (result cap, page shortfall,
    /// review list beyond its page). Rows that landed are kept; no coverage
    /// is claimed.
    Partial { reason: String },
    /// A real failure. No coverage is claimed.
    Failed { reason: String },
    /// Stopped for a rate limit; retry at `wake_at`.
    RateLimited { wake_at: String, reason: String },
    /// Stopped by the caller.
    Cancelled,
}

impl WindowStatus {
    fn code(&self) -> &'static str {
        match self {
            WindowStatus::Complete => "complete",
            WindowStatus::Partial { .. } => "partial",
            WindowStatus::Failed { .. } => "failed",
            WindowStatus::RateLimited { .. } => "rate_limited",
            WindowStatus::Cancelled => "cancelled",
        }
    }

    fn reason(&self) -> Option<&str> {
        match self {
            WindowStatus::Partial { reason } | WindowStatus::Failed { reason } => Some(reason),
            WindowStatus::RateLimited { reason, .. } => Some(reason),
            _ => None,
        }
    }

    fn wake_at(&self) -> Option<&str> {
        match self {
            WindowStatus::RateLimited { wake_at, .. } => Some(wake_at),
            _ => None,
        }
    }

    /// True for a status that stops the remaining roles of the operation.
    fn stops(&self) -> bool {
        matches!(
            self,
            WindowStatus::RateLimited { .. } | WindowStatus::Cancelled
        )
    }
}

/// Result of one role of one day window.
#[derive(Debug, Clone, Serialize)]
pub struct RoleOutcome {
    pub role: CountRole,
    pub status: WindowStatus,
    /// Sum of `issueCount` over the leaf queries that answered.
    pub provider_total: i64,
    /// Pull requests served and stored.
    pub served: i64,
    /// Matches the provider would not show (SAML, IP allow list, forbidden,
    /// not found). Expected skips: counted, never stored, never failures.
    pub inaccessible: i64,
    /// Search requests issued (every attempt).
    pub calls: u64,
    /// Response bytes received.
    pub bytes: u64,
}

/// Result of [`collect_day`].
#[derive(Debug, Clone, Serialize)]
pub struct DayWindowOutcome {
    pub login: String,
    pub day: NaiveDate,
    pub roles: Vec<RoleOutcome>,
}

impl DayWindowOutcome {
    /// True when every requested role is complete.
    pub fn complete(&self) -> bool {
        self.roles
            .iter()
            .all(|r| r.status == WindowStatus::Complete)
    }

    /// The earliest wake time any role reported, for scheduling.
    pub fn wake_at(&self) -> Option<&str> {
        self.roles.iter().filter_map(|r| r.status.wake_at()).min()
    }
}

/// The UTC second range `[start, end]` covering warehouse day `day`.
pub fn day_bounds(timezone: chrono_tz::Tz, day: NaiveDate) -> Result<(i64, i64)> {
    let local_midnight = |date: NaiveDate| -> Result<i64> {
        let naive = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| Error::InvalidArgument(format!("bad day {date}")))?;
        timezone
            .from_local_datetime(&naive)
            .earliest()
            .map(|at| at.with_timezone(&Utc).timestamp())
            .ok_or_else(|| Error::InvalidArgument(format!("no local midnight on {date}")))
    };
    let start = local_midnight(day)?;
    let next = day
        .succ_opt()
        .ok_or_else(|| Error::InvalidArgument(format!("bad day {day}")))?;
    Ok((start, local_midnight(next)? - 1))
}

fn stamp(seconds: i64) -> Result<String> {
    crate::sync::search_timestamp(seconds)
}

fn now_text(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// The provider search-index lag this module allows for, and its only
/// completeness assumption beyond the provider's own answers.
///
/// * The reviewed catch-up re-walks every pull request updated in the hour
///   before the previous seal, so an update that reached the search index up
///   to an hour late is still found by the next call.
/// * A day's coverage is `settled` only when its covering fetch (or, for
///   reviews, its seal) happened at least this long after the day ended.
///   Before that, a count is "complete as of" its fetch, and the caller should
///   re-collect the day once it can settle.
///
/// An update indexed later than this is not guaranteed to be found.
pub const INDEX_LAG_ALLOWANCE_SECONDS: i64 = 60 * 60;

/// Kept for API stability: the reviewed catch-up margin, now equal to
/// [`INDEX_LAG_ALLOWANCE_SECONDS`].
pub const REVIEW_SEAL_MARGIN_SECONDS: i64 = INDEX_LAG_ALLOWANCE_SECONDS;

/// The reviewed-role frontier for one login: every review the login
/// submitted on any day in `[oldest_day, local date of sealed_at]` was
/// stored as of `sealed_at`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewFrontier {
    pub oldest_day: NaiveDate,
    pub sealed_at: DateTime<Utc>,
}

/// True when the collection tables exist (a read-only connection to a
/// warehouse never opened by this version has none).
pub fn collection_tables_present(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'
         AND name IN ('collection_coverage', 'collection_review_frontier', 'pr_detail_state',
                      'collection_review_observation')",
        [],
        |row| row.get::<_, i64>(0),
    )? == 4)
}

pub fn review_frontier(conn: &Connection, login: &str) -> Result<Option<ReviewFrontier>> {
    if !collection_tables_present(conn)? {
        return Ok(None);
    }
    let row: Option<(String, String)> = conn
        .query_row(
            "SELECT oldest_day, sealed_at FROM collection_review_frontier WHERE login = ?1",
            [login],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(day, sealed)| {
        Ok(ReviewFrontier {
            oldest_day: NaiveDate::parse_from_str(&day, "%Y-%m-%d")
                .map_err(|e| Error::InvalidArgument(format!("bad frontier day: {e}")))?,
            sealed_at: time_dimension::parse_utc(&sealed)?,
        })
    })
    .transpose()
}

/// Collect counts for one login and one warehouse-calendar day.
///
/// * **Authored / Merged** query the exact UTC second range of the local day.
///   A created or merged instant never moves, so a closed day stays complete.
/// * **Reviewed** cannot be bounded by review date: search only filters the
///   pull request's `updated` time, and a review on day D may sit on a pull
///   request last updated any time after D. The walk therefore keeps a
///   per-login frontier. Each call queries (a) a catch-up range from the last
///   seal (minus [`REVIEW_SEAL_MARGIN_SECONDS`]) to now, and (b) the gap
///   between `day` and the frontier's oldest day (or, with no frontier, from
///   `day` to now). Every pull request in those ranges is read with ALL of
///   the login's reviews (pages past the first 100 are continued), so after
///   a successful call every day from `min(day, oldest_day)` through today is
///   complete as of the new seal, provided the provider's search index lagged
///   by no more than [`INDEX_LAG_ALLOWANCE_SECONDS`]. Newest-first calls keep
///   each range to one day plus the catch-up.
///
/// Counts read by [`day_counts`] are current observations: a pull request
/// counts for a day only if the walk that completed the day served it, and a
/// review only while the walk last listed it for the login (dismissed reviews
/// are reported apart). Stored rows are never deleted.
///
/// Windows over GitHub's 1,000-result cap are bisected by seconds. Light rows
/// land in the existing fact tables through preserving upserts. Coverage is
/// recorded only for a role whose every match was served.
pub fn collect_day(
    conn: &Connection,
    client: &mut GhClient,
    login: &str,
    day: NaiveDate,
    roles: &[CountRole],
    control: &CollectionControl,
    now: DateTime<Utc>,
) -> Result<DayWindowOutcome> {
    let login = crate::query::to_bare_login(login);
    if login.is_empty() || login.contains(char::is_whitespace) {
        return Err(Error::InvalidArgument(format!("bad login '{login}'")));
    }
    crate::storage::schema::ensure_collection_tables(conn)?;
    let timezone = time_dimension::configured_timezone(conn)?;
    let today = time_dimension::local_date_for(now, timezone);
    if day > today {
        return Err(Error::InvalidArgument(format!(
            "{day} is after today ({today}) in the warehouse calendar"
        )));
    }
    client.set_cancellation(control.cancel.clone());
    client.set_defer_waits_over(control.defer_waits_over);

    let mut outcome = DayWindowOutcome {
        login: login.clone(),
        day,
        roles: Vec::new(),
    };
    let mut stopped: Option<WindowStatus> = None;
    for &role in roles {
        if let Some(status) = &stopped {
            let role_outcome = RoleOutcome {
                role,
                status: status.clone(),
                provider_total: 0,
                served: 0,
                inaccessible: 0,
                calls: 0,
                bytes: 0,
            };
            record_coverage(conn, &login, day, &role_outcome, now, today)?;
            outcome.roles.push(role_outcome);
            continue;
        }
        let calls_before = client.requests_issued();
        let bytes_before = client.bytes_received();
        let mut walk = Walk::default();
        let ranges = match role {
            CountRole::Authored | CountRole::Merged => {
                let (start, end) = day_bounds(timezone, day)?;
                vec![(start, end.min(now.timestamp()))]
            }
            CountRole::Reviewed => reviewed_ranges(conn, &login, day, timezone, now)?,
        };
        let qualifier = match role {
            CountRole::Authored => ("author", "created"),
            CountRole::Merged => ("author", "merged"),
            CountRole::Reviewed => ("reviewed-by", "updated"),
        };
        for (start, end) in ranges {
            if walk.status.is_some() {
                break;
            }
            walk_range(
                conn, client, &login, qualifier, start, end, 0, now, &mut walk,
            )?;
        }
        let status = walk.status.take().unwrap_or(WindowStatus::Complete);
        let role_outcome = RoleOutcome {
            role,
            status,
            provider_total: walk.provider_total,
            served: walk.served,
            inaccessible: walk.inaccessible,
            calls: client.requests_issued() - calls_before,
            bytes: client.bytes_received() - bytes_before,
        };
        if role == CountRole::Reviewed && role_outcome.status == WindowStatus::Complete {
            advance_review_frontier(conn, &login, day, now)?;
        }
        record_coverage(conn, &login, day, &role_outcome, now, today)?;
        if role_outcome.status.stops() {
            stopped = Some(role_outcome.status.clone());
        }
        outcome.roles.push(role_outcome);
    }
    client.set_cancellation(None);
    client.set_defer_waits_over(None);
    Ok(outcome)
}

/// The `updated:` second ranges a reviewed call for `day` must walk.
fn reviewed_ranges(
    conn: &Connection,
    login: &str,
    day: NaiveDate,
    timezone: chrono_tz::Tz,
    now: DateTime<Utc>,
) -> Result<Vec<(i64, i64)>> {
    let (day_start, _) = day_bounds(timezone, day)?;
    let now_seconds = now.timestamp();
    let Some(frontier) = review_frontier(conn, login)? else {
        return Ok(vec![(day_start, now_seconds)]);
    };
    // Invariant kept by every successful call: each pull request whose
    // current `updatedAt` lies in [start(oldest_day), sealed_at] was fetched
    // after its latest update, so it carried every review on it. A pull
    // request updated since the seal now sits in the catch-up range; one in
    // the new gap sits in the gap range; the rest are unchanged since an
    // earlier call fetched them.
    let catch_up_start =
        (frontier.sealed_at.timestamp() - REVIEW_SEAL_MARGIN_SECONDS).min(now_seconds);
    let mut ranges = vec![(catch_up_start, now_seconds)];
    if day < frontier.oldest_day {
        let (oldest_start, _) = day_bounds(timezone, frontier.oldest_day)?;
        ranges.push((day_start, (oldest_start - 1).min(catch_up_start)));
    }
    Ok(ranges)
}

fn advance_review_frontier(
    conn: &Connection,
    login: &str,
    day: NaiveDate,
    now: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO collection_review_frontier (login, oldest_day, sealed_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT (login) DO UPDATE SET
             oldest_day = MIN(collection_review_frontier.oldest_day, excluded.oldest_day),
             sealed_at = MAX(collection_review_frontier.sealed_at, excluded.sealed_at)",
        params![login, day.format("%Y-%m-%d").to_string(), now_text(now)],
    )?;
    Ok(())
}

fn record_coverage(
    conn: &Connection,
    login: &str,
    day: NaiveDate,
    outcome: &RoleOutcome,
    now: DateTime<Utc>,
    today: NaiveDate,
) -> Result<()> {
    let complete = outcome.status == WindowStatus::Complete;
    let at = now_text(now);
    conn.execute(
        "INSERT INTO collection_coverage (
             login, role, day, complete_as_of, complete_day_closed, provider_total,
             served, inaccessible, last_attempt_at, last_status, last_reason, wake_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT (login, role, day) DO UPDATE SET
             complete_as_of = COALESCE(excluded.complete_as_of, collection_coverage.complete_as_of),
             complete_day_closed = COALESCE(excluded.complete_day_closed,
                                            collection_coverage.complete_day_closed),
             provider_total = COALESCE(excluded.provider_total, collection_coverage.provider_total),
             served = COALESCE(excluded.served, collection_coverage.served),
             inaccessible = COALESCE(excluded.inaccessible, collection_coverage.inaccessible),
             last_attempt_at = excluded.last_attempt_at,
             last_status = excluded.last_status,
             last_reason = excluded.last_reason,
             wake_at = excluded.wake_at",
        params![
            login,
            outcome.role.as_str(),
            day.format("%Y-%m-%d").to_string(),
            complete.then_some(at.as_str()),
            complete.then_some((day < today) as i64),
            complete.then_some(outcome.provider_total),
            complete.then_some(outcome.served),
            complete.then_some(outcome.inaccessible),
            at,
            outcome.status.code(),
            outcome.status.reason(),
            outcome.status.wake_at(),
        ],
    )?;
    Ok(())
}

#[derive(Default)]
struct Walk {
    provider_total: i64,
    served: i64,
    inaccessible: i64,
    status: Option<WindowStatus>,
}

/// Maximum bisection depth: a day halved 17 times is under one second.
const MAX_SPLIT_DEPTH: u32 = 24;

/// Walk one `qualifier:start..end` search to exhaustion, bisecting when the
/// provider reports more matches than it will serve.
#[allow(clippy::too_many_arguments)]
fn walk_range(
    conn: &Connection,
    client: &mut GhClient,
    login: &str,
    (role_qualifier, date_qualifier): (&str, &str),
    start: i64,
    end: i64,
    depth: u32,
    now: DateTime<Utc>,
    walk: &mut Walk,
) -> Result<()> {
    if start > end {
        return Ok(());
    }
    let expression = format!(
        "is:pr {role_qualifier}:{login} {date_qualifier}:{}..{} sort:updated-desc",
        stamp(start)?,
        stamp(end)?
    );
    let query = light_search_query();
    let mut cursor: Option<String> = None;
    let mut first = true;
    let mut total = 0;
    let mut seen = 0_i64;
    loop {
        let mut variables: Vec<(&str, &str)> = vec![("q", expression.as_str()), ("login", login)];
        if let Some(after) = cursor.as_deref() {
            variables.push(("cursor", after));
        }
        let (data, errors) = match client.graphql_search_with_errors(&query, &variables) {
            Ok(answer) => answer,
            Err(error) => {
                walk.status = Some(status_for_error(client, error)?);
                return Ok(());
            }
        };
        let page = match light::parse_light_page(&data, &errors) {
            Ok(page) => page,
            Err(light::PageError::RateLimited(reason)) => {
                let wake = data
                    .get("rateLimit")
                    .and_then(|r| r.get("resetAt"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| now_text(Utc::now() + ChronoDuration::seconds(60)));
                walk.status = Some(WindowStatus::RateLimited {
                    wake_at: wake,
                    reason,
                });
                return Ok(());
            }
            Err(light::PageError::Failed(reason)) => {
                walk.status = Some(WindowStatus::Failed { reason });
                return Ok(());
            }
        };
        if first {
            first = false;
            total = page.total_count;
            if total > SEARCH_RESULT_CAP {
                if start < end && depth < MAX_SPLIT_DEPTH {
                    let middle = start + (end - start) / 2;
                    walk_range(
                        conn,
                        client,
                        login,
                        (role_qualifier, date_qualifier),
                        start,
                        middle,
                        depth + 1,
                        now,
                        walk,
                    )?;
                    if walk.status.is_none() {
                        walk_range(
                            conn,
                            client,
                            login,
                            (role_qualifier, date_qualifier),
                            middle + 1,
                            end,
                            depth + 1,
                            now,
                            walk,
                        )?;
                    }
                    return Ok(());
                }
                walk.status = Some(WindowStatus::Partial {
                    reason: format!(
                        "'{expression}' matches {total}, above the provider's {SEARCH_RESULT_CAP}-result cap, and cannot be narrowed further"
                    ),
                });
            }
            walk.provider_total += total;
        }
        seen += page.items.len() as i64 + page.inaccessible;
        walk.inaccessible += page.inaccessible;
        for item in &page.items {
            let mut item = item.clone();
            if item.login_reviews_truncated
                && let Err(error) = light::complete_login_reviews(client, &mut item, login)
            {
                let status = match status_for_error(client, error)? {
                    WindowStatus::Failed { reason } => WindowStatus::Partial {
                        reason: format!(
                            "{}#{}: reading {login}'s reviews past the first page failed: {reason}",
                            item.repository.name_with_owner, item.number
                        ),
                    },
                    other => other,
                };
                walk.status.get_or_insert(status);
            }
            store_light(conn, &item, login, now)?;
            walk.served += 1;
        }
        if !page.has_next_page || (page.items.is_empty() && page.inaccessible == 0) {
            break;
        }
        cursor = page.end_cursor;
        if cursor.is_none() {
            break;
        }
    }
    if seen != total && walk.status.is_none() {
        walk.status = Some(WindowStatus::Partial {
            reason: format!("'{expression}': provider reported {total} matches but served {seen}"),
        });
    }
    Ok(())
}

/// Translate a client error into a window status. Database errors propagate.
fn status_for_error(client: &GhClient, error: Error) -> Result<WindowStatus> {
    Ok(match error {
        Error::Cancelled => WindowStatus::Cancelled,
        Error::RateLimited {
            wake_at, reason, ..
        } => WindowStatus::RateLimited {
            wake_at: now_text(wake_at),
            reason,
        },
        Error::QuotaReserve(reason) => WindowStatus::RateLimited {
            wake_at: client
                .observed_quota("graphql")
                .and_then(|(_, reset)| DateTime::from_timestamp(reset, 0))
                .map(now_text)
                .unwrap_or_else(|| now_text(Utc::now() + ChronoDuration::seconds(60))),
            reason,
        },
        error @ (Error::Database(_) | Error::Migration(_) | Error::SchemaIntegrity(_)) => {
            return Err(error);
        }
        other => WindowStatus::Failed {
            reason: other.to_string(),
        },
    })
}

fn resolve(conn: &Connection, actor: Option<&ActorReference>, suffix: &str) -> Result<String> {
    match actor {
        Some(actor) => repository::ensure_entity(conn, actor, suffix),
        None => repository::ensure_ghost_entity(conn),
    }
}

/// Land one light node through preserving upserts, in one savepoint.
fn store_light(
    conn: &Connection,
    item: &LightPullRequest,
    login: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    conn.execute_batch("SAVEPOINT githubdw_light")?;
    let result = store_light_inner(conn, item, login, now);
    match result {
        Ok(()) => conn.execute_batch("RELEASE SAVEPOINT githubdw_light")?,
        Err(_) => conn.execute_batch(
            "ROLLBACK TO SAVEPOINT githubdw_light; RELEASE SAVEPOINT githubdw_light",
        )?,
    }
    result
}

fn store_light_inner(
    conn: &Connection,
    item: &LightPullRequest,
    login: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let observed_at = now_text(now);
    let timezone = time_dimension::configured_timezone(conn)?;
    let core_hours = time_dimension::configured_core_hours(conn);
    let suffix = repository::bot_login_suffix(conn);
    let meta = &item.repository;
    let repo_key = repository::upsert_repository(
        conn,
        &meta.name_with_owner,
        meta.primary_language.as_deref(),
        meta.is_fork,
        meta.is_private,
        meta.default_branch.as_deref(),
        meta.created_at.as_deref(),
    )?;
    let pr_key = format!("{repo_key}#{}", item.number);
    let author_key = resolve(conn, item.author.as_ref(), &suffix)?;
    let merged_by_key = item
        .merged_by
        .as_ref()
        .map(|actor| repository::ensure_entity(conn, actor, &suffix))
        .transpose()?;
    let keys = |ts: &str| time_dimension::ensure_keys_for_timestamp(conn, ts, timezone, core_hours);
    let created = keys(&item.created_at)?;
    let updated = item.updated_at.as_deref().map(keys).transpose()?;
    let merged = item.merged_at.as_deref().map(keys).transpose()?;
    repository::upsert_pull_request_preserving(
        conn,
        &PullRequestRow {
            pr_key: &pr_key,
            number: item.number,
            repo_key: &repo_key,
            author_key: &author_key,
            state: &item.state,
            is_draft: item.is_draft,
            title: item.title.as_deref(),
            body: item.body.as_deref(),
            base_ref: item.base_ref.as_deref(),
            head_ref: item.head_ref.as_deref(),
            created_at: &item.created_at,
            updated_at: item.updated_at.as_deref(),
            merged_at: item.merged_at.as_deref(),
            closed_at: item.closed_at.as_deref(),
            merged_by_key: merged_by_key.as_deref(),
            created_date_key: &created.date_key,
            created_time_key: &created.time_key,
            updated_date_key: updated.as_ref().map(|k| k.date_key.as_str()),
            updated_time_key: updated.as_ref().map(|k| k.time_key.as_str()),
            merged_date_key: merged.as_ref().map(|k| k.date_key.as_str()),
            // Only written for a new row; see CounterSource::Light.
            comment_count: item.comment_total,
            review_count: item.review_total,
            changed_files: item.changed_files,
            additions: item.additions,
            deletions: item.deletions,
        },
        CounterSource::Light,
    )?;
    let mut seen: Vec<String> = Vec::new();
    for review in &item.login_reviews {
        let Some(submitted_at) = review.submitted_at.as_deref() else {
            continue;
        };
        let reviewer_key = resolve(conn, review.author.as_ref(), &suffix)?;
        conn.execute(
            "INSERT INTO collection_review_observation
                 (review_key, pr_key, reviewer_key, present, observed_at)
             VALUES (?1, ?2, ?3, 1, ?4)
             ON CONFLICT (review_key) DO UPDATE SET
                 pr_key = excluded.pr_key, reviewer_key = excluded.reviewer_key,
                 present = 1, observed_at = excluded.observed_at",
            params![review.id, pr_key, reviewer_key, observed_at],
        )?;
        seen.push(review.id.clone());
        let submitted = keys(submitted_at)?;
        repository::upsert_review_preserving(
            conn,
            &review.id,
            &pr_key,
            &reviewer_key,
            &review.state,
            None,
            submitted_at,
            &submitted.date_key,
            &submitted.time_key,
        )?;
    }
    // The login's complete review list on this pull request was read, so any
    // stored review by the login that the provider no longer lists (deleted,
    // or re-attributed to another account) is observed absent. Its fact row
    // stays; counts stop reading it.
    if !item.login_reviews_truncated {
        let login_key = format!("user:{login}");
        conn.execute(
            "INSERT INTO collection_review_observation
                 (review_key, pr_key, reviewer_key, present, observed_at)
             SELECT review_key, pr_key, reviewer_key, 0, ?3 FROM fact_reviews
             WHERE pr_key = ?1 AND reviewer_key = ?2
               AND review_key NOT IN (SELECT value FROM json_each(?4))
             ON CONFLICT (review_key) DO UPDATE SET
                 present = 0, observed_at = excluded.observed_at",
            params![
                pr_key,
                login_key,
                observed_at,
                serde_json::to_string(&seen)?
            ],
        )?;
    }
    conn.execute(
        "INSERT INTO pr_detail_state (pr_key, light_seen_at, base_oid, head_oid)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (pr_key) DO UPDATE SET
             light_seen_at = MAX(COALESCE(pr_detail_state.light_seen_at, ''), excluded.light_seen_at),
             base_oid = COALESCE(excluded.base_oid, pr_detail_state.base_oid),
             head_oid = COALESCE(excluded.head_oid, pr_detail_state.head_oid)",
        params![pr_key, observed_at, item.base_oid, item.head_oid],
    )?;
    Ok(())
}

/// Record that a full detail fetch landed (called by the sync store path).
pub(crate) fn mark_detail_fetched(
    conn: &Connection,
    pr_key: &str,
    pull_request: &PullRequestData,
    diff_stored: bool,
) -> Result<()> {
    crate::storage::schema::ensure_collection_tables(conn)?;
    let at = now_text(Utc::now());
    conn.execute(
        "INSERT INTO pr_detail_state (pr_key, detail_fetched_at, diff_stored_at, base_oid, head_oid)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (pr_key) DO UPDATE SET
             detail_fetched_at = excluded.detail_fetched_at,
             diff_stored_at = COALESCE(excluded.diff_stored_at, pr_detail_state.diff_stored_at),
             base_oid = COALESCE(excluded.base_oid, pr_detail_state.base_oid),
             head_oid = COALESCE(excluded.head_oid, pr_detail_state.head_oid)",
        params![
            pr_key,
            at,
            diff_stored.then_some(at.as_str()),
            pull_request.base_oid,
            pull_request.head_oid.as_deref().or(pull_request.head_sha.as_deref()),
        ],
    )?;
    Ok(())
}

// ---- Coverage-aware counts ------------------------------------------

/// Coverage of one role on one day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoleCoverage {
    /// True when the count is known (a complete fetch covers the day).
    pub known: bool,
    /// When the covering fetch completed. For reviews, the frontier seal.
    pub complete_as_of: Option<String>,
    /// True when the day had ended before the covering fetch.
    pub day_closed: Option<bool>,
    /// True when the covering fetch (for reviews, the seal) came at least
    /// [`INDEX_LAG_ALLOWANCE_SECONDS`] after the day ended, so late search
    /// indexing cannot still add to it. A known but unsettled count is
    /// "complete as of `complete_as_of`"; re-collect the day to settle it.
    pub settled: Option<bool>,
    pub inaccessible: Option<i64>,
    pub last_status: Option<String>,
    pub last_reason: Option<String>,
    pub wake_at: Option<String>,
}

/// What a [`DayCount`] covers, for display next to the numbers.
pub const COUNT_SCOPE: &str = "Pull requests and reviews that GitHub search attributes to this \
login when collected, in repositories the collecting account can see. Work done under a \
former login of a renamed account, or by a deleted account (shown by GitHub as ghost), is \
outside this scope. Inaccessible matches are counted in `inaccessible`, not in the totals.";

/// Counts for one login on one day. `None` = not collected; never a fake 0.
/// See [`COUNT_SCOPE`] for what is and is not covered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DayCount {
    pub login: String,
    pub day: NaiveDate,
    pub authored: Option<i64>,
    pub merged: Option<i64>,
    /// Reviews submitted (each review event counts once).
    pub reviews: Option<i64>,
    /// Distinct pull requests reviewed that day (not counting dismissed).
    pub reviewed_prs: Option<i64>,
    /// Reviews submitted that day and since dismissed. Not in `reviews`.
    pub reviews_dismissed: Option<i64>,
    /// Stored reviews the provider no longer lists for this login (deleted
    /// or re-attributed). Kept in the warehouse; never counted.
    pub reviews_absent: Option<i64>,
    pub authored_coverage: RoleCoverage,
    pub merged_coverage: RoleCoverage,
    pub reviewed_coverage: RoleCoverage,
    /// Informational: a legacy `synced_ranges` claim covers this day for the
    /// user. NOT used to mark any count known.
    pub legacy_range_claim: bool,
}

/// Run a per-day `(day, count)` query bound to `?1 key, ?2 start, ?3 end,
/// ?4 login`. Returns nothing when the collection tables are absent.
fn grouped(
    conn: &Connection,
    tables: bool,
    sql: &str,
    (key, start, end, login): (&str, &str, &str, &str),
) -> Result<std::collections::HashMap<String, i64>> {
    if !tables {
        return Ok(Default::default());
    }
    let mut statement = conn.prepare_cached(sql)?;
    let rows = statement.query_map(params![key, start, end, login], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Per-day counts for `logins` over `[start, end]` (inclusive, warehouse
/// calendar). Only coverage tables and indexed fact columns are read; no
/// payload column is touched.
pub fn day_counts(
    conn: &Connection,
    logins: &[&str],
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<DayCount>> {
    if start > end {
        return Ok(Vec::new());
    }
    let start_text = start.format("%Y-%m-%d").to_string();
    let end_text = end.format("%Y-%m-%d").to_string();
    let timezone = time_dimension::configured_timezone(conn)?;
    let tables = collection_tables_present(conn)?;
    let mut out = Vec::new();
    for raw in logins {
        let login = crate::query::to_bare_login(raw);
        let key = format!("user:{login}");
        let bind = (
            key.as_str(),
            start_text.as_str(),
            end_text.as_str(),
            login.as_str(),
        );
        // A pull request counts only when the walk that completed its day saw
        // it then or later (`light_seen_at >= complete_as_of`). A stored row
        // the provider no longer serves (deleted, or no longer visible) does
        // not count toward a day the provider was re-asked about.
        let authored = grouped(
            conn,
            tables,
            "SELECT p.created_date_key, COUNT(*) FROM fact_pull_requests p
             JOIN pr_detail_state s ON s.pr_key = p.pr_key
             JOIN collection_coverage c ON c.login = ?4 AND c.role = 'authored'
                  AND c.day = p.created_date_key
             WHERE p.author_key = ?1 AND p.created_date_key BETWEEN ?2 AND ?3
               AND s.light_seen_at >= c.complete_as_of
             GROUP BY p.created_date_key",
            bind,
        )?;
        let merged = grouped(
            conn,
            tables,
            "SELECT p.merged_date_key, COUNT(*) FROM fact_pull_requests p
             JOIN pr_detail_state s ON s.pr_key = p.pr_key
             JOIN collection_coverage c ON c.login = ?4 AND c.role = 'merged'
                  AND c.day = p.merged_date_key
             WHERE p.author_key = ?1 AND p.state = 'MERGED'
               AND p.merged_date_key BETWEEN ?2 AND ?3
               AND s.light_seen_at >= c.complete_as_of
             GROUP BY p.merged_date_key",
            bind,
        )?;
        // Reviews count only while the walk last observed them as present for
        // this login. Dismissed reviews are reported separately.
        let review_sql = |filter: &str| {
            format!(
                "SELECT r.submitted_date_key, {{AGG}} FROM fact_reviews r
                 JOIN collection_review_observation o ON o.review_key = r.review_key
                 WHERE r.reviewer_key = ?1 AND r.submitted_date_key BETWEEN ?2 AND ?3
                   AND length(?4) > 0 AND {filter}
                 GROUP BY r.submitted_date_key"
            )
        };
        let current = review_sql("o.present = 1 AND r.state <> 'DISMISSED'");
        let reviews = grouped(conn, tables, &current.replace("{AGG}", "COUNT(*)"), bind)?;
        let reviewed_prs = grouped(
            conn,
            tables,
            &current.replace("{AGG}", "COUNT(DISTINCT r.pr_key)"),
            bind,
        )?;
        let dismissed = grouped(
            conn,
            tables,
            &review_sql("o.present = 1 AND r.state = 'DISMISSED'").replace("{AGG}", "COUNT(*)"),
            bind,
        )?;
        let absent = grouped(
            conn,
            tables,
            &review_sql("o.present = 0").replace("{AGG}", "COUNT(*)"),
            bind,
        )?;
        let mut coverage: std::collections::HashMap<(String, String), RoleCoverage> =
            Default::default();
        if tables {
            let mut statement = conn.prepare_cached(
                "SELECT role, day, complete_as_of, complete_day_closed, inaccessible,
                        last_status, last_reason, wake_at
                 FROM collection_coverage WHERE login = ?1 AND day BETWEEN ?2 AND ?3",
            )?;
            let rows = statement.query_map(params![login, start_text, end_text], |row| {
                let complete: Option<String> = row.get(2)?;
                Ok((
                    (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                    RoleCoverage {
                        known: complete.is_some(),
                        complete_as_of: complete,
                        day_closed: row.get::<_, Option<i64>>(3)?.map(|v| v != 0),
                        settled: None,
                        inaccessible: row.get(4)?,
                        last_status: row.get(5)?,
                        last_reason: row.get(6)?,
                        wake_at: row.get(7)?,
                    },
                ))
            })?;
            for row in rows {
                let (key, value) = row?;
                coverage.insert(key, value);
            }
        }
        let frontier = review_frontier(conn, &login)?;
        let legacy = crate::storage::sync_state_repository::coverage_extent(conn, &key)?;
        let mut day = start;
        while day <= end {
            let text = day.format("%Y-%m-%d").to_string();
            let role_cov = |role: &str| {
                coverage
                    .get(&(role.to_string(), text.clone()))
                    .cloned()
                    .unwrap_or(RoleCoverage {
                        known: false,
                        complete_as_of: None,
                        day_closed: None,
                        settled: None,
                        inaccessible: None,
                        last_status: None,
                        last_reason: None,
                        wake_at: None,
                    })
            };
            let settle_after = day_bounds(timezone, day)?.1 + 1 + INDEX_LAG_ALLOWANCE_SECONDS;
            let settle = |mut coverage: RoleCoverage| -> RoleCoverage {
                coverage.settled = coverage.complete_as_of.as_deref().map(|at| {
                    time_dimension::parse_utc(at).is_ok_and(|at| at.timestamp() >= settle_after)
                });
                coverage
            };
            let authored_coverage = settle(role_cov("authored"));
            let merged_coverage = settle(role_cov("merged"));
            let mut reviewed_coverage = role_cov("reviewed");
            // Reviews are known only inside the sealed frontier.
            let sealed = frontier.as_ref().is_some_and(|f| {
                f.oldest_day <= day && day <= time_dimension::local_date_for(f.sealed_at, timezone)
            });
            reviewed_coverage.known = sealed;
            if let Some(f) = frontier.as_ref().filter(|_| sealed) {
                reviewed_coverage.complete_as_of = Some(now_text(f.sealed_at));
                reviewed_coverage.day_closed =
                    Some(day < time_dimension::local_date_for(f.sealed_at, timezone));
            }
            let reviewed_coverage = settle(reviewed_coverage);
            let pick = |known: bool, map: &std::collections::HashMap<String, i64>| {
                known.then(|| map.get(&text).copied().unwrap_or(0))
            };
            out.push(DayCount {
                login: login.clone(),
                day,
                authored: pick(authored_coverage.known, &authored),
                merged: pick(merged_coverage.known, &merged),
                reviews: pick(reviewed_coverage.known, &reviews),
                reviewed_prs: pick(reviewed_coverage.known, &reviewed_prs),
                reviews_dismissed: pick(reviewed_coverage.known, &dismissed),
                reviews_absent: pick(reviewed_coverage.known, &absent),
                legacy_range_claim: legacy.as_ref().is_some_and(|(a, b)| {
                    a.as_str() <= text.as_str() && text.as_str() <= b.as_str()
                }),
                authored_coverage,
                merged_coverage,
                reviewed_coverage,
            });
            day = day.succ_opt().unwrap_or(day);
            if day == end && end == NaiveDate::MAX {
                break;
            }
        }
    }
    Ok(out)
}

// ---- Detail on demand ------------------------------------------------

/// Result of [`fetch_pull_request_detail`].
#[derive(Debug, Clone, Serialize)]
pub struct DetailOutcome {
    pub outcome: crate::refresh::TargetOutcome,
    /// The retention the write boundary actually allowed.
    pub allowed: DiffRetention,
    /// True when new patch text was written by this call.
    pub diff_stored: bool,
    /// Revision identity (`github:v1:<repo>#<n>:<base>..<head>`), if known.
    pub revision: Option<String>,
}

/// Fetch one pull request in full (reviews with bodies, review threads,
/// conversation comments, file list, checks) and store it with preserving
/// writes. New patch text is fetched and written only when `requested` is
/// `Retain` AND the policy at `source` allows it at the write boundary.
pub fn fetch_pull_request_detail(
    conn: &Connection,
    client: &mut GhClient,
    repository: &str,
    number: i64,
    requested: DiffRetention,
    source: PolicySource,
    control: &CollectionControl,
) -> Result<DetailOutcome> {
    let target = crate::refresh::RefreshTarget {
        kind: crate::refresh::TargetKind::PullRequest,
        repository: repository.to_lowercase(),
        number,
    };
    client.set_cancellation(control.cancel.clone());
    client.set_defer_waits_over(control.defer_waits_over);
    let pr_key = format!("{}#{number}", target.repository);
    crate::storage::schema::ensure_collection_tables(conn)?;
    let before: Option<String> = conn
        .query_row(
            "SELECT diff_stored_at FROM pr_detail_state WHERE pr_key = ?1",
            [&pr_key],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    // Detail writes always preserve: this is an app-level primitive, so a
    // missing policy file must not re-enable legacy blanking or pruning.
    let source = always_app_mode(source);
    let allowed = crate::retention::RetentionGate::new(requested, source.clone())
        .decide()
        .allowed;
    let report = {
        let mut syncer = crate::sync::Syncer::new(conn, client)
            .with_diff_retention(requested)
            .with_policy_source(source);
        syncer.refresh_targets(
            std::slice::from_ref(&target),
            &crate::refresh::RefreshOptions {
                skip_diffs: requested != DiffRetention::Retain,
            },
        )
    };
    client.set_cancellation(None);
    client.set_defer_waits_over(None);
    let report = report?;
    let after: Option<(Option<String>, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT diff_stored_at, base_oid, head_oid FROM pr_detail_state WHERE pr_key = ?1",
            [&pr_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (stored_at, base, head) = after.unwrap_or((None, None, None));
    let outcome = report
        .outcomes
        .into_iter()
        .next()
        .ok_or_else(|| Error::GitHubApi("refresh returned no outcome".into()))?;
    Ok(DetailOutcome {
        outcome,
        allowed,
        diff_stored: stored_at.is_some() && stored_at != before,
        revision: revision_identity(&target.repository, number, base.as_deref(), head.as_deref()),
    })
}

/// Wrap a source so a missing file reads as app mode with the caller's
/// request as the ceiling (preserving writes, no pruning).
fn always_app_mode(source: PolicySource) -> PolicySource {
    PolicySource::Custom(Arc::new(move || match source.current() {
        crate::retention::PolicyState::Missing => {
            crate::retention::PolicyState::Present(crate::retention::PolicyFile {
                version: 1,
                mode: crate::retention::CollectionMode::Full,
                ai: false,
                diff_retention: DiffRetention::Retain,
                updated_at: None,
            })
        }
        other => other,
    }))
}

/// `github:v1:<repo>#<n>:<base>..<head>`, or `None` without both oids.
pub fn revision_identity(
    repository: &str,
    number: i64,
    base: Option<&str>,
    head: Option<&str>,
) -> Option<String> {
    Some(format!(
        "github:v1:{}#{number}:{}..{}",
        repository.to_lowercase(),
        base?,
        head?
    ))
}

/// One file of a live diff. Held in memory only.
#[derive(Clone, Serialize)]
pub struct LiveFile {
    pub path: String,
    pub previous_path: Option<String>,
    pub patch: Option<String>,
}

impl std::fmt::Debug for LiveFile {
    // Never print diff text, even in a debug log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveFile")
            .field("path", &self.path)
            .field("previous_path", &self.previous_path)
            .field("patch_bytes", &self.patch.as_ref().map(String::len))
            .finish()
    }
}

/// A diff fetched for viewing or AI. Nothing here is written anywhere.
#[derive(Debug, Clone, Serialize)]
pub struct LiveDiff {
    pub repository: String,
    pub number: i64,
    pub base_oid: String,
    pub head_oid: String,
    pub files: Vec<LiveFile>,
    /// True when GitHub's 3,000-file REST ceiling cut the listing.
    pub provider_capped: bool,
    /// `revision_identity` plus `:sha256=<hex>` over the served file list and
    /// patches, so a summary can name the exact content it saw.
    pub fingerprint: String,
}

const REVISION_QUERY: &str = r#"
query($owner: String!, $name: String!, $number: Int!) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) { baseRefOid headRefOid }
  }
}
"#;

fn revision_pair(client: &mut GhClient, repository: &str, number: i64) -> Result<(String, String)> {
    let (owner, name) = repository
        .split_once('/')
        .ok_or_else(|| Error::InvalidArgument(format!("expected owner/name, got {repository}")))?;
    let number_text = number.to_string();
    let data = client.graphql(
        REVISION_QUERY,
        &[
            ("owner", owner),
            ("name", name),
            ("number", number_text.as_str()),
        ],
    )?;
    let pull = data
        .get("repository")
        .and_then(|r| r.get("pullRequest"))
        .filter(|p| !p.is_null())
        .ok_or_else(|| Error::NotFound(format!("{repository}#{number}")))?;
    let field = |name: &str| {
        pull.get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| Error::GitHubApi(format!("{repository}#{number} has no {name}")))
    };
    Ok((field("baseRefOid")?, field("headRefOid")?))
}

/// Fetch a pull request's diff into memory. Never written to SQLite, logs,
/// temporary files, or durable retry state, in any mode. The revision pair is
/// read before and after the file walk; if the head moved in between the call
/// fails rather than return a diff that matches no single revision.
pub fn fetch_pull_request_diff_live(
    client: &mut GhClient,
    repository: &str,
    number: i64,
    control: &CollectionControl,
) -> Result<LiveDiff> {
    let repository = repository.to_lowercase();
    client.set_cancellation(control.cancel.clone());
    client.set_defer_waits_over(control.defer_waits_over);
    let result = (|| {
        let (base, head) = revision_pair(client, &repository, number)?;
        let listing = pull_requests::fetch_file_patches(client, &repository, number)?;
        let (base_after, head_after) = revision_pair(client, &repository, number)?;
        if (base_after.as_str(), head_after.as_str()) != (base.as_str(), head.as_str()) {
            return Err(Error::GitHubApi(format!(
                "{repository}#{number} moved from {base}..{head} to {base_after}..{head_after} during the diff fetch; retry"
            )));
        }
        let files: Vec<LiveFile> = listing
            .files
            .into_iter()
            .map(|(path, previous_path, patch)| LiveFile {
                path,
                previous_path,
                patch,
            })
            .collect();
        let fingerprint = format!(
            "{}:sha256={}",
            revision_identity(&repository, number, Some(&base), Some(&head)).unwrap_or_default(),
            content_digest(&files)
        );
        Ok(LiveDiff {
            repository: repository.clone(),
            number,
            base_oid: base,
            head_oid: head,
            files,
            provider_capped: listing.provider_capped,
            fingerprint,
        })
    })();
    client.set_cancellation(None);
    client.set_defer_waits_over(None);
    result
}

/// SHA-256 over the file list in path order: path, previous path, patch,
/// each length-prefixed so no two lists collide by concatenation.
pub fn content_digest(files: &[LiveFile]) -> String {
    let mut sorted: Vec<&LiveFile> = files.iter().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    let mut hasher = Sha256::new();
    for file in sorted {
        for part in [
            Some(file.path.as_str()),
            file.previous_path.as_deref(),
            file.patch.as_deref(),
        ] {
            match part {
                Some(text) => {
                    hasher.update((text.len() as u64 + 1).to_be_bytes());
                    hasher.update(text.as_bytes());
                }
                None => hasher.update(0_u64.to_be_bytes()),
            }
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// One stored file row of a pull request.
#[derive(Clone, Serialize)]
pub struct StoredFileDiff {
    pub path: String,
    pub previous_path: Option<String>,
    pub change_type: String,
    pub additions: i64,
    pub deletions: i64,
    pub patch: Option<String>,
}

impl std::fmt::Debug for StoredFileDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredFileDiff")
            .field("path", &self.path)
            .field("patch_bytes", &self.patch.as_ref().map(String::len))
            .finish()
    }
}

/// Read stored file rows (with any stored patch text) for `pr_key`. Works in
/// every mode: retention only governs new writes.
pub fn stored_pull_request_diff(conn: &Connection, pr_key: &str) -> Result<Vec<StoredFileDiff>> {
    let mut statement = conn.prepare(
        "SELECT file_path, previous_path, change_type, additions, deletions, patch
         FROM fact_file_diffs WHERE pr_key = ?1 ORDER BY file_path",
    )?;
    let rows = statement.query_map([pr_key.to_lowercase()], |row| {
        Ok(StoredFileDiff {
            path: row.get(0)?,
            previous_path: row.get(1)?,
            change_type: row.get(2)?,
            additions: row.get(3)?,
            deletions: row.get(4)?,
            patch: row.get(5)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Per pull request stage state, for the integrator's detail scheduling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DetailState {
    pub pr_key: String,
    pub light_seen_at: Option<String>,
    pub detail_fetched_at: Option<String>,
    pub diff_stored_at: Option<String>,
    pub revision: Option<String>,
}

pub fn detail_state(conn: &Connection, pr_key: &str) -> Result<Option<DetailState>> {
    let pr_key = pr_key.to_lowercase();
    if !collection_tables_present(conn)? {
        return Ok(None);
    }
    Ok(conn
        .query_row(
            "SELECT light_seen_at, detail_fetched_at, diff_stored_at, base_oid, head_oid
             FROM pr_detail_state WHERE pr_key = ?1",
            [&pr_key],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()?
        .map(|(light, detail, diff, base, head)| {
            let (repository, number) = pr_key.rsplit_once('#').unwrap_or((pr_key.as_str(), "0"));
            DetailState {
                revision: revision_identity(
                    repository,
                    number.parse().unwrap_or(0),
                    base.as_deref(),
                    head.as_deref(),
                ),
                pr_key: pr_key.clone(),
                light_seen_at: light,
                detail_fetched_at: detail,
                diff_stored_at: diff,
            }
        }))
}
