//! One shared collection pass for every process that keeps the warehouse
//! fresh: the CLI's `githubdw sync all` and any application linking the
//! crate call [`tick`], and whichever runs second does nothing.
//!
//! A tick has two lanes, each behind its own lease in `githubdw.db`:
//!
//! * **Detect** (every [`TickPolicy::detect_every`]). One light search per
//!   role (`author:` and `reviewed-by:`) names every monitored login at once
//!   and asks which pull requests changed since the shared cursor, for about
//!   1 point each. One aliased query reads every monitored repository's
//!   newest update. Only pull requests that are new, newer than the stored
//!   copy, stored incomplete, or still waiting on check runs are fetched in
//!   full, by node id. Once a day the same walk covers the last 48 hours
//!   (search-index lag), every monitored login is checked to still resolve,
//!   and repositories GitHub refused are retried.
//! * **Backfill**. Owed catch-ups first (people whose 0.5 walks stopped
//!   short), then history back to [`TickPolicy::history_from`], one created
//!   month at a time, only while the hourly budget used is below
//!   [`TickPolicy::backfill_share`] and for at most
//!   [`TickPolicy::backfill_points`] per tick. Months already covered are
//!   skipped, so a backfill an older version started resumes.
//!
//! A tick never sleeps through a long rate-limit wait while holding a lease:
//! a wait longer than [`MAX_WAIT_INSIDE_LEASE`] ends the tick with
//! [`TickOutcome::CapPaused`] and its wake time, and the next tick resumes.
//!
//! Everything here is additive in the warehouse ([`crate::storage::schema::
//! ensure_collector_tables`]), and the per-user `sync_metadata` rows,
//! the `sync_all` row and `synced_ranges` that older readers use are kept
//! up to date.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;

use crate::collection::CollectionControl;
use crate::error::{Error, Result};
use crate::fetch::GhClient;
use crate::fetch::git_files::GitRecoveryConfig;
use crate::fetch::user_search::UserRole;
use crate::retention::{DiffRetention, PolicySource};
use crate::storage::{monitor_repository, sync_state_repository as sync_state, time_dimension};
use crate::sync::{SyncOptions, SyncSummary, Syncer};

mod detect;
mod lease;
mod queue;
mod state;
#[cfg(test)]
mod tests;

pub use lease::{LeaseHolder, lease_holder};
pub use queue::{FetchSummary, PARK_AFTER_ATTEMPTS, ParkedPullRequest, parked};
pub use state::{
    BlockedSource, CollectorStatus, UserFreshness, WarehouseFreshness, blocked_sources,
    collector_status, unresolved_logins, user_freshness, warehouse_freshness,
};

/// The longest rate-limit wait a tick sleeps through while holding a lease.
/// Longer waits end the tick ([`TickOutcome::CapPaused`]) so the lease is
/// released, never left to expire under a sleeping holder.
pub const MAX_WAIT_INSIDE_LEASE: std::time::Duration = std::time::Duration::from_secs(4 * 60);

/// How long a lease lasts without renewal. Holders renew after every request.
const LEASE_TTL_MINUTES: i64 = 15;

/// Lane names in `collector_leases`.
pub const DETECT_LANE: &str = "detect";
pub const BACKFILL_LANE: &str = "backfill";

/// Logins per batched search. 1,320 were measured to work; 200 keeps each
/// query well short of any length limit.
const LOGINS_PER_SEARCH: usize = 200;

/// Window before the first detection, for a login with old per-user state.
const WALK_MARGIN_HOURS: i64 = 2;

/// History a new login or repository gets when no `history_from` is set,
/// matching the 0.5 walks' first window.
const DEFAULT_HISTORY_DAYS: i64 = 90;

/// What a tick may do and how.
#[derive(Clone)]
pub struct TickPolicy {
    /// Who is ticking, shown to other processes (`Busy`, "checked by").
    pub program: String,
    /// Detection runs when the last complete one is older than this.
    pub detect_every: Duration,
    /// How far before the cursor each detection re-reads, for search-index lag.
    pub overlap: Duration,
    /// How often the wide reconcile walk, login check and refused-source
    /// retry run.
    pub reconcile_every: Duration,
    /// How far back the reconcile walk reads.
    pub reconcile_window: Duration,
    /// Oldest created date wanted for each monitored login. Remembered per
    /// login (the earliest date any caller asked for wins), so a backfill
    /// one process asked for continues in another. `None` asks for nothing
    /// beyond each login's existing coverage, or 90 days for a new login.
    pub history_from: Option<NaiveDate>,
    /// Share of the hourly budget this tick's client may use (`None` keeps
    /// the client's own cap).
    pub max_rate_use: Option<f64>,
    /// Backfill runs only while the most used budget reading is below this
    /// share of the hour.
    pub backfill_share: f64,
    /// Most GraphQL points one tick spends on backfill.
    pub backfill_points: i64,
    /// Skip the per-file patch text (fast mode).
    pub skip_diffs: bool,
    /// Explicit diff-retention request, as [`Syncer::with_diff_retention`].
    pub diff_retention: Option<DiffRetention>,
    /// Collection policy source, as [`Syncer::with_policy_source`].
    pub policy_source: Option<PolicySource>,
    /// Complete capped file listings from git, as [`Syncer::with_git_recovery`].
    pub git_recovery: Option<GitRecoveryConfig>,
    /// Also detect changes in monitored repositories.
    pub repositories: bool,
    /// Run the wide reconcile now, whatever its last run.
    pub force_reconcile: bool,
}

impl std::fmt::Debug for TickPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TickPolicy")
            .field("program", &self.program)
            .field("detect_every", &self.detect_every)
            .field("history_from", &self.history_from)
            .field("max_rate_use", &self.max_rate_use)
            .field("backfill_share", &self.backfill_share)
            .field("backfill_points", &self.backfill_points)
            .finish_non_exhaustive()
    }
}

impl Default for TickPolicy {
    fn default() -> Self {
        Self {
            program: "githubdw".into(),
            detect_every: Duration::minutes(30),
            overlap: Duration::hours(1),
            reconcile_every: Duration::hours(24),
            reconcile_window: Duration::hours(48),
            history_from: None,
            max_rate_use: None,
            backfill_share: 0.5,
            backfill_points: 1_000,
            skip_diffs: false,
            diff_retention: None,
            policy_source: None,
            git_recovery: None,
            repositories: true,
            force_reconcile: false,
        }
    }
}

/// How the detect lane of a tick ended.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TickOutcome {
    /// The last complete detection is younger than `detect_every`; nothing
    /// was asked of GitHub for it.
    UpToDate {
        last_detect_at: DateTime<Utc>,
        by: Option<String>,
    },
    /// Another process holds the detect lease. Not a failure.
    Busy { holder: LeaseHolder },
    /// Detection ran and saw everything: the cursor moved.
    Detected,
    /// Detection ran but GitHub did not serve every window; the cursor did
    /// not move and the next tick repeats the walk.
    Incomplete { gaps: Vec<String> },
    /// Stopped at the budget cap; the next tick resumes after `wake_at`.
    CapPaused {
        wake_at: DateTime<Utc>,
        reason: String,
    },
    /// The caller cancelled or paused the tick.
    Stopped,
    /// `gh` is missing or signed out.
    SignIn { reason: String },
}

/// GraphQL points one tick spent, by phase, summed from GitHub's own
/// `rateLimit.cost` answers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PhasePoints {
    pub detect: i64,
    pub fetch: i64,
    pub reconcile: i64,
    pub logins: i64,
    pub backfill: i64,
}

impl PhasePoints {
    pub fn total(&self) -> i64 {
        self.detect + self.fetch + self.reconcile + self.logins + self.backfill
    }
}

/// What the backfill lane did.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct BackfillSummary {
    /// Why it did not run, when it did not.
    pub skipped: Option<String>,
    pub catch_ups_finished: Vec<String>,
    /// `login created-month` windows completed and recorded as covered.
    pub windows_covered: Vec<String>,
    pub fetch: FetchSummary,
    /// Logins whose backfill is still owed after this tick.
    pub remaining: usize,
}

/// Everything one tick did.
#[derive(Debug, Clone, Serialize)]
pub struct TickReport {
    pub outcome: TickOutcome,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub points: PhasePoints,
    /// REST calls (patch text, file listings), against the core budget.
    pub rest_calls: u64,
    /// Pull requests detection returned, and how many needed a full fetch.
    pub detected: i64,
    pub queued: i64,
    pub fetch: FetchSummary,
    pub backfill: BackfillSummary,
    /// Repositories GitHub refuses (SAML, forbidden), retried once a day.
    pub blocked: Vec<BlockedSource>,
    /// Pull requests parked after repeated failures, retried once a day.
    pub parked: Vec<ParkedPullRequest>,
    /// Monitored logins GitHub no longer resolves (renamed or deleted).
    pub unresolved_logins: Vec<String>,
    /// True once a day: the lists above are due to be shown to a person.
    pub report_due: bool,
    pub notes: Vec<String>,
}

impl TickReport {
    /// One line for a log: `tick: 3 points (detect 3, fetch 0, ...)`.
    pub fn cost_line(&self) -> String {
        let p = &self.points;
        format!(
            "tick: {} points (detect {}, fetch {}, reconcile {}, logins {}, backfill {}); {} REST calls",
            p.total(),
            p.detect,
            p.fetch,
            p.reconcile,
            p.logins,
            p.backfill,
            self.rest_calls
        )
    }
}

fn text(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_time(raw: &str) -> Option<DateTime<Utc>> {
    time_dimension::parse_utc(raw).ok()
}

/// Settles what the client and control flags mean for one tick, and puts
/// the client back as it was afterwards.
struct ClientScope {
    cap: Option<f64>,
}

impl ClientScope {
    fn enter(
        client: &mut GhClient,
        policy: &TickPolicy,
        control: &CollectionControl,
    ) -> Result<Self> {
        let cap = client.max_rate_use();
        if let Some(share) = policy.max_rate_use {
            client.set_max_rate_use(Some(share))?;
        }
        client.set_cancellation(control.cancel.clone());
        let limit = control
            .defer_waits_over
            .map_or(MAX_WAIT_INSIDE_LEASE, |d| d.min(MAX_WAIT_INSIDE_LEASE));
        client.set_defer_waits_over(Some(limit));
        Ok(Self { cap })
    }

    fn leave(self, client: &mut GhClient) {
        let _ = client.set_max_rate_use(self.cap);
        client.set_cancellation(None);
        client.set_defer_waits_over(None);
    }
}

fn stopped(control: &CollectionControl) -> bool {
    let set =
        |flag: &Option<Arc<AtomicBool>>| flag.as_ref().is_some_and(|f| f.load(Ordering::SeqCst));
    set(&control.cancel) || set(&control.pause)
}

/// Run one collection tick. See the module documentation.
///
/// Returns `Err` only for warehouse (SQLite) failures; every GitHub outcome
/// is in the report.
pub fn tick(
    conn: &Connection,
    client: &mut GhClient,
    policy: &TickPolicy,
    control: &CollectionControl,
) -> Result<TickReport> {
    crate::storage::schema::ensure_collection_tables(conn)?;
    crate::storage::schema::ensure_collector_tables(conn)?;
    let started_at = Utc::now();
    let rest_before = client.rest_requests_issued();
    let scope = ClientScope::enter(client, policy, control)?;
    let mut report = TickReport {
        outcome: TickOutcome::Stopped,
        started_at,
        finished_at: started_at,
        points: PhasePoints::default(),
        rest_calls: 0,
        detected: 0,
        queued: 0,
        fetch: FetchSummary::default(),
        backfill: BackfillSummary::default(),
        blocked: Vec::new(),
        parked: Vec::new(),
        unresolved_logins: Vec::new(),
        report_due: false,
        notes: Vec::new(),
    };
    let result = run(conn, client, policy, control, &mut report);
    scope.leave(client);
    result?;
    report.rest_calls = client.rest_requests_issued() - rest_before;
    report.finished_at = Utc::now();
    report.blocked = blocked_sources(conn)?;
    report.parked = parked(conn)?;
    report.unresolved_logins = unresolved_logins(conn)?;
    let last_report: Option<String> = conn
        .query_row(
            "SELECT last_report_at FROM collector_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    report.report_due = last_report
        .as_deref()
        .and_then(parse_time)
        .is_none_or(|at| report.finished_at - at >= Duration::hours(24));
    conn.execute(
        "INSERT INTO collector_state (id, last_tick_at, last_tick_by, last_tick_points,
                                      last_tick_summary, last_report_at)
         VALUES (1, ?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (id) DO UPDATE SET
             last_tick_at = excluded.last_tick_at, last_tick_by = excluded.last_tick_by,
             last_tick_points = excluded.last_tick_points,
             last_tick_summary = excluded.last_tick_summary,
             last_report_at = COALESCE(excluded.last_report_at, collector_state.last_report_at)",
        params![
            text(report.finished_at),
            policy.program,
            report.points.total(),
            report.cost_line(),
            report.report_due.then(|| text(report.finished_at)),
        ],
    )?;
    Ok(report)
}

/// Map an error that ends a lane to its outcome, or pass a warehouse error up.
fn lane_outcome(error: Error) -> Result<TickOutcome> {
    Ok(match error {
        Error::Cancelled => TickOutcome::Stopped,
        Error::RateLimited {
            wake_at, reason, ..
        } => TickOutcome::CapPaused { wake_at, reason },
        Error::QuotaReserve(reason) => TickOutcome::CapPaused {
            wake_at: Utc::now() + Duration::minutes(5),
            reason,
        },
        Error::GhUnavailable(reason) => TickOutcome::SignIn { reason },
        error @ (Error::Database(_)
        | Error::Migration(_)
        | Error::SchemaIntegrity(_)
        | Error::Io(_)) => {
            return Err(error);
        }
        Error::GhCommand { stderr, .. }
            if {
                let m = stderr.to_lowercase();
                m.contains("gh auth login")
                    || m.contains("not logged")
                    || m.contains("bad credentials")
            } =>
        {
            TickOutcome::SignIn { reason: stderr }
        }
        other => TickOutcome::Incomplete {
            gaps: vec![other.to_string()],
        },
    })
}

fn new_syncer<'a>(
    conn: &'a Connection,
    client: &'a mut GhClient,
    policy: &TickPolicy,
) -> Syncer<'a> {
    let mut syncer = Syncer::new(conn, client);
    if let Some(retention) = policy.diff_retention {
        syncer = syncer.with_diff_retention(retention);
    }
    if let Some(source) = policy.policy_source.clone() {
        syncer = syncer.with_policy_source(source);
    }
    if let Some(config) = policy.git_recovery.clone() {
        syncer = syncer.with_git_recovery(config);
    }
    syncer
}

fn sync_options(policy: &TickPolicy) -> SyncOptions {
    SyncOptions {
        skip_diffs: policy.skip_diffs,
        pull_requests_only: true,
        ..Default::default()
    }
}

fn run(
    conn: &Connection,
    client: &mut GhClient,
    policy: &TickPolicy,
    control: &CollectionControl,
    report: &mut TickReport,
) -> Result<()> {
    let members = register_members(conn, policy, &mut report.notes)?;
    report.outcome = detect_lane(conn, client, policy, control, &members, report)?;
    if !matches!(
        report.outcome,
        TickOutcome::SignIn { .. } | TickOutcome::Stopped
    ) && !matches!(report.outcome, TickOutcome::CapPaused { .. })
    {
        backfill_lane(conn, client, policy, control, report)?;
    }
    Ok(())
}

// ---- Members ------------------------------------------------------------

/// The enabled monitored logins, registered in `collector_members`. A login
/// seen for the first time is given its catch-up from older per-user state,
/// and the first registration ever seeds the shared cursor.
fn register_members(
    conn: &Connection,
    policy: &TickPolicy,
    notes: &mut Vec<String>,
) -> Result<Vec<String>> {
    let mut logins: Vec<String> = monitor_repository::list(conn)?
        .into_iter()
        .filter(|s| s.source_type == "user" && s.sync_enabled)
        .map(|s| crate::query::to_bare_login(&s.identifier))
        .collect();
    logins.sort();
    logins.dedup();
    let wanted: HashSet<&str> = logins.iter().map(String::as_str).collect();
    let known: Vec<String> = {
        let mut statement = conn.prepare("SELECT login FROM collector_members")?;
        let rows = statement.query_map([], |r| r.get(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    for login in &known {
        if !wanted.contains(login.as_str()) {
            conn.execute("DELETE FROM collector_members WHERE login = ?1", [login])?;
        }
    }
    let now = Utc::now();
    let cursor = state::detect_cursor(conn)?;
    let mut first_run_seed = None;
    if cursor.is_none() {
        first_run_seed = Some(seed_cursor(conn, &logins, now)?);
        conn.execute(
            "INSERT INTO collector_state (id, detect_cursor) VALUES (1, ?1)
             ON CONFLICT (id) DO UPDATE SET detect_cursor = excluded.detect_cursor",
            [text(first_run_seed.unwrap_or(now))],
        )?;
        let imported = queue::import_job_failures(conn)?;
        if imported > 0 {
            notes.push(format!(
                "queued {imported} pull request(s) older walks failed"
            ));
        }
    }
    let cursor = cursor.or(first_run_seed).unwrap_or(now);
    let known: HashSet<String> = known.into_iter().collect();
    for login in &logins {
        if !known.contains(login) {
            let (author, review) = owed_catch_up(conn, login, cursor)?;
            // A login with no coverage at all gets the 0.5 walks' first
            // window, remembered so later ticks keep walking back to it.
            let default_history = sync_state::coverage_extent(conn, &format!("user:{login}"))?
                .is_none()
                .then(|| {
                    time_dimension::today_as_of(conn, now).map(|today| {
                        (today - Duration::days(DEFAULT_HISTORY_DAYS))
                            .format("%Y-%m-%d")
                            .to_string()
                    })
                })
                .transpose()?;
            conn.execute(
                "INSERT INTO collector_members
                     (login, joined_at, catch_up_author, catch_up_review, history_from)
                 VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT (login) DO NOTHING",
                params![
                    login,
                    text(now),
                    author.map(text),
                    review.map(text),
                    default_history
                ],
            )?;
        }
        if let Some(from) = policy.history_from {
            conn.execute(
                "UPDATE collector_members SET history_from = ?2
                 WHERE login = ?1 AND (history_from IS NULL OR history_from > ?2)",
                params![login, from.format("%Y-%m-%d").to_string()],
            )?;
        }
        // A 0.5 incremental catch-up left mid-way is finished by this
        // collector's own catch-up from the same base.
        if let Some(base) = pending_update_checkpoint(conn, login)? {
            conn.execute(
                "UPDATE collector_members SET catch_up_review = ?2
                 WHERE login = ?1 AND (catch_up_review IS NULL OR catch_up_review > ?2)",
                params![login, text(base)],
            )?;
        }
    }
    Ok(logins)
}

/// The base of a 0.5 `user_update_checkpoint` for `reviewed-by:login`.
fn pending_update_checkpoint(conn: &Connection, login: &str) -> Result<Option<DateTime<Utc>>> {
    let raw = sync_state::load_checkpoint(
        conn,
        "user_update_checkpoint",
        &format!("{}:{login}", UserRole::Reviewer.qualifier()),
    )?;
    Ok(raw
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|v| v.get("base").and_then(Value::as_str).and_then(parse_time)))
}

/// When `login`'s last complete 0.5 walk of `role` finished, as recorded in
/// `sync_metadata`, moved back by [`WALK_MARGIN_HOURS`] for the walk's own
/// length.
fn last_walk(conn: &Connection, login: &str, role: UserRole) -> Result<Option<DateTime<Utc>>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT last_sync_at FROM sync_metadata WHERE source_type = ?1 AND source_id = ?2",
            params![role.cursor_source_type(), login],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    Ok(raw
        .as_deref()
        .and_then(parse_time)
        .map(|at| at - Duration::hours(WALK_MARGIN_HOURS)))
}

/// The shared cursor before the first detection: the oldest recent walk of
/// any member (walks older than the reconcile window become per-login
/// catch-ups instead of dragging everyone back).
fn seed_cursor(conn: &Connection, logins: &[String], now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let recent = now - Duration::hours(48);
    let mut seed = now;
    for login in logins {
        for role in UserRole::all() {
            if let Some(at) = last_walk(conn, login, role)?
                && at >= recent
            {
                seed = seed.min(at);
            }
        }
    }
    Ok(seed)
}

/// When a catch-up for one role starts, if one is owed.
type Owing = Option<DateTime<Utc>>;

/// Catch-ups owed to a login joining the detector, per role: from its last
/// 0.5 walk when that is older than the shared cursor, or from the end of
/// its coverage when it has coverage but no walk record. A login with
/// neither is new and is served by the backfill lane.
fn owed_catch_up(conn: &Connection, login: &str, cursor: DateTime<Utc>) -> Result<(Owing, Owing)> {
    let coverage_end = sync_state::coverage_extent(conn, &format!("user:{login}"))?
        .and_then(|(_, end)| NaiveDate::parse_from_str(&end, "%Y-%m-%d").ok())
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc());
    let owed = |role| -> Result<Option<DateTime<Utc>>> {
        Ok(match last_walk(conn, login, role)? {
            Some(at) if at >= cursor => None,
            Some(at) => Some(at),
            None => coverage_end.filter(|end| *end < cursor),
        })
    };
    Ok((owed(UserRole::Author)?, owed(UserRole::Reviewer)?))
}

// ---- Detect lane ----------------------------------------------------------

fn detect_lane(
    conn: &Connection,
    client: &mut GhClient,
    policy: &TickPolicy,
    control: &CollectionControl,
    members: &[String],
    report: &mut TickReport,
) -> Result<TickOutcome> {
    let now = Utc::now();
    let status = state::raw_state(conn)?;
    if let Some(at) = status.last_detect_at
        && now - at < policy.detect_every
        && !policy.force_reconcile
    {
        return Ok(TickOutcome::UpToDate {
            last_detect_at: at,
            by: status.last_detect_by,
        });
    }
    let lease = match lease::Lease::acquire(
        conn,
        DETECT_LANE,
        &policy.program,
        Duration::minutes(LEASE_TTL_MINUTES),
    )? {
        Ok(lease) => lease,
        Err(holder) => return Ok(TickOutcome::Busy { holder }),
    };
    let mut syncer = new_syncer(conn, client, policy);
    let outcome = match detect(
        &mut syncer,
        policy,
        control,
        members,
        &lease,
        &status,
        report,
    ) {
        Ok(outcome) => outcome,
        Err(error) => lane_outcome(error)?,
    };
    lease.release()?;
    Ok(outcome)
}

/// Points spent by `syncer`'s client so far.
fn points(syncer: &mut Syncer<'_>) -> i64 {
    syncer.client_mut().graphql_points_spent()
}

#[allow(clippy::too_many_arguments)]
fn detect(
    syncer: &mut Syncer<'_>,
    policy: &TickPolicy,
    control: &CollectionControl,
    members: &[String],
    lease: &lease::Lease<'_>,
    status: &state::RawState,
    report: &mut TickReport,
) -> Result<TickOutcome> {
    let conn = syncer.connection();
    let bound = Utc::now();
    let cursor = status.detect_cursor.unwrap_or(bound);
    let lower = (cursor - policy.overlap).min(bound);
    let reconcile_due = policy.force_reconcile
        || status
            .last_reconcile_at
            .is_none_or(|at| bound - at >= policy.reconcile_every);
    let mut renew = || -> Result<()> {
        if stopped(control) {
            return Err(Error::Cancelled);
        }
        if !lease.renew()? {
            return Err(Error::GitHubApi("the detect lease was taken over".into()));
        }
        Ok(())
    };
    let mut gaps = Vec::new();
    let mut found = Vec::new();

    // Daily: every monitored login still resolves.
    if reconcile_due {
        let before = points(syncer);
        check_logins(syncer, members, &mut renew)?;
        report.points.logins += points(syncer) - before;
    }

    // People: one light search per role per batch of logins.
    let before = points(syncer);
    let searchable: Vec<&String> = members.iter().filter(|l| detect::valid_login(l)).collect();
    for chunk in searchable.chunks(LOGINS_PER_SEARCH) {
        for role in UserRole::all() {
            let base = batched_expression(role, chunk);
            let walk = detect::enumerate(
                syncer.client_mut(),
                &base,
                "updated",
                lower.timestamp(),
                bound.timestamp(),
                &mut renew,
            )?;
            gaps.extend(walk.gaps);
            found.extend(walk.items);
        }
    }
    let mut people_gaps = gaps.len();
    // Repositories: one aliased head read, then light listings of the ones
    // that moved.
    if policy.repositories {
        repositories(
            syncer,
            policy,
            lower,
            reconcile_due,
            &mut found,
            &mut gaps,
            &mut renew,
        )?;
    }
    // Check runs that finished after the pull request's last update.
    let window = if reconcile_due {
        Duration::days(30)
    } else {
        Duration::hours(6)
    };
    report.queued += queue::probe_unfinished_checks(syncer, bound - window, 300, &mut renew)?;
    report.points.detect += points(syncer) - before;

    // Daily: the same walk over the 48 hours before this one (index lag).
    if reconcile_due {
        let before = points(syncer);
        let start = (bound - policy.reconcile_window).min(lower);
        for chunk in searchable.chunks(LOGINS_PER_SEARCH) {
            for role in UserRole::all() {
                let walk = detect::enumerate(
                    syncer.client_mut(),
                    &batched_expression(role, chunk),
                    "updated",
                    start.timestamp(),
                    lower.timestamp() - 1,
                    &mut renew,
                )?;
                people_gaps += walk.gaps.len();
                gaps.extend(walk.gaps);
                found.extend(walk.items);
            }
        }
        report.points.reconcile += points(syncer) - before;
    }

    report.detected += found.len() as i64;
    report.queued += queue::enqueue_changed(conn, &found)?;

    // Full fetch of everything queued (new changes, earlier failures).
    let before = points(syncer);
    let options = sync_options(policy);
    let fetch = queue::drain(syncer, &options, &mut renew, &|| stopped(control), None)?;
    report.points.fetch += points(syncer) - before;
    report.fetch = fetch;

    if people_gaps > 0 {
        return Ok(TickOutcome::Incomplete { gaps });
    }
    // People were seen in full: the shared cursor moves even when a
    // repository failed, so one broken repository cannot hold everyone back.
    stamp_detection(
        conn,
        policy,
        members,
        bound,
        lower,
        reconcile_due,
        gaps.is_empty(),
    )?;
    if !gaps.is_empty() {
        return Ok(TickOutcome::Incomplete { gaps });
    }
    Ok(TickOutcome::Detected)
}

fn batched_expression(role: UserRole, logins: &[&String]) -> String {
    let mut expression = String::from("is:pr");
    for login in logins {
        expression.push_str(&format!(" {}:{login}", role.qualifier()));
    }
    expression
}

/// Record a complete detection: the cursor, freshness, and the per-user
/// rows older readers use.
///
/// `complete` is false when only the people were seen in full (a repository
/// failed): the cursor and the people's rows move, but `last_detect_at` does
/// not, so the next tick detects again instead of reporting up to date.
#[allow(clippy::too_many_arguments)]
fn stamp_detection(
    conn: &Connection,
    policy: &TickPolicy,
    members: &[String],
    bound: DateTime<Utc>,
    lower: DateTime<Utc>,
    reconcile_done: bool,
    complete: bool,
) -> Result<()> {
    let transaction = conn.unchecked_transaction()?;
    let at = text(bound);
    transaction.execute(
        "INSERT INTO collector_state (id, detect_cursor) VALUES (1, ?1)
         ON CONFLICT (id) DO UPDATE SET
             detect_cursor = MAX(COALESCE(collector_state.detect_cursor, ''), excluded.detect_cursor)",
        [&at],
    )?;
    if complete {
        transaction.execute(
            "UPDATE collector_state SET last_detect_at = ?1, last_detect_by = ?2 WHERE id = 1",
            params![text(Utc::now()), policy.program],
        )?;
        sync_state::save_checkpoint(&transaction, "sync_all", "last_detect", &at)?;
    }
    if reconcile_done {
        transaction.execute(
            "UPDATE collector_state SET last_reconcile_at = ?1 WHERE id = 1",
            [text(Utc::now())],
        )?;
    }
    // Older readers: each person's own rows.
    let owed: HashSet<String> = {
        let mut statement = transaction.prepare(
            "SELECT login FROM collector_members
             WHERE catch_up_author IS NOT NULL OR catch_up_review IS NOT NULL
                OR unresolved_since IS NOT NULL",
        )?;
        let rows = statement.query_map([], |r| r.get(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    for login in members.iter().filter(|l| !owed.contains(*l)) {
        stamp_user(&transaction, login, bound, lower)?;
    }
    transaction.commit()?;
    Ok(())
}

/// Mark `login` current through `bound`: its `user`/`user_reviews` rows, the
/// monitored row, and its coverage when that reaches the detected window.
fn stamp_user(
    conn: &Connection,
    login: &str,
    bound: DateTime<Utc>,
    lower: DateTime<Utc>,
) -> Result<()> {
    for role in UserRole::all() {
        sync_state::advance_cursor(conn, role.cursor_source_type(), login, Some(&text(bound)))?;
    }
    monitor_repository::touch_user(conn, login)?;
    extend_coverage(conn, &format!("user:{login}"), lower)
}

/// Extend an entity's coverage through yesterday when its coverage already
/// reaches the start of a complete detection window.
fn extend_coverage(conn: &Connection, entity_key: &str, lower: DateTime<Utc>) -> Result<()> {
    let Some((_, end)) = sync_state::coverage_extent(conn, entity_key)? else {
        return Ok(());
    };
    let Ok(end) = NaiveDate::parse_from_str(&end, "%Y-%m-%d") else {
        return Ok(());
    };
    // A day of slack for the warehouse calendar being behind UTC.
    if end < lower.date_naive() - Duration::days(1) {
        return Ok(());
    }
    let today = time_dimension::today_as_of(conn, Utc::now())?;
    sync_state::record_range(
        conn,
        entity_key,
        &end.format("%Y-%m-%d").to_string(),
        &(today - Duration::days(1)).format("%Y-%m-%d").to_string(),
        0,
    )
}

/// Check every monitored login still resolves (one aliased `user(login:)`
/// per login, about 1 point per 100), and record the ones that do not.
fn check_logins(
    syncer: &mut Syncer<'_>,
    members: &[String],
    renew: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let conn = syncer.connection();
    let now = text(Utc::now());
    let valid: Vec<&String> = members.iter().filter(|l| detect::valid_login(l)).collect();
    for login in members.iter().filter(|l| !detect::valid_login(l)) {
        conn.execute(
            "UPDATE collector_members SET unresolved_since = COALESCE(unresolved_since, ?2)
             WHERE login = ?1",
            params![login, now],
        )?;
    }
    for chunk in valid.chunks(100) {
        let selections: Vec<String> = chunk
            .iter()
            .map(|login| format!("user(login: \"{login}\") {{ login }}"))
            .collect();
        let answers = detect::aliased(syncer.client_mut(), &selections)?;
        renew()?;
        for (login, (value, _)) in chunk.iter().zip(answers) {
            let resolves = value
                .as_ref()
                .and_then(|v| v.get("login"))
                .and_then(Value::as_str)
                .is_some_and(|found| found.eq_ignore_ascii_case(login));
            if resolves {
                conn.execute(
                    "UPDATE collector_members SET unresolved_since = NULL WHERE login = ?1",
                    [login],
                )?;
            } else {
                conn.execute(
                    "UPDATE collector_members SET unresolved_since = COALESCE(unresolved_since, ?2)
                     WHERE login = ?1",
                    params![login, now],
                )?;
            }
        }
    }
    Ok(())
}

const REPO_HEADS: &str = "pullRequests(first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { nodes { updatedAt } } \
     issues(first: 1, orderBy: {field: UPDATED_AT, direction: DESC}) { nodes { updatedAt } }";

const REPO_LISTING: &str = r#"
query($owner: String!, $name: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    nameWithOwner
    pullRequests(first: 100, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes { id number updatedAt }
    }
  }
}
"#;

/// Detect changes in monitored repositories: one aliased read of every
/// repository's newest pull request and issue update, then a light listing
/// down to the stored cursor for the ones that moved. Repositories GitHub
/// refuses are put on a daily retry instead of being asked every pass.
#[allow(clippy::too_many_arguments)]
fn repositories(
    syncer: &mut Syncer<'_>,
    policy: &TickPolicy,
    lower: DateTime<Utc>,
    reconcile_due: bool,
    found: &mut Vec<detect::Found>,
    gaps: &mut Vec<String>,
    renew: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let conn = syncer.connection();
    let now = Utc::now();
    let mut repos: Vec<String> = monitor_repository::list(conn)?
        .into_iter()
        .filter(|s| s.source_type == "repo" && s.sync_enabled)
        .map(|s| s.identifier.to_lowercase())
        .collect();
    let held_back: HashSet<String> = state::backed_off(conn, now)?;
    repos.retain(|r| reconcile_due || !held_back.contains(r));
    for repo in repos.iter().filter(|r| !detect::valid_repository(r)) {
        gaps.push(format!("{repo}: not a valid owner/name"));
    }
    repos.retain(|r| detect::valid_repository(r));
    let mut heads: BTreeMap<String, (Option<String>, Option<String>)> = BTreeMap::new();
    for chunk in repos.chunks(queue::ALIAS_BATCH) {
        let selections: Vec<String> = chunk
            .iter()
            .map(|repo| {
                let (owner, name) = repo.split_once('/').unwrap_or_default();
                format!("repository(owner: \"{owner}\", name: \"{name}\") {{ {REPO_HEADS} }}")
            })
            .collect();
        let answers = detect::aliased(syncer.client_mut(), &selections)?;
        renew()?;
        for (repo, (value, error)) in chunk.iter().zip(answers) {
            match (value, error) {
                (Some(value), _) => {
                    state::clear_backoff(conn, repo)?;
                    heads.insert(
                        repo.clone(),
                        (
                            detect::newest_updated(value.get("pullRequests")),
                            detect::newest_updated(value.get("issues")),
                        ),
                    );
                }
                (None, Some(error)) if crate::fetch::is_inaccessible(&error) => {
                    let reason = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("GitHub refused this repository");
                    state::back_off(conn, repo, reason, now)?;
                }
                (None, error) => gaps.push(format!(
                    "{repo}: {}",
                    error
                        .as_ref()
                        .and_then(|e| e.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("no answer")
                )),
            }
        }
    }
    for (repo, (pr_head, issue_head)) in heads {
        let pr_cursor = sync_state::last_updated_cursor(conn, "repo", &repo)?;
        let issue_cursor = sync_state::last_updated_cursor(conn, "repo_issues", &repo)?;
        let moved = |head: &Option<String>, cursor: &Option<String>| match (head, cursor) {
            (Some(head), Some(cursor)) => parse_time(head)
                .zip(parse_time(cursor))
                .is_none_or(|(h, c)| h > c),
            (Some(_), None) => true,
            (None, _) => false,
        };
        let mut complete = true;
        if moved(&pr_head, &pr_cursor) {
            let stop = pr_cursor
                .as_deref()
                .and_then(parse_time)
                .map(|c| c - policy.overlap)
                .unwrap_or(now - Duration::days(DEFAULT_HISTORY_DAYS));
            match list_repository(syncer, &repo, stop, renew)? {
                Ok((items, newest)) => {
                    found.extend(items);
                    sync_state::advance_cursor(conn, "repo", &repo, newest.as_deref())?;
                }
                Err(reason) => {
                    complete = false;
                    gaps.push(format!("{repo}: {reason}"));
                }
            }
        }
        if moved(&issue_head, &issue_cursor) {
            let (owner, name) = repo.split_once('/').unwrap_or_default();
            let entity_key = format!("repo:{repo}");
            let mut summary = SyncSummary::default();
            let walked = match sync_state::SyncLease::acquire(conn, &entity_key) {
                Ok(lease) => {
                    let walked = syncer.sync_issues(
                        owner,
                        name,
                        &repo,
                        &entity_key,
                        issue_cursor.as_deref(),
                        &mut summary,
                    );
                    lease.release()?;
                    walked
                }
                // Another sync is walking this repository right now.
                Err(Error::SyncInProgress(_)) => Ok(()),
                Err(error) => Err(error),
            };
            match walked {
                Ok(()) if summary.failed.is_empty() => {}
                Ok(()) => {
                    complete = false;
                    gaps.push(format!("{repo}: {} issue(s) failed", summary.failed.len()));
                }
                Err(error) if error.is_run_fatal() => return Err(error),
                Err(error) => {
                    complete = false;
                    gaps.push(format!("{repo} issues: {error}"));
                }
            }
            renew()?;
        }
        if complete {
            monitor_repository::touch_repo(conn, &repo)?;
            extend_coverage(conn, &format!("repo:{repo}"), lower)?;
        }
    }
    Ok(())
}

/// Light listing of a repository's pull requests, newest update first, down
/// to `stop`. Returns the items and the newest `updatedAt` seen, or why the
/// listing could not finish.
#[allow(clippy::type_complexity)]
fn list_repository(
    syncer: &mut Syncer<'_>,
    repo: &str,
    stop: DateTime<Utc>,
    renew: &mut dyn FnMut() -> Result<()>,
) -> Result<std::result::Result<(Vec<detect::Found>, Option<String>), String>> {
    let (owner, name) = repo.split_once('/').unwrap_or_default();
    let mut cursor: Option<String> = None;
    let mut items = Vec::new();
    let mut newest: Option<String> = None;
    loop {
        let mut variables = vec![("owner", owner), ("name", name)];
        if let Some(after) = cursor.as_deref() {
            variables.push(("cursor", after));
        }
        let data = match syncer.client_mut().graphql(REPO_LISTING, &variables) {
            Ok(data) => data,
            Err(error) if error.is_run_fatal() => return Err(error),
            Err(error) => return Ok(Err(error.to_string())),
        };
        renew()?;
        let Some(repository) = data.get("repository").filter(|v| !v.is_null()) else {
            return Ok(Err("repository missing from the listing".into()));
        };
        let key = repository
            .get("nameWithOwner")
            .and_then(Value::as_str)
            .unwrap_or(repo)
            .to_lowercase();
        let connection = &repository["pullRequests"];
        let nodes = connection["nodes"].as_array().cloned().unwrap_or_default();
        let mut reached_stop = false;
        for node in &nodes {
            let (Some(id), Some(number), Some(updated)) = (
                node["id"].as_str(),
                node["number"].as_i64(),
                node["updatedAt"].as_str(),
            ) else {
                return Ok(Err("listing node is missing id, number or updatedAt".into()));
            };
            if newest.is_none() {
                newest = Some(updated.to_string());
            }
            if parse_time(updated).is_some_and(|at| at < stop) {
                reached_stop = true;
                break;
            }
            items.push(detect::Found {
                pr_key: format!("{key}#{number}"),
                node_id: id.to_string(),
                updated_at: updated.to_string(),
            });
        }
        let has_next = connection["pageInfo"]["hasNextPage"]
            .as_bool()
            .unwrap_or(false);
        let next = connection["pageInfo"]["endCursor"]
            .as_str()
            .map(str::to_string);
        if reached_stop || !has_next || nodes.is_empty() {
            return Ok(Ok((items, newest)));
        }
        if next.is_none() || next == cursor {
            return Ok(Err("listing reports another page but no new cursor".into()));
        }
        cursor = next;
    }
}

// ---- Backfill lane --------------------------------------------------------

fn backfill_lane(
    conn: &Connection,
    client: &mut GhClient,
    policy: &TickPolicy,
    control: &CollectionControl,
    report: &mut TickReport,
) -> Result<()> {
    let Some(cursor) = state::raw_state(conn)?
        .last_detect_at
        .and(state::detect_cursor(conn)?)
    else {
        report.backfill.skipped = Some("waiting for a first complete detection".into());
        return Ok(());
    };
    let work = backfill_work(conn)?;
    report.backfill.remaining = work.len();
    if work.is_empty() {
        return Ok(());
    }
    if stopped(control) {
        report.backfill.skipped = Some("stopped".into());
        return Ok(());
    }
    match client.used_share("graphql") {
        Some(share) if share >= policy.backfill_share => {
            report.backfill.skipped = Some(format!(
                "{:.0}% of the hourly GitHub budget is used; backfill waits below {:.0}%",
                share * 100.0,
                policy.backfill_share * 100.0
            ));
            return Ok(());
        }
        _ => {}
    }
    let lease = match lease::Lease::acquire(
        conn,
        BACKFILL_LANE,
        &policy.program,
        Duration::minutes(LEASE_TTL_MINUTES),
    )? {
        Ok(lease) => lease,
        Err(holder) => {
            report.backfill.skipped = Some(format!(
                "{} (pid {}) is backfilling",
                holder.program, holder.pid
            ));
            return Ok(());
        }
    };
    let start_points = client.graphql_points_spent();
    let mut syncer = new_syncer(conn, client, policy);
    let result = backfill(
        &mut syncer,
        policy,
        control,
        &lease,
        cursor,
        &work,
        start_points,
        report,
    );
    report.points.backfill += syncer.client_mut().graphql_points_spent() - start_points;
    lease.release()?;
    match result {
        Ok(()) => {}
        Err(error) => match lane_outcome(error)? {
            TickOutcome::CapPaused { wake_at, .. } => {
                report.backfill.skipped =
                    Some(format!("paused at the budget cap until {}", text(wake_at)));
            }
            TickOutcome::Stopped => report.backfill.skipped = Some("stopped".into()),
            other => report.notes.push(format!("backfill: {other:?}")),
        },
    }
    report.backfill.remaining = backfill_work(conn)?.len();
    Ok(())
}

/// The backfill lane's share of one tick.
struct Budget {
    start_points: i64,
    points: i64,
    share: f64,
}

impl Budget {
    fn spent(&self, syncer: &mut Syncer<'_>) -> bool {
        let client = syncer.client_mut();
        client.graphql_points_spent() - self.start_points >= self.points
            || client
                .used_share("graphql")
                .is_some_and(|s| s >= self.share)
    }
}

/// One login's owed work.
#[derive(Debug, Clone)]
struct Owed {
    login: String,
    catch_up_author: Option<DateTime<Utc>>,
    catch_up_review: Option<DateTime<Utc>>,
    /// Oldest created date wanted, when months are still uncovered.
    history: Option<NaiveDate>,
}

fn backfill_work(conn: &Connection) -> Result<Vec<Owed>> {
    let mut statement = conn.prepare(
        "SELECT login, catch_up_author, catch_up_review, history_from FROM collector_members
         WHERE unresolved_since IS NULL ORDER BY login",
    )?;
    type Row = (String, Option<String>, Option<String>, Option<String>);
    let rows: Vec<Row> = statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let now = Utc::now();
    let today = time_dimension::today_as_of(conn, now)?;
    let yesterday = today - Duration::days(1);
    let mut work = Vec::new();
    for (login, author, review, history) in rows {
        let key = format!("user:{login}");
        let coverage = sync_state::coverage_extent(conn, &key)?;
        let target = match history
            .as_deref()
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
        {
            Some(from) => Some(from),
            None if coverage.is_none() => Some(today - Duration::days(DEFAULT_HISTORY_DAYS)),
            None => None,
        };

        let history = target.filter(|from| {
            crate::sync::month_windows(*from, yesterday)
                .into_iter()
                .any(|(s, e)| !sync_state::covers_as_of(conn, &key, s, e, now).unwrap_or(false))
        });
        let owed = Owed {
            login,
            catch_up_author: author.as_deref().and_then(parse_time),
            catch_up_review: review.as_deref().and_then(parse_time),
            history,
        };
        if owed.catch_up_author.is_some()
            || owed.catch_up_review.is_some()
            || owed.history.is_some()
        {
            work.push(owed);
        }
    }
    // Catch-ups first: they are freshness gaps, history is not.
    work.sort_by_key(|o| {
        (
            o.catch_up_author.is_none() && o.catch_up_review.is_none(),
            o.login.clone(),
        )
    });
    Ok(work)
}

#[allow(clippy::too_many_arguments)]
fn backfill(
    syncer: &mut Syncer<'_>,
    policy: &TickPolicy,
    control: &CollectionControl,
    lease: &lease::Lease<'_>,
    cursor: DateTime<Utc>,
    work: &[Owed],
    start_points: i64,
    report: &mut TickReport,
) -> Result<()> {
    let conn = syncer.connection();
    let options = sync_options(policy);
    let mut renew = || -> Result<()> {
        if stopped(control) {
            return Err(Error::Cancelled);
        }
        if !lease.renew()? {
            return Err(Error::GitHubApi("the backfill lease was taken over".into()));
        }
        Ok(())
    };
    let budget = Budget {
        start_points,
        points: policy.backfill_points,
        share: policy.backfill_share,
    };
    for owed in work {
        if budget.spent(syncer) {
            report.backfill.skipped = Some("this tick's backfill share is spent".into());
            return Ok(());
        }
        let lock = match sync_state::SyncLease::acquire(conn, &format!("user:{}", owed.login)) {
            Ok(lock) => lock,
            // Someone else is walking this person; try the next one.
            Err(Error::SyncInProgress(_)) => continue,
            Err(error) => return Err(error),
        };
        let outcome = backfill_one(syncer, &options, owed, cursor, &mut renew, &budget, report);
        lock.release()?;
        outcome?;
    }
    Ok(())
}

/// Fetch every changed pull request in `found` and say whether all of them
/// landed (or are parked, a gap reported daily).
fn land_all(
    syncer: &mut Syncer<'_>,
    options: &SyncOptions,
    found: &[detect::Found],
    renew: &mut dyn FnMut() -> Result<()>,
    report: &mut TickReport,
) -> Result<bool> {
    let conn = syncer.connection();
    let mut keys = HashSet::new();
    for item in found {
        if !queue::stored_complete(conn, item)? {
            queue::enqueue(conn, item)?;
            keys.insert(item.pr_key.clone());
        }
    }
    if keys.is_empty() {
        return Ok(true);
    }
    let fetched = queue::drain(syncer, options, renew, &|| false, Some(&keys))?;
    let summary = &mut report.backfill.fetch;
    summary.landed += fetched.landed;
    summary.failed += fetched.failed;
    summary.deferred += fetched.deferred;
    summary.gone += fetched.gone;
    summary.newly_parked += fetched.newly_parked;
    let mut open = 0_i64;
    for key in &keys {
        open += conn.query_row(
            "SELECT COUNT(*) FROM collector_pr_queue WHERE pr_key = ?1 AND parked_at IS NULL",
            [key],
            |r| r.get::<_, i64>(0),
        )?;
    }
    Ok(open == 0)
}

fn backfill_one(
    syncer: &mut Syncer<'_>,
    options: &SyncOptions,
    owed: &Owed,
    cursor: DateTime<Utc>,
    renew: &mut dyn FnMut() -> Result<()>,
    budget: &Budget,
    report: &mut TickReport,
) -> Result<()> {
    let conn = syncer.connection();
    let login = &owed.login;
    // Catch-ups: every update since the login's old state, to the shared cursor.
    for (role, since, column) in [
        (UserRole::Author, owed.catch_up_author, "catch_up_author"),
        (UserRole::Reviewer, owed.catch_up_review, "catch_up_review"),
    ] {
        let Some(since) = since else { continue };
        if !detect::valid_login(login) {
            continue;
        }
        let walk = detect::enumerate(
            syncer.client_mut(),
            &format!("is:pr {}:{login}", role.qualifier()),
            "updated",
            (since - Duration::hours(1)).timestamp(),
            cursor.timestamp(),
            renew,
        )?;
        if !walk.complete() {
            report.notes.extend(walk.gaps);
            continue;
        }
        if !land_all(syncer, options, &walk.items, renew, report)? {
            continue;
        }
        conn.execute(
            &format!("UPDATE collector_members SET {column} = NULL WHERE login = ?1"),
            [login],
        )?;
        if role == UserRole::Reviewer {
            sync_state::clear_checkpoint(
                conn,
                "user_update_checkpoint",
                &format!("{}:{login}", role.qualifier()),
            )?;
        }
        report
            .backfill
            .catch_ups_finished
            .push(format!("{} {login}", role.qualifier()));
        if budget.spent(syncer) {
            return Ok(());
        }
    }
    let caught_up: bool = conn.query_row(
        "SELECT catch_up_author IS NULL AND catch_up_review IS NULL FROM collector_members WHERE login = ?1",
        [login],
        |r| r.get(0),
    )?;
    if caught_up && (owed.catch_up_author.is_some() || owed.catch_up_review.is_some()) {
        let lower = owed
            .catch_up_author
            .into_iter()
            .chain(owed.catch_up_review)
            .min()
            .unwrap_or(cursor);
        stamp_user(conn, login, cursor, lower)?;
    }
    // History: created months, newest first, skipping covered ones.
    let Some(from) = owed.history else {
        return Ok(());
    };
    let key = format!("user:{login}");
    let now = Utc::now();
    let yesterday = time_dimension::today_as_of(conn, now)? - Duration::days(1);
    let timezone = time_dimension::configured_timezone(conn)?;
    for (start, end) in crate::sync::month_windows(from, yesterday)
        .into_iter()
        .rev()
    {
        if sync_state::covers_as_of(conn, &key, start, end, now)? {
            continue;
        }
        if budget.spent(syncer) || !detect::valid_login(login) {
            return Ok(());
        }
        let (low, _) = crate::collection::day_bounds(timezone, start)?;
        let (_, high) = crate::collection::day_bounds(timezone, end)?;
        let mut items = Vec::new();
        let mut complete = true;
        for role in UserRole::all() {
            let walk = detect::enumerate(
                syncer.client_mut(),
                &format!("is:pr {}:{login}", role.qualifier()),
                "created",
                low,
                high,
                renew,
            )?;
            complete &= walk.complete();
            report.notes.extend(walk.gaps);
            items.extend(walk.items);
        }
        if !complete || !land_all(syncer, options, &items, renew, report)? {
            return Ok(());
        }
        sync_state::record_range(
            conn,
            &key,
            &start.format("%Y-%m-%d").to_string(),
            &end.format("%Y-%m-%d").to_string(),
            items.len() as i64,
        )?;
        report
            .backfill
            .windows_covered
            .push(format!("{login} {}", start.format("%Y-%m")));
    }
    Ok(())
}
