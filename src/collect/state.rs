//! Collector state: the shared record, refused sources, and the public
//! freshness readers applications use instead of reading tables directly.

use std::collections::HashSet;

use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use super::lease::{LeaseHolder, lease_holder};
use super::queue::{ParkedPullRequest, parked};
use super::{BACKFILL_LANE, DETECT_LANE};
use crate::error::Result;
use crate::fetch::user_search::UserRole;
use crate::storage::{monitor_repository, sync_state_repository as sync_state, time_dimension};

fn parse(raw: Option<String>) -> Option<DateTime<Utc>> {
    let raw = raw?;
    time_dimension::parse_utc(&raw).ok().or_else(|| {
        // SQLite's datetime('now'), as `monitored_*.last_sync_at` is written.
        chrono::NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S")
            .ok()
            .map(|t| t.and_utc())
    })
}

fn text(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// True when the collector tables exist. A read-only connection to a
/// warehouse no 0.6 binary has opened has none.
fn tables_present(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'
         AND name IN ('collector_state', 'collector_leases', 'collector_pr_queue',
                      'collector_members', 'collector_backoff')",
        [],
        |r| r.get::<_, i64>(0),
    )? == 5)
}

#[derive(Debug, Default)]
pub(crate) struct RawState {
    pub detect_cursor: Option<DateTime<Utc>>,
    pub last_detect_at: Option<DateTime<Utc>>,
    pub last_detect_by: Option<String>,
    pub last_reconcile_at: Option<DateTime<Utc>>,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_tick_by: Option<String>,
    pub last_tick_points: Option<i64>,
    pub last_tick_summary: Option<String>,
}

pub(crate) fn raw_state(conn: &Connection) -> Result<RawState> {
    Ok(conn
        .query_row(
            "SELECT detect_cursor, last_detect_at, last_detect_by, last_reconcile_at,
                    last_tick_at, last_tick_by, last_tick_points, last_tick_summary
             FROM collector_state WHERE id = 1",
            [],
            |r| {
                Ok(RawState {
                    detect_cursor: parse(r.get(0)?),
                    last_detect_at: parse(r.get(1)?),
                    last_detect_by: r.get(2)?,
                    last_reconcile_at: parse(r.get(3)?),
                    last_tick_at: parse(r.get(4)?),
                    last_tick_by: r.get(5)?,
                    last_tick_points: r.get(6)?,
                    last_tick_summary: r.get(7)?,
                })
            },
        )
        .optional()?
        .unwrap_or_default())
}

/// The sweep's next slice and the time its next slice is owed from.
/// Kept apart from [`raw_state`] because the columns are added by
/// [`crate::storage::schema::ensure_collector_tables`], which a read-only
/// status reader may not have run.
pub(crate) fn sweep_position(conn: &Connection) -> Result<(u32, Option<DateTime<Utc>>)> {
    Ok(conn
        .query_row(
            "SELECT sweep_slice, last_sweep_at FROM collector_state WHERE id = 1",
            [],
            |r| Ok((r.get::<_, Option<u32>>(0)?.unwrap_or(0), parse(r.get(1)?))),
        )
        .optional()?
        .unwrap_or_default())
}

pub(crate) fn save_sweep_position(
    conn: &Connection,
    slice: u32,
    owed_from: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO collector_state (id, sweep_slice, last_sweep_at) VALUES (1, ?1, ?2)
         ON CONFLICT (id) DO UPDATE SET
             sweep_slice = excluded.sweep_slice, last_sweep_at = excluded.last_sweep_at",
        params![slice, text(owed_from)],
    )?;
    Ok(())
}

pub(crate) fn detect_cursor(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    Ok(raw_state(conn)?.detect_cursor)
}

/// A source GitHub refused, retried once a day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockedSource {
    /// `owner/name` for a repository.
    pub source: String,
    pub reason: String,
    pub first_seen_at: String,
    pub retry_after: String,
}

/// Sources whose daily retry is not due yet.
pub(crate) fn backed_off(conn: &Connection, now: DateTime<Utc>) -> Result<HashSet<String>> {
    let mut statement =
        conn.prepare("SELECT source FROM collector_backoff WHERE retry_after > ?1")?;
    let rows = statement.query_map([text(now)], |r| r.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub(crate) fn back_off(
    conn: &Connection,
    source: &str,
    reason: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO collector_backoff (source, reason, first_seen_at, last_attempt_at, retry_after)
         VALUES (?1, ?2, ?3, ?3, ?4)
         ON CONFLICT (source) DO UPDATE SET reason = excluded.reason,
             last_attempt_at = excluded.last_attempt_at, retry_after = excluded.retry_after",
        params![source, reason, text(now), text(now + Duration::hours(24))],
    )?;
    Ok(())
}

pub(crate) fn clear_backoff(conn: &Connection, source: &str) -> Result<()> {
    conn.execute("DELETE FROM collector_backoff WHERE source = ?1", [source])?;
    Ok(())
}

/// Every source GitHub currently refuses.
pub fn blocked_sources(conn: &Connection) -> Result<Vec<BlockedSource>> {
    if !tables_present(conn)? {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare(
        "SELECT source, reason, first_seen_at, retry_after FROM collector_backoff ORDER BY source",
    )?;
    let rows = statement.query_map([], |r| {
        Ok(BlockedSource {
            source: r.get(0)?,
            reason: r.get(1)?,
            first_seen_at: r.get(2)?,
            retry_after: r.get(3)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Monitored logins GitHub no longer resolves (renamed or deleted).
pub fn unresolved_logins(conn: &Connection) -> Result<Vec<String>> {
    if !tables_present(conn)? {
        return Ok(Vec::new());
    }
    let mut statement = conn.prepare(
        "SELECT login FROM collector_members WHERE unresolved_since IS NOT NULL ORDER BY login",
    )?;
    let rows = statement.query_map([], |r| r.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The shared collector's state, for a status line such as "GitHub checked
/// 12 min ago by githubdw".
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct CollectorStatus {
    /// When the last complete detection finished, and who ran it.
    pub last_detect_at: Option<DateTime<Utc>>,
    pub last_detect_by: Option<String>,
    /// Every change up to this instant has been detected.
    pub detected_through: Option<DateTime<Utc>>,
    pub last_reconcile_at: Option<DateTime<Utc>>,
    pub last_tick_at: Option<DateTime<Utc>>,
    pub last_tick_by: Option<String>,
    pub last_tick_points: Option<i64>,
    /// The last tick's cost line.
    pub last_tick_summary: Option<String>,
    /// Live holders of the detect and backfill leases.
    pub detecting: Option<LeaseHolder>,
    pub backfilling: Option<LeaseHolder>,
    /// Pull requests waiting for a full fetch (parked ones included).
    pub queued: i64,
    pub parked: Vec<ParkedPullRequest>,
    pub blocked: Vec<BlockedSource>,
    pub unresolved_logins: Vec<String>,
    /// Logins still owed a catch-up from older per-user state.
    pub catch_ups_owed: i64,
}

/// Read the collector's state. `None` when no 0.6 binary has opened this
/// warehouse yet.
pub fn collector_status(conn: &Connection) -> Result<Option<CollectorStatus>> {
    if !tables_present(conn)? {
        return Ok(None);
    }
    let raw = raw_state(conn)?;
    let queued = conn.query_row("SELECT COUNT(*) FROM collector_pr_queue", [], |r| r.get(0))?;
    let catch_ups_owed = conn.query_row(
        "SELECT COUNT(*) FROM collector_members
         WHERE catch_up_author IS NOT NULL OR catch_up_review IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    Ok(Some(CollectorStatus {
        last_detect_at: raw.last_detect_at,
        last_detect_by: raw.last_detect_by,
        detected_through: raw.last_detect_at.and(raw.detect_cursor),
        last_reconcile_at: raw.last_reconcile_at,
        last_tick_at: raw.last_tick_at,
        last_tick_by: raw.last_tick_by,
        last_tick_points: raw.last_tick_points,
        last_tick_summary: raw.last_tick_summary,
        detecting: lease_holder(conn, DETECT_LANE)?,
        backfilling: lease_holder(conn, BACKFILL_LANE)?,
        queued,
        parked: parked(conn)?,
        blocked: blocked_sources(conn)?,
        unresolved_logins: unresolved_logins(conn)?,
        catch_ups_owed,
    }))
}

/// How current one login's GitHub data is.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct UserFreshness {
    pub login: String,
    /// When the authored and reviewed sides were last brought current.
    pub authored_last_sync_at: Option<DateTime<Utc>>,
    pub reviews_last_sync_at: Option<DateTime<Utc>>,
    /// The older of the two; `None` unless both are known, so "synced at"
    /// never overstates what is collected.
    pub last_sync_at: Option<DateTime<Utc>>,
    /// Created-date coverage `(first day, last day)`.
    pub coverage: Option<(NaiveDate, NaiveDate)>,
    /// True while a catch-up from older per-user state is still owed.
    pub catch_up_owed: bool,
    /// True when GitHub no longer resolves the login.
    pub unresolved: bool,
}

/// Read one login's freshness.
pub fn user_freshness(conn: &Connection, login: &str) -> Result<UserFreshness> {
    let login = crate::query::to_bare_login(login);
    let read = |role: UserRole| -> Result<Option<DateTime<Utc>>> {
        Ok(parse(
            conn.query_row(
                "SELECT last_sync_at FROM sync_metadata
                 WHERE source_type = ?1 AND lower(source_id) = ?2",
                params![role.cursor_source_type(), login],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten(),
        ))
    };
    let authored = read(UserRole::Author)?;
    let reviews = read(UserRole::Reviewer)?;
    let coverage =
        sync_state::coverage_extent(conn, &format!("user:{login}"))?.and_then(|(s, e)| {
            Some((
                NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()?,
                NaiveDate::parse_from_str(&e, "%Y-%m-%d").ok()?,
            ))
        });
    let (catch_up_owed, unresolved) = if tables_present(conn)? {
        conn.query_row(
            "SELECT catch_up_author IS NOT NULL OR catch_up_review IS NOT NULL,
                    unresolved_since IS NOT NULL
             FROM collector_members WHERE login = ?1",
            [&login],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .unwrap_or((false, false))
    } else {
        (false, false)
    };
    Ok(UserFreshness {
        last_sync_at: authored.zip(reviews).map(|(a, r)| a.min(r)),
        login,
        authored_last_sync_at: authored,
        reviews_last_sync_at: reviews,
        coverage,
        catch_up_owed,
        unresolved,
    })
}

/// How current the whole monitored set is.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WarehouseFreshness {
    /// The last whole-set pass (a complete detection, or a 0.5 `sync all`).
    pub last_whole_set_sync_at: Option<DateTime<Utc>>,
    /// The last complete detection and who ran it.
    pub last_detect_at: Option<DateTime<Utc>>,
    pub last_detect_by: Option<String>,
    /// The newest per-source sync of any monitored user or repository.
    pub newest_source_sync_at: Option<DateTime<Utc>>,
    pub monitored_sources: usize,
    /// True once anything beyond per-user syncs was recorded (a repository
    /// walk, a whole-set pass), so "never collected" can be told apart from
    /// "collected per person only".
    pub collection_recorded: bool,
}

/// Read the whole set's freshness.
pub fn warehouse_freshness(conn: &Connection) -> Result<WarehouseFreshness> {
    let whole: Option<String> = conn
        .query_row(
            "SELECT MAX(last_sync_at) FROM sync_metadata WHERE source_type = 'sync_all'",
            [],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let recorded: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sync_metadata WHERE source_type NOT IN ('user', 'user_reviews'))",
        [],
        |r| r.get(0),
    )?;
    let sources = monitor_repository::list(conn)?;
    let newest = sources
        .iter()
        .filter_map(|s| s.last_sync_at.clone())
        .filter_map(|raw| parse(Some(raw)))
        .max();
    let raw = if tables_present(conn)? {
        raw_state(conn)?
    } else {
        RawState::default()
    };
    Ok(WarehouseFreshness {
        last_whole_set_sync_at: parse(whole),
        last_detect_at: raw.last_detect_at,
        last_detect_by: raw.last_detect_by,
        newest_source_sync_at: newest,
        monitored_sources: sources.len(),
        collection_recorded: recorded,
    })
}
