//! Sync-state persistence: locks, job records, synced ranges, cursors.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{Error, Result};
use crate::storage::time_dimension;

/// Locks staler than this are considered crashed and auto-released.
const STALE_LOCK_MINUTES: i64 = 30;

fn lease_token(previous: Option<&str>) -> String {
    loop {
        let token = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        if Some(token.as_str()) != previous {
            return token;
        }
    }
}

/// Acquire the lock and return an owner token (the exact `started_at` this
/// caller wrote). A live lock refuses; a lock older than the stale window, or
/// whose `started_at` cannot be parsed, is taken over. The takeover deletes
/// only the row it observed, so of two racers on one stale lock exactly one
/// inserts and the other gets [`Error::SyncInProgress`].
pub fn acquire_owned_lock(conn: &Connection, entity_key: &str) -> Result<String> {
    take_lock(conn, entity_key, None)
}

fn take_lock(conn: &Connection, entity_key: &str, owner: Option<&str>) -> Result<String> {
    let existing: Option<String> = conn
        .query_row(
            "SELECT started_at FROM sync_locks WHERE entity_key = ?1",
            [entity_key],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(started_at) = existing {
        // A value nobody can date is a crashed or hand-edited lock; waiting
        // for it to "age" would block the entity forever.
        let started = started_at
            .parse::<DateTime<Utc>>()
            .unwrap_or(DateTime::UNIX_EPOCH);
        if Utc::now() - started < Duration::minutes(STALE_LOCK_MINUTES) {
            return Err(Error::SyncInProgress(entity_key.to_string()));
        }
        conn.execute(
            "DELETE FROM sync_locks WHERE entity_key = ?1 AND started_at = ?2",
            params![entity_key, started_at],
        )?;
    }
    let token = lease_token(None);
    match conn.execute(
        "INSERT INTO sync_locks (entity_key, started_at, chunk_start_date) VALUES (?1, ?2, ?3)",
        params![entity_key, token, owner],
    ) {
        Ok(_) => Ok(token),
        Err(rusqlite::Error::SqliteFailure(failure, _))
            if failure.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            Err(Error::SyncInProgress(entity_key.to_string()))
        }
        Err(error) => Err(error.into()),
    }
}

/// Renew an owned lease. Returns the new token, or `None` when the lock is no
/// longer this caller's (taken over as stale, or released by someone else).
pub fn refresh_owned_lock(
    conn: &Connection,
    entity_key: &str,
    token: &str,
) -> Result<Option<String>> {
    let next = lease_token(Some(token));
    let changed = conn.execute(
        "UPDATE sync_locks SET started_at = ?3 WHERE entity_key = ?1 AND started_at = ?2",
        params![entity_key, token, next],
    )?;
    Ok((changed == 1).then_some(next))
}

/// Release only the lock this caller owns. Returns false, deleting nothing,
/// when the lock now belongs to someone else.
pub fn release_owned_lock(conn: &Connection, entity_key: &str, token: &str) -> Result<bool> {
    let deleted = conn.execute(
        "DELETE FROM sync_locks WHERE entity_key = ?1 AND started_at = ?2",
        params![entity_key, token],
    )?;
    Ok(deleted == 1)
}

/// A sync's hold on an entity lock. The owner id is fixed for the life of the
/// lease and stored in `chunk_start_date`, a column nothing else uses, so
/// [`update_lock_progress`] can keep renewing `started_at` without the walks
/// having to carry a token. Releasing deletes only a row that still carries
/// this owner id, so a lock taken over after going stale is never deleted by
/// the run that lost it. Dropping the guard releases too, so a panic or an
/// early return cannot leak the lock.
pub struct SyncLease<'c> {
    conn: &'c Connection,
    entity_key: String,
    owner: String,
    released: bool,
}

impl<'c> SyncLease<'c> {
    /// Take the lock for `entity_key` or fail with [`Error::SyncInProgress`].
    pub fn acquire(conn: &'c Connection, entity_key: &str) -> Result<Self> {
        let owner = lease_token(None);
        take_lock(conn, entity_key, Some(&owner))?;
        Ok(Self {
            conn,
            entity_key: entity_key.to_string(),
            owner,
            released: false,
        })
    }

    /// Release the lock. `Ok(false)` means it was no longer ours and was left alone.
    pub fn release(mut self) -> Result<bool> {
        self.released = true;
        self.delete_owned()
    }

    fn delete_owned(&self) -> Result<bool> {
        let deleted = self.conn.execute(
            "DELETE FROM sync_locks WHERE entity_key = ?1 AND chunk_start_date = ?2",
            params![self.entity_key, self.owner],
        )?;
        Ok(deleted == 1)
    }
}

impl Drop for SyncLease<'_> {
    fn drop(&mut self) {
        if !self.released {
            // Nowhere to report a failure from here; the lock then ages out.
            let _ = self.delete_owned();
        }
    }
}

/// Run `body` holding the lock on `entity_key`, then release it. The body's
/// result is the primary one: a failed release only surfaces when the body
/// succeeded, and never replaces the body's error.
pub fn with_lock<T>(
    conn: &Connection,
    entity_key: &str,
    body: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let lease = SyncLease::acquire(conn, entity_key)?;
    let result = body();
    let released = lease.release();
    match (result, released) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(_)) => Ok(value),
    }
}

/// Update live progress counters on the lock row. Also renews the lock:
/// `started_at` is how staleness is judged, so a long walk that keeps
/// recording progress is never mistaken for a crashed one.
pub fn update_lock_progress(
    conn: &Connection,
    entity_key: &str,
    current_item: i64,
    current_item_id: &str,
    synced: i64,
    skipped: i64,
    failed: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE sync_locks SET current_item = ?2, current_item_id = ?3,
             synced = ?4, skipped = ?5, failed = ?6, started_at = ?7
         WHERE entity_key = ?1",
        params![
            entity_key,
            current_item,
            current_item_id,
            synced,
            skipped,
            failed,
            Utc::now().to_rfc3339()
        ],
    )?;
    Ok(())
}

/// Start (or restart) the durable job record for an entity.
pub fn start_job(conn: &Connection, entity_key: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO sync_jobs (entity_key, status, started_at)
         VALUES (?1, 'running', ?2)
         ON CONFLICT (entity_key) DO UPDATE SET
             status = 'running', started_at = excluded.started_at,
             completed_at = NULL, synced = 0, skipped = 0,
             failed_items = NULL, error = NULL",
        params![entity_key, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

/// Mark the job completed with final counters.
pub fn complete_job(
    conn: &Connection,
    entity_key: &str,
    synced: i64,
    skipped: i64,
    failed_items: &[(String, String)],
) -> Result<()> {
    complete_job_outcome(conn, entity_key, synced, skipped, failed_items, false)
}

/// Record terminal counters and a paused catch-up state in one write. A budget
/// yield is not a completed source scan and must not be labelled as one.
pub fn complete_job_outcome(
    conn: &Connection,
    entity_key: &str,
    synced: i64,
    skipped: i64,
    failed_items: &[(String, String)],
    partial: bool,
) -> Result<()> {
    let failed_json = if failed_items.is_empty() {
        None
    } else {
        Some(serde_json::to_string(failed_items)?)
    };
    conn.execute(
        "UPDATE sync_jobs SET status=?6, completed_at = ?2,
             synced = ?3, skipped = ?4, failed_items = ?5,
             error=CASE WHEN ?6='partial' THEN 'Catch-up paused; the next scheduled run resumes saved progress.' ELSE NULL END
         WHERE entity_key = ?1",
        params![
            entity_key,
            Utc::now().to_rfc3339(),
            synced,
            skipped,
            failed_json,
            if partial { "partial" } else { "completed" }
        ],
    )?;
    Ok(())
}

/// Mark the job failed with an error message.
pub fn fail_job(conn: &Connection, entity_key: &str, error: &str) -> Result<()> {
    conn.execute(
        "UPDATE sync_jobs SET status = 'failed', completed_at = ?2, error = ?3
         WHERE entity_key = ?1",
        params![entity_key, Utc::now().to_rfc3339(), error],
    )?;
    Ok(())
}

/// Record a freshly synced `[start, end]` date range, merging with any
/// overlapping or adjacent (within one day) existing ranges.
///
/// `end_date` is clamped to the last complete day in the warehouse timezone.
/// A range may only ever claim days that have finished: an item created later
/// in a still-running day would otherwise sit inside a range recorded as fully
/// covered, and any reader that trusts the claim would never fetch it. The
/// clamp also repairs legacy rows, because the merged end is re-clamped after
/// absorbing them.
///
/// A range whose start is itself beyond the last complete day is dropped —
/// there is nothing complete to record.
pub fn record_range(
    conn: &Connection,
    entity_key: &str,
    start_date: &str,
    end_date: &str,
    item_count: i64,
) -> Result<()> {
    record_range_as_of(
        conn,
        entity_key,
        start_date,
        end_date,
        item_count,
        Utc::now(),
    )
}

/// [`record_range`] anchored to an explicit instant.
pub fn record_range_as_of(
    conn: &Connection,
    entity_key: &str,
    start_date: &str,
    end_date: &str,
    item_count: i64,
    instant: DateTime<Utc>,
) -> Result<()> {
    let start = parse_date(start_date)?;
    let end = parse_date(end_date)?;
    let sealable = time_dimension::last_complete_day_as_of(conn, instant)?;
    let end = end.min(sealable);
    if start > end {
        return Ok(());
    }

    let mut statement = conn.prepare(
        "SELECT start_date, end_date, item_count FROM synced_ranges
         WHERE entity_key = ?1
           AND start_date <= ?2
           AND end_date >= ?3",
    )?;
    let adjacent_end = (end + Duration::days(1)).format("%Y-%m-%d").to_string();
    let adjacent_start = (start - Duration::days(1)).format("%Y-%m-%d").to_string();
    let overlapping: Vec<(String, String, i64)> = statement
        .query_map(params![entity_key, adjacent_end, adjacent_start], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<std::result::Result<_, _>>()?;

    let mut merged_start = start;
    let mut merged_end = end;
    let mut merged_count = item_count;
    for (existing_start, existing_end, existing_count) in &overlapping {
        merged_start = merged_start.min(parse_date(existing_start)?);
        merged_end = merged_end.max(parse_date(existing_end)?);
        merged_count += existing_count;
        conn.execute(
            "DELETE FROM synced_ranges WHERE entity_key = ?1 AND start_date = ?2 AND end_date = ?3",
            params![entity_key, existing_start, existing_end],
        )?;
    }
    let merged_end = merged_end.min(sealable);
    conn.execute(
        "INSERT INTO synced_ranges (entity_key, start_date, end_date, synced_at, item_count)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            entity_key,
            merged_start.format("%Y-%m-%d").to_string(),
            merged_end.format("%Y-%m-%d").to_string(),
            Utc::now().to_rfc3339(),
            merged_count,
        ],
    )?;
    Ok(())
}

/// Stored ranges with every end clamped to the last complete day in the
/// warehouse timezone, and ranges entirely beyond it dropped.
///
/// Clamping on read as well as on write means a database written by an older
/// version — whose rows can claim the day that was in progress at the time —
/// is still interpreted honestly, with no migration and no lost day.
fn clamped_ranges(
    conn: &Connection,
    entity_key: &str,
    instant: DateTime<Utc>,
) -> Result<Vec<(NaiveDate, NaiveDate)>> {
    let sealable = time_dimension::last_complete_day_as_of(conn, instant)?;
    let mut statement = conn.prepare(
        "SELECT start_date, end_date FROM synced_ranges
         WHERE entity_key = ?1 ORDER BY start_date",
    )?;
    let rows: Vec<(String, String)> = statement
        .query_map([entity_key], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    let mut ranges = Vec::with_capacity(rows.len());
    for (start_text, end_text) in rows {
        let start = parse_date(&start_text)?;
        let end = parse_date(&end_text)?.min(sealable);
        if start <= end {
            ranges.push((start, end));
        }
    }
    Ok(ranges)
}

/// Uncovered `[start, end]` windows between coverage and `target_date`.
/// No ranges at all -> empty result (caller picks the initial window).
pub fn gaps(
    conn: &Connection,
    entity_key: &str,
    target_date: &str,
) -> Result<Vec<(String, String)>> {
    gaps_as_of(conn, entity_key, target_date, Utc::now())
}

/// [`gaps`] anchored to an explicit instant.
pub fn gaps_as_of(
    conn: &Connection,
    entity_key: &str,
    target_date: &str,
    instant: DateTime<Utc>,
) -> Result<Vec<(String, String)>> {
    let target = parse_date(target_date)?;
    let ranges = clamped_ranges(conn, entity_key, instant)?;
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    let mut previous_end = ranges[0].1;
    for (start, end) in ranges.iter().skip(1) {
        if *start > previous_end + Duration::days(1) {
            result.push((
                (previous_end + Duration::days(1))
                    .format("%Y-%m-%d")
                    .to_string(),
                (*start - Duration::days(1)).format("%Y-%m-%d").to_string(),
            ));
        }
        previous_end = previous_end.max(*end);
    }
    if target > previous_end {
        result.push((
            (previous_end + Duration::days(1))
                .format("%Y-%m-%d")
                .to_string(),
            target.format("%Y-%m-%d").to_string(),
        ));
    }
    Ok(result)
}

/// The `(MIN(start_date), MAX(end_date))` of coverage, if any.
///
/// The end is clamped to the last complete day in the warehouse timezone, so
/// coverage is never reported as reaching into a day that is still running.
pub fn coverage_extent(conn: &Connection, entity_key: &str) -> Result<Option<(String, String)>> {
    coverage_extent_as_of(conn, entity_key, Utc::now())
}

/// [`coverage_extent`] anchored to an explicit instant.
pub fn coverage_extent_as_of(
    conn: &Connection,
    entity_key: &str,
    instant: DateTime<Utc>,
) -> Result<Option<(String, String)>> {
    let ranges = clamped_ranges(conn, entity_key, instant)?;
    let Some(start) = ranges.iter().map(|(start, _)| *start).min() else {
        return Ok(None);
    };
    let end = ranges
        .iter()
        .map(|(_, end)| *end)
        .max()
        .unwrap_or(start)
        .max(start);
    Ok(Some((
        start.format("%Y-%m-%d").to_string(),
        end.format("%Y-%m-%d").to_string(),
    )))
}

/// Read the `updated_at` high-water cursor for a source.
pub fn last_updated_cursor(
    conn: &Connection,
    source_type: &str,
    source_id: &str,
) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT last_updated_cursor FROM sync_metadata
             WHERE source_type = ?1 AND source_id = ?2",
            params![source_type, source_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten())
}

/// Advance the cursor + last-sync stamp for a source.
pub fn advance_cursor(
    conn: &Connection,
    source_type: &str,
    source_id: &str,
    cursor: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO sync_metadata (source_type, source_id, last_sync_at, last_updated_cursor)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (source_type, source_id) DO UPDATE SET
             last_sync_at = excluded.last_sync_at,
             last_updated_cursor = COALESCE(
                 MAX(excluded.last_updated_cursor, sync_metadata.last_updated_cursor),
                 sync_metadata.last_updated_cursor,
                 excluded.last_updated_cursor)",
        params![source_type, source_id, Utc::now().to_rfc3339(), cursor],
    )?;
    Ok(())
}

/// Read a resumable-walk checkpoint stored for a source.
///
/// Checkpoints live in `sync_metadata` under their own `source_type`, so no
/// schema change is needed and readers that select a specific source type
/// (`repo`, `user`, `user_reviews`) never see them. The value is opaque to
/// storage; the syncer owns its format and validates it on load.
pub fn load_checkpoint(
    conn: &Connection,
    checkpoint_type: &str,
    source_id: &str,
) -> Result<Option<String>> {
    last_updated_cursor(conn, checkpoint_type, source_id)
}

/// Replace a resumable-walk checkpoint. Unlike [`advance_cursor`] this is a
/// plain overwrite: a checkpoint describes a position, not a high-water mark.
pub fn save_checkpoint(
    conn: &Connection,
    checkpoint_type: &str,
    source_id: &str,
    checkpoint: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO sync_metadata (source_type, source_id, last_sync_at, last_updated_cursor)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (source_type, source_id) DO UPDATE SET
             last_sync_at = excluded.last_sync_at,
             last_updated_cursor = excluded.last_updated_cursor",
        params![
            checkpoint_type,
            source_id,
            Utc::now().to_rfc3339(),
            checkpoint
        ],
    )?;
    Ok(())
}

/// Drop a checkpoint (idempotent).
pub fn clear_checkpoint(conn: &Connection, checkpoint_type: &str, source_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM sync_metadata WHERE source_type = ?1 AND source_id = ?2",
        params![checkpoint_type, source_id],
    )?;
    Ok(())
}

/// A sync currently holding its entity's lock, with live progress counters.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ActiveSync {
    pub entity_key: String,
    pub started_at: String,
    pub current_item: i64,
    pub current_item_id: Option<String>,
    pub synced: i64,
    pub skipped: i64,
    pub failed: i64,
}

/// One recorded sync job.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SyncJob {
    pub entity_key: String,
    pub status: String,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub synced: i64,
}

/// Syncs in progress right now, oldest first.
pub fn active_syncs(conn: &Connection) -> Result<Vec<ActiveSync>> {
    let mut statement = conn.prepare(
        // The lock's own started_at is renewed as the sync progresses; the job
        // row keeps the real start.
        "SELECT l.entity_key, COALESCE(j.started_at, l.started_at), l.current_item,
                l.current_item_id, l.synced, l.skipped, l.failed
         FROM sync_locks l LEFT JOIN sync_jobs j ON j.entity_key = l.entity_key
         ORDER BY 2",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(ActiveSync {
            entity_key: row.get(0)?,
            started_at: row.get(1)?,
            current_item: row.get(2)?,
            current_item_id: row.get(3)?,
            synced: row.get(4)?,
            skipped: row.get(5)?,
            failed: row.get(6)?,
        })
    })?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

/// The most recent `limit` sync jobs, newest first.
pub fn recent_jobs(conn: &Connection, limit: u32) -> Result<Vec<SyncJob>> {
    let mut statement = conn.prepare(
        "SELECT entity_key, status, started_at, completed_at, synced
         FROM sync_jobs ORDER BY started_at DESC LIMIT ?1",
    )?;
    let rows = statement.query_map([limit], |row| {
        Ok(SyncJob {
            entity_key: row.get(0)?,
            status: row.get(1)?,
            started_at: row.get(2)?,
            completed_at: row.get(3)?,
            synced: row.get(4)?,
        })
    })?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

/// Finish a walk atomically: advance the real high-water cursor and drop the
/// checkpoint in one transaction, so a crash can never leave the cursor
/// advanced while the checkpoint still points into the old walk, or the
/// checkpoint gone while the cursor still sits at the old watermark.
pub fn complete_walk(
    conn: &Connection,
    source_type: &str,
    checkpoint_type: &str,
    source_id: &str,
    cursor: Option<&str>,
) -> Result<()> {
    let transaction = conn.unchecked_transaction()?;
    advance_cursor(&transaction, source_type, source_id, cursor)?;
    clear_checkpoint(&transaction, checkpoint_type, source_id)?;
    transaction.commit()?;
    Ok(())
}

fn parse_date(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|error| Error::InvalidArgument(format!("bad date '{text}': {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;
    use chrono::TimeZone;

    /// 2026-08-17T03:00:00Z is 2026-08-16 20:00 in Los Angeles: the 16th is
    /// still running locally, and UTC has already rolled to the 17th.
    fn evening_in_los_angeles() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 17, 3, 0, 0).unwrap()
    }

    fn warehouse_in_los_angeles() -> GithubDW {
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

    fn stored_range(conn: &Connection, entity_key: &str) -> (String, String) {
        conn.query_row(
            "SELECT start_date, end_date FROM synced_ranges WHERE entity_key = ?1",
            [entity_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    const KEY: &str = "repo:octocat/hello";

    fn age_lock(conn: &Connection, minutes: i64) {
        let aged = (Utc::now() - Duration::minutes(minutes)).to_rfc3339();
        conn.execute("UPDATE sync_locks SET started_at = ?1", [aged])
            .unwrap();
    }

    fn lock_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn lock_lifecycle_rejects_double_acquire() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let lease = SyncLease::acquire(conn, KEY).unwrap();
        assert!(matches!(
            SyncLease::acquire(conn, KEY),
            Err(Error::SyncInProgress(_))
        ));
        assert!(lease.release().unwrap());
        assert_eq!(lock_rows(conn), 0);
        SyncLease::acquire(conn, KEY).unwrap().release().unwrap();
    }

    /// Ages are minutes since the last renewal: 29 is live, 31 is stale.
    #[test]
    fn staleness_boundary_is_thirty_minutes() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let _held = SyncLease::acquire(conn, KEY).unwrap();
        age_lock(conn, 29);
        assert!(matches!(
            SyncLease::acquire(conn, KEY),
            Err(Error::SyncInProgress(_))
        ));
        age_lock(conn, 31);
        assert!(SyncLease::acquire(conn, KEY).is_ok());
    }

    /// A walk older than 30 minutes that keeps recording progress keeps its lock.
    #[test]
    fn recording_progress_renews_the_lock() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let _held = SyncLease::acquire(conn, KEY).unwrap();
        age_lock(conn, 31);
        update_lock_progress(conn, KEY, 1, "octocat/hello#1", 1, 0, 0).unwrap();
        assert!(matches!(
            SyncLease::acquire(conn, KEY),
            Err(Error::SyncInProgress(_))
        ));
        // Without the renewal the same age is stale (previous test), so the
        // lock above survived because of the heartbeat.
    }

    /// A takes the lock, goes stale, B takes over; A finishing must not free B's lock.
    #[test]
    fn a_stale_holder_does_not_release_its_successors_lock() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let a = SyncLease::acquire(conn, KEY).unwrap();
        age_lock(conn, 31);
        let b = SyncLease::acquire(conn, KEY).unwrap();
        assert!(!a.release().unwrap(), "A no longer owns the lock");
        assert_eq!(lock_rows(conn), 1, "B's lock is intact");
        assert!(matches!(
            SyncLease::acquire(conn, KEY),
            Err(Error::SyncInProgress(_))
        ));
        assert!(b.release().unwrap());
        assert_eq!(lock_rows(conn), 0);
    }

    /// Dropping an unreleased lease frees it, but not after it was taken over.
    #[test]
    fn dropping_a_lease_releases_only_its_own_lock() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        drop(SyncLease::acquire(conn, KEY).unwrap());
        assert_eq!(lock_rows(conn), 0);
        let a = SyncLease::acquire(conn, KEY).unwrap();
        age_lock(conn, 31);
        let _b = SyncLease::acquire(conn, KEY).unwrap();
        drop(a);
        assert_eq!(lock_rows(conn), 1);
    }

    /// Unparsable, empty and non-RFC3339 `started_at` values are all reclaimable.
    #[test]
    fn a_corrupt_started_at_is_reclaimable() {
        for bad in ["garbage", "", "2026-13-45", "0"] {
            let warehouse = GithubDW::open_in_memory().unwrap();
            let conn = warehouse.connection();
            conn.execute(
                "INSERT INTO sync_locks (entity_key, started_at) VALUES (?1, ?2)",
                params![KEY, bad],
            )
            .unwrap();
            acquire_owned_lock(conn, KEY).unwrap_or_else(|e| panic!("{bad:?}: {e}"));
        }
    }

    /// The body's error wins over a release problem, and the lock is freed either way.
    #[test]
    fn with_lock_keeps_the_body_error_and_releases() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let result: Result<()> =
            with_lock(conn, KEY, || Err(Error::InvalidArgument("body".into())));
        assert!(matches!(result, Err(Error::InvalidArgument(m)) if m == "body"));
        assert_eq!(lock_rows(conn), 0);
        // The lock is gone mid-body (taken over): the body's Ok stays Ok.
        let result = with_lock(conn, KEY, || {
            conn.execute("DELETE FROM sync_locks", []).unwrap();
            Ok(7)
        });
        assert_eq!(result.unwrap(), 7);
    }

    /// A range may not claim the day that is still in progress, and "in
    /// progress" is decided in the warehouse's own calendar — not UTC's.
    #[test]
    fn recorded_end_is_capped_at_the_last_complete_local_day() {
        let warehouse = warehouse_in_los_angeles();
        let conn = warehouse.connection();
        let entity = "repo:octocat/hello";

        record_range_as_of(
            conn,
            entity,
            "2026-06-01",
            "2026-08-17",
            10,
            evening_in_los_angeles(),
        )
        .unwrap();

        let (start, end) = stored_range(conn, entity);
        assert_eq!(start, "2026-06-01");
        assert_eq!(
            end, "2026-08-15",
            "the 16th is still running locally and the 17th has not begun"
        );
    }

    /// Nothing is recorded when the whole requested range is still in progress.
    #[test]
    fn a_range_entirely_in_the_running_day_is_not_recorded() {
        let warehouse = warehouse_in_los_angeles();
        let conn = warehouse.connection();
        let entity = "repo:octocat/hello";

        record_range_as_of(
            conn,
            entity,
            "2026-08-16",
            "2026-08-16",
            3,
            evening_in_los_angeles(),
        )
        .unwrap();

        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM synced_ranges WHERE entity_key = ?1",
                [entity],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
        assert!(
            coverage_extent_as_of(conn, entity, evening_in_los_angeles())
                .unwrap()
                .is_none()
        );
    }

    /// A row written by an older version can claim a day that was in progress
    /// when it was written. Reads clamp it back, so the day resurfaces as work
    /// to do instead of being silently treated as covered.
    #[test]
    fn legacy_optimistic_rows_are_clamped_on_read_and_repaired_on_write() {
        let warehouse = warehouse_in_los_angeles();
        let conn = warehouse.connection();
        let entity = "repo:octocat/hello";
        let instant = evening_in_los_angeles();

        // Simulate what 0.2.1 wrote: coverage through "today", in UTC terms.
        conn.execute(
            "INSERT INTO synced_ranges (entity_key, start_date, end_date, synced_at, item_count)
             VALUES (?1, '2026-06-01', '2026-08-17', '2026-08-17T03:00:00Z', 42)",
            [entity],
        )
        .unwrap();

        let extent = coverage_extent_as_of(conn, entity, instant)
            .unwrap()
            .unwrap();
        assert_eq!(extent, ("2026-06-01".to_string(), "2026-08-15".to_string()));

        // The clamped end means the trailing days show up as a gap again.
        let holes = gaps_as_of(conn, entity, "2026-08-16", instant).unwrap();
        assert_eq!(
            holes,
            vec![("2026-08-16".to_string(), "2026-08-16".to_string())]
        );

        // The next write repairs the stored row rather than merging its
        // optimistic end forward.
        record_range_as_of(conn, entity, "2026-08-10", "2026-08-15", 5, instant).unwrap();
        let (start, end) = stored_range(conn, entity);
        assert_eq!(start, "2026-06-01");
        assert_eq!(end, "2026-08-15");
    }

    #[test]
    fn ranges_merge_overlapping_and_adjacent() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let entity = "repo:octocat/hello";
        // Fixed instant well past every range below, so the completeness clamp
        // never participates and the test does not depend on the wall clock.
        let now = Utc.with_ymd_and_hms(2027, 1, 1, 12, 0, 0).unwrap();

        // Recent window, then backfill older, then fill the middle hole.
        record_range_as_of(conn, entity, "2026-06-01", "2026-06-30", 10, now).unwrap();
        record_range_as_of(conn, entity, "2026-01-01", "2026-03-31", 20, now).unwrap();
        record_range_as_of(conn, entity, "2026-04-01", "2026-05-31", 5, now).unwrap();

        let extent = coverage_extent_as_of(conn, entity, now).unwrap().unwrap();
        assert_eq!(extent, ("2026-01-01".to_string(), "2026-06-30".to_string()));
        let row_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM synced_ranges WHERE entity_key = ?1",
                [entity],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(row_count, 1, "all three ranges collapse into one");
        let total: i64 = conn
            .query_row(
                "SELECT item_count FROM synced_ranges WHERE entity_key = ?1",
                [entity],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(total, 35);
    }

    #[test]
    fn gap_detection_finds_holes_and_trailing_gap() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let entity = "repo:octocat/hello";
        let now = Utc.with_ymd_and_hms(2027, 1, 1, 12, 0, 0).unwrap();

        assert!(
            gaps_as_of(conn, entity, "2026-07-01", now)
                .unwrap()
                .is_empty()
        );

        record_range_as_of(conn, entity, "2026-01-01", "2026-01-31", 1, now).unwrap();
        record_range_as_of(conn, entity, "2026-03-01", "2026-03-31", 1, now).unwrap();

        let holes = gaps_as_of(conn, entity, "2026-04-15", now).unwrap();
        assert_eq!(
            holes,
            vec![
                ("2026-02-01".to_string(), "2026-02-28".to_string()),
                ("2026-04-01".to_string(), "2026-04-15".to_string()),
            ]
        );
    }

    #[test]
    fn cursor_only_moves_forward() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        advance_cursor(conn, "repo", "octocat/hello", Some("2026-06-01T00:00:00Z")).unwrap();
        advance_cursor(conn, "repo", "octocat/hello", Some("2026-01-01T00:00:00Z")).unwrap();
        let cursor = last_updated_cursor(conn, "repo", "octocat/hello").unwrap();
        assert_eq!(cursor.as_deref(), Some("2026-06-01T00:00:00Z"));
    }

    #[test]
    fn watch_lists_live_syncs_then_finished_jobs() {
        let warehouse = crate::GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let lease = SyncLease::acquire(conn, "repo:o/r").unwrap();
        start_job(conn, "repo:o/r").unwrap();
        let live = active_syncs(conn).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].entity_key, "repo:o/r");
        // Progress renews the lock but must not move the reported start.
        let started = live[0].started_at.clone();
        update_lock_progress(conn, "repo:o/r", 1, "o/r#1", 1, 0, 0).unwrap();
        assert_eq!(active_syncs(conn).unwrap()[0].started_at, started);
        complete_job(conn, "repo:o/r", 3, 0, &[]).unwrap();
        lease.release().unwrap();
        assert!(active_syncs(conn).unwrap().is_empty());
        let jobs = recent_jobs(conn, 5).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].entity_key, "repo:o/r");
        assert_eq!(jobs[0].synced, 3);
        assert!(jobs[0].completed_at.is_some());
    }
}
