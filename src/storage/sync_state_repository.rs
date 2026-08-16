//! Sync-state persistence: locks, job records, synced ranges, cursors.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{Error, Result};
use crate::storage::time_dimension;

/// Locks staler than this are considered crashed and auto-released.
const STALE_LOCK_MINUTES: i64 = 30;

/// Acquire the sync lock for an entity. Fails fast if a live lock exists;
/// auto-releases a stale one.
pub fn acquire_lock(conn: &Connection, entity_key: &str) -> Result<()> {
    let existing: Option<String> = conn
        .query_row(
            "SELECT started_at FROM sync_locks WHERE entity_key = ?1",
            [entity_key],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(started_at) = existing {
        let started = started_at
            .parse::<chrono::DateTime<Utc>>()
            .unwrap_or_else(|_| Utc::now());
        if Utc::now() - started < Duration::minutes(STALE_LOCK_MINUTES) {
            return Err(Error::SyncInProgress(entity_key.to_string()));
        }
        conn.execute("DELETE FROM sync_locks WHERE entity_key = ?1", [entity_key])?;
    }
    conn.execute(
        "INSERT INTO sync_locks (entity_key, started_at) VALUES (?1, ?2)",
        params![entity_key, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

/// Release the lock (idempotent).
pub fn release_lock(conn: &Connection, entity_key: &str) -> Result<()> {
    conn.execute("DELETE FROM sync_locks WHERE entity_key = ?1", [entity_key])?;
    Ok(())
}

/// Update live progress counters on the lock row.
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
             synced = ?4, skipped = ?5, failed = ?6
         WHERE entity_key = ?1",
        params![
            entity_key,
            current_item,
            current_item_id,
            synced,
            skipped,
            failed
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
    let failed_json = if failed_items.is_empty() {
        None
    } else {
        Some(serde_json::to_string(failed_items)?)
    };
    conn.execute(
        "UPDATE sync_jobs SET status = 'completed', completed_at = ?2,
             synced = ?3, skipped = ?4, failed_items = ?5
         WHERE entity_key = ?1",
        params![
            entity_key,
            Utc::now().to_rfc3339(),
            synced,
            skipped,
            failed_json
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

    #[test]
    fn lock_lifecycle_rejects_double_acquire() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        acquire_lock(conn, "repo:octocat/hello").unwrap();
        assert!(matches!(
            acquire_lock(conn, "repo:octocat/hello"),
            Err(Error::SyncInProgress(_))
        ));
        release_lock(conn, "repo:octocat/hello").unwrap();
        acquire_lock(conn, "repo:octocat/hello").unwrap();
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
}
