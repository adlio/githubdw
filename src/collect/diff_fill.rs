//! Diff fill: under [`DiffRetention::Retain`](crate::retention::DiffRetention),
//! store patch text that is missing for pull requests already in the
//! warehouse, a bounded number per tick.
//!
//! A pull request is owed its diff when it has stored file rows, none of them
//! holds patch text, and no stored diff is recorded for it
//! (`pr_detail_state.diff_stored_at`). So a pull request synced while another
//! caller ran `Transient` or `Never` is filled in by later `Retain` runs, and
//! one whose diff was stored and later purged is not fetched again. Each
//! attempt is recorded in `collector_diff_fill`; a pull request is tried at
//! most once per [`RETRY_AFTER_HOURS`]. Only the REST files endpoint is
//! called, under the client's rate cap. Writes only fill in: no stored patch
//! is replaced or blanked.

use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, params};
use serde::Serialize;

use crate::error::Result;
use crate::fetch::GhClient;
use crate::fetch::pull_requests;
use crate::storage::repository;

/// Hours before a pull request whose fill attempt failed is tried again.
pub const RETRY_AFTER_HOURS: i64 = 24;

/// What the diff-fill lane did in one tick.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct DiffFillSummary {
    /// Why it did not run, when it was due and did not.
    pub skipped: Option<String>,
    /// Pull requests whose patch text was stored.
    pub filled: Vec<String>,
    /// Pull requests whose attempt failed, with the error. Retried after
    /// [`RETRY_AFTER_HOURS`].
    pub failed: Vec<(String, String)>,
    /// Pull requests still owed a diff after this tick, retry wait included.
    pub remaining: i64,
}

const OWED: &str = "FROM fact_pull_requests p
     LEFT JOIN pr_detail_state s ON s.pr_key = p.pr_key
     WHERE s.diff_stored_at IS NULL
       AND EXISTS (SELECT 1 FROM fact_file_diffs f WHERE f.pr_key = p.pr_key)
       AND NOT EXISTS (SELECT 1 FROM fact_file_diffs f
                       WHERE f.pr_key = p.pr_key AND f.patch IS NOT NULL)";

/// Pull requests owed a diff, including ones waiting out a failed attempt.
pub fn owed_count(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(&format!("SELECT COUNT(*) {OWED}"), [], |r| r.get(0))?)
}

/// Up to `limit` pull requests owed a diff and not attempted since
/// `retry_before`, newest update first: `(pr_key, repo_key, number)`.
fn owed(
    conn: &Connection,
    limit: usize,
    retry_before: DateTime<Utc>,
) -> Result<Vec<(String, String, i64)>> {
    let mut statement = conn.prepare(&format!(
        "SELECT p.pr_key, p.repo_key, p.number {OWED}
           AND NOT EXISTS (SELECT 1 FROM collector_diff_fill a
                           WHERE a.pr_key = p.pr_key AND a.attempted_at >= ?1)
         ORDER BY COALESCE(p.updated_at, p.created_at) DESC
         LIMIT ?2"
    ))?;
    let rows = statement.query_map(params![super::text(retry_before), limit as i64], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn record_attempt(conn: &Connection, pr_key: &str, error: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT INTO collector_diff_fill (pr_key, attempted_at, attempts, last_error)
         VALUES (?1, ?2, 1, ?3)
         ON CONFLICT (pr_key) DO UPDATE SET
             attempted_at = excluded.attempted_at,
             attempts = collector_diff_fill.attempts + 1,
             last_error = excluded.last_error",
        params![pr_key, super::text(Utc::now()), error],
    )?;
    Ok(())
}

/// Fetch and store the patch text for one pull request. The network call
/// comes first; every write lands in one savepoint.
fn fill_one(
    conn: &Connection,
    client: &mut GhClient,
    pr_key: &str,
    repo_key: &str,
    number: i64,
) -> Result<()> {
    let listing = pull_requests::fetch_file_patches(client, repo_key, number)?;
    conn.execute_batch("SAVEPOINT githubdw_diff_fill")?;
    let written = (|| -> Result<()> {
        for (path, previous_path, patch) in &listing.files {
            repository::set_file_diff_patch_preserving(
                conn,
                pr_key,
                path,
                previous_path.as_deref(),
                patch.as_deref(),
            )?;
        }
        conn.execute(
            "INSERT INTO pr_detail_state (pr_key, diff_stored_at) VALUES (?1, ?2)
             ON CONFLICT (pr_key) DO UPDATE SET
                 diff_stored_at = COALESCE(pr_detail_state.diff_stored_at, excluded.diff_stored_at)",
            params![pr_key, super::text(Utc::now())],
        )?;
        record_attempt(conn, pr_key, None)
    })();
    match written {
        Ok(()) => {
            conn.execute_batch("RELEASE SAVEPOINT githubdw_diff_fill")?;
            Ok(())
        }
        Err(error) => {
            conn.execute_batch(
                "ROLLBACK TO SAVEPOINT githubdw_diff_fill; RELEASE SAVEPOINT githubdw_diff_fill",
            )?;
            Err(error)
        }
    }
}

/// Fill up to `limit` owed diffs. `keep_going` is asked before each pull
/// request (budget, cancellation, lease); a run-fatal error (rate limit,
/// cancellation, database) ends the lane and is returned, any other error is
/// recorded against its pull request.
pub(super) fn fill(
    conn: &Connection,
    client: &mut GhClient,
    limit: usize,
    mut keep_going: impl FnMut(&mut GhClient) -> Option<String>,
    summary: &mut DiffFillSummary,
) -> Result<()> {
    let work = owed(conn, limit, Utc::now() - Duration::hours(RETRY_AFTER_HOURS))?;
    for (pr_key, repo_key, number) in work {
        if let Some(why) = keep_going(client) {
            summary.skipped = Some(why);
            break;
        }
        match fill_one(conn, client, &pr_key, &repo_key, number) {
            Ok(()) => summary.filled.push(pr_key),
            Err(error) if error.is_run_fatal() => return Err(error),
            Err(error) => {
                record_attempt(conn, &pr_key, Some(&error.to_string()))?;
                summary.failed.push((pr_key, error.to_string()));
            }
        }
    }
    Ok(())
}
