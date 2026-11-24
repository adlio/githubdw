//! Collector leases: who is detecting or backfilling right now.
//!
//! A lease row names its holder by pid, the pid's process start time, host
//! and program. It is free once it expires, or at once when its holder on
//! this host is gone: the pid no longer exists, or now belongs to a process
//! that started at a different time (pid reuse). The holder renews it after
//! every request, and the tick never sleeps longer than
//! [`super::MAX_WAIT_INSIDE_LEASE`], so a live holder never ages out.

use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::error::Result;

/// A lease's holder, for "GitHub checked by …" and `Busy` reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LeaseHolder {
    /// `detect` or `backfill`.
    pub lane: String,
    /// What the holder called itself in [`super::TickPolicy::program`].
    pub program: String,
    pub pid: i64,
    pub host: String,
    pub acquired_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// A held lease. Dropping it releases it.
pub(crate) struct Lease<'c> {
    conn: &'c Connection,
    lane: String,
    token: String,
    ttl: Duration,
    released: bool,
}

/// This host's name, as the kernel reports it.
pub(crate) fn host_name() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

/// The start time of `pid` in clock ticks since boot (Linux), which tells a
/// reused pid apart from the original holder. `None` elsewhere.
pub(crate) fn process_start(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name is parenthesised and may contain spaces; fields after
    // the last ')' are space separated, starting at field 3.
    let rest = &stat[stat.rfind(')')? + 1..];
    rest.split_whitespace().nth(19).map(str::to_string)
}

fn parse(text: &str) -> DateTime<Utc> {
    text.parse::<DateTime<Utc>>()
        .unwrap_or(DateTime::UNIX_EPOCH)
}

fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

struct Row {
    pid: i64,
    start: Option<String>,
    host: String,
    program: String,
    acquired_at: String,
    expires_at: String,
}

fn read_row(conn: &Connection, lane: &str) -> Result<Option<Row>> {
    Ok(conn
        .query_row(
            "SELECT holder_pid, holder_start, host, program, acquired_at, expires_at
             FROM collector_leases WHERE lane = ?1",
            [lane],
            |r| {
                Ok(Row {
                    pid: r.get(0)?,
                    start: r.get(1)?,
                    host: r.get(2)?,
                    program: r.get(3)?,
                    acquired_at: r.get(4)?,
                    expires_at: r.get(5)?,
                })
            },
        )
        .optional()?)
}

/// True while the row's holder may still be working.
fn live(row: &Row, now: DateTime<Utc>) -> bool {
    if parse(&row.expires_at) <= now {
        return false;
    }
    if row.host != host_name() {
        // Another machine: only the expiry can say.
        return true;
    }
    let Ok(pid) = u32::try_from(row.pid) else {
        return false;
    };
    if !crate::storage::sync_state_repository::process_alive(pid) {
        return false;
    }
    match (&row.start, process_start(pid)) {
        (Some(recorded), Some(current)) => *recorded == current,
        _ => true,
    }
}

fn holder(lane: &str, row: Row) -> LeaseHolder {
    LeaseHolder {
        lane: lane.to_string(),
        program: row.program,
        pid: row.pid,
        host: row.host,
        acquired_at: parse(&row.acquired_at),
        expires_at: parse(&row.expires_at),
    }
}

/// The live holder of `lane`, if any.
pub fn lease_holder(conn: &Connection, lane: &str) -> Result<Option<LeaseHolder>> {
    let now = Utc::now();
    Ok(read_row(conn, lane)?
        .filter(|row| live(row, now))
        .map(|row| holder(lane, row)))
}

impl<'c> Lease<'c> {
    /// Take `lane` for `program`, or return the live holder. The read and the
    /// write happen in one `BEGIN IMMEDIATE` transaction, so two processes
    /// racing for a free lease cannot both win.
    pub(crate) fn acquire(
        conn: &'c Connection,
        lane: &str,
        program: &str,
        ttl: Duration,
    ) -> Result<std::result::Result<Self, LeaseHolder>> {
        let own = conn.is_autocommit();
        if own {
            conn.execute_batch("BEGIN IMMEDIATE")?;
        }
        let outcome = (|| -> Result<std::result::Result<String, LeaseHolder>> {
            let now = Utc::now();
            if let Some(row) = read_row(conn, lane)?
                && live(&row, now)
            {
                return Ok(Err(holder(lane, row)));
            }
            let token = format!("{}-{}", std::process::id(), stamp(now));
            let pid = std::process::id();
            conn.execute(
                "INSERT INTO collector_leases
                     (lane, token, holder_pid, holder_start, host, program, acquired_at, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT (lane) DO UPDATE SET
                     token = excluded.token, holder_pid = excluded.holder_pid,
                     holder_start = excluded.holder_start, host = excluded.host,
                     program = excluded.program, acquired_at = excluded.acquired_at,
                     expires_at = excluded.expires_at",
                params![
                    lane,
                    token,
                    pid,
                    process_start(pid),
                    host_name(),
                    program,
                    stamp(now),
                    stamp(now + ttl)
                ],
            )?;
            Ok(Ok(token))
        })();
        if own {
            match &outcome {
                Ok(_) => conn.execute_batch("COMMIT")?,
                Err(_) => {
                    let _ = conn.execute_batch("ROLLBACK");
                }
            }
        }
        Ok(outcome?.map(|token| Self {
            conn,
            lane: lane.to_string(),
            token,
            ttl,
            released: false,
        }))
    }

    /// Push the expiry out by the lease's TTL. `false` when the lease is no
    /// longer ours (it expired and another process took it).
    pub(crate) fn renew(&self) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE collector_leases SET expires_at = ?3 WHERE lane = ?1 AND token = ?2",
            params![self.lane, self.token, stamp(Utc::now() + self.ttl)],
        )?;
        Ok(changed == 1)
    }

    pub(crate) fn release(mut self) -> Result<()> {
        self.released = true;
        self.delete()
    }

    fn delete(&self) -> Result<()> {
        self.conn.execute(
            "DELETE FROM collector_leases WHERE lane = ?1 AND token = ?2",
            params![self.lane, self.token],
        )?;
        Ok(())
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if !self.released {
            // Nowhere to report from a drop; the lease then expires.
            let _ = self.delete();
        }
    }
}
