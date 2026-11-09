use super::*;
use crate::{Error, Result};

fn lock_rows(conn: &rusqlite::Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap()
}

fn failing_user_sync(conn: &rusqlite::Connection) -> Result<SyncSummary> {
    monitor_repository::add_user(conn, "octocat").unwrap();
    // No canned responses: the first search fails with "no more fixture responses".
    let transport = FixtureTransport::new(vec![]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    Syncer::new(conn, &mut client)
        .as_of(evening_instant())
        .sync_user("octocat", &fast_user_options(Some(1)))
}

const BREAK_JOB_WRITES: &str = "CREATE TRIGGER break_job BEFORE UPDATE ON sync_jobs
    BEGIN SELECT RAISE(ABORT, 'job write broke'); END";

#[test]
fn a_failing_job_record_releases_the_lock_and_keeps_the_walk_error() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    conn.execute(BREAK_JOB_WRITES, []).unwrap();
    let error = failing_user_sync(conn).unwrap_err().to_string();
    assert!(error.contains("no more fixture responses"), "{error}");
    assert!(!error.contains("job write broke"), "{error}");
    assert_eq!(lock_rows(conn), 0);
}

#[test]
fn a_failing_completion_record_releases_the_lock_and_is_reported() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    monitor_repository::add_user(conn, "octocat").unwrap();
    conn.execute(BREAK_JOB_WRITES, []).unwrap();
    let transport = FixtureTransport::new(vec![
        Ok(search_response(0, vec![])),
        Ok(search_response(0, vec![])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let error = Syncer::new(conn, &mut client)
        .as_of(evening_instant())
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap_err()
        .to_string();
    assert!(error.contains("job write broke"), "{error}");
    assert_eq!(lock_rows(conn), 0);
}

#[test]
fn a_live_lock_refuses_a_second_sync_and_is_left_in_place() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    monitor_repository::add_user(conn, "octocat").unwrap();
    let _held = sync_state::SyncLease::acquire(conn, "user:octocat").unwrap();
    let error = failing_user_sync(conn).unwrap_err();
    assert!(matches!(error, Error::SyncInProgress(_)), "{error}");
    assert_eq!(lock_rows(conn), 1);
}
