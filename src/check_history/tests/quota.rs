use super::*;

/// A queued fixture response carrying `x-ratelimit-*` headers.
fn hdr(remaining: i64, body: Result<String>) -> Result<String> {
    body.map(|body| with_rate_headers("core", remaining, &body))
}

fn gql(remaining: i64, body: Result<String>) -> Result<String> {
    body.map(|body| with_rate_headers("graphql", remaining, &body))
}

#[test]
fn low_observed_quota_stops_the_batch_with_resumable_not_attempted_targets() {
    // The first data response says the bucket is nearly spent: the target
    // writes nothing, the next is not attempted, no lock is left.
    let conn = warehouse();
    let (mut client, transport) = shared_client(vec![
        hdr(104, Ok(pull_json(H2, 1, "o/r"))),
        hdr(103, Ok(json!([{"sha": H2}]).to_string())),
        gql(4000, no_force_pushes()),
    ]);
    let report =
        repair_check_history(&conn, &mut client, &[target(), nine()], &guarded(100)).unwrap();
    assert!(report.stopped.is_some());
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert_eq!(report.outcomes[0].rows_unchanged, Some(true));
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .unwrap()
            .contains("headroom")
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert_eq!(report.not_complete(), 2);
    assert_eq!(transport.calls.lock().unwrap().len(), 3);
    assert_eq!(lock_count(&conn), 0);
}

#[test]
fn a_data_response_without_rate_limit_headers_fails_closed() {
    let conn = warehouse();
    let (mut client, transport) = shared_client(vec![Ok(pull_json(H2, 1, "o/r"))]);
    let report =
        repair_check_history(&conn, &mut client, &[target(), nine()], &guarded(100)).unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .unwrap()
            .contains("no rate-limit headers"),
        "{:?}",
        report.outcomes[0]
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert_eq!(transport.calls.lock().unwrap().len(), 1);
    assert!(transport.calls.lock().unwrap()[0].contains(&"--include".to_string()));
}

#[test]
fn a_first_call_403_stops_the_batch_without_touching_rows_or_the_lock() {
    // Before any quota is observed the first call is a single probe: a 403
    // is not retried, stops the whole batch, and leaves rows and lock alone.
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    let mut responses = Vec::new();
    for _ in 0..7 {
        responses.push(Err(Error::GhCommand {
            exit_code: Some(1),
            stderr: "gh: Resource not accessible (HTTP 403)".into(),
        }));
    }
    let (mut client, transport) = shared_client(responses);
    let report =
        repair_check_history(&conn, &mut client, &[target(), nine()], &guarded(100)).unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed);
    assert_eq!(first.rows_unchanged, Some(true));
    assert!(
        first.error.as_deref().unwrap().contains("HTTP 403"),
        "{first:?}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("quota/permission")
    );
    assert_eq!(
        transport.calls.lock().unwrap().len(),
        1,
        "no call after the stop"
    );
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_a".into(), H1.into())]);
    assert_eq!(lock_count(&conn), 0);
}

#[test]
fn a_rate_limit_403_after_observation_spends_retries_against_the_reserve() {
    // Observed 103 with reserve 100: three attempts are left, retries
    // included; the fourth is refused locally and never sent.
    let conn = warehouse();
    let mut responses = vec![hdr(103, Ok(pull_json(H2, 1, "o/r")))];
    for _ in 0..7 {
        responses.push(Err(Error::GhCommand {
            exit_code: Some(1),
            stderr: "gh: API rate limit exceeded (HTTP 403)".into(),
        }));
    }
    let (mut client, transport) = shared_client(responses);
    let report = repair_check_history(&conn, &mut client, &[target()], &guarded(100)).unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .unwrap()
            .contains("reserve"),
        "{:?}",
        report.outcomes[0]
    );
    assert_eq!(transport.calls.lock().unwrap().len(), 4);
}

#[test]
fn the_reserve_follows_each_response_not_a_separate_rate_limit_read() {
    // Real probe: rate_limit said 4999 while data headers said 4349. Here
    // the headers drop under the walk (another consumer spending) and the
    // walk stops on the observed value, before the reserve.
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    ROUTED_REMAINING.with(|r| r.set(103));
    let responses = vec![
        hdr(140, Ok(pull_json(H2, 2, "o/r"))),
        hdr(139, Ok(json!([{"sha": H1}, {"sha": H2}]).to_string())),
        gql(4000, no_force_pushes()),
        hdr(138, runs(vec![run("CR_a", H1)])),
        hdr(137, suites()),
        // Another consumer spent 33 between two of our responses.
        hdr(104, owners(&[])),
    ];
    let (mut client, transport) = shared_client(responses);
    let report = repair_check_history(&conn, &mut client, &[target()], &guarded(100)).unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Failed, "{outcome:?}");
    assert_eq!(outcome.rows_unchanged, Some(true));
    assert!(
        outcome.error.as_deref().unwrap().contains("reserve"),
        "{outcome:?}"
    );
    // H1's workflow runs answered 103: H2 is never started.
    assert_eq!(transport.calls.lock().unwrap().len(), 7);
    assert_eq!(status_of(&conn, "CR_a"), "IN_PROGRESS");
    assert_eq!(lock_count(&conn), 0);
}

#[test]
fn incomplete_alone_counts_as_not_complete() {
    let conn = warehouse();
    let mut client = client(current_head_only(vec![run_in("CR_nob", H2, 5)]));
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.count(HistoryStatus::Incomplete), 1);
    assert_eq!(report.not_complete(), 1);
}

#[test]
fn a_stale_lock_is_taken_over_and_only_our_lease_is_released() {
    let conn = warehouse();
    conn.execute(
        "INSERT INTO sync_locks (entity_key, started_at) VALUES ('repo:o/r', ?1)",
        [(chrono::Utc::now() - chrono::Duration::minutes(45)).to_rfc3339()],
    )
    .unwrap();
    let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
    assert_eq!(outcome.status, HistoryStatus::Complete);
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 0);
}

#[test]
fn a_lease_lost_to_another_owner_is_never_released_or_renewed() {
    let conn = warehouse();
    let token = sync_state::acquire_owned_lock(&conn, "repo:o/r").unwrap();
    // A fresh lock refuses a second owner.
    assert!(matches!(
        sync_state::acquire_owned_lock(&conn, "repo:o/r"),
        Err(Error::SyncInProgress(_))
    ));
    let renewed = sync_state::refresh_owned_lock(&conn, "repo:o/r", &token)
        .unwrap()
        .unwrap();
    // Another process takes the lock over (e.g. after a stale window).
    conn.execute(
        "UPDATE sync_locks SET started_at = 'other-owner' WHERE entity_key = 'repo:o/r'",
        [],
    )
    .unwrap();
    assert_eq!(
        sync_state::refresh_owned_lock(&conn, "repo:o/r", &renewed).unwrap(),
        None
    );
    assert!(!sync_state::release_owned_lock(&conn, "repo:o/r", &renewed).unwrap());
    let holder: String = conn
        .query_row("SELECT started_at FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(holder, "other-owner");
}
