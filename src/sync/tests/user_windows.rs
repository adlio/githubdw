use super::*;

#[test]
fn user_update_window_failure_retains_checkpoint_and_cursor() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    let now = evening_instant();
    let base = search_timestamp(now.timestamp());
    sync_state::advance_cursor(conn, "user", "octocat", Some(&base)).unwrap();
    let (mut client, _) = shared_client(vec![Ok(search_response(
        1,
        vec![search_node(1, "acme/widgets", "bad-created-date", &base)],
    ))]);
    let mut summary = SyncSummary::default();
    Syncer::new(conn, &mut client)
        .as_of(now)
        .walk_user_update_windows(
            "octocat",
            UserRole::Author,
            &base,
            "user:octocat",
            &fast_user_options(None),
            &mut summary,
        )
        .unwrap();
    assert!(summary.partial);
    assert_eq!(summary.failed.len(), 1);
    let saved: UserUpdateCheckpoint = serde_json::from_str(
        &sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(saved.pending, vec![(now.timestamp(), now.timestamp())]);
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
        Some(base)
    );
}

#[test]
fn user_update_windows_ingest_complete_search_results_with_newer_node_timestamps() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    let now = evening_instant();
    let base = search_timestamp(now.timestamp() - 60);
    let later = search_timestamp(now.timestamp() + 86400);
    // GitHub can match the older search-index timestamp while returning a
    // newer PR node. This reproduces the live #14430 catch-up blocker.
    let (mut client, _) = shared_client(vec![Ok(search_response(
        2,
        vec![
            search_node(1, "acme/widgets", "2026-08-14T18:00:00Z", &base),
            search_node(2, "acme/widgets", "2026-08-14T18:00:00Z", &later),
        ],
    ))]);
    let mut summary = SyncSummary::default();
    Syncer::new(conn, &mut client)
        .as_of(now)
        .walk_user_update_windows(
            "octocat",
            UserRole::Author,
            &base,
            "user:octocat",
            &fast_user_options(None),
            &mut summary,
        )
        .unwrap();
    assert!(!summary.partial);
    assert_eq!(summary.pull_requests_synced, 2);
    assert_eq!(count(conn, "SELECT count(*) FROM fact_pull_requests"), 2);
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
        Some(search_timestamp(now.timestamp())),
        "a newer returned node must not advance coverage beyond the frozen search bound"
    );
    assert!(
        sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
            .unwrap()
            .is_none()
    );
}

#[test]
fn user_update_windows_keep_unsplittable_ties_and_incomplete_responses_open() {
    for response in [search_response(1001, vec![]), search_response(1, vec![])] {
        let warehouse = los_angeles_warehouse();
        let conn = warehouse.connection();
        let now = evening_instant();
        let base = search_timestamp(now.timestamp());
        let (mut client, _) = shared_client(vec![Ok(response)]);
        let mut summary = SyncSummary::default();
        Syncer::new(conn, &mut client)
            .as_of(now)
            .walk_user_update_windows(
                "octocat",
                UserRole::Author,
                &base,
                "user:octocat",
                &fast_user_options(None),
                &mut summary,
            )
            .unwrap();
        assert!(summary.partial);
        assert!(
            sync_state::last_updated_cursor(conn, "user", "octocat")
                .unwrap()
                .is_none()
        );
        assert!(
            sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn user_incremental_success_covers_days_missed_between_runs() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    let now = evening_instant();
    sync_state::record_range_as_of(conn, "user:octocat", "2026-08-01", "2026-08-15", 0, now)
        .unwrap();
    for role in UserRole::all() {
        sync_state::advance_cursor(
            conn,
            role.cursor_source_type(),
            "octocat",
            Some("2026-08-15T09:00:00Z"),
        )
        .unwrap();
    }
    let (mut client, _) = shared_client(vec![
        Ok(search_response(0, vec![])),
        Ok(search_response(0, vec![])),
    ]);
    let summary = Syncer::new(conn, &mut client)
        .as_of(now + Duration::days(3))
        .sync_user("octocat", &fast_user_options(None))
        .unwrap();
    assert!(!summary.partial);
    let extent = sync_state::coverage_extent_as_of(conn, "user:octocat", now + Duration::days(3))
        .unwrap()
        .unwrap();
    assert_eq!(extent.1.to_string(), "2026-08-18");
    assert_eq!(
        count(
            conn,
            "SELECT count(*) FROM synced_ranges WHERE entity_key='user:octocat'"
        ),
        1,
        "missed days join the existing range, not an isolated latest day"
    );
}

#[test]
fn user_update_checkpoint_survives_transport_failure_and_restarts_invalid_state() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    let now = evening_instant();
    let base = search_timestamp(now.timestamp() - 1);
    let saved = UserUpdateCheckpoint {
        version: CHECKPOINT_VERSION,
        base: base.clone(),
        upper: now.timestamp(),
        pending: vec![(now.timestamp(), now.timestamp())],
    };
    sync_state::save_checkpoint(
        conn,
        "user_update_checkpoint",
        "author:octocat",
        &serde_json::to_string(&saved).unwrap(),
    )
    .unwrap();
    let (mut client, _) = shared_client(vec![]);
    let mut summary = SyncSummary::default();
    assert!(
        Syncer::new(conn, &mut client)
            .as_of(now)
            .walk_user_update_windows(
                "octocat",
                UserRole::Author,
                &base,
                "user:octocat",
                &fast_user_options(None),
                &mut summary,
            )
            .is_err()
    );
    let after: UserUpdateCheckpoint = serde_json::from_str(
        &sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(after.pending, saved.pending);
    assert!(
        sync_state::last_updated_cursor(conn, "user", "octocat")
            .unwrap()
            .is_none()
    );
    // Malformed bounds must restart at the real cursor, never panic or skip.
    let bad = UserUpdateCheckpoint {
        pending: vec![(i64::MAX, i64::MAX), (0, i64::MAX)],
        ..saved
    };
    sync_state::save_checkpoint(
        conn,
        "user_update_checkpoint",
        "author:octocat",
        &serde_json::to_string(&bad).unwrap(),
    )
    .unwrap();
    let (mut client, transport) = shared_client(vec![Ok(search_response(0, vec![]))]);
    Syncer::new(conn, &mut client)
        .as_of(now)
        .walk_user_update_windows(
            "octocat",
            UserRole::Author,
            &base,
            "user:octocat",
            &fast_user_options(None),
            &mut summary,
        )
        .unwrap();
    let calls = transport.calls.lock().unwrap();
    assert!(calls[0].iter().any(|a| a.contains(&format!(
        "updated:{base}..{}",
        search_timestamp(now.timestamp())
    ))));
    assert!(
        sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
            .unwrap()
            .is_none()
    );
}

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
