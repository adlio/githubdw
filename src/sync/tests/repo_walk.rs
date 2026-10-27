use super::*;

#[test]
fn syncs_two_pages_and_records_state() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let transport = FixtureTransport::new(vec![
        Ok(page_response(true, Some("C1"), &[1, 2])),
        Ok(page_response(false, None, &[3])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let options = fast_pr_options();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

    assert_eq!(summary.pull_requests_synced, 3);
    assert_eq!(summary.pages_fetched, 2);
    assert!(summary.failed.is_empty());

    let conn = warehouse.connection();
    let pr_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM fact_pull_requests", [], |r| r.get(0))
        .unwrap();
    assert_eq!(pr_count, 3);
    let review_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM fact_reviews", [], |r| r.get(0))
        .unwrap();
    assert_eq!(review_count, 3);
    let diff_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM fact_file_diffs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(diff_count, 3);

    // Job completed, lock released, range recorded, cursor advanced.
    let status: String = conn
        .query_row(
            "SELECT status FROM sync_jobs WHERE entity_key = 'repo:octocat/hello'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "completed");
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 0);
    let cursor = sync_state::last_updated_cursor(conn, "repo", "octocat/hello").unwrap();
    assert_eq!(cursor.as_deref(), Some("2026-01-08T09:00:00Z"));
}

#[test]
fn incremental_rerun_stops_at_cursor() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let transport = FixtureTransport::new(vec![
        Ok(page_response(false, None, &[1, 2])),
        // Second run returns the same (unchanged) PRs.
        Ok(page_response(false, None, &[1, 2])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let options = fast_pr_options();

    {
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let first = syncer.sync_repository("octocat/hello", &options).unwrap();
        assert_eq!(first.pull_requests_synced, 2);
    }
    {
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let second = syncer.sync_repository("octocat/hello", &options).unwrap();
        assert_eq!(second.pull_requests_synced, 0, "no-op on unchanged data");
        assert!(second.up_to_date);
    }
}

/// An item updated in the *same second* as the cursor was late to the
/// previous run's page. The stop must be strict so the boundary second is
/// re-read, otherwise that item is skipped for good: nothing short of
/// another update (which moves its timestamp) would ever surface it.
#[test]
fn same_second_late_update_is_not_lost() {
    let boundary = "2026-01-10T10:00:00Z";
    let older = "2026-01-09T10:00:00Z";
    let warehouse = GithubDW::open_in_memory().unwrap();
    let transport = FixtureTransport::new(vec![
        // Run 1 sees only PR 1; the cursor lands exactly on `boundary`.
        Ok(page_of(
            false,
            None,
            vec![pr_node(1, "2026-01-05T18:00:00Z", boundary)],
        )),
        // Run 2: PR 2 was updated in the same second but missed page 1,
        // followed by the already-ingested PR 1 and a strictly older PR 3.
        Ok(page_of(
            false,
            None,
            vec![
                pr_node(2, "2026-01-05T18:00:00Z", boundary),
                pr_node(1, "2026-01-05T18:00:00Z", boundary),
                pr_node(3, "2026-01-05T18:00:00Z", older),
            ],
        )),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let options = fast_pr_options();

    {
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        syncer.sync_repository("octocat/hello", &options).unwrap();
    }
    {
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        syncer.sync_repository("octocat/hello", &options).unwrap();
    }

    let conn = warehouse.connection();
    let numbers: Vec<i64> = conn
        .prepare("SELECT number FROM fact_pull_requests ORDER BY number")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(
        numbers,
        vec![1, 2],
        "the same-second item is picked up, and the strictly older one \
         still terminates the walk"
    );
}

/// The cursor must not move past an item that failed to persist. If it
/// does, the next run's stop condition fires before reaching that item and
/// the gap never heals.
#[test]
fn cursor_does_not_advance_past_a_failed_upsert() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    // Newest-first, as GitHub serves it. PR 9 has an unparseable
    // `createdAt`, so its upsert fails; PR 8 is fine.
    let transport = FixtureTransport::new(vec![Ok(page_of(
        false,
        None,
        vec![
            pr_node(9, "not-a-timestamp", "2026-01-20T10:00:00Z"),
            pr_node(8, "2026-01-05T18:00:00Z", "2026-01-19T10:00:00Z"),
        ],
    ))]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let options = fast_pr_options();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

    assert_eq!(summary.pull_requests_synced, 1);
    assert_eq!(summary.failed.len(), 1, "PR 9 is reported as failed");

    let cursor =
        sync_state::last_updated_cursor(warehouse.connection(), "repo", "octocat/hello").unwrap();
    assert_eq!(
        cursor.as_deref(),
        Some("2026-01-19T10:00:00Z"),
        "the watermark stays below the failed item so it is retried"
    );
}

#[test]
fn repository_path_follows_nested_connection_cursors() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let (mut client, _) = routing_client(
        vec![page_of(false, None, vec![heavy_pr_node(5)])],
        heavy_pr_continuations(),
    );
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    assert!(summary.notes.is_empty(), "{:?}", summary.notes);
    let pr_key: String = warehouse
        .connection()
        .query_row("SELECT pr_key FROM fact_pull_requests", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_heavy_pr_complete(warehouse.connection(), &pr_key);
}

/// A failure in the MIDDLE of an updated-desc walk must cap the committed
/// cursor at the failed item. Taking the newest landed item instead would
/// put the cursor above the failure and the next run would stop before it.
#[test]
fn failed_middle_item_caps_the_committed_cursor() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let (mut client, _) = shared_client(vec![
        Ok(page_of(
            false,
            None,
            vec![
                pr_node(9, "2026-01-05T18:00:00Z", "2026-01-20T10:00:00Z"),
                pr_node(8, "not-a-timestamp", "2026-01-19T10:00:00Z"),
                pr_node(7, "2026-01-05T18:00:00Z", "2026-01-18T10:00:00Z"),
            ],
        )),
        // Run 2: PR 8 is fixed and still at its old timestamp.
        Ok(page_of(
            false,
            None,
            vec![
                pr_node(9, "2026-01-05T18:00:00Z", "2026-01-20T10:00:00Z"),
                pr_node(8, "2026-01-05T18:00:00Z", "2026-01-19T10:00:00Z"),
                pr_node(7, "2026-01-05T18:00:00Z", "2026-01-18T10:00:00Z"),
            ],
        )),
    ]);
    let options = fast_pr_options();
    let first = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert_eq!(first.failed.len(), 1);
    assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-19T10:00:00Z"));

    Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM fact_pull_requests WHERE number = 8"
        ),
        1,
        "the failed middle item is retried and lands"
    );
    assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-20T10:00:00Z"));
}

/// A budgeted run yields at a page boundary without claiming coverage or
/// moving the cursor; the next run continues. An item updated between the
/// runs jumps to the front of the updated-desc order, past the resume
/// position — the committed watermark stays at or below its new
/// timestamp, so the following walk picks it up.
#[test]
fn budgeted_walk_yields_resumes_and_catches_items_that_moved() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let options = fast_pr_options();

    let (mut client, _) = shared_client(vec![Ok(page_response(true, Some("C1"), &[10, 9]))]);
    let first = Syncer::new(conn, &mut client)
        .with_item_budget(2)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert!(first.partial);
    assert!(!first.up_to_date);
    assert_eq!(pr_cursor(conn), None);
    assert_eq!(count(conn, "SELECT COUNT(*) FROM synced_ranges"), 0);
    let status: String = conn
        .query_row(
            "SELECT status FROM sync_jobs WHERE entity_key = 'repo:octocat/hello'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "partial");
    assert_eq!(count(conn, "SELECT COUNT(*) FROM sync_locks"), 0);

    // Between runs PR 7 is edited and moves to the front, so the resumed
    // page no longer contains it.
    let (mut client, transport) = shared_client(vec![Ok(page_response(false, None, &[8]))]);
    let second = Syncer::new(conn, &mut client)
        .with_item_budget(2)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert_eq!(requested_cursors(&transport), vec![Some("C1".to_string())]);
    assert!(!second.partial);
    assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-15T09:00:00Z"));
    assert!(saved_checkpoint(conn).is_none());

    // Run 3: PR 7 at its new time leads the walk; PR 10 sits exactly on
    // the cursor (same second, re-read); PR 9 is strictly older and stops.
    let moved = "2026-01-21T09:00:00Z";
    let (mut client, _) = shared_client(vec![Ok(page_of(
        false,
        None,
        vec![
            pr_node(7, "2026-01-05T18:00:00Z", moved),
            pr_node(10, "2026-01-05T18:00:00Z", "2026-01-15T09:00:00Z"),
            pr_node(9, "2026-01-05T18:00:00Z", "2026-01-14T09:00:00Z"),
        ],
    ))]);
    let third = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert_eq!(third.pull_requests_synced, 2);
    let stored: String = conn
        .query_row(
            "SELECT updated_at FROM fact_pull_requests WHERE number = 7",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, moved);
    assert_eq!(pr_cursor(conn).as_deref(), Some(moved));
}

/// A saved page cursor GitHub rejects restarts the walk from the newest
/// page once, keeping its base and watermark, rather than failing every
/// future run on the same dead cursor.
#[test]
fn rejected_saved_cursor_restarts_the_walk_once() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let options = fast_pr_options();
    let (mut client, _) = shared_client(vec![Ok(page_response(true, Some("C1"), &[10, 9]))]);
    Syncer::new(conn, &mut client)
        .with_item_budget(1)
        .sync_repository("octocat/hello", &options)
        .unwrap();

    let invalid = json!({"data": null, "errors": [
        {"message": "`C1` does not appear to be a valid cursor."}
    ]})
    .to_string();
    let (mut client, transport) = shared_client(vec![
        Ok(invalid),
        Ok(page_response(false, None, &[10, 9, 8])),
    ]);
    let summary = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert_eq!(
        requested_cursors(&transport),
        vec![Some("C1".to_string()), None],
        "one rejected resume, then one restart from the newest page"
    );
    assert!(summary.notes.iter().any(|note| note.contains("rejected")));
    assert_eq!(pr_cursor(conn).as_deref(), Some("2026-01-15T09:00:00Z"));
    assert!(saved_checkpoint(conn).is_none());
}

/// A checkpoint that is corrupt, or was taken from a different starting
/// cursor, is not trusted: the walk starts from the newest page.
#[test]
fn foreign_or_corrupt_checkpoint_is_discarded() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let options = fast_pr_options();
    for stale in [
        "not json".to_string(),
        serde_json::to_string(&PullRequestWalkCheckpoint {
            version: CHECKPOINT_VERSION,
            base: Some("2020-01-01T00:00:00Z".into()),
            after: "OLD".into(),
            watermark: WalkWatermark::default(),
            pages: 9,
            items: 9,
            restarts: 0,
        })
        .unwrap(),
    ] {
        sync_state::save_checkpoint(conn, REPO_PR_CHECKPOINT, "octocat/hello", &stale).unwrap();
        let (mut client, transport) = shared_client(vec![Ok(page_response(false, None, &[10]))]);
        Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(requested_cursors(&transport), vec![None]);
        assert!(saved_checkpoint(conn).is_none());
    }
}

#[test]
fn watermark_commits_below_the_oldest_failure() {
    let mut watermark = WalkWatermark::default();
    assert_eq!(watermark.committable(), None);
    watermark.landed(Some("2026-01-03T00:00:00Z"));
    watermark.failed(Some("2026-01-02T00:00:00Z"));
    watermark.landed(Some("2026-01-01T00:00:00Z"));
    watermark.failed(Some("2026-01-02T12:00:00Z"));
    assert_eq!(
        watermark.committable().as_deref(),
        Some("2026-01-02T00:00:00Z")
    );
    watermark.failed(None);
    assert_eq!(
        watermark.committable(),
        None,
        "an unbounded failure pins the cursor"
    );
}

/// Coverage is recorded in the warehouse's own calendar and stops at the
/// last completed day there. The injected instant is evening in Los
/// Angeles, which is already the next day in UTC — so a UTC-derived
/// "today" would seal two days that are not finished locally.
#[test]
fn recorded_coverage_stops_at_the_last_complete_local_day() {
    let warehouse = los_angeles_warehouse();
    let transport = FixtureTransport::new(vec![Ok(page_response(false, None, &[1]))]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let options = fast_pr_options();

    // 2026-08-16 20:00 PDT.
    let instant = chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 8, 17, 3, 0, 0).unwrap();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(instant);
    syncer.sync_repository("octocat/hello", &options).unwrap();

    let (start, end): (String, String) = warehouse
        .connection()
        .query_row(
            "SELECT start_date, end_date FROM synced_ranges
             WHERE entity_key = 'repo:octocat/hello'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        end, "2026-08-15",
        "neither the running local day (08-16) nor the UTC day (08-17)"
    );
    // Initial window: 90 days back from the local day, 2026-08-16.
    assert_eq!(start, "2026-05-18");
}
