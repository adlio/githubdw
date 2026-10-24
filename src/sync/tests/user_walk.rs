use super::*;

/// A page of `count` PRs, all in one repo, numbered from `first`.
fn search_page(repository: &str, numbers: &[i64]) -> String {
    let nodes = numbers
        .iter()
        .map(|number| {
            search_node(
                *number,
                repository,
                "2026-08-14T18:00:00Z",
                &format!("2026-08-{:02}T09:00:00Z", 10 + number),
            )
        })
        .collect();
    search_response(numbers.len() as i64, nodes)
}

/// The whole point of the feature: a monitored user's PRs and reviews land
/// in the warehouse through the same pipeline a repository sync uses, and
/// the source's `last_sync_at` finally moves.
#[test]
fn syncs_a_user_across_repositories() {
    let warehouse = los_angeles_warehouse();
    monitor_repository::add_user(warehouse.connection(), "octocat").unwrap();
    let transport = FixtureTransport::new(vec![
        // author: two PRs in a repo nobody monitors
        Ok(search_page("acme/widgets", &[1, 2])),
        // reviewed-by: one PR in a third-party repo
        Ok(search_page("other/thing", &[3])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    let summary = syncer
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();

    assert_eq!(summary.pull_requests_synced, 3);
    assert_eq!(summary.pages_fetched, 2, "one search per role");
    assert!(summary.failed.is_empty());
    assert!(summary.notes.is_empty());

    let conn = warehouse.connection();
    let prs: i64 = conn
        .query_row("SELECT COUNT(*) FROM fact_pull_requests", [], |r| r.get(0))
        .unwrap();
    assert_eq!(prs, 3);
    let reviews: i64 = conn
        .query_row("SELECT COUNT(*) FROM fact_reviews", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        reviews, 3,
        "nested reviews ingest exactly as on the repo path"
    );

    // Both repositories exist as dimensions...
    let repos: Vec<String> = conn
        .prepare("SELECT repo_key FROM dim_repositories ORDER BY repo_key")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(repos, vec!["acme/widgets", "other/thing"]);

    // ...and neither was promoted into the monitored set. Syncing a person
    // says nothing about wanting their repositories tracked.
    let monitored: i64 = conn
        .query_row("SELECT COUNT(*) FROM monitored_repos", [], |r| r.get(0))
        .unwrap();
    assert_eq!(monitored, 0);

    // The source stamp advances, which is what `monitor list` reads.
    let last_sync: Option<String> = conn
        .query_row(
            "SELECT last_sync_at FROM monitored_users WHERE login = 'octocat'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(last_sync.is_some(), "last_sync_at no longer stays NULL");

    // Coverage and job state are recorded under the user's own entity key.
    let (start, end): (String, String) = conn
        .query_row(
            "SELECT start_date, end_date FROM synced_ranges WHERE entity_key = 'user:octocat'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((start.as_str(), end.as_str()), ("2026-08-15", "2026-08-15"));
    let status: String = conn
        .query_row(
            "SELECT status FROM sync_jobs WHERE entity_key = 'user:octocat'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "completed");
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 0);
}

/// Authored and reviewed work are disjoint result sets, so one watermark
/// cannot serve both: whichever side moved more recently would carry the
/// cursor past items the other side has not fetched.
#[test]
fn each_role_keeps_its_own_watermark() {
    let warehouse = los_angeles_warehouse();
    let transport = FixtureTransport::new(vec![
        Ok(search_response(
            1,
            vec![search_node(
                1,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-15T09:00:00Z",
            )],
        )),
        Ok(search_response(
            1,
            vec![search_node(
                2,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-12T09:00:00Z",
            )],
        )),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    syncer
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();

    let conn = warehouse.connection();
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat")
            .unwrap()
            .as_deref(),
        Some("2026-08-15T09:00:00Z")
    );
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user_reviews", "octocat")
            .unwrap()
            .as_deref(),
        Some("2026-08-12T09:00:00Z")
    );
}

/// Once a user has coverage and a watermark, a re-run walks updated-desc and
/// stops at the first strictly-older item instead of re-reading history.
#[test]
fn user_rerun_stops_at_the_watermark() {
    let warehouse = los_angeles_warehouse();
    // Newest-first within the page, as `sort:updated-desc` serves it.
    let fresh = search_response(
        2,
        vec![
            search_node(
                2,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-15T09:00:00Z",
            ),
            search_node(
                1,
                "acme/widgets",
                "2026-08-13T18:00:00Z",
                "2026-08-14T09:00:00Z",
            ),
        ],
    );
    let transport = FixtureTransport::new(vec![
        Ok(fresh.clone()), // run 1, author
        Ok(fresh.clone()), // run 1, reviewed-by
        Ok(fresh.clone()), // run 2, author — everything at or below cursor
        Ok(fresh.clone()), // run 2, reviewed-by
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();

    {
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let first = syncer
            .sync_user("octocat", &fast_user_options(Some(1)))
            .unwrap();
        assert_eq!(first.pull_requests_synced, 4, "both roles, both PRs");
    }
    {
        // No --days: coverage exists and both cursors are set, so this run
        // is incremental — one unwindowed search per role.
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        let second = syncer
            .sync_user("octocat", &fast_user_options(None))
            .unwrap();
        assert_eq!(
            second.pull_requests_synced, 2,
            "the boundary-second item is re-read; the strictly older one stops the walk"
        );
        assert_eq!(
            second.pages_fetched, 2,
            "no date windowing on an incremental run"
        );
    }
}

/// Same invariant the repository path holds: a watermark that moves past an
/// item the warehouse failed to store turns a retryable failure into a
/// permanent hole, because the next run's stop fires before reaching it.
#[test]
fn user_cursor_does_not_advance_past_a_failed_upsert() {
    let warehouse = los_angeles_warehouse();
    let transport = FixtureTransport::new(vec![
        Ok(search_response(
            2,
            vec![
                // Newest, and unstorable: `createdAt` will not parse.
                search_node(9, "acme/widgets", "not-a-timestamp", "2026-08-15T10:00:00Z"),
                search_node(
                    8,
                    "acme/widgets",
                    "2026-08-14T18:00:00Z",
                    "2026-08-14T10:00:00Z",
                ),
            ],
        )),
        Ok(search_response(0, vec![])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    let summary = syncer
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();

    assert_eq!(summary.pull_requests_synced, 1);
    assert_eq!(summary.failed.len(), 1);
    assert_eq!(
        sync_state::last_updated_cursor(warehouse.connection(), "user", "octocat")
            .unwrap()
            .as_deref(),
        Some("2026-08-14T10:00:00Z"),
        "the watermark stays below the failed item so the next run retries it"
    );
}

/// Search serves at most 1,000 results per query however many it reports
/// matching. A window over the cap is bisected until each half fits, so a
/// prolific contributor's backfill is complete rather than truncated.
#[test]
fn a_window_over_the_result_cap_is_split() {
    let warehouse = los_angeles_warehouse();
    let transport = FixtureTransport::new(vec![
        // author, 2026-08-13..2026-08-15: more matches than search serves
        Ok(search_response(
            2500,
            vec![search_node(
                1,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-15T09:00:00Z",
            )],
        )),
        // first half (08-13..08-14) and second half (08-15..08-15)
        Ok(search_page("acme/widgets", &[1])),
        Ok(search_page("acme/widgets", &[2])),
        // reviewed-by, whole window, under the cap
        Ok(search_page("acme/widgets", &[3])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    let summary = syncer
        .sync_user("octocat", &fast_user_options(Some(3)))
        .unwrap();

    assert_eq!(summary.pages_fetched, 4);
    assert_eq!(
        summary.pull_requests_synced, 3,
        "only the split halves are ingested; the over-cap query is abandoned before work"
    );
    assert!(
        summary.notes.is_empty(),
        "a window that could be narrowed needs no caveat"
    );
}

/// When the window is already a single day there is nothing left to narrow.
/// The shortfall is reported rather than passed off as full coverage.
#[test]
fn an_unsplittable_over_cap_window_is_reported() {
    let warehouse = los_angeles_warehouse();
    let transport = FixtureTransport::new(vec![
        Ok(search_response(
            4200,
            vec![search_node(
                1,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                "2026-08-15T09:00:00Z",
            )],
        )),
        Ok(search_response(0, vec![])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    let summary = syncer
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();

    assert_eq!(summary.notes.len(), 1);
    let note = &summary.notes[0];
    assert!(note.contains("4200"), "the match count is named: {note}");
    assert!(note.contains("1000"), "so is the cap: {note}");
    assert!(summary.partial, "a truncated day is not complete coverage");
    assert!(
        sync_state::last_updated_cursor(warehouse.connection(), "user", "octocat")
            .unwrap()
            .is_none(),
        "do not advance past records hidden by the cap"
    );
    assert!(
        sync_state::coverage_extent_as_of(
            warehouse.connection(),
            "user:octocat",
            evening_instant(),
        )
        .unwrap()
        .is_none(),
        "do not record a completely covered date range"
    );
}

/// `--issues-only` has no user-scoped counterpart. Returning zero quietly
/// would read as "this user has no issues"; the run says what it skipped.
#[test]
fn issues_only_on_a_user_reports_instead_of_no_opping() {
    let warehouse = los_angeles_warehouse();
    let transport = FixtureTransport::new(vec![]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    let summary = syncer
        .sync_user(
            "octocat",
            &SyncOptions {
                issues_only: true,
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(summary.pages_fetched, 0);
    assert_eq!(summary.notes.len(), 1);
    assert!(summary.notes[0].contains("--issues-only"));
}

/// A namespaced spelling reaches the same row `monitor add-user` wrote.
#[test]
fn user_sync_accepts_a_namespaced_login() {
    let warehouse = los_angeles_warehouse();
    monitor_repository::add_user(warehouse.connection(), "octocat").unwrap();
    let transport = FixtureTransport::new(vec![
        Ok(search_page("acme/widgets", &[1])),
        Ok(search_response(0, vec![])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    syncer
        .sync_user("user:OctoCat", &fast_user_options(Some(1)))
        .unwrap();

    let last_sync: Option<String> = warehouse
        .connection()
        .query_row(
            "SELECT last_sync_at FROM monitored_users WHERE login = 'octocat'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(last_sync.is_some());
}

/// A user with thousands of lifetime matches is ordinary. An incremental
/// walk that reaches its watermark has fetched everything that changed, so
/// the lifetime match count is not a shortfall and must not be reported as
/// one — the live case that produced this test was a real login whose
/// `reviewed-by:` query matches 1,122 items while only five had changed.
#[test]
fn an_incremental_walk_that_reaches_its_watermark_reports_nothing() {
    let warehouse = los_angeles_warehouse();
    let seed = search_response(
        1,
        vec![search_node(
            1,
            "acme/widgets",
            "2026-08-14T18:00:00Z",
            "2026-08-15T09:00:00Z",
        )],
    );
    // Run 2 reports a huge lifetime total but its first node is already
    // below the watermark, so the walk stops on the first item.
    let over_cap_but_converging = search_response(
        1122,
        vec![search_node(
            1,
            "acme/widgets",
            "2026-08-01T18:00:00Z",
            "2026-08-02T09:00:00Z",
        )],
    );
    let transport = FixtureTransport::new(vec![
        Ok(seed.clone()),
        Ok(seed.clone()),
        Ok(over_cap_but_converging.clone()),
        Ok(over_cap_but_converging.clone()),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();

    {
        let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
        syncer
            .sync_user("octocat", &fast_user_options(Some(1)))
            .unwrap();
    }
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    let second = syncer
        .sync_user("octocat", &fast_user_options(None))
        .unwrap();

    assert_eq!(second.pull_requests_synced, 0);
    assert!(
        second.notes.is_empty(),
        "reaching the watermark is convergence, whatever the match count: {:?}",
        second.notes
    );
}

/// Large incremental searches switch to bounded time windows before
/// ingesting the truncated first page. A later process resumes the saved
/// oldest unfinished window without replaying the completed prefix.
#[test]
fn large_user_walk_resumes_time_windows_without_search_offsets() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    let now = evening_instant();
    let base = search_timestamp(now.timestamp() - 1).unwrap();
    sync_state::record_range_as_of(conn, "user:octocat", "2026-08-15", "2026-08-15", 0, now)
        .unwrap();
    for role in UserRole::all() {
        sync_state::advance_cursor(conn, role.cursor_source_type(), "octocat", Some(&base))
            .unwrap();
    }
    let leaf = |number, at: &str| {
        search_response(
            1,
            vec![search_node(
                number,
                "acme/widgets",
                "2026-08-14T18:00:00Z",
                at,
            )],
        )
    };
    let (mut client, transport) = shared_client(vec![
        // Legacy probe, oversized bounded interval, oldest single second.
        Ok(search_response(5000, vec![])),
        Ok(search_response(1001, vec![])),
        Ok(leaf(1, &base)),
        // The other role has nothing newer than its existing cursor.
        Ok(search_response(0, vec![])),
        // Resume must fetch only the remaining second, no legacy probe.
        Ok(leaf(2, &search_timestamp(now.timestamp()).unwrap())),
        Ok(search_response(0, vec![])),
    ]);
    let first = Syncer::new(conn, &mut client)
        .as_of(now)
        .with_item_budget(1)
        .sync_user("octocat", &fast_user_options(None))
        .unwrap();
    assert!(first.partial);
    assert_eq!(first.pull_requests_synced, 1);
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat")
            .unwrap()
            .as_deref(),
        Some(base.as_str())
    );
    let checkpoint: UserUpdateCheckpoint = serde_json::from_str(
        &sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(checkpoint.pending, vec![(now.timestamp(), now.timestamp())]);
    assert_eq!(
        count(
            conn,
            "SELECT count(*) FROM sync_jobs WHERE status='partial'"
        ),
        1
    );
    // Even when the process starts later, the frozen bound stays fixed and
    // freshness does not claim the intervening minute was fetched.
    let second = Syncer::new(conn, &mut client)
        .as_of(now + Duration::minutes(1))
        .with_item_budget(1)
        .sync_user("octocat", &fast_user_options(None))
        .unwrap();
    assert!(second.partial);
    assert_eq!(second.pull_requests_synced, 1);
    assert!(
        sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
        Some(search_timestamp(now.timestamp()).unwrap())
    );
    let stamp: String = conn.query_row("SELECT last_sync_at FROM sync_metadata WHERE source_type='user' AND source_id='octocat'", [], |r| r.get(0)).unwrap();
    assert_eq!(stamp, search_timestamp(now.timestamp()).unwrap());
    assert!(requested_cursors(&transport).iter().all(Option::is_none));
    let calls = transport.calls.lock().unwrap();
    let last_author_query = calls[4].iter().find(|a| a.starts_with("q=")).unwrap();
    assert!(last_author_query.contains(&format!(
        "updated:{0}..{0}",
        search_timestamp(now.timestamp()).unwrap()
    )));
}
