use super::*;

#[test]
fn user_path_follows_nested_connection_cursors() {
    let warehouse = los_angeles_warehouse();
    monitor_repository::add_user(warehouse.connection(), "octocat").unwrap();
    let mut node = heavy_pr_node(5);
    node["updatedAt"] = json!("2026-08-15T09:00:00Z");
    node["createdAt"] = json!("2026-08-14T18:00:00Z");
    node["repository"] = json!({
        "nameWithOwner": "acme/widgets", "primaryLanguage": {"name": "Rust"},
        "isFork": false, "isPrivate": false, "defaultBranchRef": {"name": "main"},
        "createdAt": "2020-01-01T00:00:00Z"
    });
    let (mut client, _) = routing_client(
        vec![search_response(1, vec![node]), search_response(0, vec![])],
        heavy_pr_continuations(),
    );
    let mut syncer = Syncer::new(warehouse.connection(), &mut client).as_of(evening_instant());
    let summary = syncer
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    assert_eq!(summary.pull_requests_synced, 1);
    let pr_key: String = warehouse
        .connection()
        .query_row("SELECT pr_key FROM fact_pull_requests", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_heavy_pr_complete(warehouse.connection(), &pr_key);
}

/// A page that claims more nodes but carries no cursor cannot be completed;
/// the PR is recorded as failed (and retried) rather than stored short.
#[test]
fn continuation_without_cursor_fails_the_record_instead_of_truncating() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let mut node = pr_node(6, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["id"] = json!("PR_6");
    node["comments"] = json!({
        "pageInfo": {"hasNextPage": true, "endCursor": null},
        "nodes": [conversation_comment("IC1")]
    });
    let (mut client, _) = routing_client(vec![page_of(false, None, vec![node])], vec![]);
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert_eq!(summary.failed.len(), 1);
    assert!(
        summary.failed[0].1.contains("no endCursor"),
        "{:?}",
        summary.failed
    );
    let conn = warehouse.connection();
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 0);
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_issue_comments"), 0);
}

/// A continuation request that errors fails the record; nothing partial is stored.
#[test]
fn failed_continuation_page_fails_the_record() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let mut continuations = heavy_pr_continuations();
    continuations.retain(|(field, cursor, _)| !(*field == "comments" && *cursor == "COM2"));
    let (mut client, _) = routing_client(
        vec![page_of(false, None, vec![heavy_pr_node(7)])],
        continuations,
    );
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert_eq!(summary.failed.len(), 1, "{:?}", summary.failed);
    let conn = warehouse.connection();
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 0);
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_issue_comments"), 0);
}

/// GitHub stops listing files at a cap; the shortfall is surfaced as a note
/// rather than the list being treated as complete.
#[test]
fn provider_file_cap_is_reported_as_truncation() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let mut node = pr_node(8, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["id"] = json!("PR_8");
    node["changedFiles"] = json!(4443);
    let (mut client, _) = routing_client(vec![page_of(false, None, vec![node])], vec![]);
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert!(summary.failed.is_empty());
    assert_eq!(summary.notes.len(), 1, "{:?}", summary.notes);
    assert!(
        summary.notes[0].contains("1 of 4443"),
        "{}",
        summary.notes[0]
    );
}

#[test]
fn issue_comments_follow_their_cursor() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let mut page: serde_json::Value =
        serde_json::from_str(&issue_page_response(false, None, &[1])).unwrap();
    page["data"]["repository"]["issues"]["nodes"][0]["comments"]["pageInfo"] =
        json!({"hasNextPage": true, "endCursor": "IC_1"});
    let (mut client, _) = routing_client(
        vec![page.to_string()],
        vec![(
            "comments",
            "IC_1",
            connection_page(
                "comments",
                false,
                None,
                vec![conversation_comment("ICOM_X")],
            ),
        )],
    );
    let options = SyncOptions {
        issues_only: true,
        ..Default::default()
    };
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer.sync_repository("octocat/hello", &options).unwrap();
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    assert_eq!(
        count(
            warehouse.connection(),
            "SELECT COUNT(*) FROM fact_issue_comments"
        ),
        2
    );
}

/// The reported failure: a run dies on page 3 (here the transport fails
/// at every page size; in production the scheduler kills it). The next
/// run must resume at page 3 from the durable checkpoint, not re-walk
/// pages 1-2, and the high-water cursor must not move until the walk is
/// complete.
#[test]
fn failed_page_three_resumes_durably_instead_of_restarting() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let options = fast_pr_options();

    // Run 1: two good pages, then page 3 fails at every page size.
    let (mut client, _) = shared_client(vec![
        Ok(page_response(true, Some("C1"), &[10, 9])),
        Ok(page_response(true, Some("C2"), &[8, 7])),
    ]);
    let error = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &options)
        .unwrap_err();
    assert!(error.to_string().contains("no more fixture responses"));
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 4);
    assert_eq!(
        pr_cursor(conn),
        None,
        "no high-water claim for a broken walk"
    );
    assert_eq!(
        count(conn, "SELECT COUNT(*) FROM synced_ranges"),
        0,
        "no coverage claim for a broken walk"
    );
    let checkpoint = saved_checkpoint(conn).expect("position is durable");
    assert_eq!(checkpoint.after, "C2");
    assert_eq!(checkpoint.pages, 2);
    assert_eq!(checkpoint.base, None);

    // Run 2: resumes after C2 and finishes.
    let (mut client, transport) = shared_client(vec![Ok(page_response(false, None, &[6]))]);
    let summary = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert_eq!(
        requested_cursors(&transport),
        vec![Some("C2".to_string())],
        "resumes at page 3, not page 1"
    );
    assert_eq!(summary.pull_requests_synced, 1);
    assert!(!summary.partial);
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 5);
    assert_eq!(
        count(conn, "SELECT COUNT(*) FROM fact_reviews"),
        5,
        "review data from the interrupted run is preserved"
    );
    assert_eq!(
        pr_cursor(conn).as_deref(),
        Some("2026-01-15T09:00:00Z"),
        "the completed walk commits its newest landed item (PR 10)"
    );
    assert!(
        saved_checkpoint(conn).is_none(),
        "checkpoint cleared on completion"
    );
    assert_eq!(count(conn, "SELECT COUNT(*) FROM synced_ranges"), 1);
}
