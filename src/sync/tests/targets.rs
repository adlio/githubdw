use super::*;

fn rest_patch_page(range: std::ops::Range<usize>) -> String {
    serde_json::Value::Array(
        range
            .map(|i| json!({"filename": format!("f{i}.rs"), "patch": "@@ -1 +1 @@"}))
            .collect(),
    )
    .to_string()
}

fn hundred_and_one_file_pr() -> serde_json::Value {
    let mut node = pr_node(4, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["id"] = json!("PR_4");
    node["changedFiles"] = json!(101);
    let files: Vec<_> = (0..100).map(|i| file(&format!("f{i}.rs"))).collect();
    node["files"] = json!({"pageInfo": {"hasNextPage": true, "endCursor": "F1"}, "nodes": files});
    node
}

fn second_file_page() -> Vec<(&'static str, &'static str, String)> {
    vec![(
        "files",
        "F1",
        connection_page("files", false, None, vec![file("f100.rs")]),
    )]
}

#[test]
fn refresh_completes_an_old_shipped_pr_without_moving_any_sync_state() {
    let warehouse = warehouse_with_shipped_pr();
    let conn = warehouse.connection();
    assert!(pr_cursor(conn).is_some(), "walk recorded a cursor");

    // The incremental walk stops at the cursor: it cannot repair PR 5.
    let (mut client, transport) = routing_client(
        vec![page_of(
            false,
            None,
            vec![pr_node(5, "2026-01-05T18:00:00Z", "2026-01-06T08:00:00Z")],
        )],
        vec![],
    );
    let summary = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert_eq!(summary.pull_requests_synced, 0);
    assert!(transport.continuation_calls.lock().unwrap().is_empty());
    let state_before = state_snapshot(conn);

    let mut node = heavy_pr_node(5);
    node["title"] = json!("Refreshed title");
    let (mut client, transport) =
        routing_client(vec![single_pr_response(node)], heavy_pr_continuations());
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#5")],
            &RefreshOptions { skip_diffs: true },
        )
        .unwrap();
    assert_eq!(report.failed(), 0, "{report:?}");
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Refreshed);
    assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
    assert_eq!(outcome.before.comments, 1);
    assert_eq!(outcome.after.comments, 3);
    assert_eq!(outcome.before.files, 1);
    assert_eq!(outcome.after.files, 3);
    assert_eq!(outcome.after.review_comments, 3);
    assert_eq!(transport.continuation_calls.lock().unwrap().len(), 5);
    assert_heavy_pr_complete(conn, "octocat/hello#5");
    assert_eq!(title_of(conn, "octocat/hello#5"), "Refreshed title");
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 1);
    assert_eq!(
        state_snapshot(conn),
        state_before,
        "no cursor, checkpoint, job, range, lock, or monitor row changed"
    );
}

#[test]
fn failed_refresh_preserves_stored_rows_and_state() {
    let warehouse = warehouse_with_shipped_pr();
    let conn = warehouse.connection();
    let state_before = state_snapshot(conn);

    // A continuation page fails: nothing is fetched completely, nothing written.
    let mut continuations = heavy_pr_continuations();
    continuations.retain(|(field, cursor, _)| !(*field == "comments" && *cursor == "COM2"));
    let (mut client, _) = routing_client(vec![single_pr_response(heavy_pr_node(5))], continuations);
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#5")],
            &RefreshOptions { skip_diffs: true },
        )
        .unwrap();
    assert_eq!(report.failed(), 1);
    assert_eq!(report.outcomes[0].before, report.outcomes[0].after);
    assert!(report.outcomes[0].error.is_some());

    // A failure AFTER the PR row was written (a review with an unparsable
    // timestamp) rolls the whole target back, title included.
    let mut node = pr_node(5, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["title"] = json!("must not land");
    node["reviews"]["nodes"][0]["submittedAt"] = json!("not a timestamp");
    let (mut client, _) = routing_client(vec![single_pr_response(node)], vec![]);
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#5")],
            &RefreshOptions { skip_diffs: true },
        )
        .unwrap();
    assert_eq!(report.failed(), 1, "{report:?}");
    assert_eq!(title_of(conn, "octocat/hello#5"), "PR 5");
    assert_eq!(state_snapshot(conn), state_before);
}

#[test]
fn refresh_honours_a_running_sync_lock_and_leaves_it_in_place() {
    let warehouse = warehouse_with_shipped_pr();
    let conn = warehouse.connection();
    sync_state::acquire_lock(conn, "repo:octocat/hello").unwrap();
    let state_before = state_snapshot(conn);
    let (mut client, transport) =
        routing_client(vec![single_pr_response(heavy_pr_node(5))], vec![]);
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#5")],
            &RefreshOptions::default(),
        )
        .unwrap();
    assert_eq!(report.failed(), 1);
    let error = report.outcomes[0].error.as_deref().unwrap();
    assert!(error.contains("in progress"), "{error}");
    assert_eq!(
        transport.pages.lock().unwrap().len(),
        1,
        "no request was made"
    );
    assert_eq!(
        state_snapshot(conn),
        state_before,
        "the other run's lock survives"
    );
}

#[test]
fn each_target_fails_or_lands_on_its_own() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let mut capped = pr_node(8, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    capped["id"] = json!("PR_8");
    capped["changedFiles"] = json!(4443);
    let mut renamed: serde_json::Value = serde_json::from_str(&single_pr_response(pr_node(
        9,
        "2026-01-05T18:00:00Z",
        "2026-01-06T09:00:00Z",
    )))
    .unwrap();
    renamed["data"]["repository"]["nameWithOwner"] = json!("octocat/renamed");
    let mut wrong_number = pr_node(11, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    wrong_number["number"] = json!(12);
    let (mut client, _) = routing_client(
        vec![
            single_pr_response(serde_json::Value::Null),
            single_pr_response(capped),
            renamed.to_string(),
            single_pr_response(wrong_number),
        ],
        vec![],
    );
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[
                target(TargetKind::PullRequest, "octocat/hello#7"),
                target(TargetKind::PullRequest, "octocat/hello#8"),
                target(TargetKind::PullRequest, "octocat/hello#9"),
                target(TargetKind::PullRequest, "octocat/hello#11"),
                target(TargetKind::PullRequest, "octocat/hello#7"),
            ],
            &RefreshOptions { skip_diffs: true },
        )
        .unwrap();
    assert_eq!(report.outcomes.len(), 4, "duplicate collapsed");
    let by_number = |number: i64| {
        report
            .outcomes
            .iter()
            .find(|outcome| outcome.target.number == number)
            .unwrap()
    };
    assert!(by_number(7).error.as_deref().unwrap().contains("not found"));
    assert_eq!(by_number(8).status, TargetStatus::Refreshed);
    assert!(
        by_number(8).caveats[0].contains("1 of 4443"),
        "{:?}",
        by_number(8).caveats
    );
    assert_eq!(report.incomplete(), 1);
    assert!(
        by_number(9)
            .error
            .as_deref()
            .unwrap()
            .contains("octocat/renamed")
    );
    assert!(
        by_number(11)
            .error
            .as_deref()
            .unwrap()
            .contains("returned #12")
    );
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 1);
    assert_eq!(
        count(conn, "SELECT COUNT(*) FROM sync_locks"),
        0,
        "lock released"
    );
    assert_eq!(
        count(conn, "SELECT COUNT(*) FROM sync_jobs"),
        0,
        "no job record"
    );
    assert_eq!(
        count(conn, "SELECT COUNT(*) FROM monitored_repos"),
        0,
        "not enrolled"
    );
}

#[test]
fn transport_failure_fails_only_its_target() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let mut node = pr_node(3, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["id"] = json!("PR_3");
    let (mut client, _) = shared_client(vec![
        Err(crate::Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 404: Not Found".into(),
        }),
        Ok(single_pr_response(node)),
    ]);
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[
                target(TargetKind::PullRequest, "octocat/hello#2"),
                target(TargetKind::PullRequest, "octocat/hello#3"),
            ],
            &RefreshOptions { skip_diffs: true },
        )
        .unwrap();
    assert_eq!(report.outcomes[0].status, TargetStatus::Failed);
    assert_eq!(report.outcomes[1].status, TargetStatus::Refreshed);
}

#[test]
fn refresh_walks_rest_patch_pages_and_reports_a_failed_page() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let (mut client, _) = routing_client(
        vec![
            single_pr_response(hundred_and_one_file_pr()),
            rest_patch_page(0..100),
            rest_patch_page(100..101),
        ],
        second_file_page(),
    );
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#4")],
            &RefreshOptions::default(),
        )
        .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
    assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
    assert_eq!(outcome.after.files, 101);
    assert_eq!(
        outcome.after.files_with_patch, 101,
        "page-two patches landed"
    );

    // The second REST page fails: stored, but explicitly incomplete.
    let (mut client, _) = routing_client(
        vec![
            single_pr_response(hundred_and_one_file_pr()),
            rest_patch_page(0..100),
        ],
        second_file_page(),
    );
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#4")],
            &RefreshOptions::default(),
        )
        .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Refreshed);
    assert!(
        outcome.caveats[0].contains("patch text not retrieved"),
        "{:?}",
        outcome.caveats
    );
    assert_eq!(report.incomplete(), 1);
}

#[test]
fn refresh_patch_write_failure_rolls_back_the_entire_target() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let selected = target(TargetKind::PullRequest, "octocat/hello#4");
    let (mut client, _) = routing_client(
        vec![
            single_pr_response(hundred_and_one_file_pr()),
            rest_patch_page(0..100),
            rest_patch_page(100..101),
        ],
        second_file_page(),
    );
    let initial = Syncer::new(conn, &mut client)
        .refresh_targets(std::slice::from_ref(&selected), &RefreshOptions::default())
        .unwrap();
    assert_eq!(initial.failed(), 0);
    let before = refresh::stored_counts(conn, &selected).unwrap();
    let old_title = title_of(conn, "octocat/hello#4");
    let state = state_snapshot(conn);
    conn.execute_batch(
        "CREATE TRIGGER reject_second_patch BEFORE UPDATE OF patch ON fact_file_diffs
        WHEN NEW.file_diff_key = 'octocat/hello#4:f1.rs'
        BEGIN SELECT RAISE(ABORT, 'synthetic patch write failure'); END;",
    )
    .unwrap();
    let mut changed = hundred_and_one_file_pr();
    changed["title"] = json!("must not land");
    let (mut client, _) = routing_client(
        vec![
            single_pr_response(changed),
            rest_patch_page(0..100).replace("@@ -1 +1 @@", "new patch"),
            rest_patch_page(100..101),
        ],
        second_file_page(),
    );
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(&[selected], &RefreshOptions::default())
        .unwrap();
    assert_eq!(report.failed(), 1);
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .unwrap()
            .contains("synthetic patch write failure")
    );
    assert_eq!(report.outcomes[0].after, before);
    assert_eq!(title_of(conn, "octocat/hello#4"), old_title);
    let patch: String = conn
        .query_row(
            "SELECT patch FROM fact_file_diffs WHERE file_diff_key='octocat/hello#4:f0.rs'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        patch, "@@ -1 +1 @@",
        "the first patch update also rolls back"
    );
    assert_eq!(state_snapshot(conn), state);
}
