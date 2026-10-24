use super::*;

fn single_issue_response(node: serde_json::Value) -> String {
    json!({"data": {
        "repository": {
            "nameWithOwner": "octocat/hello", "primaryLanguage": null,
            "isFork": false, "isPrivate": false, "defaultBranchRef": {"name": "main"},
            "createdAt": "2020-01-01T00:00:00Z",
            "issue": node
        }
    }})
    .to_string()
}

/// A patch write failing after the prune rolls back the prune, the PR
/// metadata, and every other write for that target.
fn assert_patch_failure_rolls_back_prune(walk: bool) {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let seed = vec![
        page_of(
            false,
            None,
            vec![
                pr_with_files(6, "2026-01-07T09:00:00Z", &["z.rs"], Some(1)),
                pr_with_files(
                    5,
                    "2026-01-06T09:00:00Z",
                    &["a.rs", "b.rs", "c.rs"],
                    Some(3),
                ),
            ],
        ),
        rest_patches(&["z.rs"], "old"),
        rest_patches(&["a.rs", "b.rs", "c.rs"], "old"),
    ];
    let (mut client, _) = routing_client(seed, vec![]);
    let options = SyncOptions {
        skip_diffs: false,
        pull_requests_only: true,
        ..Default::default()
    };
    let summary = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &options)
        .unwrap();
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    assert_eq!(
        stored_paths(conn, "octocat/hello#5"),
        ["a.rs", "b.rs", "c.rs"]
    );
    let selected = target(TargetKind::PullRequest, "octocat/hello#5");
    let before = refresh::stored_counts(conn, &selected).unwrap();
    let old_title = title_of(conn, "octocat/hello#5");
    let state = state_snapshot(conn);
    conn.execute_batch(
        "CREATE TRIGGER reject_b_patch BEFORE UPDATE OF patch ON fact_file_diffs
         WHEN NEW.file_diff_key = 'octocat/hello#5:b.rs'
         BEGIN SELECT RAISE(ABORT, 'synthetic patch write failure'); END;",
    )
    .unwrap();
    let mut changed = pr_with_files(5, "2026-01-09T09:00:00Z", &["a.rs", "b.rs"], Some(2));
    changed["title"] = json!("must not land");
    if walk {
        let (mut client, _) = routing_client(
            vec![
                page_of(false, None, vec![changed]),
                rest_patches(&["a.rs", "b.rs"], "new"),
            ],
            vec![],
        );
        let summary = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert_eq!(summary.failed.len(), 1, "{summary:?}");
        assert!(
            summary.failed[0]
                .1
                .contains("synthetic patch write failure")
        );
    } else {
        let report = refresh_pr5(
            conn,
            vec![
                single_pr_response(changed),
                rest_patches(&["a.rs", "b.rs"], "new"),
            ],
            false,
        );
        assert_eq!(report.failed(), 1, "{report:?}");
        assert_eq!(state_snapshot(conn), state);
    }
    assert_eq!(refresh::stored_counts(conn, &selected).unwrap(), before);
    assert_eq!(
        stored_paths(conn, "octocat/hello#5"),
        ["a.rs", "b.rs", "c.rs"]
    );
    assert_eq!(title_of(conn, "octocat/hello#5"), old_title);
    let patch: String = conn
        .query_row(
            "SELECT patch FROM fact_file_diffs WHERE file_diff_key = 'octocat/hello#5:a.rs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(patch, "old", "the earlier patch update also rolls back");
    let changed_files: i64 = conn
        .query_row(
            "SELECT changed_files FROM fact_pull_requests WHERE pr_key = 'octocat/hello#5'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(changed_files, 3);
    assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
}

#[test]
fn complete_refresh_prunes_files_that_left_the_pull_request() {
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    conn.execute(
        "UPDATE fact_file_diffs SET patch = 'kept patch' WHERE file_diff_key = 'octocat/hello#5:a.rs'",
        [],
    )
    .unwrap();
    let report = refresh_pr5(
        conn,
        vec![single_pr_response(pr_with_files(
            5,
            "2026-01-08T09:00:00Z",
            &["a.rs", "b.rs"],
            Some(2),
        ))],
        true,
    );
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
    assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
    assert_eq!(outcome.before.files, 3);
    assert_eq!(outcome.after.files, 2);
    assert_eq!(stored_paths(conn, "octocat/hello#5"), ["a.rs", "b.rs"]);
    let patch: Option<String> = conn
        .query_row(
            "SELECT patch FROM fact_file_diffs WHERE file_diff_key = 'octocat/hello#5:a.rs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        patch.as_deref(),
        Some("kept patch"),
        "skip_diffs keeps a retained file's patch"
    );
    assert_eq!(
        stored_paths(conn, "octocat/hello#6"),
        ["z.rs"],
        "unrelated PR untouched"
    );
}

#[test]
fn complete_walk_prunes_files_that_left_the_pull_request() {
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    let (mut client, _) = routing_client(
        vec![page_of(
            false,
            None,
            vec![pr_with_files(
                5,
                "2026-01-09T09:00:00Z",
                &["a.rs", "d.rs"],
                Some(2),
            )],
        )],
        vec![],
    );
    let summary = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap();
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    assert_eq!(summary.pull_requests_synced, 1);
    assert_eq!(stored_paths(conn, "octocat/hello#5"), ["a.rs", "d.rs"]);
    assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
}

#[test]
fn complete_empty_file_set_prunes_every_stored_file() {
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    let report = refresh_pr5(
        conn,
        vec![single_pr_response(pr_with_files(
            5,
            "2026-01-08T09:00:00Z",
            &[],
            Some(0),
        ))],
        false,
    );
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
    assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
    assert!(stored_paths(conn, "octocat/hello#5").is_empty());
    assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
}

#[test]
fn incomplete_or_capped_listing_keeps_stored_files_with_a_caveat() {
    for (changed, paths) in [
        (Some(5), vec!["a.rs", "b.rs"]),    // short of the reported total
        (Some(4443), vec!["a.rs", "b.rs"]), // provider file cap
        (None, vec!["a.rs", "b.rs"]),       // total not reported at all
        (None, vec![]),                     // missing total is not an empty set
    ] {
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        let report = refresh_pr5(
            conn,
            vec![single_pr_response(pr_with_files(
                5,
                "2026-01-08T09:00:00Z",
                &paths,
                changed,
            ))],
            true,
        );
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
        assert_eq!(
            stored_paths(conn, "octocat/hello#5"),
            ["a.rs", "b.rs", "c.rs"],
            "{changed:?} {paths:?}"
        );
        let kept = 3 - paths.len();
        assert!(
            outcome
                .caveats
                .iter()
                .any(|caveat| caveat.contains(&format!("kept {kept} stored file row(s)"))),
            "{changed:?}: {:?}",
            outcome.caveats
        );
        assert_eq!(report.incomplete(), 1);
    }
}

#[test]
fn duplicate_or_overcounted_listing_never_prunes_silently() {
    for (changed, paths, expected) in [
        (Some(3), vec!["a.rs", "a.rs", "b.rs"], "more than once"),
        (Some(2), vec!["a.rs", "a.rs", "b.rs"], "more than once"),
        (
            Some(1),
            vec!["a.rs", "b.rs"],
            "2 distinct paths for 1 changed files",
        ),
        (Some(2), vec!["a.rs", ""], "empty path"),
    ] {
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        let report = refresh_pr5(
            conn,
            vec![single_pr_response(pr_with_files(
                5,
                "2026-01-08T09:00:00Z",
                &paths,
                changed,
            ))],
            true,
        );
        let outcome = &report.outcomes[0];
        assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
        let stored = stored_paths(conn, "octocat/hello#5");
        for path in ["a.rs", "b.rs", "c.rs"] {
            assert!(stored.iter().any(|p| p == path), "{paths:?}: {stored:?}");
        }
        assert!(
            outcome
                .caveats
                .iter()
                .any(|caveat| caveat.contains(expected)
                    && caveat.contains("not provably complete")),
            "{paths:?}: {:?}",
            outcome.caveats
        );
    }
}

#[test]
fn refresh_patch_failure_rolls_back_the_prune_and_metadata() {
    assert_patch_failure_rolls_back_prune(false);
}

#[test]
fn walk_patch_failure_rolls_back_the_prune_and_metadata() {
    assert_patch_failure_rolls_back_prune(true);
}

#[test]
fn repair_plan_flags_stale_excess_file_rows() {
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    assert!(
        refresh::repair_candidates(conn, None, 64)
            .unwrap()
            .is_empty()
    );
    conn.execute(
        "UPDATE fact_pull_requests SET changed_files = 2 WHERE pr_key = 'octocat/hello#5'",
        [],
    )
    .unwrap();
    let candidates = refresh::repair_candidates(conn, None, 64).unwrap();
    assert_eq!(candidates.len(), 1, "{candidates:?}");
    assert_eq!(candidates[0].target.reference(), "octocat/hello#5");
    assert!(
        candidates[0].reasons[0].contains("3 file rows stored for 2 changed files"),
        "{:?}",
        candidates[0].reasons
    );
}

#[test]
fn refresh_completes_an_issue_with_paged_comments_labels_and_assignees() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let node = json!({
        "id": "ISS9", "number": 9, "title": "Heavy issue", "body": "b",
        "state": "OPEN", "stateReason": null,
        "createdAt": "2026-02-01T12:00:00Z", "updatedAt": "2026-02-03T12:00:00Z",
        "closedAt": null, "author": {"login": "octocat", "__typename": "User"},
        "milestone": null,
        "labels": {"pageInfo": {"hasNextPage": true, "endCursor": "L1"},
                   "nodes": [{"id": "LAB1", "name": "bug", "color": "f00", "description": ""}]},
        "assignees": {"pageInfo": {"hasNextPage": true, "endCursor": "A1"},
                      "nodes": [{"login": "hubot", "__typename": "User"}]},
        "comments": {"pageInfo": {"hasNextPage": true, "endCursor": "C1"},
                     "nodes": [conversation_comment("IC1")]}
    });
    let label_two = json!({"id": "LAB2", "name": "p1", "color": "0f0", "description": ""});
    let (mut client, _) = routing_client(
        vec![single_issue_response(node)],
        vec![
            (
                "labels",
                "L1",
                connection_page("labels", false, None, vec![label_two]),
            ),
            (
                "assignees",
                "A1",
                connection_page(
                    "assignees",
                    false,
                    None,
                    vec![json!({"login": "octocat", "__typename": "User"})],
                ),
            ),
            (
                "comments",
                "C1",
                connection_page("comments", false, None, vec![conversation_comment("IC2")]),
            ),
        ],
    );
    let report = Syncer::new(conn, &mut client)
        .refresh_targets(
            &[target(TargetKind::Issue, "octocat/hello#9")],
            &RefreshOptions::default(),
        )
        .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
    assert!(!outcome.before.present);
    assert_eq!(outcome.after.comments, 2);
    assert_eq!(outcome.after.labels, 2);
    assert_eq!(outcome.after.assignees, 2);
}

#[test]
fn repair_plan_selects_first_slice_shaped_items_from_stored_rows() {
    let warehouse = warehouse_with_shipped_pr();
    let conn = warehouse.connection();
    assert!(
        refresh::repair_candidates(conn, None, 64)
            .unwrap()
            .is_empty()
    );
    conn.execute(
        "UPDATE fact_pull_requests SET changed_files = 4443 WHERE pr_key = 'octocat/hello#5'",
        [],
    )
    .unwrap();
    let candidates = refresh::repair_candidates(conn, Some("OctoCat/Hello"), 64).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].target.reference(), "octocat/hello#5");
    assert!(
        candidates[0].reasons[0].contains("1 of 4443"),
        "{:?}",
        candidates[0].reasons
    );
    assert!(
        refresh::repair_candidates(conn, Some("other/repo"), 64)
            .unwrap()
            .is_empty()
    );
    assert!(
        refresh::repair_candidates(conn, None, 0)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn repair_plan_flags_items_stopped_at_a_first_slice() {
    let warehouse = warehouse_with_shipped_pr();
    let conn = warehouse.connection();
    let author = repository::ensure_ghost_entity(conn).unwrap();
    let keys = time_dimension::ensure_keys_for_timestamp(
        conn,
        "2026-01-05T19:00:00Z",
        chrono_tz::UTC,
        (9, 17),
    )
    .unwrap();
    for index in 2..=50 {
        repository::upsert_issue_comment(
            conn,
            &format!("EXTRA{index}"),
            "pull_request",
            "octocat/hello#5",
            &author,
            None,
            None,
            "2026-01-05T19:00:00Z",
            &keys.date_key,
            &keys.time_key,
        )
        .unwrap_or_else(|error| panic!("seed comment {index}: {error}"));
    }
    let candidates = refresh::repair_candidates(conn, None, 64).unwrap();
    assert_eq!(candidates.len(), 1, "{candidates:?}");
    assert!(candidates[0].reasons[0].contains("50 comments"));
}

#[test]
fn walk_fails_closed_on_an_empty_page_that_claims_more() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let (mut client, _) = routing_client(vec![page_of(true, Some("C1"), vec![])], vec![]);
    let error = Syncer::new(warehouse.connection(), &mut client)
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap_err();
    assert!(error.to_string().contains("holds none"), "{error}");
    assert!(pr_cursor(warehouse.connection()).is_none());
}

#[test]
fn walk_fails_closed_on_a_repeated_page_cursor() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let (mut client, _) = routing_client(
        vec![
            page_response(true, Some("C1"), &[3]),
            page_response(true, Some("C2"), &[2]),
            page_response(true, Some("C1"), &[1]),
        ],
        vec![],
    );
    let error = Syncer::new(warehouse.connection(), &mut client)
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap_err();
    assert!(error.to_string().contains("repeated"), "{error}");
    assert!(pr_cursor(warehouse.connection()).is_none());
}

#[test]
fn walk_fails_closed_on_missing_page_metadata() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let mut page: serde_json::Value =
        serde_json::from_str(&page_response(false, None, &[1])).unwrap();
    page["data"]["repository"]["pullRequests"]
        .as_object_mut()
        .unwrap()
        .remove("pageInfo");
    let (mut client, _) = routing_client(vec![page.to_string(); 6], vec![]);
    let result = Syncer::new(warehouse.connection(), &mut client)
        .sync_repository("octocat/hello", &fast_pr_options());
    assert!(result.is_err());
    assert!(pr_cursor(warehouse.connection()).is_none());
}
