use super::*;

/// PR 5 as GitHub serves a capped pull request: the first 3,000 git
/// records through GraphQL and the REST walk, totals for all of them.
fn capped_pages(
    git: &[GitFile],
    rest_additions_offset: i64,
    fixture: &test_repo::FixtureRepo,
) -> Vec<String> {
    let lines = |file: &GitFile| file.lines.unwrap_or((0, 0));
    let change = |file: &GitFile| match file.status {
        'A' => "ADDED",
        'D' => "DELETED",
        'R' => "RENAMED",
        _ => "MODIFIED",
    };
    let additions: i64 = git.iter().map(|f| lines(f).0).sum();
    let deletions: i64 = git.iter().map(|f| lines(f).1).sum();
    let mut node = pr_node(5, "2026-01-05T18:00:00Z", "2026-01-08T09:00:00Z");
    node["id"] = json!("PR_5");
    node["title"] = json!("refreshed title");
    node["changedFiles"] = json!(git.len());
    node["additions"] = json!(additions);
    node["deletions"] = json!(deletions);
    let served: Vec<_> = git[..3000]
        .iter()
        .map(|f| {
            json!({"path": f.path, "changeType": change(f),
                        "additions": lines(f).0, "deletions": lines(f).1})
        })
        .collect();
    node["files"] = json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": served});
    let mut pages = vec![single_pr_response(node)];
    for chunk in git[..3000].chunks(100) {
        pages.push(
            serde_json::Value::Array(
                chunk
                    .iter()
                    .map(|f| {
                        json!({"filename": f.path, "previous_filename": f.previous_path,
                                    "patch": f.patch})
                    })
                    .collect(),
            )
            .to_string(),
        );
    }
    pages.push(
        json!({"base": {"sha": fixture.base}, "head": {"sha": fixture.head},
               "changed_files": git.len(), "additions": additions + rest_additions_offset,
               "deletions": deletions})
        .to_string(),
    );
    pages.push(json!({"merge_base_commit": {"sha": fixture.base}}).to_string());
    pages
}

fn refresh_pr5_recovering(
    conn: &Connection,
    pages: Vec<String>,
    fixture: &test_repo::FixtureRepo,
) -> RefreshReport {
    let (mut client, _) = routing_client(pages, vec![]);
    Syncer::new(conn, &mut client)
        .with_git_recovery(fixture.config.clone())
        .refresh_targets(
            &[target(TargetKind::PullRequest, "octocat/hello#5")],
            &RefreshOptions { skip_diffs: false },
        )
        .unwrap()
}

#[test]
fn capped_listing_is_completed_from_git_and_stale_rows_pruned_atomically() {
    let fixture = test_repo::build();
    let git = fixture.records();
    assert!(git.len() > 3000);
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    // A prior revision stored the rename source as its own file. It must
    // disappear as a record when the complete set contains only the target.
    repository::upsert_file_diff(
        conn,
        "octocat/hello#5",
        "octocat/hello",
        "old name/basename.rs",
        None,
        "MODIFIED",
        None,
        1,
        0,
    )
    .unwrap();
    let report = refresh_pr5_recovering(conn, capped_pages(&git, 0, &fixture), &fixture);
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Refreshed, "{outcome:?}");
    assert!(outcome.caveats.is_empty(), "{:?}", outcome.caveats);
    let stored = stored_paths(conn, "octocat/hello#5");
    assert_eq!(
        stored.len(),
        git.len(),
        "every record, and the stale a/b/c pruned"
    );
    assert!(!stored.iter().any(|p| p == "a.rs"));
    assert_eq!(stored_paths(conn, "octocat/hello#6"), ["z.rs"]);
    let (changed, previous, tail_patch): (i64, Option<String>, Option<String>) = conn
        .query_row(
            "SELECT pr.changed_files,
                    (SELECT previous_path FROM fact_file_diffs WHERE file_diff_key = 'octocat/hello#5:new dir/basename.rs'),
                    (SELECT patch FROM fact_file_diffs WHERE file_diff_key = ?1)
             FROM fact_pull_requests pr WHERE pr.pr_key = 'octocat/hello#5'",
            [format!("octocat/hello#5:{}", git.last().unwrap().path)],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(changed, git.len() as i64);
    assert_eq!(previous.as_deref(), Some("old name/basename.rs"));
    assert_eq!(tail_patch, git.last().unwrap().patch, "tail patch from git");
    assert_eq!(title_of(conn, "octocat/hello#5"), "refreshed title");
}

#[test]
fn unprovable_recovery_fails_closed_and_keeps_stored_rows() {
    let fixture = test_repo::build();
    let git = fixture.records();
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    let before = stored_paths(conn, "octocat/hello#5");
    let title_before = title_of(conn, "octocat/hello#5");
    // REST totals disagree with GraphQL: a stale snapshot, nothing proven.
    let report = refresh_pr5_recovering(conn, capped_pages(&git, 7, &fixture), &fixture);
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, TargetStatus::Failed, "{outcome:?}");
    assert!(outcome.error.as_deref().unwrap().contains("stale snapshot"));
    assert_eq!(stored_paths(conn, "octocat/hello#5"), before);
    assert_eq!(title_of(conn, "octocat/hello#5"), title_before);
    assert_eq!(outcome.before, outcome.after);
    assert_eq!(report.failed(), 1);
}

#[test]
fn recovered_write_failure_rolls_back_the_whole_target() {
    let fixture = test_repo::build();
    let git = fixture.records();
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    let tail = git.last().unwrap().path.clone();
    conn.execute_batch(&format!(
        "CREATE TRIGGER refuse_tail BEFORE INSERT ON fact_file_diffs
         WHEN NEW.file_path = '{}' BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        tail.replace('\'', "''")
    ))
    .unwrap();
    let title_before = title_of(conn, "octocat/hello#5");
    let report = refresh_pr5_recovering(conn, capped_pages(&git, 0, &fixture), &fixture);
    assert_eq!(
        report.outcomes[0].status,
        TargetStatus::Failed,
        "{:?}",
        report.outcomes[0]
    );
    assert!(
        report.outcomes[0]
            .error
            .as_deref()
            .is_some_and(|e| e.contains("injected")),
        "{:?}",
        report.outcomes[0]
    );
    assert_eq!(
        stored_paths(conn, "octocat/hello#5"),
        ["a.rs", "b.rs", "c.rs"]
    );
    assert_eq!(title_of(conn, "octocat/hello#5"), title_before);
}
