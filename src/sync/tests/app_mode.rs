//! App mode (a collection policy file is present): every repository-walk
//! write preserves stored payloads and bridge rows.

use std::collections::HashSet;
use std::sync::Arc;

use super::*;
use crate::fetch::git_files::{RecoveredFile, RecoveredFileSet};
use crate::retention::{CollectionMode, DiffRetention, PolicyFile, PolicySource, PolicyState};

fn app_policy() -> PolicySource {
    PolicySource::Custom(Arc::new(|| {
        PolicyState::Present(PolicyFile {
            version: 1,
            mode: CollectionMode::Full,
            ai: true,
            diff_retention: DiffRetention::Retain,
            updated_at: None,
        })
    }))
}

fn issues_options() -> SyncOptions {
    SyncOptions {
        issues_only: true,
        ..Default::default()
    }
}

/// Sync one page of issues. The incremental cursor re-ingests an issue
/// updated in the cursor's own second, so repeating `&[1]` re-reads ISS1.
fn sync_issues(conn: &Connection, source: PolicySource, numbers: &[i64]) -> SyncSummary {
    let transport = FixtureTransport::new(vec![Ok(issue_page_response(false, None, numbers))]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    Syncer::new(conn, &mut client)
        .with_policy_source(source)
        .sync_repository("octocat/hello", &issues_options())
        .unwrap()
}

/// An extra label and assignee on ISS1 that the provider no longer lists.
fn add_stale_bridges(conn: &Connection) {
    let alice = repository::ensure_entity(
        conn,
        &crate::fetch::pull_requests::ActorReference {
            login: "alice".into(),
            type_name: "User".into(),
        },
        "[bot]",
    )
    .unwrap();
    assert_eq!(alice, "user:alice");
    conn.execute_batch(
        "INSERT INTO labels (id, repo_key, name, color, description)
             VALUES ('LAB9', 'octocat/hello', 'stale', 'ffffff', '');
         INSERT INTO issue_labels (issue_id, label_id) VALUES ('ISS1', 'LAB9');
         INSERT INTO issue_assignees (issue_id, entity_key) VALUES ('ISS1', 'user:alice');
         UPDATE issues SET body = 'kept body' WHERE id = 'ISS1';",
    )
    .unwrap();
}

#[test]
fn app_mode_issue_sync_adds_bridges_and_never_removes_them() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let summary = sync_issues(conn, app_policy(), &[1]);
    assert_eq!(summary.issues_synced, 1, "{:?}", summary.failed);
    assert_eq!(count(conn, "SELECT COUNT(*) FROM issue_labels"), 1);
    assert_eq!(count(conn, "SELECT COUNT(*) FROM issue_assignees"), 1);
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM fact_issue_comments WHERE parent_type = 'issue' AND body = 'on it'"
        ),
        1
    );
    assert_eq!(count(conn, "SELECT COUNT(*) FROM milestones"), 1);

    add_stale_bridges(conn);
    let summary = sync_issues(conn, app_policy(), &[1]);
    assert_eq!(summary.issues_synced, 1);
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM issue_labels WHERE issue_id = 'ISS1'"
        ),
        2,
        "app mode keeps the label the provider stopped listing"
    );
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM issue_assignees WHERE issue_id = 'ISS1'"
        ),
        2
    );
    // The provider's non-empty body still replaces a stored one.
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM issues WHERE id = 'ISS1' AND body = 'issue body'"
        ),
        1
    );

    // Standalone mode mirrors the provider: the stale links go.
    let summary = sync_issues(conn, PolicySource::Standalone, &[1]);
    assert_eq!(summary.issues_synced, 1);
    assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM issue_labels WHERE issue_id = 'ISS1'"
        ),
        1
    );
    assert_eq!(
        count(
            conn,
            "SELECT COUNT(*) FROM issue_assignees WHERE issue_id = 'ISS1'"
        ),
        1
    );
}

#[test]
fn an_issue_write_failure_is_reported_per_issue_in_both_modes() {
    for source in [app_policy(), PolicySource::Standalone] {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        conn.execute_batch(
            "CREATE TRIGGER refuse_issue BEFORE INSERT ON issues
             WHEN NEW.id = 'ISS2' BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
        let summary = sync_issues(conn, source, &[1, 2]);
        assert_eq!(summary.issues_synced, 1);
        assert_eq!(summary.failed.len(), 1);
        assert!(
            summary.failed[0].1.contains("injected"),
            "{:?}",
            summary.failed
        );
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM issues WHERE id = 'ISS2'"),
            0
        );
    }
}

fn threaded_pr(body: &str, comment_body: &str, line: Option<i64>) -> String {
    let mut node = pr_node(5, "2026-01-05T18:00:00Z", "2026-01-06T09:00:00Z");
    node["id"] = json!("PR_5");
    node["body"] = json!(body);
    let mut comment = review_comment("RC1");
    comment["body"] = json!(comment_body);
    comment["line"] = json!(line);
    node["reviewThreads"] = json!({
        "pageInfo": {"hasNextPage": false, "endCursor": null},
        "nodes": [{"id": "THREAD_A", "comments": {
            "pageInfo": {"hasNextPage": false, "endCursor": null},
            "nodes": [comment]
        }}]
    });
    page_of(false, None, vec![node])
}

fn sync_prs(conn: &Connection, page: String) -> SyncSummary {
    let (mut client, _) = routing_client(vec![page], vec![]);
    Syncer::new(conn, &mut client)
        .with_policy_source(app_policy())
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap()
}

#[test]
fn app_mode_pull_request_sync_never_blanks_review_comments_or_bodies() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let summary = sync_prs(conn, threaded_pr("body text", "nit", Some(3)));
    assert_eq!(summary.pull_requests_synced, 1, "{:?}", summary.failed);
    let summary = sync_prs(conn, threaded_pr("", "", None));
    assert_eq!(summary.pull_requests_synced, 1, "{:?}", summary.failed);
    let (body, comment, line): (String, String, i64) = conn
        .query_row(
            "SELECT pr.body, rc.body, rc.line FROM fact_pull_requests pr
             JOIN fact_review_comments rc ON rc.pr_key = pr.pr_key
             WHERE rc.comment_key = 'RC1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (body.as_str(), comment.as_str(), line),
        ("body text", "nit", 3)
    );
}

#[test]
fn app_mode_pull_request_write_failures_are_reported_per_item() {
    for trigger in [
        "CREATE TRIGGER refuse BEFORE INSERT ON fact_pull_requests
         BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        "CREATE TRIGGER refuse BEFORE INSERT ON fact_file_diffs
         BEGIN SELECT RAISE(ABORT, 'injected'); END;",
    ] {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        conn.execute_batch(trigger).unwrap();
        let summary = sync_prs(conn, threaded_pr("body text", "nit", Some(3)));
        assert_eq!(summary.pull_requests_synced, 0);
        assert_eq!(summary.failed.len(), 1, "{trigger}");
        assert!(
            summary.failed[0].1.contains("injected"),
            "{:?}",
            summary.failed
        );
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM fact_pull_requests"),
            0,
            "rolled back"
        );
    }
}

fn recovered(
    files: &[(&str, Option<&str>)],
    changed_files: i64,
    oversized: usize,
) -> RecoveredFileSet {
    RecoveredFileSet {
        files: files
            .iter()
            .map(|(path, patch)| RecoveredFile {
                path: (*path).into(),
                previous_path: None,
                change_type: "MODIFIED".into(),
                additions: 1,
                deletions: 0,
                patch: patch.map(str::to_string),
                from_git: true,
            })
            .collect(),
        keep: files
            .iter()
            .map(|(path, _)| path.to_string())
            .collect::<HashSet<_>>(),
        changed_files,
        additions: files.len() as i64,
        deletions: 0,
        base_sha: "base".into(),
        head_sha: "head".into(),
        merge_base_sha: "base".into(),
        git_records: files.len(),
        oversized_patches: oversized,
    }
}

fn patch_of(conn: &Connection, path: &str) -> Option<String> {
    conn.query_row(
        "SELECT patch FROM fact_file_diffs WHERE file_diff_key = ?1",
        [format!("octocat/hello#5:{path}")],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn app_mode_recovered_files_land_without_pruning_or_blanking() {
    let warehouse = warehouse_with_abc_and_unrelated();
    let conn = warehouse.connection();
    let set = recovered(&[("b.rs", Some("@@ b @@")), ("d.rs", None)], 2, 1);
    let caveats = Syncer::store_recovered_files(
        conn,
        "octocat/hello",
        "octocat/hello#5",
        5,
        &set,
        true,
        true,
    )
    .unwrap();
    assert_eq!(
        caveats,
        ["PR #5: 1 recovered patch(es) exceeded the size bound and were not stored"]
    );
    assert_eq!(
        stored_paths(conn, "octocat/hello#5"),
        ["a.rs", "b.rs", "c.rs", "d.rs"],
        "app mode keeps rows outside the set"
    );
    assert_eq!(patch_of(conn, "b.rs").as_deref(), Some("@@ b @@"));

    // A later set without patch text keeps the stored patch.
    let set = recovered(&[("b.rs", None)], 1, 0);
    let caveats = Syncer::store_recovered_files(
        conn,
        "octocat/hello",
        "octocat/hello#5",
        5,
        &set,
        true,
        true,
    )
    .unwrap();
    assert!(caveats.is_empty());
    assert_eq!(patch_of(conn, "b.rs").as_deref(), Some("@@ b @@"));

    // A set that does not account for every changed file is refused.
    let set = recovered(&[("b.rs", None)], 3, 0);
    let error = Syncer::store_recovered_files(
        conn,
        "octocat/hello",
        "octocat/hello#5",
        5,
        &set,
        false,
        true,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("1 of 1 recovered records landed for 3 changed files"),
        "{error}"
    );
}

#[test]
fn recovered_patch_write_failures_propagate_in_both_modes() {
    for preserve in [true, false] {
        let warehouse = warehouse_with_abc_and_unrelated();
        let conn = warehouse.connection();
        conn.execute_batch(
            "CREATE TRIGGER refuse_patch BEFORE UPDATE OF patch ON fact_file_diffs
             BEGIN SELECT RAISE(ABORT, 'injected patch'); END;",
        )
        .unwrap();
        let set = recovered(&[("d.rs", Some("@@ d @@"))], 1, 0);
        let error = Syncer::store_recovered_files(
            conn,
            "octocat/hello",
            "octocat/hello#5",
            5,
            &set,
            true,
            preserve,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("injected patch"),
            "{preserve}: {error}"
        );

        conn.execute_batch("DROP TRIGGER refuse_patch; PRAGMA query_only = ON")
            .unwrap();
        let error = Syncer::store_recovered_files(
            conn,
            "octocat/hello",
            "octocat/hello#5",
            5,
            &set,
            false,
            preserve,
        )
        .unwrap_err();
        assert!(
            matches!(error, crate::Error::Database(_)),
            "{preserve}: {error}"
        );
    }
}
