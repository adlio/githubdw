use super::*;

#[test]
fn syncs_issues_with_labels_and_comments() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let transport = FixtureTransport::new(vec![
        Ok(issue_page_response(true, Some("IC1"), &[1, 2])),
        Ok(issue_page_response(false, None, &[3])),
    ]);
    let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
    let options = SyncOptions {
        issues_only: true,
        ..Default::default()
    };
    let mut syncer = Syncer::new(warehouse.connection(), &mut client);
    let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

    assert_eq!(summary.issues_synced, 3);
    assert_eq!(summary.pages_fetched, 2);

    let conn = warehouse.connection();
    let issue_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM issues", [], |r| r.get(0))
        .unwrap();
    assert_eq!(issue_count, 3);
    let label_links: i64 = conn
        .query_row("SELECT COUNT(*) FROM issue_labels", [], |r| r.get(0))
        .unwrap();
    assert_eq!(label_links, 3);
    let assignees: i64 = conn
        .query_row("SELECT COUNT(*) FROM issue_assignees", [], |r| r.get(0))
        .unwrap();
    assert_eq!(assignees, 3);
    let comments: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM fact_issue_comments WHERE parent_type = 'issue'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(comments, 3);
    let milestones: i64 = conn
        .query_row("SELECT COUNT(*) FROM milestones", [], |r| r.get(0))
        .unwrap();
    assert_eq!(milestones, 1);
    let fts: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM issues_fts WHERE issues_fts MATCH 'ssue'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(fts, 3, "issues_fts populated via trigger");
}
