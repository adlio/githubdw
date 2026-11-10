//! User sync over search pages that carry GraphQL `errors`.
//!
//! The transport behaves like real `gh api graphql`: it exits 1 and prints the
//! JSON body whenever the body has `errors`, and `interpret_gh_output` decides
//! what the client sees.

use super::*;
use std::sync::Mutex;

const SAML: &str = "Resource protected by organization SAML enforcement.";

struct GhLike(Mutex<Vec<String>>);

impl crate::fetch::GhTransport for GhLike {
    fn run(&self, arguments: &[String]) -> Result<String> {
        let mut queue = self.0.lock().unwrap();
        assert!(!queue.is_empty(), "unexpected extra search");
        let body = queue.remove(0);
        let has_errors = body.contains("\"errors\"");
        crate::fetch::interpret_gh_output(
            arguments,
            Some(i32::from(has_errors)),
            &body,
            "gh: partial",
        )
    }
}

fn gh_like(bodies: Vec<String>) -> GhClient {
    GhClient::with_transport(Box::new(GhLike(Mutex::new(bodies)))).without_sleeping()
}

/// A search body: `nodes` as given (null where a node was hidden), `errors`
/// attached when non-empty.
fn body(total: i64, nodes: Vec<serde_json::Value>, errors: Vec<serde_json::Value>) -> String {
    let mut value: serde_json::Value =
        serde_json::from_str(&search_response(total, nodes)).unwrap();
    if !errors.is_empty() {
        value["errors"] = json!(errors);
    }
    value.to_string()
}

fn error(kind: &str, message: &str, index: i64) -> serde_json::Value {
    json!({"type": kind, "message": message, "path": ["search", "nodes", index]})
}

fn node(number: i64, updated: &str) -> serde_json::Value {
    search_node(number, "acme/widgets", "2026-08-10T18:00:00Z", updated)
}

fn empty() -> String {
    body(0, vec![], vec![])
}

/// Three PRs, newest first; the middle one is hidden by SAML.
fn saml_middle_page() -> String {
    body(
        3,
        vec![
            node(1, "2026-08-15T09:00:00Z"),
            serde_json::Value::Null,
            node(3, "2026-08-14T09:00:00Z"),
        ],
        vec![error("FORBIDDEN", SAML, 1)],
    )
}

#[test]
fn a_forbidden_node_is_skipped_and_the_rest_of_the_page_is_stored() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    // run 1: author, reviewed-by; run 2 (incremental): author, reviewed-by
    let mut client = gh_like(vec![
        saml_middle_page(),
        empty(),
        saml_middle_page(),
        empty(),
    ]);

    let first = Syncer::new(conn, &mut client)
        .as_of(evening_instant())
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();
    assert_eq!(first.pull_requests_synced, 2);
    assert_eq!(first.skipped, 1);
    assert!(first.failed.is_empty() && !first.partial);
    assert_eq!(
        first.notes,
        vec![format!(
            "octocat (author): skipped a pull request GitHub would not show: {SAML}"
        )]
    );
    assert_eq!(count(conn, "SELECT count(*) FROM fact_pull_requests"), 2);
    assert_eq!(
        count(
            conn,
            "SELECT count(*) FROM fact_pull_requests WHERE pr_key LIKE '%#2'"
        ),
        0,
        "the hidden node is never stored"
    );
    // The newest stored PR sets the cursor; the hidden one does not hold it.
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
        Some("2026-08-15T09:00:00Z".to_string())
    );

    // Second run: same search result. Nothing new is stored, nothing fails,
    // and the hidden node is not a retry that blocks the cursor.
    let second = Syncer::new(conn, &mut client)
        .as_of(evening_instant())
        .sync_user("octocat", &fast_user_options(None))
        .unwrap();
    assert!(second.failed.is_empty() && !second.partial);
    assert_eq!(second.skipped, 1);
    assert_eq!(count(conn, "SELECT count(*) FROM fact_pull_requests"), 2);
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
        Some("2026-08-15T09:00:00Z".to_string())
    );
}

#[test]
fn an_error_that_is_not_forbidden_still_fails_the_page() {
    for errors in [
        // Right place, wrong kind.
        vec![error("INTERNAL", "boom", 1)],
        // Forbidden-looking, but not about a search node.
        vec![json!({"type": "FORBIDDEN", "message": "no", "path": ["viewer"]})],
        // One skippable error does not excuse a second, fatal one.
        vec![error("FORBIDDEN", SAML, 1), error("INTERNAL", "boom", 0)],
    ] {
        let warehouse = los_angeles_warehouse();
        let page = body(
            3,
            vec![
                node(1, "2026-08-15T09:00:00Z"),
                serde_json::Value::Null,
                node(3, "2026-08-14T09:00:00Z"),
            ],
            errors,
        );
        let mut client = gh_like(vec![page]);
        let result = Syncer::new(warehouse.connection(), &mut client)
            .as_of(evening_instant())
            .sync_user("octocat", &fast_user_options(Some(1)));
        assert!(result.is_err());
        assert_eq!(
            count(
                warehouse.connection(),
                "SELECT count(*) FROM fact_pull_requests"
            ),
            0
        );
    }
}

#[test]
fn a_saml_message_without_a_type_is_skipped_but_a_null_node_without_an_error_fails() {
    let warehouse = los_angeles_warehouse();
    let typeless = json!({"message": SAML, "path": ["search", "nodes", 0]});
    let mut client = gh_like(vec![
        body(
            2,
            vec![serde_json::Value::Null, node(2, "2026-08-14T09:00:00Z")],
            vec![typeless],
        ),
        empty(),
    ]);
    let summary = Syncer::new(warehouse.connection(), &mut client)
        .as_of(evening_instant())
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();
    assert_eq!((summary.pull_requests_synced, summary.skipped), (1, 1));

    let warehouse = los_angeles_warehouse();
    let mut client = gh_like(vec![body(1, vec![serde_json::Value::Null], vec![])]);
    assert!(
        Syncer::new(warehouse.connection(), &mut client)
            .as_of(evening_instant())
            .sync_user("octocat", &fast_user_options(Some(1)))
            .is_err(),
        "a null node nobody explained is a malformed page"
    );
}

#[test]
fn a_page_of_only_hidden_nodes_still_reaches_the_next_page() {
    let warehouse = los_angeles_warehouse();
    let mut first: serde_json::Value = serde_json::from_str(&body(
        2,
        vec![serde_json::Value::Null],
        vec![error("FORBIDDEN", SAML, 0)],
    ))
    .unwrap();
    first["data"]["search"]["pageInfo"] = json!({"hasNextPage": true, "endCursor": "c1"});
    let mut client = gh_like(vec![
        first.to_string(),
        body(2, vec![node(2, "2026-08-14T09:00:00Z")], vec![]),
        empty(),
    ]);
    let summary = Syncer::new(warehouse.connection(), &mut client)
        .as_of(evening_instant())
        .sync_user("octocat", &fast_user_options(Some(1)))
        .unwrap();
    assert_eq!((summary.pull_requests_synced, summary.skipped), (1, 1));
    assert_eq!(summary.pages_fetched, 3);
}

#[test]
fn the_catch_up_walk_skips_a_forbidden_node_and_still_closes_its_window() {
    let warehouse = los_angeles_warehouse();
    let conn = warehouse.connection();
    let now = evening_instant();
    let base = search_timestamp(now.timestamp() - 60).unwrap();
    // total 3 = 2 stored + 1 hidden: the window is complete, not "incomplete".
    let page = body(
        3,
        vec![node(1, &base), serde_json::Value::Null, node(3, &base)],
        vec![error("FORBIDDEN", SAML, 1)],
    );
    let mut client = gh_like(vec![page]);
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
    assert_eq!((summary.pull_requests_synced, summary.skipped), (2, 1));
    assert_eq!(
        sync_state::last_updated_cursor(conn, "user", "octocat").unwrap(),
        Some(search_timestamp(now.timestamp()).unwrap())
    );
    assert!(
        sync_state::load_checkpoint(conn, "user_update_checkpoint", "author:octocat")
            .unwrap()
            .is_none()
    );
}
