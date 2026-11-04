//! Edge cases of the counts walk and the detail/diff primitives: argument
//! checks, provider responses the pinned world never produces, and write
//! failures.

use std::sync::Mutex;

use serde_json::{Value, json};

use super::fake::light_node;
use super::*;

/// Replays canned search responses, one per request.
struct Scripted(Mutex<Vec<crate::Result<String>>>);

impl crate::fetch::GhTransport for Scripted {
    fn run(&self, _arguments: &[String]) -> crate::Result<String> {
        let mut responses = self.0.lock().unwrap();
        if responses.is_empty() {
            return Err(crate::Error::GitHubApi("script exhausted".into()));
        }
        responses.remove(0)
    }
}

fn scripted(responses: Vec<crate::Result<String>>) -> GhClient {
    GhClient::with_transport(Box::new(Scripted(Mutex::new(responses)))).without_sleeping()
}

fn search_page(nodes: Vec<Value>, total: i64, extra: Value) -> crate::Result<String> {
    let mut response = json!({"data": {
        "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999,
                      "resetAt": "2099-01-01T00:00:00Z"},
        "search": {"issueCount": total,
                   "pageInfo": {"hasNextPage": false, "endCursor": null},
                   "nodes": nodes}
    }});
    if let Value::Object(fields) = extra {
        for (key, value) in fields {
            response[key] = value;
        }
    }
    Ok(response.to_string())
}

fn all_roles(conn: &Connection, client: &mut GhClient, d: &str) -> crate::Result<DayWindowOutcome> {
    collect_day(
        conn,
        client,
        "alice",
        day(d),
        &CountRole::all(),
        &CollectionControl::default(),
        now(),
    )
}

fn alice_pr(number: i64) -> FakePr {
    FakePr::new(
        "acme/app",
        number,
        "alice",
        "2026-11-02T20:00:00Z",
        "2026-11-02T21:00:00Z",
    )
}

#[test]
fn bad_logins_and_future_days_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "args.db");
    let mut client = scripted(vec![]);
    for login in ["", "alice bob"] {
        let error = collect_day(
            dw.connection(),
            &mut client,
            login,
            day("2026-11-02"),
            &CountRole::all(),
            &CollectionControl::default(),
            now(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("bad login"), "{error}");
    }
    let error = all_roles(dw.connection(), &mut client, "2026-11-04").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("2026-11-04 is after today (2026-11-03)"),
        "{error}"
    );
    assert_eq!(client.requests_issued(), 0);
    assert!(
        day_counts(
            dw.connection(),
            &["alice"],
            day("2026-11-03"),
            day("2026-11-02")
        )
        .unwrap()
        .is_empty(),
        "an inverted range has no days"
    );
}

#[test]
fn a_page_level_rate_limit_stops_the_day_with_a_wake_time() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "page-limit.db");
    let limited =
        json!({"errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]});
    let mut client = scripted(vec![search_page(vec![], 0, limited)]);
    let outcome = all_roles(dw.connection(), &mut client, "2026-11-02").unwrap();
    assert_eq!(client.requests_issued(), 1, "later roles are not searched");
    for role in &outcome.roles {
        assert_eq!(
            role.status,
            WindowStatus::RateLimited {
                wake_at: "2099-01-01T00:00:00Z".into(),
                reason: "API rate limit exceeded".into(),
            },
            "{:?}",
            role.role
        );
    }

    // With no rateLimit block the wake time is a minute out.
    let mut response: Value =
        serde_json::from_str(&search_page(vec![], 0, json!({})).unwrap()).unwrap();
    response["data"]
        .as_object_mut()
        .unwrap()
        .remove("rateLimit");
    response["errors"] = json!([{"type": "RATE_LIMITED", "message": "slow down"}]);
    let mut client = scripted(vec![Ok(response.to_string())]);
    let before = Utc::now();
    let outcome = all_roles(dw.connection(), &mut client, "2026-11-01").unwrap();
    let wake = outcome.wake_at().unwrap();
    let wake = time_dimension::parse_utc(wake).unwrap();
    assert!(wake >= before + chrono::Duration::seconds(59), "{wake}");
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-01"),
        day("2026-11-01"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, None);
    assert_eq!(
        counts[0].merged_coverage.last_status.as_deref(),
        Some("rate_limited")
    );
}

#[test]
fn a_page_level_error_fails_the_role_and_claims_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "page-error.db");
    let failed = json!({"errors": [{"type": "INTERNAL", "message": "something went wrong"}]});
    let mut client = scripted(vec![search_page(vec![], 0, failed)]);
    let outcome = collect_day(
        dw.connection(),
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert_eq!(
        outcome.roles[0].status,
        WindowStatus::Failed {
            reason: "something went wrong".into()
        }
    );
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-02"),
        day("2026-11-02"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, None);
    assert_eq!(
        counts[0].authored_coverage.last_reason.as_deref(),
        Some("something went wrong")
    );
}

#[test]
fn a_served_count_short_of_the_total_is_partial() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "short.db");
    let mut client = scripted(vec![search_page(
        vec![light_node(&alice_pr(1), "alice")],
        2,
        json!({}),
    )]);
    let outcome = collect_day(
        dw.connection(),
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    match &outcome.roles[0].status {
        WindowStatus::Partial { reason } => {
            assert!(
                reason.ends_with("provider reported 2 matches but served 1"),
                "{reason}"
            )
        }
        other => panic!("expected Partial, got {other:?}"),
    }
    assert_eq!(outcome.roles[0].served, 1, "the served row still lands");
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-02"),
        day("2026-11-02"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, None, "no coverage claimed");
}

#[test]
fn a_deleted_author_is_the_ghost_and_pending_reviews_are_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "ghost.db");
    let conn = dw.connection();
    let pr = FakePr::new(
        "acme/lib",
        9,
        "ghost-to-be",
        "2026-11-02T18:00:00Z",
        "2026-11-02T21:00:00Z",
    )
    .reviewed("SUBMITTED", "alice", "2026-11-02T20:00:00Z")
    .reviewed("PENDING", "alice", "2026-11-02T20:30:00Z");
    let mut node = light_node(&pr, "alice");
    node["author"] = Value::Null;
    node["loginReviews"]["nodes"][1]["submittedAt"] = Value::Null;
    let mut client = scripted(vec![search_page(vec![node], 1, json!({}))]);
    let outcome = collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Reviewed],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert!(outcome.complete(), "{outcome:?}");
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_pull_requests WHERE pr_key = 'acme/lib#9' AND author_key = 'user:ghost'"
        ),
        1
    );
    assert_eq!(scalar(conn, "SELECT COUNT(*) FROM fact_reviews"), 1);
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM collection_review_observation WHERE review_key = 'PENDING'"
        ),
        0
    );
    let counts = day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap();
    assert_eq!(counts[0].reviews, Some(1));
}

#[test]
fn a_spent_request_allowance_is_a_rate_limit_not_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "allowance.db");
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    client.set_request_allowance(Some(0));
    let outcome = all_roles(dw.connection(), &mut client, "2026-11-02").unwrap();
    match &outcome.roles[0].status {
        WindowStatus::RateLimited { reason, wake_at } => {
            assert!(reason.contains("request allowance spent"), "{reason}");
            assert!(time_dimension::parse_utc(wake_at).is_ok(), "{wake_at}");
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }
    assert_eq!(github.requests().light_searches, 0);
}

#[test]
fn a_review_continuation_stopped_by_the_allowance_is_a_rate_limit() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "allowance-reviews.db");
    let mut pr = FakePr::new(
        "acme/lib",
        40,
        "bob",
        "2026-11-02T16:00:00Z",
        "2026-11-02T23:00:00Z",
    );
    let base = Utc.with_ymd_and_hms(2026, 11, 2, 17, 0, 0).unwrap();
    for i in 0..120 {
        let at = (base + chrono::Duration::minutes(i)).to_rfc3339_opts(SecondsFormat::Secs, true);
        pr = pr.reviewed(&format!("M{i}"), "alice", &at);
    }
    let github = FakeGitHub::new(vec![pr]);
    let mut client = github.client();
    client.set_request_allowance(Some(1));
    let outcome = collect_day(
        dw.connection(),
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Reviewed],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert!(
        matches!(outcome.roles[0].status, WindowStatus::RateLimited { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        github.requests().review_pages,
        0,
        "the continuation was never sent"
    );
    assert!(review_frontier(dw.connection(), "alice").unwrap().is_none());
}

#[test]
fn a_database_error_from_the_client_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "db-error.db");
    let mut client = scripted(vec![Err(crate::Error::Database(
        rusqlite::Error::InvalidQuery,
    ))]);
    let error = all_roles(dw.connection(), &mut client, "2026-11-02").unwrap_err();
    assert!(matches!(error, crate::Error::Database(_)), "{error}");
}

#[test]
fn a_failed_light_write_rolls_back_the_whole_item() {
    for table in [
        "dim_repositories",
        "fact_pull_requests",
        "collection_review_observation",
        "fact_reviews",
        "pr_detail_state",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let dw = warehouse(dir.path(), "rollback.db");
        let conn = dw.connection();
        crate::storage::schema::ensure_collection_tables(conn).unwrap();
        conn.execute_batch(&format!(
            "CREATE TRIGGER refuse BEFORE INSERT ON {table}
             BEGIN SELECT RAISE(ABORT, 'injected'); END;"
        ))
        .unwrap();
        let pr = alice_pr(3).reviewed("RV", "alice", "2026-11-02T20:30:00Z");
        let mut client = scripted(vec![search_page(
            vec![light_node(&pr, "alice")],
            1,
            json!({}),
        )]);
        let error = collect_day(
            conn,
            &mut client,
            "alice",
            day("2026-11-02"),
            &[CountRole::Reviewed],
            &CollectionControl::default(),
            now(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("injected"), "{table}: {error}");
        for stored in ["fact_pull_requests", "fact_reviews", "pr_detail_state"] {
            assert_eq!(
                scalar(conn, &format!("SELECT COUNT(*) FROM {stored}")),
                0,
                "{table}: {stored} rolled back"
            );
        }
    }
}

#[test]
fn a_failed_catch_up_skips_the_gap_range() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "gap.db");
    let conn = dw.connection();
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    let reviewed = [CountRole::Reviewed];
    let control = CollectionControl::default();
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-03"),
        &reviewed,
        &control,
        now(),
    )
    .unwrap();
    github.reset_requests();
    github.with(|w| {
        w.fail_next.push(crate::Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 401: Bad credentials".into(),
        })
    });
    let outcome = collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-10-31"),
        &reviewed,
        &control,
        now(),
    )
    .unwrap();
    assert!(
        matches!(outcome.roles[0].status, WindowStatus::Failed { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        github.requests().light_searches,
        0,
        "the failed catch-up never reached search, and the gap was not tried"
    );
    assert_eq!(
        review_frontier(conn, "alice").unwrap().unwrap().oldest_day,
        day("2026-11-03"),
        "the frontier did not move"
    );
}

#[test]
fn an_inverted_gap_range_issues_no_search() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "inverted.db");
    let conn = dw.connection();
    crate::storage::schema::ensure_collection_tables(conn).unwrap();
    // A seal recorded before the frontier's own oldest day began.
    conn.execute(
        "INSERT INTO collection_review_frontier (login, oldest_day, sealed_at)
         VALUES ('alice', '2026-11-03', '2026-10-01T00:00:00Z')",
        [],
    )
    .unwrap();
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    let outcome = collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Reviewed],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert!(outcome.complete(), "{outcome:?}");
    assert_eq!(
        github.requests().light_searches,
        1,
        "only the catch-up range is searched"
    );
    let frontier = review_frontier(conn, "alice").unwrap().unwrap();
    assert_eq!(frontier.oldest_day, day("2026-11-02"));
    assert_eq!(frontier.sealed_at, now());
}

#[test]
fn detail_without_a_policy_file_still_preserves_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "standalone-detail.db");
    let conn = dw.connection();
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    let fetch = |client: &mut GhClient| {
        fetch_pull_request_detail(
            conn,
            client,
            "acme/app",
            2,
            DiffRetention::Never,
            PolicySource::Standalone,
            &CollectionControl::default(),
        )
        .unwrap()
    };
    let first = fetch(&mut client);
    assert_eq!(first.allowed, DiffRetention::Never);
    assert!(!first.diff_stored);
    github.with(|w| {
        for pr in &mut w.prs {
            pr.title = String::new();
            pr.body = String::new();
        }
    });
    let second = fetch(&mut client);
    assert_eq!(
        second.outcome.status,
        crate::refresh::TargetStatus::Refreshed
    );
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_pull_requests WHERE pr_key = 'acme/app#2' AND title = 'Title 2' AND body = 'Body 2'"
        ),
        1,
        "a missing file is still app mode for detail writes"
    );
}

/// Moves PR 2's head after the REST file walk, as a push during the fetch.
struct PushDuringFetch(FakeGitHub);

impl crate::fetch::GhTransport for PushDuringFetch {
    fn run(&self, arguments: &[String]) -> crate::Result<String> {
        let answer = self.0.run(arguments);
        if arguments.get(1).map(String::as_str) != Some("graphql") {
            self.0.with(|w| {
                w.prs
                    .iter_mut()
                    .for_each(|pr| pr.head_oid = "pushed".into())
            });
        }
        answer
    }
}

#[test]
fn a_live_diff_fails_when_the_head_moves_or_the_revision_is_unreadable() {
    let github = FakeGitHub::new(pinned_world());
    let mut client =
        GhClient::with_transport(Box::new(PushDuringFetch(github.clone()))).without_sleeping();
    let error =
        fetch_pull_request_diff_live(&mut client, "acme/app", 2, &CollectionControl::default())
            .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("acme/app#2 moved from base0002..head0002 to base0002..pushed"),
        "{message}"
    );

    github.with(|w| {
        w.fail_next.push(crate::Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 401: Bad credentials".into(),
        })
    });
    let mut client = github.client();
    let error =
        fetch_pull_request_diff_live(&mut client, "acme/app", 2, &CollectionControl::default())
            .unwrap_err();
    assert!(error.to_string().contains("Bad credentials"), "{error}");
    assert_eq!(
        github.requests().rest_files,
        1,
        "only the first fetch walked files"
    );
}

#[test]
fn stored_file_debug_output_never_includes_patch_text() {
    let file = StoredFileDiff {
        path: "src/lib.rs".into(),
        previous_path: None,
        change_type: "MODIFIED".into(),
        additions: 1,
        deletions: 0,
        patch: Some("SECRET-PATCH".into()),
    };
    let printed = format!("{file:?}");
    assert_eq!(
        printed,
        "StoredFileDiff { path: \"src/lib.rs\", patch_bytes: Some(12) }"
    );
}
