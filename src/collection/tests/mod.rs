//! Collection-mode tests against a pinned in-memory GitHub and real SQLite
//! files.

mod edges;
pub(crate) mod fake;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rusqlite::Connection;

use super::*;
use crate::GithubDW;
use crate::retention::{CollectionMode, PolicyFile, PolicySource, PolicyState, write_policy};
use crate::sync::{SyncOptions, Syncer};
use fake::{FakeGitHub, FakePr};

/// 2026-11-03T20:00:00Z: noon on Nov 3 in Los Angeles (PST, after the
/// Nov 1 DST change).
fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 3, 20, 0, 0).unwrap()
}

fn day(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

fn warehouse(dir: &Path, name: &str) -> GithubDW {
    GithubDW::open(dir.join(name)).unwrap()
}

/// The pinned world shared by the equivalence and shape tests. `alice` is
/// the person being counted. Local days are America/Los_Angeles.
fn pinned_world() -> Vec<FakePr> {
    vec![
        // Authored + merged today (Nov 3 local).
        FakePr::new(
            "acme/app",
            1,
            "alice",
            "2026-11-03T17:00:00Z",
            "2026-11-03T19:00:00Z",
        )
        .merged("2026-11-03T19:00:00Z")
        .reviewed("R1", "bob", "2026-11-03T18:30:00Z"),
        // Created 2026-11-02T07:30Z = Nov 1 23:30 PST: counts on Nov 1, not
        // Nov 2. Merged Nov 2 local.
        FakePr::new(
            "acme/app",
            2,
            "alice",
            "2026-11-02T07:30:00Z",
            "2026-11-02T18:00:00Z",
        )
        .merged("2026-11-02T18:00:00Z")
        .commented("C2", "bob", "2026-11-02T17:00:00Z"),
        // Reviewed by alice on Nov 1, last updated TODAY.
        FakePr::new(
            "acme/lib",
            3,
            "bob",
            "2026-11-01T10:00:00Z",
            "2026-11-03T18:00:00Z",
        )
        .reviewed("R3", "alice", "2026-11-01T12:00:00Z")
        .commented("C3", "carol", "2026-11-03T18:00:00Z"),
        // Two reviews by alice on different days; updated Nov 2.
        FakePr::new(
            "acme/lib",
            4,
            "carol",
            "2026-10-31T20:00:00Z",
            "2026-11-02T16:00:00Z",
        )
        .reviewed("R4a", "alice", "2026-10-31T21:00:00Z")
        .reviewed("R4b", "alice", "2026-11-02T16:00:00Z"),
        // Authored Oct 31, open, with a comment.
        FakePr::new(
            "acme/app",
            5,
            "alice",
            "2026-10-31T15:00:00Z",
            "2026-10-31T16:00:00Z",
        )
        .commented("C5", "bob", "2026-10-31T16:00:00Z"),
    ]
}

const DAYS_NEWEST_FIRST: [&str; 4] = ["2026-11-03", "2026-11-02", "2026-11-01", "2026-10-31"];

fn collect_all(conn: &Connection, github: &FakeGitHub, login: &str) -> Vec<DayWindowOutcome> {
    let mut client = github.client();
    DAYS_NEWEST_FIRST
        .iter()
        .map(|d| {
            collect_day(
                conn,
                &mut client,
                login,
                day(d),
                &CountRole::all(),
                &CollectionControl::default(),
                now(),
            )
            .unwrap()
        })
        .collect()
}

fn scalar(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

/// The dashboard's own per-day SQL (page_stats.rs / person.rs shapes).
fn dashboard_counts(conn: &Connection, login: &str, d: &str) -> (i64, i64, i64, i64) {
    let key = format!("user:{login}");
    let q = |sql: &str| -> i64 {
        conn.query_row(sql, rusqlite::params![key, d], |row| row.get(0))
            .unwrap()
    };
    (
        q(
            "SELECT COUNT(*) FROM fact_pull_requests WHERE author_key = ?1 AND created_date_key = ?2",
        ),
        q(
            "SELECT COUNT(*) FROM fact_pull_requests WHERE author_key = ?1 AND state = 'MERGED' AND merged_date_key = ?2",
        ),
        q("SELECT COUNT(*) FROM fact_reviews WHERE reviewer_key = ?1 AND submitted_date_key = ?2"),
        q(
            "SELECT COUNT(DISTINCT pr_key) FROM fact_reviews WHERE reviewer_key = ?1 AND submitted_date_key = ?2",
        ),
    )
}

fn policy(dir: &Path, mode: CollectionMode, retention: DiffRetention) -> PathBuf {
    let path = dir.join("xdg").join("adlio-collection").join("policy.json");
    write_policy(
        &path,
        &PolicyFile {
            version: 1,
            mode,
            ai: mode != CollectionMode::Counts,
            diff_retention: retention,
            updated_at: Some("2026-11-03T13:00:00Z".into()),
        },
    )
    .unwrap();
    path
}

/// Payload size actually stored, in bytes of text.
fn payload_bytes(conn: &Connection) -> i64 {
    scalar(
        conn,
        "SELECT
           (SELECT COALESCE(SUM(LENGTH(title) + LENGTH(COALESCE(body,''))), 0) FROM fact_pull_requests)
         + (SELECT COALESCE(SUM(LENGTH(COALESCE(body,''))), 0) FROM fact_reviews)
         + (SELECT COALESCE(SUM(LENGTH(COALESCE(body,''))), 0) FROM fact_issue_comments)
         + (SELECT COALESCE(SUM(LENGTH(COALESCE(body,''))), 0) FROM fact_review_comments)
         + (SELECT COALESCE(SUM(LENGTH(COALESCE(patch,''))), 0) FROM fact_file_diffs)",
    )
}

// ---------------------------------------------------------------- counts

#[test]
fn counts_only_collection_fetches_no_comment_or_diff_history() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "counts.db");
    let github = FakeGitHub::new(pinned_world());
    let outcomes = collect_all(dw.connection(), &github, "alice");
    for outcome in &outcomes {
        assert!(outcome.complete(), "{outcome:?}");
    }
    let requests = github.requests();
    assert!(requests.light_searches > 0);
    assert_eq!(requests.detailed_searches, 0, "no full-node search");
    assert_eq!(requests.single_pr, 0, "no per-PR detail query");
    assert_eq!(requests.rest_files, 0, "no REST diff walk");
    let conn = dw.connection();
    assert_eq!(scalar(conn, "SELECT COUNT(*) FROM fact_issue_comments"), 0);
    assert_eq!(scalar(conn, "SELECT COUNT(*) FROM fact_review_comments"), 0);
    assert_eq!(scalar(conn, "SELECT COUNT(*) FROM fact_file_diffs"), 0);
    // Titles and descriptions are kept when fetched, even in counts mode.
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_pull_requests WHERE body LIKE 'Body %' AND title LIKE 'Title %'"
        ),
        scalar(conn, "SELECT COUNT(*) FROM fact_pull_requests")
    );
    // Light reviews carry no body.
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_reviews WHERE body IS NOT NULL"
        ),
        0
    );
    // Light rows record their revision pair for later fingerprints.
    assert_eq!(
        detail_state(conn, "acme/app#1")
            .unwrap()
            .unwrap()
            .revision
            .as_deref(),
        Some("github:v1:acme/app#1:base0001..head0001")
    );
}

#[test]
fn day_counts_render_without_detail_and_never_fake_zero() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "counts.db");
    let github = FakeGitHub::new(pinned_world());
    let conn = dw.connection();
    // Nothing collected: every count is unknown, not zero.
    let before = day_counts(conn, &["alice"], day("2026-10-31"), day("2026-11-03")).unwrap();
    assert!(
        before
            .iter()
            .all(|c| c.authored.is_none() && c.merged.is_none() && c.reviews.is_none())
    );
    // Today only: today is known, older days stay unknown.
    let mut client = github.client();
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-03"),
        &CountRole::all(),
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    let counts = day_counts(conn, &["alice"], day("2026-10-31"), day("2026-11-03")).unwrap();
    let today = counts.iter().find(|c| c.day == day("2026-11-03")).unwrap();
    assert_eq!(
        (today.authored, today.merged, today.reviews),
        (Some(1), Some(1), Some(0))
    );
    assert_eq!(today.authored_coverage.day_closed, Some(false));
    let nov2 = counts.iter().find(|c| c.day == day("2026-11-02")).unwrap();
    assert_eq!(nov2.authored, None);
    assert_eq!(nov2.reviews, None, "not sealed for Nov 2 yet");
    // A legacy synced_ranges claim alone never makes a count known.
    crate::storage::sync_state_repository::record_range(
        conn,
        "user:alice",
        "2026-10-01",
        "2026-11-02",
        0,
    )
    .unwrap();
    let counts = day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap();
    assert!(counts[0].legacy_range_claim);
    assert_eq!(counts[0].authored, None);
}

/// Counts from the light path equal counts from the original detailed
/// collection of the same pinned world, day by day, measured with the
/// dashboard's own SQL. Also records calls, bytes, and storage for both.
#[test]
fn counts_match_the_detailed_collection_on_the_same_world() {
    let dir = tempfile::tempdir().unwrap();
    let github = FakeGitHub::new(pinned_world());

    let detailed = warehouse(dir.path(), "detailed.db");
    let mut client = github.client();
    {
        let mut syncer = Syncer::new(detailed.connection(), &mut client).as_of(now());
        let summary = syncer
            .sync_user(
                "alice",
                &SyncOptions {
                    days: Some(5),
                    pull_requests_only: true,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(summary.failed.is_empty(), "{:?}", summary.failed);
    }
    let detailed_calls = client.requests_issued();
    let detailed_bytes = client.bytes_received();
    let detailed_requests = github.requests();
    github.reset_requests();

    let light = warehouse(dir.path(), "light.db");
    let mut client = github.client();
    for d in DAYS_NEWEST_FIRST {
        let outcome = collect_day(
            light.connection(),
            &mut client,
            "alice",
            day(d),
            &CountRole::all(),
            &CollectionControl::default(),
            now(),
        )
        .unwrap();
        assert!(outcome.complete(), "{outcome:?}");
    }
    let light_calls = client.requests_issued();
    let light_bytes = client.bytes_received();
    let light_requests = github.requests();

    // The detailed sync claims closed days only (through Nov 2); compare
    // those. Today is checked against the world directly below.
    let counts = day_counts(
        light.connection(),
        &["alice"],
        day("2026-10-31"),
        day("2026-11-03"),
    )
    .unwrap();
    let mut compared = Vec::new();
    for d in ["2026-10-31", "2026-11-01", "2026-11-02"] {
        let want = dashboard_counts(detailed.connection(), "alice", d);
        let got_sql = dashboard_counts(light.connection(), "alice", d);
        assert_eq!(got_sql, want, "dashboard SQL on {d}");
        let c = counts.iter().find(|c| c.day == day(d)).unwrap();
        assert_eq!(
            (c.authored, c.merged, c.reviews, c.reviewed_prs),
            (Some(want.0), Some(want.1), Some(want.2), Some(want.3)),
            "day_counts on {d}"
        );
        compared.push((d, want));
    }
    // Expected values from the world itself, so equality is not 0 == 0.
    assert_eq!(compared[0].1, (1, 0, 1, 1), "Oct 31: PR5 authored, R4a");
    assert_eq!(
        compared[1].1,
        (1, 0, 1, 1),
        "Nov 1: PR2 authored (23:30 PST), R3"
    );
    assert_eq!(compared[2].1, (0, 1, 1, 1), "Nov 2: PR2 merged, R4b");
    let today = counts.iter().find(|c| c.day == day("2026-11-03")).unwrap();
    assert_eq!(
        (today.authored, today.merged, today.reviews),
        (Some(1), Some(1), Some(0))
    );

    let light_payload = payload_bytes(light.connection());
    let detailed_payload = payload_bytes(detailed.connection());
    assert!(light_payload < detailed_payload);
    assert_eq!(
        scalar(
            light.connection(),
            "SELECT COUNT(*) FROM fact_file_diffs WHERE patch IS NOT NULL"
        ),
        0
    );
    assert!(
        scalar(
            detailed.connection(),
            "SELECT COUNT(*) FROM fact_file_diffs WHERE patch IS NOT NULL"
        ) > 0
    );
    let report = serde_json::json!({
        "fixture": "pinned_world (5 PRs, login alice, days 2026-10-31..2026-11-03, America/Los_Angeles)",
        "detailed": {"calls": detailed_calls, "bytes": detailed_bytes,
                     "searches": detailed_requests.detailed_searches,
                     "rest_file_pages": detailed_requests.rest_files,
                     "payload_text_bytes": detailed_payload,
                     "rows": table_counts(detailed.connection())},
        "light": {"calls": light_calls, "bytes": light_bytes,
                  "searches": light_requests.light_searches,
                  "rest_file_pages": light_requests.rest_files,
                  "payload_text_bytes": light_payload,
                  "rows": table_counts(light.connection())},
        "compared_days": compared.iter().map(|(d, c)| serde_json::json!({
            "day": d, "authored": c.0, "merged": c.1, "reviews": c.2, "reviewed_prs": c.3
        })).collect::<Vec<_>>(),
    });
    if let Some(path) = std::env::var_os("GITHUBDW_MEASURE_OUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

fn table_counts(conn: &Connection) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for table in [
        "fact_pull_requests",
        "fact_reviews",
        "fact_issue_comments",
        "fact_review_comments",
        "fact_file_diffs",
    ] {
        out.insert(
            table.into(),
            scalar(conn, &format!("SELECT COUNT(*) FROM {table}")).into(),
        );
    }
    out.into()
}

// ------------------------------------------------------ reviewed frontier

/// A review on an older day sits on a pull request last updated later. The
/// newest-first walk must find it and the older day must not read 0.
#[test]
fn a_review_on_an_older_day_of_a_later_updated_pr_is_found() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "frontier.db");
    let conn = dw.connection();
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    let reviewed = [CountRole::Reviewed];
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-03"),
        &reviewed,
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    // After only today's call, Nov 1 is not sealed: unknown, not zero.
    let nov1 = &day_counts(conn, &["alice"], day("2026-11-01"), day("2026-11-01")).unwrap()[0];
    assert_eq!(nov1.reviews, None);
    // R3 was already stored by the today call (PR 3 updated today).
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_reviews WHERE review_key = 'R3'"
        ),
        1
    );
    for d in ["2026-11-02", "2026-11-01"] {
        collect_day(
            conn,
            &mut client,
            "alice",
            day(d),
            &reviewed,
            &CollectionControl::default(),
            now(),
        )
        .unwrap();
    }
    let nov1 = &day_counts(conn, &["alice"], day("2026-11-01"), day("2026-11-01")).unwrap()[0];
    assert_eq!(nov1.reviews, Some(1));
}

/// A pull request reviewed on Nov 1 whose last update moves from Nov 1 to
/// today between two calls sits in neither day window when its own day is
/// walked; the catch-up range must find it.
#[test]
fn a_pr_that_moves_between_calls_is_found_by_the_catch_up() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "frontier.db");
    let conn = dw.connection();
    let mut pr = FakePr::new(
        "acme/lib",
        9,
        "bob",
        "2026-11-01T10:00:00Z",
        "2026-11-01T12:00:00Z",
    )
    .reviewed("R9", "alice", "2026-11-01T12:00:00Z");
    pr.updated_at = "2026-11-01T12:00:00Z".into();
    let github = FakeGitHub::new(vec![pr]);
    let mut client = github.client();
    let reviewed = [CountRole::Reviewed];
    let t0 = now();
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-03"),
        &reviewed,
        &CollectionControl::default(),
        t0,
    )
    .unwrap();
    assert_eq!(
        scalar(conn, "SELECT COUNT(*) FROM fact_reviews"),
        0,
        "not in today's range yet"
    );
    // The PR is touched after the seal: now outside every older day window.
    github.with(|w| w.prs[0].updated_at = "2026-11-03T20:05:00Z".into());
    let t1 = t0 + chrono::Duration::minutes(10);
    for d in ["2026-11-02", "2026-11-01"] {
        let outcome = collect_day(
            conn,
            &mut client,
            "alice",
            day(d),
            &reviewed,
            &CollectionControl::default(),
            t1,
        )
        .unwrap();
        assert!(outcome.complete());
    }
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_reviews WHERE review_key = 'R9'"
        ),
        1
    );
    let nov1 = &day_counts(conn, &["alice"], day("2026-11-01"), day("2026-11-01")).unwrap()[0];
    assert_eq!(nov1.reviews, Some(1), "never sealed as 0");
}

#[test]
fn the_frontier_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let github = FakeGitHub::new(pinned_world());
    {
        let dw = warehouse(dir.path(), "restart.db");
        let mut client = github.client();
        collect_day(
            dw.connection(),
            &mut client,
            "alice",
            day("2026-11-03"),
            &CountRole::all(),
            &CollectionControl::default(),
            now(),
        )
        .unwrap();
    }
    let dw = warehouse(dir.path(), "restart.db");
    let frontier = review_frontier(dw.connection(), "alice").unwrap().unwrap();
    assert_eq!(frontier.oldest_day, day("2026-11-03"));
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-03"),
        day("2026-11-03"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, Some(1));
}

// ------------------------------------------------- caps, pages, failures

#[test]
fn pagination_and_cap_bisection_serve_every_match() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "cap.db");
    // 1,150 PRs on Nov 2 local at distinct seconds: one query reports more
    // than the 1,000 cap, so the day is bisected; pages are 100 each.
    let base = Utc.with_ymd_and_hms(2026, 11, 2, 9, 0, 0).unwrap();
    let prs: Vec<FakePr> = (0..1150)
        .map(|i| {
            let at = (base + chrono::Duration::seconds(i * 30))
                .to_rfc3339_opts(SecondsFormat::Secs, true);
            FakePr::new("acme/big", i + 1, "alice", &at, &at)
        })
        .collect();
    let github = FakeGitHub::new(prs);
    let mut client = github.client();
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
    assert!(outcome.complete(), "{outcome:?}");
    assert_eq!(outcome.roles[0].served, 1150);
    assert_eq!(outcome.roles[0].provider_total, 1150);
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-02"),
        day("2026-11-02"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, Some(1150));
}

#[test]
fn an_unsplittable_capped_second_is_partial_not_complete() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "cap1.db");
    let prs: Vec<FakePr> = (0..1001)
        .map(|i| {
            FakePr::new(
                "acme/big",
                i + 1,
                "alice",
                "2026-11-02T09:00:00Z",
                "2026-11-02T09:00:00Z",
            )
        })
        .collect();
    let github = FakeGitHub::new(prs);
    let mut client = github.client();
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
    assert!(
        matches!(outcome.roles[0].status, WindowStatus::Partial { .. }),
        "{outcome:?}"
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
        counts[0].authored_coverage.last_status.as_deref(),
        Some("partial")
    );
}

#[test]
fn inaccessible_items_are_expected_skips_and_other_errors_fail() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "skip.db");
    let mut hidden = FakePr::new(
        "secret/repo",
        7,
        "alice",
        "2026-11-02T20:00:00Z",
        "2026-11-02T20:00:00Z",
    );
    hidden.inaccessible = true;
    let visible = FakePr::new(
        "acme/app",
        8,
        "alice",
        "2026-11-02T21:00:00Z",
        "2026-11-02T21:00:00Z",
    );
    let github = FakeGitHub::new(vec![hidden, visible]);
    let mut client = github.client();
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
    assert!(outcome.complete());
    assert_eq!(
        (outcome.roles[0].served, outcome.roles[0].inaccessible),
        (1, 1)
    );
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-02"),
        day("2026-11-02"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, Some(1));
    assert_eq!(counts[0].authored_coverage.inaccessible, Some(1));

    // A non-retryable failure is a failure, and claims nothing.
    github.with(|w| {
        w.fail_next.push(crate::Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 401: Bad credentials".into(),
        })
    });
    let outcome = collect_day(
        dw.connection(),
        &mut client,
        "alice",
        day("2026-11-01"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert!(
        matches!(outcome.roles[0].status, WindowStatus::Failed { .. }),
        "{outcome:?}"
    );
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-01"),
        day("2026-11-01"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, None);
    assert_eq!(
        counts[0].authored_coverage.last_status.as_deref(),
        Some("failed")
    );
}

#[test]
fn rate_limits_report_a_wake_time_and_cancellation_claims_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "limits.db");
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    github.with(|w| {
        w.fail_next.push(crate::Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 403: You have exceeded a secondary rate limit".into(),
        })
    });
    let control = CollectionControl {
        cancel: None,
        defer_waits_over: Some(Duration::from_secs(5)),
    };
    let outcome = collect_day(
        dw.connection(),
        &mut client,
        "alice",
        day("2026-11-03"),
        &CountRole::all(),
        &control,
        now(),
    )
    .unwrap();
    assert!(
        outcome
            .roles
            .iter()
            .all(|r| matches!(r.status, WindowStatus::RateLimited { .. })),
        "{outcome:?}"
    );
    assert!(outcome.wake_at().is_some());
    let counts = day_counts(
        dw.connection(),
        &["alice"],
        day("2026-11-03"),
        day("2026-11-03"),
    )
    .unwrap();
    assert_eq!(counts[0].authored, None);
    assert_eq!(
        counts[0].authored_coverage.last_status.as_deref(),
        Some("rate_limited")
    );
    assert!(counts[0].authored_coverage.wake_at.is_some());

    let cancel = Arc::new(AtomicBool::new(true));
    let control = CollectionControl {
        cancel: Some(cancel.clone()),
        defer_waits_over: None,
    };
    let outcome = collect_day(
        dw.connection(),
        &mut client,
        "alice",
        day("2026-11-03"),
        &CountRole::all(),
        &control,
        now(),
    )
    .unwrap();
    assert!(
        outcome
            .roles
            .iter()
            .all(|r| r.status == WindowStatus::Cancelled)
    );
    assert!(review_frontier(dw.connection(), "alice").unwrap().is_none());
    cancel.store(false, Ordering::SeqCst);
    let outcome = collect_day(
        dw.connection(),
        &mut client,
        "alice",
        day("2026-11-03"),
        &CountRole::all(),
        &control,
        now(),
    )
    .unwrap();
    assert!(
        outcome.complete(),
        "the next call after cancellation proceeds"
    );
}

#[test]
fn day_bounds_follow_the_local_calendar_across_dst() {
    let tz: chrono_tz::Tz = "America/Los_Angeles".parse().unwrap();
    let (start, end) = day_bounds(tz, day("2026-11-01")).unwrap();
    assert_eq!(end - start + 1, 25 * 3600, "DST ends Nov 1: a 25-hour day");
    assert_eq!(stamp(start).unwrap(), "2026-11-01T07:00:00Z");
    assert_eq!(stamp(end).unwrap(), "2026-11-02T07:59:59Z");
}

// --------------------------------------------- retention and preservation

/// Seed sentinel payloads through a Full (retain) detail fetch.
fn seed_full(conn: &Connection, github: &FakeGitHub, policy_path: &Path, number: i64) {
    let mut client = github.client();
    let outcome = fetch_pull_request_detail(
        conn,
        &mut client,
        "acme/app",
        number,
        DiffRetention::Retain,
        PolicySource::File(policy_path.into()),
        &CollectionControl::default(),
    )
    .unwrap();
    assert_eq!(
        outcome.outcome.status,
        crate::refresh::TargetStatus::Refreshed,
        "{:?}",
        outcome.outcome.error
    );
    assert!(outcome.diff_stored);
}

fn snapshot(conn: &Connection) -> Vec<String> {
    let mut rows = Vec::new();
    for (table, key) in [
        ("fact_pull_requests", "pr_key"),
        ("fact_reviews", "review_key"),
        ("fact_issue_comments", "comment_key"),
        ("fact_review_comments", "comment_key"),
        ("fact_file_diffs", "file_diff_key"),
    ] {
        let sql = match table {
            "fact_pull_requests" => "SELECT pr_key || '|' || COALESCE(title,'∅') || '|' || COALESCE(body,'∅') FROM fact_pull_requests".to_string(),
            "fact_file_diffs" => "SELECT file_diff_key || '|' || COALESCE(patch,'∅') || '|' || COALESCE(previous_path,'∅') FROM fact_file_diffs".to_string(),
            _ => format!("SELECT {key} || '|' || COALESCE(body,'∅') FROM {table}"),
        };
        let mut statement = conn.prepare(&sql).unwrap();
        let mut found: Vec<String> = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|r| format!("{table}:{}", r.unwrap()))
            .collect();
        rows.append(&mut found);
    }
    rows.sort();
    rows
}

#[test]
fn full_retains_new_diffs_and_basic_or_counts_never_write_new_ones() {
    for (mode, retention, expect_patch) in [
        (CollectionMode::Full, DiffRetention::Retain, true),
        (CollectionMode::Basic, DiffRetention::Transient, false),
        (CollectionMode::Counts, DiffRetention::Never, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = policy(dir.path(), mode, retention);
        let dw = warehouse(dir.path(), "policy.db");
        let github = FakeGitHub::new(pinned_world());
        let mut client = github.client();
        // The caller asks for Retain; the file is the ceiling.
        let outcome = fetch_pull_request_detail(
            dw.connection(),
            &mut client,
            "acme/app",
            2,
            DiffRetention::Retain,
            PolicySource::File(path),
            &CollectionControl::default(),
        )
        .unwrap();
        assert_eq!(outcome.allowed, retention.min(DiffRetention::Retain));
        let conn = dw.connection();
        // Title, body, and comments are stored in every mode.
        assert_eq!(
            scalar(
                conn,
                "SELECT COUNT(*) FROM fact_pull_requests WHERE pr_key='acme/app#2' AND title='Title 2' AND body='Body 2'"
            ),
            1
        );
        assert_eq!(
            scalar(
                conn,
                "SELECT COUNT(*) FROM fact_issue_comments WHERE body='comment body C2'"
            ),
            1
        );
        let patches = scalar(
            conn,
            "SELECT COUNT(*) FROM fact_file_diffs WHERE patch IS NOT NULL",
        );
        assert_eq!(patches > 0, expect_patch, "{mode:?}");
        assert_eq!(outcome.diff_stored, expect_patch);
        assert_eq!(
            github.requests().rest_files > 0,
            expect_patch,
            "{mode:?}: patch walk only when retained"
        );
        assert!(outcome.revision.is_some());
    }
}

#[test]
fn missing_policy_is_legacy_and_invalid_policy_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("none").join("policy.json");
    assert_eq!(
        crate::retention::read_policy(&missing),
        PolicyState::Missing
    );
    let dw = warehouse(dir.path(), "m.db");
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    // Legacy repository-less path: a plain Syncer with no file retains.
    {
        let mut syncer = Syncer::new(dw.connection(), &mut client)
            .with_policy_source(PolicySource::File(missing.clone()));
        syncer
            .refresh_targets(
                &[crate::refresh::RefreshTarget {
                    kind: crate::refresh::TargetKind::PullRequest,
                    repository: "acme/app".into(),
                    number: 1,
                }],
                &crate::refresh::RefreshOptions { skip_diffs: false },
            )
            .unwrap();
    }
    assert!(
        scalar(
            dw.connection(),
            "SELECT COUNT(*) FROM fact_file_diffs WHERE patch IS NOT NULL"
        ) > 0
    );

    let invalid = dir.path().join("bad.json");
    std::fs::write(&invalid, b"{\"version\":1,\"mode\":\"full\"").unwrap();
    let dw = warehouse(dir.path(), "i.db");
    github.reset_requests();
    {
        let mut syncer = Syncer::new(dw.connection(), &mut client)
            .with_policy_source(PolicySource::File(invalid));
        syncer
            .refresh_targets(
                &[crate::refresh::RefreshTarget {
                    kind: crate::refresh::TargetKind::PullRequest,
                    repository: "acme/app".into(),
                    number: 1,
                }],
                &crate::refresh::RefreshOptions { skip_diffs: false },
            )
            .unwrap();
    }
    assert_eq!(
        scalar(
            dw.connection(),
            "SELECT COUNT(*) FROM fact_file_diffs WHERE patch IS NOT NULL"
        ),
        0
    );
    assert_eq!(github.requests().rest_files, 0);
}

/// A diff fetched while the ceiling allowed retention is dropped, unwritten,
/// when the ceiling falls before the write.
#[test]
fn a_mid_flight_policy_change_stops_the_diff_write() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "flight.db");
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = reads.clone();
    let source = PolicySource::Custom(Arc::new(move || {
        let n = counter.fetch_add(1, Ordering::SeqCst);
        let retention = if n == 0 {
            DiffRetention::Retain
        } else {
            DiffRetention::Transient
        };
        PolicyState::Present(PolicyFile {
            version: 1,
            mode: CollectionMode::Full,
            ai: true,
            diff_retention: retention,
            updated_at: None,
        })
    }));
    let mut syncer = Syncer::new(dw.connection(), &mut client)
        .with_diff_retention(DiffRetention::Retain)
        .with_policy_source(source);
    let report = syncer
        .refresh_targets(
            &[crate::refresh::RefreshTarget {
                kind: crate::refresh::TargetKind::PullRequest,
                repository: "acme/app".into(),
                number: 2,
            }],
            &crate::refresh::RefreshOptions { skip_diffs: false },
        )
        .unwrap();
    assert_eq!(report.failed(), 0);
    drop(syncer);
    assert!(github.requests().rest_files > 0, "fetched while allowed");
    assert!(
        reads.load(Ordering::SeqCst) >= 2,
        "re-read at the write boundary"
    );
    assert_eq!(
        scalar(
            dw.connection(),
            "SELECT COUNT(*) FROM fact_file_diffs WHERE patch IS NOT NULL"
        ),
        0
    );
    assert_no_sentinel_on_disk(&dir.path().join("flight.db"), "PATCH-SENTINEL-2");
}

fn assert_no_sentinel_on_disk(db: &Path, sentinel: &str) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let path = PathBuf::from(format!("{}{suffix}", db.display()));
        if let Ok(bytes) = std::fs::read(&path) {
            assert!(
                !bytes
                    .windows(sentinel.len())
                    .any(|w| w == sentinel.as_bytes()),
                "{sentinel} leaked into {}",
                path.display()
            );
        }
    }
}

/// Full -> Basic -> Counts -> Full: every stored row and payload survives,
/// including stored diffs, and lightweight or sparse updates never blank them.
#[test]
fn every_policy_transition_preserves_existing_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let path = policy(dir.path(), CollectionMode::Full, DiffRetention::Retain);
    let dw = warehouse(dir.path(), "transitions.db");
    let github = FakeGitHub::new(pinned_world());
    let conn = dw.connection();
    seed_full(conn, &github, &path, 2);
    seed_full(conn, &github, &path, 1);
    let baseline = snapshot(conn);
    assert!(baseline.iter().any(|r| r.contains("PATCH-SENTINEL-2")));

    // Provider now returns blank payloads and a shorter file list.
    github.with(|w| {
        for pr in &mut w.prs {
            pr.title = String::new();
            pr.body = String::new();
            pr.files.clear();
            for c in &mut pr.comments {
                c.body = String::new();
            }
            for r in &mut pr.reviews {
                r.body = String::new();
            }
        }
    });
    for (mode, retention) in [
        (CollectionMode::Basic, DiffRetention::Transient),
        (CollectionMode::Counts, DiffRetention::Never),
        (CollectionMode::Full, DiffRetention::Retain),
    ] {
        policy(dir.path(), mode, retention);
        let mut client = github.client();
        for number in [1, 2] {
            fetch_pull_request_detail(
                conn,
                &mut client,
                "acme/app",
                number,
                DiffRetention::Retain,
                PolicySource::File(path.clone()),
                &CollectionControl::default(),
            )
            .unwrap();
        }
        // The scheduled legacy path, in app mode, through the same file.
        {
            let mut syncer = Syncer::new(conn, &mut client)
                .as_of(now())
                .with_policy_source(PolicySource::File(path.clone()));
            syncer
                .sync_user(
                    "alice",
                    &SyncOptions {
                        days: Some(5),
                        pull_requests_only: true,
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        // And the counts path.
        for d in DAYS_NEWEST_FIRST {
            collect_day(
                conn,
                &mut client,
                "alice",
                day(d),
                &CountRole::all(),
                &CollectionControl::default(),
                now(),
            )
            .unwrap();
        }
        let after = snapshot(conn);
        for row in &baseline {
            assert!(after.contains(row), "{mode:?} lost or changed {row}");
        }
        // Stored diffs stay readable in every mode.
        let stored = stored_pull_request_diff(conn, "acme/app#2").unwrap();
        assert!(stored.iter().any(|f| {
            f.patch
                .as_deref()
                .is_some_and(|p| p.contains("PATCH-SENTINEL-2"))
        }));
    }
}

/// The legacy (no-policy-file) path keeps its documented behaviour.
#[test]
fn without_a_policy_file_the_legacy_writes_are_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "legacy.db");
    let github = FakeGitHub::new(pinned_world());
    let mut client = github.client();
    let target = [crate::refresh::RefreshTarget {
        kind: crate::refresh::TargetKind::PullRequest,
        repository: "acme/app".into(),
        number: 2,
    }];
    Syncer::new(dw.connection(), &mut client)
        .refresh_targets(
            &target,
            &crate::refresh::RefreshOptions { skip_diffs: true },
        )
        .unwrap();
    github.with(|w| w.prs.iter_mut().for_each(|p| p.body = String::new()));
    Syncer::new(dw.connection(), &mut client)
        .refresh_targets(
            &target,
            &crate::refresh::RefreshOptions { skip_diffs: true },
        )
        .unwrap();
    assert_eq!(
        scalar(
            dw.connection(),
            "SELECT COUNT(*) FROM fact_pull_requests WHERE pr_key='acme/app#2' AND body = ''"
        ),
        1,
        "standalone mode mirrors the provider exactly as before"
    );
}

#[test]
fn a_live_diff_is_returned_in_memory_and_written_nowhere() {
    let dir = tempfile::tempdir().unwrap();
    let path = policy(dir.path(), CollectionMode::Counts, DiffRetention::Never);
    let dw = warehouse(dir.path(), "live.db");
    let github = FakeGitHub::new(pinned_world());
    let conn = dw.connection();
    let mut client = github.client();
    fetch_pull_request_detail(
        conn,
        &mut client,
        "acme/app",
        2,
        DiffRetention::Transient,
        PolicySource::File(path),
        &CollectionControl::default(),
    )
    .unwrap();
    let before = snapshot(conn);
    let diff =
        fetch_pull_request_diff_live(&mut client, "acme/app", 2, &CollectionControl::default())
            .unwrap();
    assert!(
        diff.files[0]
            .patch
            .as_deref()
            .unwrap()
            .contains("PATCH-SENTINEL-2")
    );
    assert!(
        diff.fingerprint
            .starts_with("github:v1:acme/app#2:base0002..head0002:sha256=")
    );
    let again =
        fetch_pull_request_diff_live(&mut client, "acme/app", 2, &CollectionControl::default())
            .unwrap();
    assert_eq!(
        diff.fingerprint, again.fingerprint,
        "stable for the same content"
    );
    assert!(
        !format!("{diff:?}").contains("PATCH-SENTINEL"),
        "Debug never prints patch text"
    );
    assert_eq!(snapshot(conn), before);
    conn.execute_batch("PRAGMA wal_checkpoint(FULL)").unwrap();
    assert_no_sentinel_on_disk(&dir.path().join("live.db"), "PATCH-SENTINEL-2");
    // Content change changes the fingerprint.
    github.with(|w| w.prs[1].files[0].patch.push('x'));
    let changed =
        fetch_pull_request_diff_live(&mut client, "acme/app", 2, &CollectionControl::default())
            .unwrap();
    assert_ne!(changed.fingerprint, diff.fingerprint);
}

#[test]
fn light_upserts_cannot_blank_detail_payloads_or_counters() {
    let dir = tempfile::tempdir().unwrap();
    let path = policy(dir.path(), CollectionMode::Full, DiffRetention::Retain);
    let dw = warehouse(dir.path(), "sparse.db");
    let github = FakeGitHub::new(pinned_world());
    let conn = dw.connection();
    seed_full(conn, &github, &path, 2);
    let counters: (i64, i64) = conn
        .query_row(
            "SELECT comment_count, review_count FROM fact_pull_requests WHERE pr_key='acme/app#2'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let baseline = snapshot(conn);
    github.with(|w| {
        w.prs.iter_mut().for_each(|p| {
            p.title.clear();
            p.body.clear();
            p.comments.clear();
        })
    });
    let mut client = github.client();
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-01"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert_eq!(snapshot(conn), baseline);
    let after: (i64, i64) = conn
        .query_row(
            "SELECT comment_count, review_count FROM fact_pull_requests WHERE pr_key='acme/app#2'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(after, counters, "light rows keep detail counters");
}

/// A heavier pinned world: 48 pull requests over Oct 31 - Nov 2, each with
/// 20 conversation comments, 12 files, and ~1.5 KB patches, reviewed by
/// alice. Counts must match the detailed collection; the cost is recorded.
#[test]
fn heavy_world_counts_match_and_cost_is_measured() {
    let dir = tempfile::tempdir().unwrap();
    let mut prs = Vec::new();
    for i in 0..48_i64 {
        let created =
            Utc.with_ymd_and_hms(2026, 10, 31, 16, 0, 0).unwrap() + chrono::Duration::hours(i);
        let at = |offset: i64| {
            (created + chrono::Duration::minutes(offset)).to_rfc3339_opts(SecondsFormat::Secs, true)
        };
        let author = if i % 2 == 0 { "alice" } else { "bob" };
        let mut pr = FakePr::new("acme/heavy", i + 1, author, &at(0), &at(90));
        if i % 3 != 0 {
            pr = pr.merged(&at(90));
        }
        if author == "bob" {
            pr = pr.reviewed(&format!("HR{i}"), "alice", &at(60));
        }
        for c in 0..20 {
            pr = pr.commented(&format!("HC{i}-{c}"), "carol", &at(10 + c));
        }
        pr.files = (0..12)
            .map(|f| fake::FakeFile {
                path: format!("src/m{f}.rs"),
                additions: 40,
                deletions: 10,
                patch: format!("@@ -1,10 +1,40 @@ {}", "+ line of code\n".repeat(100)),
            })
            .collect();
        prs.push(pr);
    }
    let github = FakeGitHub::new(prs);
    let detailed = warehouse(dir.path(), "heavy-detailed.db");
    let mut client = github.client();
    Syncer::new(detailed.connection(), &mut client)
        .as_of(now())
        .sync_user(
            "alice",
            &SyncOptions {
                days: Some(5),
                pull_requests_only: true,
                ..Default::default()
            },
        )
        .unwrap();
    let detailed_cost = (client.requests_issued(), client.bytes_received());
    let light = warehouse(dir.path(), "heavy-light.db");
    let mut client = github.client();
    for d in DAYS_NEWEST_FIRST {
        assert!(
            collect_day(
                light.connection(),
                &mut client,
                "alice",
                day(d),
                &CountRole::all(),
                &CollectionControl::default(),
                now()
            )
            .unwrap()
            .complete()
        );
    }
    let light_cost = (client.requests_issued(), client.bytes_received());
    let mut totals = (0, 0, 0);
    for d in ["2026-10-31", "2026-11-01", "2026-11-02"] {
        let want = dashboard_counts(detailed.connection(), "alice", d);
        assert_eq!(
            dashboard_counts(light.connection(), "alice", d),
            want,
            "{d}"
        );
        let c = &day_counts(light.connection(), &["alice"], day(d), day(d)).unwrap()[0];
        assert_eq!(
            (c.authored, c.merged, c.reviews),
            (Some(want.0), Some(want.1), Some(want.2))
        );
        totals = (totals.0 + want.0, totals.1 + want.1, totals.2 + want.2);
    }
    assert!(totals.0 > 0 && totals.1 > 0 && totals.2 > 0, "{totals:?}");
    detailed
        .connection()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    light
        .connection()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let file_size = |name: &str| std::fs::metadata(dir.path().join(name)).unwrap().len();
    let report = serde_json::json!({
        "fixture": "heavy_world: 48 PRs Oct 31-Nov 2 (24 by alice, 24 by bob reviewed by alice), 20 comments, 12 files, ~1.5 KB patch each",
        "closed_day_totals": {"authored": totals.0, "merged": totals.1, "reviews": totals.2},
        "detailed": {"calls": detailed_cost.0, "bytes": detailed_cost.1,
                     "payload_text_bytes": payload_bytes(detailed.connection()),
                     "db_file_bytes": file_size("heavy-detailed.db"),
                     "rows": table_counts(detailed.connection())},
        "light": {"calls": light_cost.0, "bytes": light_cost.1,
                  "payload_text_bytes": payload_bytes(light.connection()),
                  "db_file_bytes": file_size("heavy-light.db"),
                  "rows": table_counts(light.connection())},
    });
    if let Some(path) = std::env::var_os("GITHUBDW_MEASURE_HEAVY_OUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

/// The collection tables are not a numbered migration, so a warehouse used
/// by this version still opens under the 0.3.4 migration set (user_version
/// stays 8), and a read-only reader of a warehouse that never saw this
/// version gets "unknown" counts rather than an error.
#[test]
fn schema_stays_openable_by_the_previous_release() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compat.db");
    {
        let dw = GithubDW::open(&path).unwrap();
        let github = FakeGitHub::new(pinned_world());
        let mut client = github.client();
        collect_day(
            dw.connection(),
            &mut client,
            "alice",
            day("2026-11-03"),
            &CountRole::all(),
            &CollectionControl::default(),
            now(),
        )
        .unwrap();
    }
    let mut conn = Connection::open(&path).unwrap();
    assert_eq!(scalar(&conn, "PRAGMA user_version"), 8);
    // The 0.3.4 migration list is exactly the first eight steps.
    crate::storage::schema::migrations()
        .to_latest(&mut conn)
        .expect("0.3.3 migrations accept the file");

    // A pre-existing 0.3.3 warehouse read through a read-only handle.
    let old = dir.path().join("old.db");
    {
        let mut c = Connection::open(&old).unwrap();
        crate::storage::schema::migrations()
            .to_latest(&mut c)
            .unwrap();
    }
    let ro = GithubDW::open_read_only(&old).unwrap();
    let counts = day_counts(
        ro.connection(),
        &["alice"],
        day("2026-11-02"),
        day("2026-11-03"),
    )
    .unwrap();
    assert!(
        counts
            .iter()
            .all(|c| c.authored.is_none() && c.reviews.is_none())
    );
    assert!(review_frontier(ro.connection(), "alice").unwrap().is_none());
    assert!(
        detail_state(ro.connection(), "acme/app#1")
            .unwrap()
            .is_none()
    );
}

// ------------------------------------------ correctness follow-ups (round 2)

/// A review deleted at the provider (or re-attributed) after it was stored
/// stays in the warehouse but stops counting once the walk re-reads that
/// pull request; a dismissed review moves to `reviews_dismissed`.
#[test]
fn deleted_and_dismissed_reviews_stop_counting_without_deleting_rows() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "dismissed.db");
    let conn = dw.connection();
    let github = FakeGitHub::new(vec![
        FakePr::new(
            "acme/lib",
            20,
            "bob",
            "2026-11-02T17:00:00Z",
            "2026-11-02T18:30:00Z",
        )
        .reviewed("D1", "alice", "2026-11-02T18:00:00Z")
        .reviewed("D2", "alice", "2026-11-02T18:30:00Z"),
        FakePr::new(
            "acme/lib",
            21,
            "bob",
            "2026-11-02T17:00:00Z",
            "2026-11-02T19:00:00Z",
        )
        .reviewed("D3", "alice", "2026-11-02T19:00:00Z"),
    ]);
    let mut client = github.client();
    let reviewed = [CountRole::Reviewed];
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-02"),
        &reviewed,
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    let c = &day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap()[0];
    assert_eq!(
        (
            c.reviews,
            c.reviewed_prs,
            c.reviews_dismissed,
            c.reviews_absent
        ),
        (Some(3), Some(2), Some(0), Some(0))
    );

    // D2 is deleted, D3 is dismissed; both PRs are touched, so the next
    // call's catch-up re-reads them.
    let later = now() + chrono::Duration::hours(2);
    let touched = (later - chrono::Duration::minutes(5)).to_rfc3339_opts(SecondsFormat::Secs, true);
    github.with(|w| {
        w.prs[0].reviews.retain(|r| r.id != "D2");
        w.prs[0].updated_at = touched.clone();
        w.prs[1].reviews[0].state = "DISMISSED".into();
        w.prs[1].updated_at = touched.clone();
    });
    let outcome = collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-03"),
        &reviewed,
        &CollectionControl::default(),
        later,
    )
    .unwrap();
    assert!(outcome.complete());
    let c = &day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap()[0];
    assert_eq!(c.reviews, Some(1), "only D1 is a current review");
    assert_eq!(c.reviewed_prs, Some(1));
    assert_eq!(c.reviews_dismissed, Some(1), "D3");
    assert_eq!(c.reviews_absent, Some(1), "D2");
    // Nothing was deleted.
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_reviews WHERE review_key IN ('D1','D2','D3')"
        ),
        3
    );
    // Re-appearing (for example a re-attribution undone) counts again.
    github.with(|w| {
        w.prs[0].reviews.push(fake::FakeReview {
            id: "D2".into(),
            reviewer: "alice".into(),
            state: "APPROVED".into(),
            body: String::new(),
            submitted_at: "2026-11-02T18:30:00Z".into(),
        });
        w.prs[0].updated_at =
            (later + chrono::Duration::minutes(10)).to_rfc3339_opts(SecondsFormat::Secs, true);
    });
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-03"),
        &reviewed,
        &CollectionControl::default(),
        later + chrono::Duration::minutes(20),
    )
    .unwrap();
    let c = &day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap()[0];
    assert_eq!((c.reviews, c.reviews_absent), (Some(2), Some(0)));
}

/// A stored pull request the provider no longer serves for a day it was
/// re-asked about is not counted for that day (its row stays).
#[test]
fn a_stored_pr_the_provider_stops_serving_is_not_counted_after_a_recheck() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "gone.db");
    let conn = dw.connection();
    let github = FakeGitHub::new(vec![
        FakePr::new(
            "acme/app",
            30,
            "alice",
            "2026-11-02T17:00:00Z",
            "2026-11-02T17:00:00Z",
        ),
        FakePr::new(
            "acme/app",
            31,
            "alice",
            "2026-11-02T18:00:00Z",
            "2026-11-02T18:00:00Z",
        ),
    ]);
    let mut client = github.client();
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert_eq!(
        day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap()[0].authored,
        Some(2)
    );
    github.with(|w| w.prs.retain(|p| p.number != 31));
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        now() + chrono::Duration::hours(1),
    )
    .unwrap();
    assert_eq!(
        day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap()[0].authored,
        Some(1)
    );
    assert_eq!(
        scalar(
            conn,
            "SELECT COUNT(*) FROM fact_pull_requests WHERE pr_key = 'acme/app#31'"
        ),
        1,
        "row kept"
    );
}

/// More than 100 reviews by one login on one pull request are read in full
/// through the review-list continuation; a failed continuation is Partial.
#[test]
fn reviews_beyond_the_first_page_are_paginated() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "many.db");
    let conn = dw.connection();
    let mut pr = FakePr::new(
        "acme/lib",
        40,
        "bob",
        "2026-11-02T16:00:00Z",
        "2026-11-02T23:00:00Z",
    );
    let base = Utc.with_ymd_and_hms(2026, 11, 2, 17, 0, 0).unwrap();
    for i in 0..230 {
        let at = (base + chrono::Duration::minutes(i)).to_rfc3339_opts(SecondsFormat::Secs, true);
        pr = pr.reviewed(&format!("M{i}"), "alice", &at);
    }
    let github = FakeGitHub::new(vec![pr]);
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
        github.requests().review_pages,
        2,
        "pages 2 and 3 of 230 reviews"
    );
    let c = &day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-03")).unwrap();
    let total: i64 = c.iter().filter_map(|c| c.reviews).sum();
    assert_eq!(total, 230);

    // A continuation that fails leaves the role Partial, never complete.
    let dir2 = tempfile::tempdir().unwrap();
    let dw2 = warehouse(dir2.path(), "many2.db");
    let github2 = FakeGitHub::new(github.with(|w| w.prs.clone()));
    let mut client2 = github2.client();
    github2.with(|w| w.fail_review_pages = true);
    let outcome = collect_day(
        dw2.connection(),
        &mut client2,
        "alice",
        day("2026-11-02"),
        &[CountRole::Reviewed],
        &CollectionControl::default(),
        now(),
    )
    .unwrap();
    assert!(
        matches!(outcome.roles[0].status, WindowStatus::Partial { .. }),
        "{outcome:?}"
    );
    assert!(
        review_frontier(dw2.connection(), "alice")
            .unwrap()
            .is_none(),
        "no seal"
    );
    // The truncated list must not mark the unread reviews absent.
    assert_eq!(
        scalar(
            dw2.connection(),
            "SELECT COUNT(*) FROM collection_review_observation WHERE present = 0"
        ),
        0
    );
}

/// An update the search index reports late (within the one-hour allowance)
/// is found by the next call's catch-up; coverage reports `settled` only once
/// the covering fetch is an allowance past the day's end.
#[test]
fn late_indexed_updates_within_the_allowance_are_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let dw = warehouse(dir.path(), "lag.db");
    let conn = dw.connection();
    // Reviewed and updated 30 minutes before the first call, but the index
    // still shows the PR's old update time.
    let t0 = now();
    let review_at = (t0 - chrono::Duration::minutes(30)).to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut pr = FakePr::new("acme/lib", 50, "bob", "2026-10-01T16:00:00Z", &review_at)
        .reviewed("L1", "alice", &review_at);
    pr.search_updated_at = Some("2026-10-01T16:00:00Z".into());
    let github = FakeGitHub::new(vec![pr]);
    let mut client = github.client();
    let reviewed = [CountRole::Reviewed];
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-03"),
        &reviewed,
        &CollectionControl::default(),
        t0,
    )
    .unwrap();
    assert_eq!(
        scalar(conn, "SELECT COUNT(*) FROM fact_reviews"),
        0,
        "index not caught up yet"
    );
    let today = &day_counts(conn, &["alice"], day("2026-11-03"), day("2026-11-03")).unwrap()[0];
    assert_eq!(today.reviews, Some(0));
    assert_eq!(
        today.reviewed_coverage.settled,
        Some(false),
        "a count as of t0, not settled"
    );
    // The index catches up 40 minutes later; the next call (for an older
    // day) re-walks the hour before the previous seal and finds it.
    github.with(|w| w.prs[0].search_updated_at = None);
    let t1 = t0 + chrono::Duration::minutes(40);
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-02"),
        &reviewed,
        &CollectionControl::default(),
        t1,
    )
    .unwrap();
    let today = &day_counts(conn, &["alice"], day("2026-11-03"), day("2026-11-03")).unwrap()[0];
    assert_eq!(today.reviews, Some(1));

    // Settled: Nov 2 authored, collected at t0 (12 h after Nov 2 ended).
    collect_day(
        conn,
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        t0,
    )
    .unwrap();
    let nov2 = &day_counts(conn, &["alice"], day("2026-11-02"), day("2026-11-02")).unwrap()[0];
    assert_eq!(nov2.authored_coverage.settled, Some(true));
    assert_eq!(nov2.reviewed_coverage.settled, Some(true));
    // Collected 10 minutes after midnight: complete as of then, not settled.
    let just_after = Utc.with_ymd_and_hms(2026, 11, 3, 8, 10, 0).unwrap();
    let dw2 = warehouse(dir.path(), "lag2.db");
    collect_day(
        dw2.connection(),
        &mut client,
        "alice",
        day("2026-11-02"),
        &[CountRole::Authored],
        &CollectionControl::default(),
        just_after,
    )
    .unwrap();
    let nov2 = &day_counts(
        dw2.connection(),
        &["alice"],
        day("2026-11-02"),
        day("2026-11-02"),
    )
    .unwrap()[0];
    assert_eq!(nov2.authored_coverage.day_closed, Some(true));
    assert_eq!(nov2.authored_coverage.settled, Some(false));
}
