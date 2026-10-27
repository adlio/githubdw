use super::*;
use crate::GithubDW;
use crate::storage::time_dimension;

/// Seed dim rows + PRs across two quarters for delta checks.
fn seed(warehouse: &GithubDW) {
    let conn = warehouse.connection();
    for date in ["2026-01-10", "2026-04-05", "2026-04-10", "2026-04-20"] {
        time_dimension::ensure_date_row(conn, date).unwrap();
    }
    time_dimension::ensure_time_row(conn, "10:00", (9, 17)).unwrap();
    conn.execute_batch(
        "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
         VALUES ('user:alice', 'user', 'alice', 1, 0, 'alice'),
                ('user:bob', 'user', 'bob', 1, 0, 'bob');
         INSERT INTO dim_repositories (repo_key, owner, name)
         VALUES ('octo/alpha', 'octo', 'alpha');
         INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title,
             created_at, created_date_key, created_time_key, additions, deletions)
         VALUES
           -- Q1: one merged PR by alice
           ('octo/alpha#1', 1, 'octo/alpha', 'user:alice', 'MERGED', 'Q1 change',
            '2026-01-10T18:00:00Z', '2026-01-10', '10:00', 100, 10),
           -- Q2: two PRs by alice, one by bob
           ('octo/alpha#2', 2, 'octo/alpha', 'user:alice', 'MERGED', 'Q2 change A',
            '2026-04-05T18:00:00Z', '2026-04-05', '10:00', 30, 3),
           ('octo/alpha#3', 3, 'octo/alpha', 'user:alice', 'OPEN', 'Q2 change B',
            '2026-04-10T18:00:00Z', '2026-04-10', '10:00', 20, 2),
           ('octo/alpha#4', 4, 'octo/alpha', 'user:bob', 'MERGED', 'Q2 bob change',
            '2026-04-20T18:00:00Z', '2026-04-20', '10:00', 5, 1);
         INSERT INTO fact_reviews (review_key, pr_key, reviewer_key, state, submitted_at,
             submitted_date_key, submitted_time_key)
         VALUES
           ('R1', 'octo/alpha#2', 'user:bob', 'APPROVED', '2026-04-06T10:00:00Z', '2026-04-05', '10:00'),
           ('R2', 'octo/alpha#4', 'user:alice', 'APPROVED', '2026-04-20T10:00:00Z', '2026-04-20', '10:00');",
    )
    .unwrap();
}

fn set_timezone(conn: &rusqlite::Connection, name: &str) {
    conn.execute(
        "INSERT INTO config (key, value) VALUES ('timezone', ?1)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [name],
    )
    .unwrap();
}

/// The default reference date must come from the warehouse's own calendar,
/// not the UTC one, because it is compared against `dim_date.date_key`
/// values built in the configured zone. Two zones 26 hours apart can never
/// share a local date, so this holds at every instant without a fake clock.
#[test]
fn reference_date_follows_the_configured_timezone() {
    let east = GithubDW::open_in_memory().unwrap();
    set_timezone(east.connection(), "Pacific/Kiritimati"); // UTC+14
    let west = GithubDW::open_in_memory().unwrap();
    set_timezone(west.connection(), "Etc/GMT+12"); // UTC-12

    let east_reference = MetricsEngine::new(east.connection()).reference_date();
    let west_reference = MetricsEngine::new(west.connection()).reference_date();
    assert_ne!(
        east_reference, west_reference,
        "reference date is config-driven, not a single global UTC date"
    );
    assert_eq!(
        east_reference,
        crate::storage::time_dimension::today(east.connection()).unwrap(),
    );
    assert_eq!(
        west_reference,
        crate::storage::time_dimension::today(west.connection()).unwrap(),
    );
}

#[test]
fn user_metrics_with_deltas() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    // Reference date after Q2 ends so the period is complete (no truncation).
    let engine = MetricsEngine::new(warehouse.connection())
        .as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let metrics = engine
        .user_metrics("alice", &Period::Quarter(2026, 2))
        .unwrap();
    assert_eq!(metrics.prs_opened.current, 2);
    assert_eq!(metrics.prs_opened.previous, 1);
    assert_eq!(metrics.prs_opened.delta, 1);
    assert_eq!(metrics.prs_merged.current, 1);
    assert_eq!(metrics.reviews_given.current, 1, "alice reviewed bob's PR");
    assert_eq!(
        metrics.reviews_received.current, 1,
        "bob reviewed alice's PR"
    );
    assert_eq!(metrics.lines_added.current, 50);
    assert_eq!(metrics.lines_added.previous, 100);
}

/// A bot's activity must be reportable by its bare login, exactly as a
/// person's is.
#[test]
fn user_metrics_resolve_bot_logins() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();
    conn.execute_batch(
        "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
         VALUES ('bot:github-actions', 'bot', 'github-actions', 0, 1, 'github-actions');
         INSERT INTO fact_reviews (review_key, pr_key, reviewer_key, state, submitted_at,
             submitted_date_key, submitted_time_key)
         VALUES ('R-BOT', 'octo/alpha#3', 'bot:github-actions', 'APPROVED',
             '2026-04-10T20:00:00Z', '2026-04-10', '10:00');",
    )
    .unwrap();
    let engine = MetricsEngine::new(conn).as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());

    let metrics = engine
        .user_metrics("GitHub-Actions", &Period::Quarter(2026, 2))
        .unwrap();
    assert_eq!(
        metrics.login, "github-actions",
        "the report is labeled with the bare login, not the warehouse key"
    );
    assert_eq!(metrics.entity_type, "bot");
    assert_eq!(metrics.entity_key, "bot:github-actions");
    assert_eq!(
        metrics.display_login(),
        "github-actions [bot]",
        "a bot is marked, so it cannot be read as a person of the same name"
    );
    assert_eq!(metrics.reviews_given.current, 1);
    assert_eq!(metrics.prs_opened.current, 0);

    let aggregations = engine
        .user_aggregations("github-actions", &Period::Quarter(2026, 2), 5)
        .unwrap();
    assert_eq!(
        aggregations.top_reviewed_authors[0].entity_key,
        "user:alice"
    );

    // The group path resolves each member the same way.
    let group = engine
        .user_group_metrics(
            "automation",
            &["github-actions".to_string()],
            &Period::Quarter(2026, 2),
        )
        .unwrap();
    assert_eq!(group.reviews_given.unwrap().current, 1);
}

#[test]
fn partial_period_truncates_previous_window() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    // Reference = Apr 8, day 8 of Q2: only previous days 1-8 of Q1 count.
    let engine = MetricsEngine::new(warehouse.connection())
        .as_of(NaiveDate::from_ymd_opt(2026, 4, 8).unwrap());
    let metrics = engine
        .user_metrics("alice", &Period::Quarter(2026, 2))
        .unwrap();
    // Current window Apr 1-8 has one PR (#2, Apr 5).
    assert_eq!(metrics.prs_opened.current, 1);
    // Q1 PR was Jan 10 = day 10 of Q1 > 8 elapsed days, so excluded.
    assert_eq!(metrics.prs_opened.previous, 0);
    // Reported previous range ends at day 8 of Q1.
    assert_eq!(metrics.previous_date_range.1, "2026-01-08");
}

#[test]
fn repo_metrics_and_leaderboards() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let engine = MetricsEngine::new(warehouse.connection())
        .as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let period = Period::Quarter(2026, 2);

    let metrics = engine.repo_metrics("octo/alpha", &period).unwrap();
    assert_eq!(metrics.prs_opened.current, 3);
    assert_eq!(metrics.prs_opened.previous, 1);
    assert_eq!(metrics.total_reviews.current, 2);

    let aggregations = engine.repo_aggregations("octo/alpha", &period, 10).unwrap();
    assert_eq!(aggregations.top_contributors.len(), 2);
    assert_eq!(aggregations.top_contributors[0].entity_key, "user:alice");
    assert_eq!(aggregations.top_contributors[0].current, 2);
    assert_eq!(aggregations.top_contributors[0].rank_current, 1);
}

#[test]
fn group_metrics() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let engine = MetricsEngine::new(warehouse.connection())
        .as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let period = Period::Quarter(2026, 2);

    let group = engine
        .user_group_metrics("core", &["alice".into()], &period)
        .unwrap();
    assert_eq!(group.prs_opened.current, 2);
    assert_eq!(group.member_count, 1);
    // alice reviewed bob (outsider): counts as given-to-outsiders.
    assert_eq!(group.reviews_given.as_ref().unwrap().current, 1);

    let repo_group = engine
        .repo_group_metrics("all", &["octo/alpha".into()], &period)
        .unwrap();
    assert_eq!(repo_group.prs_opened.current, 3);
    assert_eq!(repo_group.total_reviews.as_ref().unwrap().current, 2);
}
