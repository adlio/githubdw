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
             created_at, created_date_key, created_time_key, additions, deletions, merged_date_key)
         VALUES
           -- Q1: one merged PR by alice
           ('octo/alpha#1', 1, 'octo/alpha', 'user:alice', 'MERGED', 'Q1 change',
            '2026-01-10T18:00:00Z', '2026-01-10', '10:00', 100, 10, '2026-01-10'),
           -- Q2: two PRs by alice, one by bob
           ('octo/alpha#2', 2, 'octo/alpha', 'user:alice', 'MERGED', 'Q2 change A',
            '2026-04-05T18:00:00Z', '2026-04-05', '10:00', 30, 3, '2026-04-05'),
           ('octo/alpha#3', 3, 'octo/alpha', 'user:alice', 'OPEN', 'Q2 change B',
            '2026-04-10T18:00:00Z', '2026-04-10', '10:00', 20, 2, NULL),
           ('octo/alpha#4', 4, 'octo/alpha', 'user:bob', 'MERGED', 'Q2 bob change',
            '2026-04-20T18:00:00Z', '2026-04-20', '10:00', 5, 1, '2026-04-20');
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

/// Comments on two repositories, on an issue (not a pull request), in both
/// windows and outside them, so the repository comment source has rows to
/// wrongly include as well as rows to keep.
fn seed_comments(warehouse: &GithubDW) {
    seed(warehouse);
    let conn = warehouse.connection();
    for date in ["2026-03-31", "2026-04-11", "2026-05-02", "2026-07-02"] {
        time_dimension::ensure_date_row(conn, date).unwrap();
    }
    conn.execute_batch(
        "INSERT INTO dim_repositories (repo_key, owner, name) VALUES ('octo/beta', 'octo', 'beta');
         INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title,
             created_at, created_date_key, created_time_key, additions, deletions)
         VALUES ('octo/beta#1', 1, 'octo/beta', 'user:bob', 'OPEN', 'beta change',
                 '2026-04-11T18:00:00Z', '2026-04-11', '10:00', 1, 1);
         INSERT INTO fact_review_comments (comment_key, pr_key, author_key, created_at,
             created_date_key, created_time_key)
         VALUES ('RC1', 'octo/alpha#2', 'user:bob', '2026-04-05T19:00:00Z', '2026-04-05', '10:00'),
                ('RC2', 'octo/alpha#2', 'user:bob', '2026-04-11T19:00:00Z', '2026-04-11', '10:00'),
                -- Same (author, pr, day) as RC2: a de-duplicating UNION would drop it.
                ('RC2b', 'octo/alpha#2', 'user:bob', '2026-04-11T20:00:00Z', '2026-04-11', '10:00'),
                ('RC3', 'octo/alpha#1', 'user:alice', '2026-01-10T19:00:00Z', '2026-01-10', '10:00'),
                ('RC4', 'octo/beta#1', 'user:alice', '2026-04-11T19:00:00Z', '2026-04-11', '10:00'),
                ('RC5', 'octo/alpha#4', 'user:alice', '2026-07-02T19:00:00Z', '2026-07-02', '10:00');
         INSERT INTO fact_issue_comments (comment_key, parent_type, parent_key, author_key,
             created_at, created_date_key, created_time_key)
         VALUES ('IC1', 'pull_request', 'octo/alpha#3', 'user:alice', '2026-04-11T19:00:00Z', '2026-04-11', '10:00'),
                ('IC2', 'pull_request', 'octo/alpha#4', 'user:bob', '2026-05-02T19:00:00Z', '2026-05-02', '10:00'),
                ('IC3', 'pull_request', 'octo/alpha#1', 'user:bob', '2026-03-31T19:00:00Z', '2026-03-31', '10:00'),
                ('IC4', 'pull_request', 'octo/beta#1', 'user:bob', '2026-04-11T19:00:00Z', '2026-04-11', '10:00'),
                ('IC5', 'issue', 'octo/alpha#2', 'user:alice', '2026-04-11T19:00:00Z', '2026-04-11', '10:00'),
                ('IC6', 'pull_request', 'octo/gone#9', 'user:bob', '2026-04-11T19:00:00Z', '2026-04-11', '10:00');",
    )
    .unwrap();
}

/// Comment counts for `seed_comments`, counted by hand from the rows above.
/// Q2 is 2026-04-01..06-30 and the previous window is Q1.
///
/// octo/alpha keeps RC1 RC2 RC2b RC3 RC5 (review comments) and IC1 IC2 IC3
/// (issue comments on its PRs). RC4 and IC4 sit on octo/beta; IC5 is on an
/// issue; IC6 is on a PR that has no row. RC5 (July) is in neither window.
///
/// As of 2026-07-01 (Q2 complete):
///   current  RC1 RC2 RC2b IC1 IC2 = 5   (bob 4: RC1 RC2 RC2b IC2; alice 1: IC1)
///   previous RC3 IC3 = 2                (alice 1: RC3; bob 1: IC3)
///   with beta: current 5 + RC4 + IC4 = 7, previous 2
///
/// As of 2026-04-12 (12 days into Q2, so Q1 is cut to 01-01..01-12):
///   current  RC1 RC2 RC2b IC1 = 4      (bob 3, alice 1; IC2 on 05-02 is later)
///   previous RC3 = 1                   (alice 1; IC3 on 03-31 is past day 12)
///   with beta: current 4 + RC4 + IC4 = 6, previous 1
#[test]
fn repo_comment_metrics_match_hand_counts() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed_comments(&warehouse);
    let conn = warehouse.connection();
    let period = Period::Quarter(2026, 2);
    // (as of, alpha total, alpha board as (key, current, previous,
    // previous rank), alpha+beta total)
    type Row = (&'static str, u64, u64, Option<u32>);
    type Case = (NaiveDate, (u64, u64), [Row; 2], (u64, u64));
    let cases: [Case; 2] = [
        (
            NaiveDate::from_ymd_opt(2026, 7, 1).unwrap(),
            (5, 2),
            [("user:bob", 4, 1, Some(2)), ("user:alice", 1, 1, Some(1))],
            (7, 2),
        ),
        (
            NaiveDate::from_ymd_opt(2026, 4, 12).unwrap(),
            (4, 1),
            [("user:bob", 3, 0, None), ("user:alice", 1, 1, Some(1))],
            (6, 1),
        ),
    ];
    for (as_of, total, board_rows, group_total) in cases {
        let engine = MetricsEngine::new(conn).as_of(as_of);

        let metrics = engine.repo_metrics("octo/alpha", &period).unwrap();
        assert_eq!(
            (
                metrics.total_comments.current,
                metrics.total_comments.previous
            ),
            total,
            "alpha total as of {as_of}"
        );

        let board = engine
            .repo_aggregations("octo/alpha", &period, 10)
            .unwrap()
            .top_commenters;
        let got: Vec<_> = board
            .iter()
            .map(|e| {
                (
                    e.entity_key.as_str(),
                    e.current,
                    e.previous,
                    e.rank_previous,
                )
            })
            .collect();
        assert_eq!(got, board_rows, "alpha leaderboard as of {as_of}");
        assert_eq!(
            board.iter().map(|e| e.rank_current).collect::<Vec<_>>(),
            [1, 2],
            "as of {as_of}"
        );

        let group = engine
            .repo_group_metrics("both", &["octo/alpha".into(), "octo/beta".into()], &period)
            .unwrap()
            .total_comments
            .unwrap();
        assert_eq!(
            (group.current, group.previous),
            group_total,
            "group total as of {as_of}"
        );
    }
}

/// Hand count for alice, Q2 (2026-04-01..06-30) against Q1:
///   #10 opened Q1, merged Q2   -> Q2 merged
///   #11 opened Q1, merged Q1   -> Q1 merged
///   #12 opened Q2, still open  -> neither
///   #13 opened Q2, merged Q2   -> Q2 merged
/// So merged is 2 now and 1 before; opened is 2 now and 2 before.
fn seed_merge_dates(warehouse: &GithubDW) {
    let conn = warehouse.connection();
    for date in [
        "2026-02-01",
        "2026-03-01",
        "2026-04-02",
        "2026-04-09",
        "2026-04-15",
    ] {
        time_dimension::ensure_date_row(conn, date).unwrap();
    }
    time_dimension::ensure_time_row(conn, "10:00", (9, 17)).unwrap();
    conn.execute_batch(
        "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
         VALUES ('user:alice', 'user', 'alice', 1, 0, 'alice');
         INSERT INTO dim_repositories (repo_key, owner, name) VALUES ('octo/alpha', 'octo', 'alpha');
         INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title,
             created_at, created_date_key, created_time_key, additions, deletions, merged_date_key)
         VALUES
           ('octo/alpha#10', 10, 'octo/alpha', 'user:alice', 'MERGED', 'a', '2026-02-01T18:00:00Z', '2026-02-01', '10:00', 1, 1, '2026-04-02'),
           ('octo/alpha#11', 11, 'octo/alpha', 'user:alice', 'MERGED', 'b', '2026-02-01T18:00:00Z', '2026-02-01', '10:00', 1, 1, '2026-03-01'),
           ('octo/alpha#12', 12, 'octo/alpha', 'user:alice', 'OPEN',   'c', '2026-04-09T18:00:00Z', '2026-04-09', '10:00', 1, 1, NULL),
           ('octo/alpha#13', 13, 'octo/alpha', 'user:alice', 'MERGED', 'd', '2026-04-09T18:00:00Z', '2026-04-09', '10:00', 1, 1, '2026-04-15');",
    )
    .unwrap();
}

#[test]
fn prs_merged_counts_by_merge_date_for_every_scope() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed_merge_dates(&warehouse);
    let engine = MetricsEngine::new(warehouse.connection())
        .as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let period = Period::Quarter(2026, 2);

    let user = engine.user_metrics("alice", &period).unwrap();
    assert_eq!((user.prs_merged.current, user.prs_merged.previous), (2, 1));
    assert_eq!((user.prs_opened.current, user.prs_opened.previous), (2, 2));

    let repo = engine.repo_metrics("octo/alpha", &period).unwrap();
    assert_eq!((repo.prs_merged.current, repo.prs_merged.previous), (2, 1));

    let group = engine
        .user_group_metrics("g", &["alice".into()], &period)
        .unwrap();
    assert_eq!(
        (group.prs_merged.current, group.prs_merged.previous),
        (2, 1)
    );

    let repo_group = engine
        .repo_group_metrics("rg", &["octo/alpha".into()], &period)
        .unwrap();
    assert_eq!(
        (
            repo_group.prs_merged.current,
            repo_group.prs_merged.previous
        ),
        (2, 1)
    );

    let mergers = engine
        .repo_aggregations("octo/alpha", &period, 5)
        .unwrap()
        .top_mergers;
    assert_eq!(mergers.len(), 1);
    assert_eq!((mergers[0].current, mergers[0].previous), (2, 1));
}

/// Reference Apr 8 = day 8 of Q2. Nothing merged in Q1 days 1-8 counts, and
/// the Apr 2 merge is inside the current window.
#[test]
fn prs_merged_partial_period_truncates_by_merge_date() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed_merge_dates(&warehouse);
    let engine = MetricsEngine::new(warehouse.connection())
        .as_of(NaiveDate::from_ymd_opt(2026, 4, 8).unwrap());
    let user = engine
        .user_metrics("alice", &Period::Quarter(2026, 2))
        .unwrap();
    assert_eq!((user.prs_merged.current, user.prs_merged.previous), (1, 0));
}

#[test]
fn empty_group_metrics_are_zero() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed_merge_dates(&warehouse);
    let engine = MetricsEngine::new(warehouse.connection())
        .as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let period = Period::Quarter(2026, 2);
    let users = engine.user_group_metrics("none", &[], &period).unwrap();
    assert_eq!(users.prs_opened.current, 0);
    let repos = engine.repo_group_metrics("none", &[], &period).unwrap();
    assert_eq!(repos.prs_opened.current, 0);
}

/// alice leaves one review of each state on bob's PR: APPROVED,
/// CHANGES_REQUESTED, COMMENTED, PENDING, DISMISSED. PENDING never reaches
/// `fact_reviews` in production (no `submitted_at` to key a date off), but a
/// row is seeded here anyway to prove the metrics query itself excludes it,
/// not just the sync path. By hand: only APPROVED, CHANGES_REQUESTED and
/// COMMENTED count, so every review count and leaderboard reads 3, never 5.
fn seed_review_states(warehouse: &GithubDW) {
    seed(warehouse);
    let conn = warehouse.connection();
    conn.execute_batch(
        "INSERT INTO fact_reviews (review_key, pr_key, reviewer_key, state, submitted_at,
             submitted_date_key, submitted_time_key)
         VALUES
           ('RS1', 'octo/alpha#4', 'user:alice', 'APPROVED', '2026-04-21T10:00:00Z', '2026-04-20', '10:00'),
           ('RS2', 'octo/alpha#4', 'user:alice', 'CHANGES_REQUESTED', '2026-04-21T11:00:00Z', '2026-04-20', '10:00'),
           ('RS3', 'octo/alpha#4', 'user:alice', 'COMMENTED', '2026-04-21T12:00:00Z', '2026-04-20', '10:00'),
           ('RS4', 'octo/alpha#4', 'user:alice', 'PENDING', '2026-04-21T13:00:00Z', '2026-04-20', '10:00'),
           ('RS5', 'octo/alpha#4', 'user:alice', 'DISMISSED', '2026-04-21T14:00:00Z', '2026-04-20', '10:00');",
    )
    .unwrap();
}

#[test]
fn review_counts_exclude_pending_and_dismissed() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed_review_states(&warehouse);
    let conn = warehouse.connection();
    let engine = MetricsEngine::new(conn).as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let period = Period::Quarter(2026, 2);

    // user_metrics: reviews_given for alice already includes the pre-existing
    // R2 (APPROVED, on bob's PR #4) from `seed`, plus the three counted new
    // ones here = 4; PENDING and DISMISSED must not add to it.
    let user = engine.user_metrics("alice", &period).unwrap();
    assert_eq!(user.reviews_given.current, 4, "1 pre-seeded + 3 of 5 new");

    // reviews_received for bob (PR #4's author) must match the same filter.
    let bob = engine.user_metrics("bob", &period).unwrap();
    assert_eq!(bob.reviews_received.current, 4);

    // Reviewer leaderboard on PR #4's repo: alice's count is 4, not 6.
    let repo_board = engine
        .repo_aggregations("octo/alpha", &period, 10)
        .unwrap()
        .top_reviewers;
    let alice_repo_count = repo_board
        .iter()
        .find(|e| e.entity_key == "user:alice")
        .expect("alice reviewed in this repo")
        .current;
    assert_eq!(alice_repo_count, 4);

    // Repo total_reviews: bob's R1 (APPROVED, PR #2) + alice's 4 = 5.
    let repo = engine.repo_metrics("octo/alpha", &period).unwrap();
    assert_eq!(repo.total_reviews.current, 5);

    // user_group_metrics and repo_group_metrics apply the same filter.
    let user_group = engine
        .user_group_metrics("g", &["alice".into()], &period)
        .unwrap();
    assert_eq!(user_group.reviews_given.unwrap().current, 4);

    let repo_group = engine
        .repo_group_metrics("rg", &["octo/alpha".into()], &period)
        .unwrap();
    assert_eq!(repo_group.total_reviews.unwrap().current, 5);

    // top_reviewed_authors (bob is reviewed by alice): 4, not 6.
    let reviewed_authors = engine
        .user_aggregations("alice", &period, 10)
        .unwrap()
        .top_reviewed_authors;
    let bob_reviewed_count = reviewed_authors
        .iter()
        .find(|e| e.entity_key == "user:bob")
        .expect("bob was reviewed by alice")
        .current;
    assert_eq!(bob_reviewed_count, 4);

    // Previous-period comparisons apply the same state filter: seed the
    // same five states in Q1 and confirm the previous-window count is 3,
    // not 5, exactly like the current window.
    conn.execute_batch(
        "INSERT INTO fact_reviews (review_key, pr_key, reviewer_key, state, submitted_at,
             submitted_date_key, submitted_time_key)
         VALUES
           ('RQ1', 'octo/alpha#1', 'user:bob', 'APPROVED', '2026-01-10T19:00:00Z', '2026-01-10', '10:00'),
           ('RQ2', 'octo/alpha#1', 'user:bob', 'CHANGES_REQUESTED', '2026-01-10T20:00:00Z', '2026-01-10', '10:00'),
           ('RQ3', 'octo/alpha#1', 'user:bob', 'COMMENTED', '2026-01-10T21:00:00Z', '2026-01-10', '10:00'),
           ('RQ4', 'octo/alpha#1', 'user:bob', 'PENDING', '2026-01-10T22:00:00Z', '2026-01-10', '10:00'),
           ('RQ5', 'octo/alpha#1', 'user:bob', 'DISMISSED', '2026-01-10T23:00:00Z', '2026-01-10', '10:00');",
    )
    .unwrap();
    let bob_previous = engine.user_metrics("bob", &period).unwrap();
    assert_eq!(
        bob_previous.reviews_given.previous, 3,
        "previous window applies the same APPROVED/CHANGES_REQUESTED/COMMENTED filter"
    );
}

/// Case sensitivity: the fixture recipes and GitHub's API both write review
/// states upper case, but the filter must not silently drop a differently
/// cased row — it should still be counted if it matches case-insensitively.
#[test]
fn review_state_filter_is_case_insensitive() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();
    conn.execute_batch(
        "INSERT INTO fact_reviews (review_key, pr_key, reviewer_key, state, submitted_at,
             submitted_date_key, submitted_time_key)
         VALUES ('RC-LOWER', 'octo/alpha#4', 'user:alice', 'approved',
             '2026-04-21T10:00:00Z', '2026-04-20', '10:00');",
    )
    .unwrap();
    let engine = MetricsEngine::new(conn).as_of(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let user = engine
        .user_metrics("alice", &Period::Quarter(2026, 2))
        .unwrap();
    // 1 pre-seeded (R2, APPROVED) + 1 lower-case 'approved' = 2.
    assert_eq!(user.reviews_given.current, 2);
}
