use super::*;
use crate::GithubDW;

fn seed(warehouse: &GithubDW) {
    warehouse
        .connection()
        .execute_batch(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES ('user:alice', 'user', 'alice', 1, 0, 'Alice'),
                    ('user:bob', 'user', 'bob', 1, 0, 'Bob');
             INSERT INTO dim_repositories (repo_key, owner, name)
             VALUES ('octo/alpha', 'octo', 'alpha'), ('octo/beta', 'octo', 'beta');
             INSERT INTO dim_date (date_key, year, quarter, month, day_of_month, day_of_week,
                 is_weekend, week_of_year, week_key, month_key, quarter_key, year_key, half_key)
             VALUES ('2026-01-10', 2026, 1, 1, 10, 6, 1, 2, '2026-W02', '2026-01', '2026-Q1', '2026', '2026-H1'),
                    ('2026-04-10', 2026, 2, 4, 10, 5, 0, 15, '2026-W15', '2026-04', '2026-Q2', '2026', '2026-H1');
             INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
             VALUES ('10:00', 10, 10, 'AM', 'Morning', 1);
             INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title,
                 created_at, created_date_key, created_time_key, additions, deletions)
             VALUES
               ('octo/alpha#1', 1, 'octo/alpha', 'user:alice', 'MERGED', 'First change',
                '2026-01-10T18:00:00Z', '2026-01-10', '10:00', 100, 10),
               ('octo/alpha#2', 2, 'octo/alpha', 'user:bob', 'OPEN', 'Second change',
                '2026-04-10T18:00:00Z', '2026-04-10', '10:00', 50, 5),
               ('octo/beta#1', 1, 'octo/beta', 'user:alice', 'MERGED', 'Beta feature',
                '2026-04-10T19:00:00Z', '2026-04-10', '10:00', 20, 2);
             INSERT INTO fact_reviews (review_key, pr_key, reviewer_key, state, submitted_at,
                 submitted_date_key, submitted_time_key)
             VALUES ('R1', 'octo/alpha#1', 'user:bob', 'APPROVED', '2026-01-10T19:00:00Z',
                 '2026-01-10', '10:00');",
        )
        .unwrap();
}

#[test]
fn filters_compose_and_execute() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();

    let merged_by_alice = QueryBuilder::new(conn)
        .author("Alice")
        .merged()
        .pull_requests()
        .unwrap();
    assert_eq!(merged_by_alice.len(), 2);

    let q1_count = QueryBuilder::new(conn)
        .period_str("2026-Q1")
        .count()
        .unwrap();
    assert_eq!(q1_count, 1);

    let reviewed_by_bob = QueryBuilder::new(conn).reviewer("bob").count().unwrap();
    assert_eq!(reviewed_by_bob, 1);

    let org_count = QueryBuilder::new(conn).org("octo").count().unwrap();
    assert_eq!(org_count, 3);

    let (added, removed) = QueryBuilder::new(conn)
        .author("alice")
        .lines_changed()
        .unwrap();
    assert_eq!((added, removed), (120, 12));
}

#[test]
fn grouped_counts_and_outputs() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();

    let by_state = QueryBuilder::new(conn).count_by_state().unwrap();
    assert_eq!(by_state[0], ("MERGED".to_string(), 2));

    let by_author = QueryBuilder::new(conn).count_by_author().unwrap();
    assert_eq!(by_author[0], ("alice".to_string(), 2));

    let by_period = QueryBuilder::new(conn).count_by_period().unwrap();
    assert_eq!(by_period.len(), 2);

    let json = QueryBuilder::new(conn).merged().to_json().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 2);

    let csv = QueryBuilder::new(conn).to_csv().unwrap();
    assert!(csv.starts_with("pr_key,"));
    assert_eq!(csv.lines().count(), 4);
}

/// Seed a bot whose login collides with a person's, the case a single
/// identity column has to keep distinguishable.
fn seed_colliding_bot(warehouse: &GithubDW) {
    warehouse
        .connection()
        .execute_batch(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES ('bot:alice', 'bot', 'alice', 0, 1, 'alice');
             INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title,
                 created_at, created_date_key, created_time_key, additions, deletions)
             VALUES ('octo/beta#2', 2, 'octo/beta', 'bot:alice', 'OPEN', 'Automated bump',
                '2026-04-10T20:00:00Z', '2026-04-10', '10:00', 1, 1);",
        )
        .unwrap();
}

/// The identity a caller reads is the login GitHub itself uses. The
/// warehouse's namespaced key is a join target, not a name to show a person.
#[test]
fn pull_requests_report_the_bare_login() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();

    let rows = QueryBuilder::new(conn)
        .author("alice")
        .pull_requests()
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.author, "alice", "the display field is bare");
        assert_eq!(row.author_type, "user");
        assert_eq!(row.author_key, "user:alice", "the key is still available");
        assert_eq!(row.author_display(), "alice");
    }
}

/// Nothing is lost by showing the bare login: the type travels with it, and
/// a bot is marked so it cannot be read as the person of the same name.
#[test]
fn bot_authors_are_marked_rather_than_prefixed() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    seed_colliding_bot(&warehouse);
    let conn = warehouse.connection();

    let rows = QueryBuilder::new(conn)
        .author("bot:alice")
        .pull_requests()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].author, "alice");
    assert_eq!(rows[0].author_type, "bot");
    assert_eq!(rows[0].author_key, "bot:alice");
    assert_eq!(
        rows[0].author_display(),
        "alice [bot]",
        "a bot is distinguishable from the person sharing its login"
    );

    // The two identities remain separate groups, and neither is shown
    // as a raw key.
    let by_author = QueryBuilder::new(conn).count_by_author().unwrap();
    let labels: Vec<&str> = by_author.iter().map(|(label, _)| label.as_str()).collect();
    assert!(labels.contains(&"alice"), "got {labels:?}");
    assert!(labels.contains(&"alice [bot]"), "got {labels:?}");
    assert!(
        labels.iter().all(|label| !label.contains(':')),
        "no group label carries a namespace: {labels:?}"
    );
}

#[test]
fn json_and_csv_carry_a_bare_login_and_its_type() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    seed_colliding_bot(&warehouse);
    let conn = warehouse.connection();

    let json = QueryBuilder::new(conn).to_json().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    for row in parsed.as_array().unwrap() {
        let author = row["author"].as_str().unwrap();
        assert!(!author.contains(':'), "author '{author}' is bare");
        assert!(
            matches!(row["author_type"].as_str(), Some("user") | Some("bot")),
            "the type is carried separately"
        );
        let key = row["author_key"].as_str().unwrap();
        assert_eq!(
            key,
            format!("{}:{author}", row["author_type"].as_str().unwrap())
        );
    }

    let csv = QueryBuilder::new(conn).to_csv().unwrap();
    let header = csv.lines().next().unwrap();
    assert!(header.contains("author,author_type"), "got {header}");
    for line in csv.lines().skip(1) {
        assert!(
            !line.contains("user:") && !line.contains("bot:"),
            "no namespaced identity in: {line}"
        );
    }
}

#[test]
fn pagination_and_ordering() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();
    let page = QueryBuilder::new(conn)
        .limit(1)
        .offset(1)
        .pull_requests()
        .unwrap();
    assert_eq!(page.len(), 1);
    // Ordered created_at DESC; offset 1 skips the newest (beta 19:00Z).
    assert_eq!(page[0].pr_key, "octo/alpha#2");
}

/// Bots are keyed under their own namespace, so a bare login must resolve
/// namespace-agnostically or every bot identity is silently unreachable.
#[test]
fn bare_login_matches_bot_entities() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();
    conn.execute_batch(
        "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
         VALUES ('bot:github-actions', 'bot', 'github-actions', 0, 1, 'github-actions');
         INSERT INTO fact_reviews (review_key, pr_key, reviewer_key, state, submitted_at,
             submitted_date_key, submitted_time_key)
         VALUES ('R-BOT', 'octo/alpha#2', 'bot:github-actions', 'APPROVED',
             '2026-04-10T20:00:00Z', '2026-04-10', '10:00');",
    )
    .unwrap();

    let bare = QueryBuilder::new(conn)
        .reviewer("GitHub-Actions")
        .count()
        .unwrap();
    assert_eq!(bare, 1, "bare login must reach a bot-namespaced reviewer");

    let prefixed = QueryBuilder::new(conn)
        .reviewer("bot:github-actions")
        .count()
        .unwrap();
    assert_eq!(prefixed, 1, "explicit namespace still resolves");

    let unknown = QueryBuilder::new(conn).reviewer("nobody").count().unwrap();
    assert_eq!(unknown, 0, "an unknown login must not widen the result set");

    let bot_author = QueryBuilder::new(conn)
        .authors(&["github-actions".to_string()])
        .count()
        .unwrap();
    assert_eq!(bot_author, 0, "the bot authored no PRs in this fixture");
}

#[test]
fn entity_key_resolution_and_bare_reduction() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();
    conn.execute_batch(
        "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
         VALUES ('bot:github-actions', 'bot', 'github-actions', 0, 1, 'github-actions'),
                ('user:shared-name', 'user', 'shared-name', 1, 0, 'shared-name'),
                ('bot:shared-name', 'bot', 'shared-name', 0, 1, 'shared-name');",
    )
    .unwrap();

    assert_eq!(resolve_entity_keys(conn, "Alice"), vec!["user:alice"]);
    assert_eq!(
        resolve_entity_keys(conn, "GitHub-Actions"),
        vec!["bot:github-actions"]
    );
    // A login present in both namespaces matches both, rather than one.
    assert_eq!(
        resolve_entity_keys(conn, "shared-name"),
        vec!["bot:shared-name", "user:shared-name"]
    );
    // Explicit input is honored verbatim (lowercased).
    assert_eq!(
        resolve_entity_keys(conn, "Bot:GitHub-Actions"),
        vec!["bot:github-actions"]
    );
    // An unknown login keeps the historical spelling: still selective.
    assert_eq!(resolve_entity_keys(conn, "nobody"), vec!["user:nobody"]);

    assert_eq!(to_bare_login("user:Alice"), "alice");
    assert_eq!(to_bare_login("BOT:GitHub-Actions"), "github-actions");
    assert_eq!(to_bare_login("Alice"), "alice");
    // An unrecognized prefix is not a namespace and must survive intact.
    assert_eq!(to_bare_login("team:backend"), "team:backend");
}

#[test]
fn raw_query_is_select_only() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    seed(&warehouse);
    let conn = warehouse.connection();
    let rows = raw_query(conn, "SELECT COUNT(*) AS n FROM fact_pull_requests").unwrap();
    assert_eq!(rows[0]["n"], serde_json::Value::from(3));
    assert!(raw_query(conn, "DELETE FROM fact_pull_requests").is_err());
    assert!(raw_query(conn, "SELECT 1; DROP TABLE issues").is_err());
}
