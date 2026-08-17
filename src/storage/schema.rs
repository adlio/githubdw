//! Schema management: connection setup and forward-only migrations.

use rusqlite::Connection;
use rusqlite_migration::{M, Migrations};

use crate::error::Result;

/// The full, ordered migration set. Forward-only; never edit an applied step.
pub fn migrations() -> Migrations<'static> {
    Migrations::new(vec![
        M::up(include_str!("migrations/001_initial.sql")),
        M::up(include_str!("migrations/002_groups.sql")),
        M::up(include_str!("migrations/003_pr_summaries.sql")),
        M::up(include_str!("migrations/004_entity_invariants.sql")),
    ])
}

/// Configure the connection (WAL, foreign keys) and apply pending migrations.
pub fn init(conn: &mut Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    // Adding a constraint to an existing table means rebuilding it, which
    // requires replacing a table other tables reference. SQLite only permits
    // that with foreign key enforcement off, and `PRAGMA foreign_keys` is a
    // no-op inside a transaction — where every migration necessarily runs. So
    // it is toggled around the migration run and the schema is re-checked
    // before the connection is handed out: a rebuild that lost a parent row
    // fails here rather than surfacing later as a query that silently omits it.
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let outcome = migrations().to_latest(conn);
    conn.pragma_update(None, "foreign_keys", "ON")?;
    outcome?;
    assert_referential_integrity(conn)?;
    Ok(())
}

/// Fail if any row references a parent that does not exist.
fn assert_referential_integrity(conn: &Connection) -> Result<()> {
    let mut statement = conn.prepare("PRAGMA foreign_key_check")?;
    let mut rows = statement.query([])?;
    if let Some(row) = rows.next()? {
        let child: String = row.get(0)?;
        let parent: String = row.get(2)?;
        return Err(crate::error::Error::SchemaIntegrity(format!(
            "{child} holds a row with no matching {parent}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_in_memory() -> Connection {
        let mut conn = Connection::open_in_memory().expect("open in-memory db");
        init(&mut conn).expect("init schema");
        conn
    }

    #[test]
    fn migrations_validate() {
        migrations().validate().expect("migrations are valid");
    }

    #[test]
    fn schema_initializes() {
        let conn = open_in_memory();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN (
                    'dim_entities', 'dim_repositories', 'dim_date', 'dim_period', 'dim_time',
                    'fact_pull_requests', 'fact_reviews', 'fact_review_comments',
                    'fact_issue_comments', 'fact_check_runs', 'fact_file_diffs',
                    'issues', 'labels', 'issue_labels', 'milestones', 'issue_assignees',
                    'sync_metadata', 'sync_locks', 'sync_jobs', 'synced_ranges',
                    'monitored_users', 'monitored_repos', 'monitored_orgs', 'config'
                )",
                [],
                |row| row.get(0),
            )
            .expect("count tables");
        assert_eq!(count, 24, "all base tables should exist");
    }

    #[test]
    fn schema_init_is_idempotent() {
        let mut conn = Connection::open_in_memory().expect("open in-memory db");
        init(&mut conn).expect("first init");
        init(&mut conn).expect("second init is a no-op");
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        assert_eq!(version, 4);
    }

    /// Seed one well-formed entity, the shape every writer is expected to
    /// produce.
    fn insert_entity(
        conn: &Connection,
        key: &str,
        kind: &str,
        login: &str,
    ) -> rusqlite::Result<()> {
        conn.execute(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES (?1, ?2, ?3, 1, 0, 'n')",
            rusqlite::params![key, kind, login],
        )
        .map(|_| ())
    }

    #[test]
    fn entity_invariants_admit_well_formed_rows() {
        let conn = open_in_memory();
        insert_entity(&conn, "user:octocat", "user", "octocat").expect("a user is accepted");
        insert_entity(&conn, "bot:dependabot", "bot", "dependabot").expect("a bot is accepted");
        insert_entity(&conn, "org:octo-org", "org", "octo-org").expect("an org is accepted");
    }

    /// A namespaced value stored in `login` is the defect the bare-login
    /// lookups cannot see: the row exists and never matches.
    #[test]
    fn entity_invariants_reject_a_namespaced_login() {
        let conn = open_in_memory();
        let error = insert_entity(&conn, "user:user:octocat", "user", "user:octocat")
            .expect_err("a colon in login is refused");
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "expected a CHECK failure, got: {error}"
        );
    }

    #[test]
    fn entity_invariants_reject_an_empty_login() {
        let conn = open_in_memory();
        let error =
            insert_entity(&conn, "user:", "user", "").expect_err("an empty login is refused");
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "expected a CHECK failure, got: {error}"
        );
    }

    /// The key is the join target of every fact table. One that disagrees with
    /// its own parts splits a single identity in two.
    #[test]
    fn entity_invariants_reject_a_key_that_disagrees_with_its_parts() {
        let conn = open_in_memory();
        let error = insert_entity(&conn, "user:octocat", "bot", "octocat")
            .expect_err("type/key mismatch is refused");
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "expected a CHECK failure, got: {error}"
        );

        let error = insert_entity(&conn, "octocat", "user", "octocat")
            .expect_err("an unnamespaced key is refused");
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "expected a CHECK failure, got: {error}"
        );
    }

    #[test]
    fn entity_invariants_reject_an_unknown_namespace() {
        let conn = open_in_memory();
        let error = insert_entity(&conn, "team:reviewers", "team", "reviewers")
            .expect_err("an unminted namespace is refused");
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "expected a CHECK failure, got: {error}"
        );
    }

    /// Upgrading a warehouse whose rows already satisfy the invariants must
    /// preserve every row, its indexes, and the child foreign keys.
    #[test]
    fn upgrade_to_entity_invariants_preserves_clean_rows() {
        let mut conn = Connection::open_in_memory().expect("open in-memory db");
        migrations()
            .to_version(&mut conn, 3)
            .expect("migrate to the pre-constraint schema");
        conn.execute_batch(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES ('user:octocat', 'user', 'octocat', 1, 0, 'Octocat'),
                    ('bot:dependabot', 'bot', 'dependabot', 0, 1, 'dependabot');
             INSERT INTO dim_repositories (repo_key, owner, name)
             VALUES ('octocat/hello', 'octocat', 'hello');
             INSERT INTO dim_date (date_key, year, quarter, month, day_of_month, day_of_week,
                 is_weekend, week_of_year, week_key, month_key, quarter_key, year_key, half_key)
             VALUES ('2026-01-05', 2026, 1, 1, 5, 1, 0, 2, '2026-W02', '2026-01', '2026-Q1',
                 '2026', '2026-H1');
             INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
             VALUES ('10:00', 10, 10, 'AM', 'Morning', 1);
             INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
                 title, created_at, created_date_key, created_time_key)
             VALUES ('octocat/hello#1', 1, 'octocat/hello', 'user:octocat', 'OPEN',
                 'A change', '2026-01-05T18:00:00Z', '2026-01-05', '10:00');",
        )
        .expect("seed clean pre-upgrade data");

        init(&mut conn).expect("clean data upgrades as a no-op");
        let entities: i64 = conn
            .query_row("SELECT COUNT(*) FROM dim_entities", [], |row| row.get(0))
            .unwrap();
        assert_eq!(entities, 2, "every entity row survives the rebuild");

        // The child FK still resolves against the rebuilt parent, and the
        // login index the bare-login lookups depend on is back.
        let violations: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fact_pull_requests p
                 LEFT JOIN dim_entities e ON e.entity_key = p.author_key
                 WHERE e.entity_key IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(violations, 0, "no fact row is orphaned by the rebuild");
        let indexes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index'
                 AND name IN ('idx_entities_type', 'idx_entities_login')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 2, "both entity indexes are recreated");
        assert!(
            conn.execute_batch("PRAGMA foreign_key_check").is_ok(),
            "the rebuilt schema passes a foreign key check"
        );
    }

    /// Failing loudly is the migration's job: a warehouse carrying a row the
    /// invariant forbids must refuse to upgrade rather than carry it forward.
    #[test]
    fn upgrade_to_entity_invariants_refuses_violating_rows() {
        let mut conn = Connection::open_in_memory().expect("open in-memory db");
        migrations()
            .to_version(&mut conn, 3)
            .expect("migrate to the pre-constraint schema");
        conn.execute(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES ('user:user:octocat', 'user', 'user:octocat', 1, 0, 'Octocat')",
            [],
        )
        .expect("the pre-constraint schema accepts it");

        let error = init(&mut conn).expect_err("the upgrade refuses a violating row");
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "expected a CHECK failure, got: {error}"
        );
    }

    #[test]
    fn fts5_trigram_is_available() {
        let conn = open_in_memory();
        // The virtual tables were created by the migration; prove trigram
        // matching works end-to-end via the insert trigger.
        conn.execute_batch(
            "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
             VALUES ('user:octocat', 'user', 'octocat', 1, 0, 'Octocat');
             INSERT INTO dim_repositories (repo_key, owner, name)
             VALUES ('octocat/hello', 'octocat', 'hello');
             INSERT INTO dim_date (date_key, year, quarter, month, day_of_month, day_of_week,
                 is_weekend, week_of_year, week_key, month_key, quarter_key, year_key, half_key)
             VALUES ('2026-01-05', 2026, 1, 1, 5, 1, 0, 2, '2026-W02', '2026-01', '2026-Q1',
                 '2026', '2026-H1');
             INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
             VALUES ('10:00', 10, 10, 'AM', 'Morning', 1);
             INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
                 title, created_at, created_date_key, created_time_key)
             VALUES ('octocat/hello#1', 1, 'octocat/hello', 'user:octocat', 'OPEN',
                 'Fix secondary rate limiting', '2026-01-05T18:00:00Z', '2026-01-05', '10:00');",
        )
        .expect("seed rows");

        // Substring (trigram) match on a partial word.
        let hit: String = conn
            .query_row(
                "SELECT pr_key FROM pull_requests_fts WHERE pull_requests_fts MATCH 'econdary'",
                [],
                |row| row.get(0),
            )
            .expect("trigram substring match");
        assert_eq!(hit, "octocat/hello#1");
    }

    #[test]
    fn config_defaults_are_seeded() {
        let conn = open_in_memory();
        let tz: String = conn
            .query_row(
                "SELECT value FROM config WHERE key = 'timezone'",
                [],
                |row| row.get(0),
            )
            .expect("timezone config");
        assert_eq!(tz, "America/Los_Angeles");
    }
}
