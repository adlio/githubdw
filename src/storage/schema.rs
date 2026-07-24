//! Schema management: connection setup and forward-only migrations.

use rusqlite::Connection;
use rusqlite_migration::{M, Migrations};

use crate::error::Result;

/// The full, ordered migration set. Forward-only; never edit an applied step.
pub fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(include_str!("migrations/001_initial.sql"))])
}

/// Configure the connection (WAL, foreign keys) and apply pending migrations.
pub fn init(conn: &mut Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    migrations().to_latest(conn)?;
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
        assert_eq!(version, 1);
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
