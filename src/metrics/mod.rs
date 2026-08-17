//! MetricsEngine: period-over-period analytics over the star schema.

pub mod types;

use chrono::NaiveDate;
use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;

use crate::error::Result;
use crate::period::Period;
use crate::query::{entity_namespace, resolve_entity_keys, to_bare_login};
use crate::storage::time_dimension;
pub use types::*;

/// The key a user report is labeled with when a login resolves to several,
/// normalized so it is always `<type>:<login>`.
fn primary_key(keys: &[String], login: &str) -> String {
    let key = keys
        .first()
        .cloned()
        .unwrap_or_else(|| login.to_lowercase());
    format!("{}:{}", entity_namespace(&key), to_bare_login(&key))
}

/// Computes user/repo/group metrics with apples-to-apples previous windows.
pub struct MetricsEngine<'a> {
    connection: &'a Connection,
    reference_date: NaiveDate,
}

/// The two comparison windows, partial-period aware.
struct Windows {
    current_start: String,
    current_end: String,
    previous_start: String,
    previous_end: String,
    /// Extra guard on the previous window (`AND dd.day_of_quarter <= N`).
    previous_position_filter: String,
    period_key: String,
    previous_period_key: String,
}

impl<'a> MetricsEngine<'a> {
    /// Reference date defaults to today in the warehouse's configured
    /// timezone — the same calendar every `date_key` is built in. A UTC
    /// calendar date here would misplace `effective_end_date`, `is_partial`,
    /// and `days_elapsed` by a day whenever the two calendars disagree,
    /// skewing the partial-period truncation used for period-over-period
    /// deltas.
    pub fn new(connection: &'a Connection) -> Self {
        Self {
            connection,
            reference_date: time_dimension::today_or_default(connection),
        }
    }

    /// Override the reference date (tests, historical reports).
    pub fn as_of(mut self, reference: NaiveDate) -> Self {
        self.reference_date = reference;
        self
    }

    /// The date relative windows and partial-period truncation resolve against.
    pub fn reference_date(&self) -> NaiveDate {
        self.reference_date
    }

    fn windows(&self, period: &Period) -> Windows {
        let (current_start, _) = period.date_range();
        let current_end = period.effective_end_date(self.reference_date);
        let previous = period.previous();
        let (previous_start, mut previous_end) = previous.date_range();

        let mut position_filter = String::new();
        if period.is_partial(self.reference_date)
            && let Some(column) = period.position_column()
        {
            let elapsed = period.days_elapsed(self.reference_date);
            position_filter = format!(" AND dd.{column} <= {elapsed}");
            // Truncate the reported previous window too (honest labeling).
            let truncated = previous_start + chrono::Duration::days(elapsed - 1);
            previous_end = previous_end.min(truncated);
        }

        Windows {
            current_start: current_start.format("%Y-%m-%d").to_string(),
            current_end: current_end.format("%Y-%m-%d").to_string(),
            previous_start: previous_start.format("%Y-%m-%d").to_string(),
            previous_end: previous_end.format("%Y-%m-%d").to_string(),
            previous_position_filter: position_filter,
            period_key: period.to_key(),
            previous_period_key: previous.to_key(),
        }
    }

    /// One conditional-aggregation query returning (current, previous) sums.
    fn split_measure(
        &self,
        from_clause: &str,
        date_column: &str,
        extra_conditions: &str,
        value_expression: &str,
        parameters: &[SqlValue],
        windows: &Windows,
    ) -> Result<MetricWithDelta> {
        let sql = format!(
            "SELECT
                COALESCE(SUM(CASE WHEN dd.date_key BETWEEN '{cs}' AND '{ce}'
                    THEN {value} ELSE 0 END), 0) AS current_value,
                COALESCE(SUM(CASE WHEN dd.date_key BETWEEN '{ps}' AND '{pe}'{pos}
                    THEN {value} ELSE 0 END), 0) AS previous_value
             FROM {from_clause}
             JOIN dim_date dd ON dd.date_key = {date_column}
             WHERE ((dd.date_key BETWEEN '{cs}' AND '{ce}')
                 OR (dd.date_key BETWEEN '{ps}' AND '{pe}'))
                 {extra}",
            cs = windows.current_start,
            ce = windows.current_end,
            ps = windows.previous_start,
            pe = windows.previous_end,
            pos = windows.previous_position_filter,
            value = value_expression,
            extra = extra_conditions,
        );
        let (current, previous): (i64, i64) = self.connection.query_row(
            &sql,
            rusqlite::params_from_iter(parameters.iter().cloned()),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(MetricWithDelta::new(
            current.max(0) as u64,
            previous.max(0) as u64,
        ))
    }

    /// Generic two-window leaderboard with rank movement.
    #[allow(clippy::too_many_arguments)]
    fn leaderboard(
        &self,
        entity_expression: &str,
        from_clause: &str,
        date_column: &str,
        extra_conditions: &str,
        parameters: &[SqlValue],
        windows: &Windows,
        top: u32,
    ) -> Result<Vec<EntityMetric>> {
        let sql = format!(
            "WITH counts AS (
                SELECT {entity} AS entity_key,
                    SUM(CASE WHEN dd.date_key BETWEEN '{cs}' AND '{ce}' THEN 1 ELSE 0 END) AS current_count,
                    SUM(CASE WHEN dd.date_key BETWEEN '{ps}' AND '{pe}'{pos} THEN 1 ELSE 0 END) AS previous_count
                FROM {from_clause}
                JOIN dim_date dd ON dd.date_key = {date_column}
                WHERE ((dd.date_key BETWEEN '{cs}' AND '{ce}')
                    OR (dd.date_key BETWEEN '{ps}' AND '{pe}'))
                    {extra}
                GROUP BY entity_key
             ),
             ranked AS (
                SELECT entity_key, current_count, previous_count,
                    ROW_NUMBER() OVER (ORDER BY current_count DESC, entity_key) AS rank_current,
                    CASE WHEN previous_count > 0
                        THEN ROW_NUMBER() OVER (ORDER BY previous_count DESC, entity_key)
                        ELSE NULL END AS rank_previous
                FROM counts
             )
             SELECT r.entity_key, COALESCE(e.name, dr.name, r.entity_key),
                    r.current_count, r.previous_count, r.rank_current, r.rank_previous
             FROM ranked r
             LEFT JOIN dim_entities e ON e.entity_key = r.entity_key
             LEFT JOIN dim_repositories dr ON dr.repo_key = r.entity_key
             WHERE r.current_count > 0
             ORDER BY r.rank_current
             LIMIT {top}",
            entity = entity_expression,
            cs = windows.current_start,
            ce = windows.current_end,
            ps = windows.previous_start,
            pe = windows.previous_end,
            pos = windows.previous_position_filter,
            extra = extra_conditions,
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(
            rusqlite::params_from_iter(parameters.iter().cloned()),
            |row| {
                let current: i64 = row.get(2)?;
                let previous: i64 = row.get(3)?;
                Ok(EntityMetric {
                    entity_key: row.get(0)?,
                    entity_name: row.get(1)?,
                    current: current as u64,
                    previous: previous as u64,
                    delta: current - previous,
                    rank_current: row.get::<_, i64>(4)? as u32,
                    rank_previous: row.get::<_, Option<i64>>(5)?.map(|rank| rank as u32),
                })
            },
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ===== User metrics =====

    pub fn user_metrics(&self, login: &str, period: &Period) -> Result<UserMetrics> {
        // An identity may be keyed under any namespace ingestion mints, so match
        // every key the login resolves to instead of one assumed spelling.
        let user_keys = resolve_entity_keys(self.connection, login);
        let placeholders = vec!["?"; user_keys.len()].join(", ");
        let windows = self.windows(period);
        let user_parameter: Vec<SqlValue> = user_keys
            .iter()
            .map(|key| SqlValue::Text(key.clone()))
            .collect();
        let both_parameter: Vec<SqlValue> = user_parameter
            .iter()
            .chain(user_parameter.iter())
            .cloned()
            .collect();

        let prs_opened = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.author_key IN ({placeholders})"),
            "1",
            &user_parameter,
            &windows,
        )?;
        let prs_merged = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.author_key IN ({placeholders}) AND p.state = 'MERGED'"),
            "1",
            &user_parameter,
            &windows,
        )?;
        let reviews_given = self.split_measure(
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            &format!(
                "AND fr.reviewer_key IN ({placeholders}) AND p.author_key NOT IN ({placeholders})"
            ),
            "1",
            &both_parameter,
            &windows,
        )?;
        let reviews_received = self.split_measure(
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            &format!(
                "AND p.author_key IN ({placeholders}) AND fr.reviewer_key NOT IN ({placeholders})"
            ),
            "1",
            &both_parameter,
            &windows,
        )?;
        let comments_given = self.split_measure(
            "(SELECT author_key, pr_key, created_date_key FROM fact_review_comments
              UNION ALL
              SELECT author_key, parent_key AS pr_key, created_date_key
              FROM fact_issue_comments WHERE parent_type = 'pull_request') fc
             JOIN fact_pull_requests p ON p.pr_key = fc.pr_key",
            "fc.created_date_key",
            &format!(
                "AND fc.author_key IN ({placeholders}) AND p.author_key NOT IN ({placeholders})"
            ),
            "1",
            &both_parameter,
            &windows,
        )?;
        let lines_added = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.author_key IN ({placeholders})"),
            "p.additions",
            &user_parameter,
            &windows,
        )?;
        let lines_removed = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.author_key IN ({placeholders})"),
            "p.deletions",
            &user_parameter,
            &windows,
        )?;

        let entity_key = primary_key(&user_keys, login);
        Ok(UserMetrics {
            login: to_bare_login(&entity_key),
            entity_type: entity_namespace(&entity_key),
            entity_key,
            period_key: windows.period_key.clone(),
            previous_period_key: windows.previous_period_key.clone(),
            current_date_range: (windows.current_start.clone(), windows.current_end.clone()),
            previous_date_range: (windows.previous_start.clone(), windows.previous_end.clone()),
            prs_opened,
            prs_merged,
            reviews_given,
            reviews_received,
            comments_given,
            lines_added,
            lines_removed,
        })
    }

    pub fn user_aggregations(
        &self,
        login: &str,
        period: &Period,
        top: u32,
    ) -> Result<UserAggregations> {
        let user_keys = resolve_entity_keys(self.connection, login);
        let placeholders = vec!["?"; user_keys.len()].join(", ");
        let windows = self.windows(period);
        let user_parameter: Vec<SqlValue> = user_keys
            .iter()
            .map(|key| SqlValue::Text(key.clone()))
            .collect();
        let both_parameter: Vec<SqlValue> = user_parameter
            .iter()
            .chain(user_parameter.iter())
            .cloned()
            .collect();

        let top_repos = self.leaderboard(
            "p.repo_key",
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.author_key IN ({placeholders})"),
            &user_parameter,
            &windows,
            top,
        )?;
        let top_reviewers = self.leaderboard(
            "fr.reviewer_key",
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            &format!(
                "AND p.author_key IN ({placeholders}) AND fr.reviewer_key NOT IN ({placeholders})"
            ),
            &both_parameter,
            &windows,
            top,
        )?;
        let top_reviewed_authors = self.leaderboard(
            "p.author_key",
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            &format!(
                "AND fr.reviewer_key IN ({placeholders}) AND p.author_key NOT IN ({placeholders})"
            ),
            &both_parameter,
            &windows,
            top,
        )?;

        Ok(UserAggregations {
            top_repos,
            top_reviewers,
            top_reviewed_authors,
        })
    }

    // ===== Repo metrics =====

    pub fn repo_metrics(&self, repository: &str, period: &Period) -> Result<RepoMetrics> {
        let repo_key = repository.to_lowercase();
        let windows = self.windows(period);
        let repo_parameter = vec![SqlValue::Text(repo_key.clone())];

        let prs_opened = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            "AND p.repo_key = ?",
            "1",
            &repo_parameter,
            &windows,
        )?;
        let prs_merged = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            "AND p.repo_key = ? AND p.state = 'MERGED'",
            "1",
            &repo_parameter,
            &windows,
        )?;
        let total_reviews = self.split_measure(
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            "AND p.repo_key = ?",
            "1",
            &repo_parameter,
            &windows,
        )?;
        let total_comments = self.split_measure(
            "(SELECT author_key, pr_key, created_date_key FROM fact_review_comments
              UNION ALL
              SELECT author_key, parent_key AS pr_key, created_date_key
              FROM fact_issue_comments WHERE parent_type = 'pull_request') fc
             JOIN fact_pull_requests p ON p.pr_key = fc.pr_key",
            "fc.created_date_key",
            "AND p.repo_key = ?",
            "1",
            &repo_parameter,
            &windows,
        )?;

        // Check failure rate over PRs created in the current window.
        let check_failure_rate: Option<f64> = self
            .connection
            .query_row(
                &format!(
                    "SELECT CAST(SUM(CASE WHEN c.conclusion IN ('FAILURE', 'TIMED_OUT') THEN 1 ELSE 0 END) AS REAL),
                            CAST(COUNT(*) AS REAL)
                     FROM fact_check_runs c
                     JOIN fact_pull_requests p ON p.pr_key = c.pr_key
                     JOIN dim_date dd ON dd.date_key = p.created_date_key
                     WHERE p.repo_key = ?1
                       AND dd.date_key BETWEEN '{}' AND '{}'",
                    windows.current_start, windows.current_end
                ),
                [&repo_key],
                |row| {
                    let failed: Option<f64> = row.get(0)?;
                    let total: f64 = row.get(1)?;
                    Ok(failed.and_then(|failed| {
                        if total > 0.0 { Some(failed / total) } else { None }
                    }))
                },
            )
            .unwrap_or(None);

        Ok(RepoMetrics {
            repository: repo_key,
            period_key: windows.period_key.clone(),
            previous_period_key: windows.previous_period_key.clone(),
            current_date_range: (windows.current_start.clone(), windows.current_end.clone()),
            previous_date_range: (windows.previous_start.clone(), windows.previous_end.clone()),
            prs_opened,
            prs_merged,
            total_reviews,
            total_comments,
            check_failure_rate,
        })
    }

    pub fn repo_aggregations(
        &self,
        repository: &str,
        period: &Period,
        top: u32,
    ) -> Result<RepoAggregations> {
        let repo_key = repository.to_lowercase();
        let windows = self.windows(period);
        let repo_parameter = vec![SqlValue::Text(repo_key.clone())];

        let top_contributors = self.leaderboard(
            "p.author_key",
            "fact_pull_requests p",
            "p.created_date_key",
            "AND p.repo_key = ?",
            &repo_parameter,
            &windows,
            top,
        )?;
        let top_mergers = self.leaderboard(
            "p.author_key",
            "fact_pull_requests p",
            "p.created_date_key",
            "AND p.repo_key = ? AND p.state = 'MERGED'",
            &repo_parameter,
            &windows,
            top,
        )?;
        let top_reviewers = self.leaderboard(
            "fr.reviewer_key",
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            "AND p.repo_key = ?",
            &repo_parameter,
            &windows,
            top,
        )?;
        let top_commenters = self.leaderboard(
            "fc.author_key",
            "(SELECT author_key, pr_key, created_date_key FROM fact_review_comments
              UNION ALL
              SELECT author_key, parent_key AS pr_key, created_date_key
              FROM fact_issue_comments WHERE parent_type = 'pull_request') fc
             JOIN fact_pull_requests p ON p.pr_key = fc.pr_key",
            "fc.created_date_key",
            "AND p.repo_key = ?",
            &repo_parameter,
            &windows,
            top,
        )?;

        Ok(RepoAggregations {
            top_contributors,
            top_mergers,
            top_reviewers,
            top_commenters,
        })
    }

    // ===== Group metrics =====

    pub fn user_group_metrics(
        &self,
        group_name: &str,
        logins: &[String],
        period: &Period,
    ) -> Result<GroupMetrics> {
        let member_keys: Vec<String> = logins
            .iter()
            .flat_map(|login| resolve_entity_keys(self.connection, login))
            .collect();
        let windows = self.windows(period);
        let placeholders = vec!["?"; member_keys.len()].join(", ");
        let member_parameters: Vec<SqlValue> = member_keys
            .iter()
            .map(|key| SqlValue::Text(key.clone()))
            .collect();
        let doubled_parameters: Vec<SqlValue> = member_parameters
            .iter()
            .chain(member_parameters.iter())
            .cloned()
            .collect();

        let prs_opened = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.author_key IN ({placeholders})"),
            "1",
            &member_parameters,
            &windows,
        )?;
        let prs_merged = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.author_key IN ({placeholders}) AND p.state = 'MERGED'"),
            "1",
            &member_parameters,
            &windows,
        )?;
        let reviews_given = self.split_measure(
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            &format!(
                "AND fr.reviewer_key IN ({placeholders}) AND p.author_key NOT IN ({placeholders})"
            ),
            "1",
            &doubled_parameters,
            &windows,
        )?;

        Ok(GroupMetrics {
            group_name: group_name.to_string(),
            kind: "user".into(),
            member_count: member_keys.len() as u64,
            period_key: windows.period_key,
            previous_period_key: windows.previous_period_key,
            prs_opened,
            prs_merged,
            reviews_given: Some(reviews_given),
            total_reviews: None,
            total_comments: None,
        })
    }

    pub fn repo_group_metrics(
        &self,
        group_name: &str,
        repos: &[String],
        period: &Period,
    ) -> Result<GroupMetrics> {
        let repo_keys: Vec<String> = repos.iter().map(|repo| repo.to_lowercase()).collect();
        let windows = self.windows(period);
        let placeholders = vec!["?"; repo_keys.len()].join(", ");
        let repo_parameters: Vec<SqlValue> = repo_keys
            .iter()
            .map(|key| SqlValue::Text(key.clone()))
            .collect();

        let prs_opened = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.repo_key IN ({placeholders})"),
            "1",
            &repo_parameters,
            &windows,
        )?;
        let prs_merged = self.split_measure(
            "fact_pull_requests p",
            "p.created_date_key",
            &format!("AND p.repo_key IN ({placeholders}) AND p.state = 'MERGED'"),
            "1",
            &repo_parameters,
            &windows,
        )?;
        let total_reviews = self.split_measure(
            "fact_reviews fr JOIN fact_pull_requests p ON p.pr_key = fr.pr_key",
            "fr.submitted_date_key",
            &format!("AND p.repo_key IN ({placeholders})"),
            "1",
            &repo_parameters,
            &windows,
        )?;
        let total_comments = self.split_measure(
            "(SELECT author_key, pr_key, created_date_key FROM fact_review_comments
              UNION ALL
              SELECT author_key, parent_key AS pr_key, created_date_key
              FROM fact_issue_comments WHERE parent_type = 'pull_request') fc
             JOIN fact_pull_requests p ON p.pr_key = fc.pr_key",
            "fc.created_date_key",
            &format!("AND p.repo_key IN ({placeholders})"),
            "1",
            &repo_parameters,
            &windows,
        )?;

        Ok(GroupMetrics {
            group_name: group_name.to_string(),
            kind: "repo".into(),
            member_count: repo_keys.len() as u64,
            period_key: windows.period_key,
            previous_period_key: windows.previous_period_key,
            prs_opened,
            prs_merged,
            reviews_given: None,
            total_reviews: Some(total_reviews),
            total_comments: Some(total_comments),
        })
    }
}

#[cfg(test)]
mod tests {
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
}
