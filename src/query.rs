//! Fluent QueryBuilder: chainable filters compiled into parameterized SQL.

use chrono::NaiveDate;
use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use serde::Serialize;

use crate::error::{Error, Result};
use crate::period::Period;

/// Pull-request state filter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PrState {
    Open,
    Merged,
    Closed,
}

impl PrState {
    fn as_sql(&self) -> &'static str {
        match self {
            PrState::Open => "OPEN",
            PrState::Merged => "MERGED",
            PrState::Closed => "CLOSED",
        }
    }
}

/// One pull-request result row.
#[derive(Debug, Clone, Serialize)]
pub struct PullRequestSummary {
    pub pr_key: String,
    pub number: i64,
    pub repo_key: String,
    pub author: String,
    pub state: String,
    pub title: Option<String>,
    pub created_at: String,
    pub merged_at: Option<String>,
    pub comment_count: i64,
    pub review_count: i64,
    pub additions: i64,
    pub deletions: i64,
}

/// Entity-key namespaces ingestion mints into `dim_entities.entity_type`.
const ENTITY_NAMESPACES: [&str; 2] = ["user", "bot"];

/// Namespace assumed for a bare login the warehouse has never seen.
const DEFAULT_ENTITY_NAMESPACE: &str = "user";

/// Strip a known `user:` / `bot:` namespace prefix and lowercase the result.
///
/// Roster tables (`monitored_users.login`, `user_group_member.login`) store
/// *bare* logins, so every writer must funnel caller input through here. A
/// prefixed spelling stored verbatim becomes a row no bare-login read can ever
/// match, and the write reports success.
pub fn to_bare_login(value: &str) -> String {
    let lowered = value.to_lowercase();
    for namespace in ENTITY_NAMESPACES {
        if let Some(bare) = lowered.strip_prefix(&format!("{namespace}:")) {
            return bare.to_string();
        }
    }
    lowered
}

/// Resolve caller input to every entity key ingestion may have stored for it.
///
/// Ingestion keys an identity by its type — `user:<login>` for people,
/// `bot:<login>` for apps — so assuming `user:` makes bot identities
/// unreachable by their bare login (a bare `github-actions` would silently
/// return zero rows while `bot:github-actions` matched). A bare login is
/// therefore resolved through the indexed `dim_entities.login` column and
/// matches whichever namespaces actually exist for it. Explicitly prefixed
/// input is honored verbatim, so a caller can still disambiguate.
///
/// A login the warehouse does not know falls back to `user:<login>`, keeping a
/// miss an empty result set rather than a match-everything wildcard.
pub fn resolve_entity_keys(connection: &Connection, login: &str) -> Vec<String> {
    let login = login.to_lowercase();
    if login.contains(':') {
        return vec![login];
    }
    // A lookup failure degrades to the default namespace rather than
    // propagating: the caller's filter is then exactly as selective as it was
    // before this resolution existed.
    let matched = lookup_entity_keys(connection, &login).unwrap_or_default();
    if matched.is_empty() {
        vec![format!("{DEFAULT_ENTITY_NAMESPACE}:{login}")]
    } else {
        matched
    }
}

/// Every `dim_entities.entity_key` carrying this bare login, ordered for
/// deterministic output.
fn lookup_entity_keys(connection: &Connection, bare_login: &str) -> Result<Vec<String>> {
    let mut statement =
        connection.prepare("SELECT entity_key FROM dim_entities WHERE login = ?1 ORDER BY 1")?;
    let rows = statement.query_map([bare_login], |row| row.get::<_, String>(0))?;
    let mut keys = Vec::new();
    for row in rows {
        keys.push(row?);
    }
    Ok(keys)
}

/// Chainable query over the warehouse.
pub struct QueryBuilder<'a> {
    connection: &'a Connection,
    authors: Vec<String>,
    reviewers: Vec<String>,
    repos: Vec<String>,
    orgs: Vec<String>,
    states: Vec<String>,
    labels: Vec<String>,
    since: Option<NaiveDate>,
    until: Option<NaiveDate>,
    period: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(connection: &'a Connection) -> Self {
        Self {
            connection,
            authors: Vec::new(),
            reviewers: Vec::new(),
            repos: Vec::new(),
            orgs: Vec::new(),
            states: Vec::new(),
            labels: Vec::new(),
            since: None,
            until: None,
            period: None,
            limit: None,
            offset: None,
        }
    }

    pub fn author(mut self, login: &str) -> Self {
        let keys = resolve_entity_keys(self.connection, login);
        self.authors.extend(keys);
        self
    }

    pub fn authors(mut self, logins: &[String]) -> Self {
        for login in logins {
            let keys = resolve_entity_keys(self.connection, login);
            self.authors.extend(keys);
        }
        self
    }

    pub fn reviewer(mut self, login: &str) -> Self {
        let keys = resolve_entity_keys(self.connection, login);
        self.reviewers.extend(keys);
        self
    }

    pub fn repo(mut self, name: &str) -> Self {
        self.repos.push(name.to_lowercase());
        self
    }

    pub fn repos(mut self, names: &[String]) -> Self {
        self.repos.extend(names.iter().map(|n| n.to_lowercase()));
        self
    }

    pub fn org(mut self, owner: &str) -> Self {
        self.orgs.push(owner.to_lowercase());
        self
    }

    pub fn state(mut self, state: PrState) -> Self {
        self.states.push(state.as_sql().to_string());
        self
    }

    pub fn merged(self) -> Self {
        self.state(PrState::Merged)
    }

    pub fn open(self) -> Self {
        self.state(PrState::Open)
    }

    pub fn closed(self) -> Self {
        self.state(PrState::Closed)
    }

    pub fn label(mut self, name: &str) -> Self {
        self.labels.push(name.to_string());
        self
    }

    pub fn since(mut self, date: NaiveDate) -> Self {
        self.since = Some(date);
        self
    }

    pub fn until(mut self, date: NaiveDate) -> Self {
        self.until = Some(date);
        self
    }

    pub fn between(self, start: NaiveDate, end: NaiveDate) -> Self {
        self.since(start).until(end)
    }

    pub fn period(mut self, period: Period) -> Self {
        self.period = Some(period.to_key());
        self
    }

    pub fn period_str(mut self, key: &str) -> Self {
        self.period = Some(key.to_string());
        self
    }

    pub fn limit(mut self, n: u32) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, n: u32) -> Self {
        self.offset = Some(n);
        self
    }

    /// Assemble joins/conditions/parameters shared by all terminals.
    fn assemble(&self) -> (String, Vec<String>, Vec<SqlValue>) {
        let mut joins: Vec<String> = Vec::new();
        let mut conditions: Vec<String> = Vec::new();
        let mut parameters: Vec<SqlValue> = Vec::new();

        let add_in_clause = |column: &str,
                             values: &[String],
                             conditions: &mut Vec<String>,
                             parameters: &mut Vec<SqlValue>| {
            if values.is_empty() {
                return;
            }
            let placeholders = vec!["?"; values.len()].join(", ");
            conditions.push(format!("{column} IN ({placeholders})"));
            parameters.extend(values.iter().map(|v| SqlValue::Text(v.clone())));
        };

        add_in_clause(
            "p.author_key",
            &self.authors,
            &mut conditions,
            &mut parameters,
        );
        add_in_clause("p.repo_key", &self.repos, &mut conditions, &mut parameters);
        add_in_clause("p.state", &self.states, &mut conditions, &mut parameters);

        if !self.orgs.is_empty() {
            joins.push("JOIN dim_repositories dr ON dr.repo_key = p.repo_key".into());
            add_in_clause("dr.owner", &self.orgs, &mut conditions, &mut parameters);
        }
        if !self.reviewers.is_empty() {
            joins.push("JOIN fact_reviews qr ON qr.pr_key = p.pr_key".into());
            add_in_clause(
                "qr.reviewer_key",
                &self.reviewers,
                &mut conditions,
                &mut parameters,
            );
        }
        if !self.labels.is_empty() {
            joins.push(
                "JOIN pr_labels pl ON pl.pr_key = p.pr_key \
                 JOIN labels lbl ON lbl.id = pl.label_id"
                    .into(),
            );
            add_in_clause("lbl.name", &self.labels, &mut conditions, &mut parameters);
        }
        if let Some(period_key) = self.period.as_deref() {
            let column = Period::column_for_key(period_key);
            joins.push("JOIN dim_date dd ON dd.date_key = p.created_date_key".into());
            conditions.push(format!("dd.{column} = ?"));
            parameters.push(SqlValue::Text(period_key.to_string()));
        }
        if let Some(since) = self.since {
            conditions.push("p.created_date_key >= ?".into());
            parameters.push(SqlValue::Text(since.format("%Y-%m-%d").to_string()));
        }
        if let Some(until) = self.until {
            conditions.push("p.created_date_key <= ?".into());
            parameters.push(SqlValue::Text(until.format("%Y-%m-%d").to_string()));
        }

        let join_clause = joins.join(" ");
        (join_clause, conditions, parameters)
    }

    fn where_clause(conditions: &[String]) -> String {
        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    fn pagination_clause(&self) -> String {
        let mut clause = String::new();
        if let Some(limit) = self.limit {
            clause.push_str(&format!(" LIMIT {limit}"));
            if let Some(offset) = self.offset {
                clause.push_str(&format!(" OFFSET {offset}"));
            }
        } else if let Some(offset) = self.offset {
            clause.push_str(&format!(" LIMIT -1 OFFSET {offset}"));
        }
        clause
    }

    /// The generated SQL for debugging.
    pub fn to_sql(&self) -> String {
        let (joins, conditions, parameters) = self.assemble();
        format!(
            "SELECT ... FROM fact_pull_requests p {joins} {} -- params: {parameters:?}",
            Self::where_clause(&conditions)
        )
    }

    /// Matching PRs ordered by created_at DESC. Honors limit/offset.
    pub fn pull_requests(self) -> Result<Vec<PullRequestSummary>> {
        let (joins, conditions, parameters) = self.assemble();
        let sql = format!(
            "SELECT DISTINCT p.pr_key, p.number, p.repo_key, p.author_key, p.state, p.title,
                    p.created_at, p.merged_at, p.comment_count, p.review_count,
                    p.additions, p.deletions
             FROM fact_pull_requests p {joins} {}
             ORDER BY p.created_at DESC{}",
            Self::where_clause(&conditions),
            self.pagination_clause(),
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(parameters), |row| {
            Ok(PullRequestSummary {
                pr_key: row.get(0)?,
                number: row.get(1)?,
                repo_key: row.get(2)?,
                author: row.get(3)?,
                state: row.get(4)?,
                title: row.get(5)?,
                created_at: row.get(6)?,
                merged_at: row.get(7)?,
                comment_count: row.get(8)?,
                review_count: row.get(9)?,
                additions: row.get(10)?,
                deletions: row.get(11)?,
            })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// `COUNT(DISTINCT pr_key)` — ignores limit/offset.
    pub fn count(self) -> Result<u64> {
        let (joins, conditions, parameters) = self.assemble();
        let sql = format!(
            "SELECT COUNT(DISTINCT p.pr_key) FROM fact_pull_requests p {joins} {}",
            Self::where_clause(&conditions)
        );
        let count: i64 =
            self.connection
                .query_row(&sql, rusqlite::params_from_iter(parameters), |row| {
                    row.get(0)
                })?;
        Ok(count as u64)
    }

    /// `(SUM(additions), SUM(deletions))`.
    pub fn lines_changed(self) -> Result<(u64, u64)> {
        let (joins, conditions, parameters) = self.assemble();
        let sql = format!(
            "SELECT COALESCE(SUM(additions), 0), COALESCE(SUM(deletions), 0) FROM (
                SELECT DISTINCT p.pr_key, p.additions, p.deletions
                FROM fact_pull_requests p {joins} {}
             )",
            Self::where_clause(&conditions)
        );
        let (added, removed): (i64, i64) =
            self.connection
                .query_row(&sql, rusqlite::params_from_iter(parameters), |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
        Ok((added as u64, removed as u64))
    }

    fn count_grouped(self, group_expression: &str) -> Result<Vec<(String, u64)>> {
        let (joins, conditions, parameters) = self.assemble();
        let needs_date_join = group_expression.starts_with("dd.") && !joins.contains("dim_date");
        let date_join = if needs_date_join {
            " JOIN dim_date dd ON dd.date_key = p.created_date_key"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {group_expression} AS grp, COUNT(DISTINCT p.pr_key) AS cnt
             FROM fact_pull_requests p {joins}{date_join} {}
             GROUP BY grp ORDER BY cnt DESC, grp",
            Self::where_clause(&conditions)
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(parameters), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn count_by_author(self) -> Result<Vec<(String, u64)>> {
        self.count_grouped("p.author_key")
    }

    pub fn count_by_repo(self) -> Result<Vec<(String, u64)>> {
        self.count_grouped("p.repo_key")
    }

    pub fn count_by_state(self) -> Result<Vec<(String, u64)>> {
        self.count_grouped("p.state")
    }

    pub fn count_by_period(self) -> Result<Vec<(String, u64)>> {
        self.count_grouped("dd.month_key")
    }

    /// CSV of the row output (fixed header).
    pub fn to_csv(self) -> Result<String> {
        let rows = self.pull_requests()?;
        let mut output = String::from(
            "pr_key,number,repo,author,state,title,created_at,merged_at,comments,reviews,additions,deletions\n",
        );
        for row in rows {
            output.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{}\n",
                row.pr_key,
                row.number,
                row.repo_key,
                row.author,
                row.state,
                csv_escape(row.title.as_deref().unwrap_or("")),
                row.created_at,
                row.merged_at.as_deref().unwrap_or(""),
                row.comment_count,
                row.review_count,
                row.additions,
                row.deletions,
            ));
        }
        Ok(output)
    }

    /// Pretty JSON of the row output.
    pub fn to_json(self) -> Result<String> {
        let rows = self.pull_requests()?;
        Ok(serde_json::to_string_pretty(&rows)?)
    }
}

fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Run an arbitrary read-only SELECT; returns rows as JSON objects.
pub fn raw_query(
    connection: &Connection,
    sql: &str,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
    validate_select_only(sql)?;
    let mut statement = connection.prepare(sql)?;
    let column_names: Vec<String> = statement
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let mut rows_out = Vec::new();
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let mut object = serde_json::Map::new();
        for (index, name) in column_names.iter().enumerate() {
            let value = match row.get_ref(index)? {
                rusqlite::types::ValueRef::Null => serde_json::Value::Null,
                rusqlite::types::ValueRef::Integer(i) => serde_json::Value::from(i),
                rusqlite::types::ValueRef::Real(f) => serde_json::Value::from(f),
                rusqlite::types::ValueRef::Text(t) => {
                    serde_json::Value::from(String::from_utf8_lossy(t).into_owned())
                }
                rusqlite::types::ValueRef::Blob(_) => serde_json::Value::from("<blob>"),
            };
            object.insert(name.clone(), value);
        }
        rows_out.push(object);
    }
    Ok(rows_out)
}

/// Reject anything that is not a single SELECT statement.
pub fn validate_select_only(sql: &str) -> Result<()> {
    let trimmed = sql.trim_start();
    let first_token = trimmed
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_uppercase();
    if first_token != "SELECT" && first_token != "WITH" {
        return Err(Error::InvalidArgument(
            "only SELECT queries are allowed".into(),
        ));
    }
    // Reject multiple statements.
    if trimmed.trim_end().trim_end_matches(';').contains(';') {
        return Err(Error::InvalidArgument(
            "multiple statements are not allowed".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
        assert_eq!(by_author[0], ("user:alice".to_string(), 2));

        let by_period = QueryBuilder::new(conn).count_by_period().unwrap();
        assert_eq!(by_period.len(), 2);

        let json = QueryBuilder::new(conn).merged().to_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 2);

        let csv = QueryBuilder::new(conn).to_csv().unwrap();
        assert!(csv.starts_with("pr_key,"));
        assert_eq!(csv.lines().count(), 4);
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
}
