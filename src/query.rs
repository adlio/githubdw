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
    /// Parse `open`, `merged` or `closed` (any case).
    pub fn parse(text: &str) -> Result<Self> {
        match text.to_lowercase().as_str() {
            "open" => Ok(PrState::Open),
            "merged" => Ok(PrState::Merged),
            "closed" => Ok(PrState::Closed),
            other => Err(Error::InvalidArgument(format!(
                "unknown state '{other}', use: open, merged, closed"
            ))),
        }
    }

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
    /// The author's login, exactly as GitHub spells it — never namespaced.
    pub author: String,
    /// The namespace that login was stored under: `user` or `bot`.
    pub author_type: String,
    /// The warehouse key for the author, for callers joining back to the
    /// star schema. Always `author_type` + `:` + `author`.
    pub author_key: String,
    pub state: String,
    pub title: Option<String>,
    pub created_at: String,
    pub merged_at: Option<String>,
    pub comment_count: i64,
    pub review_count: i64,
    pub additions: i64,
    pub deletions: i64,
}

impl PullRequestSummary {
    /// The author for a single-column display: the bare login, with bots
    /// marked. A row has one author column in table output, so the marker is
    /// what keeps a bot from reading as the person who shares its login.
    pub fn author_display(&self) -> String {
        display_identity(&self.author, &self.author_type)
    }
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

/// The namespace half of an entity key (`bot:builder` → `bot`).
///
/// Only used to recover the type when a key could not be joined back to
/// `dim_entities`; an unrecognized spelling degrades to the default namespace
/// rather than surfacing the raw key to a caller.
pub fn entity_namespace(entity_key: &str) -> String {
    entity_key
        .split_once(':')
        .map(|(namespace, _)| namespace.to_lowercase())
        .filter(|namespace| ENTITY_NAMESPACES.contains(&namespace.as_str()))
        .unwrap_or_else(|| DEFAULT_ENTITY_NAMESPACE.to_string())
}

/// Render one identity for a single display column: the bare login, with bots
/// marked.
///
/// Human-facing output shows the login GitHub itself uses, so a value read off
/// the screen can be typed back into any GitHub surface. That drops the
/// namespace, and with it the one thing the namespace was doing for a reader —
/// telling a bot apart from a person of the same name — so bots carry a visible
/// marker instead of a machine prefix.
pub fn display_identity(login: &str, entity_type: &str) -> String {
    if entity_type == "bot" {
        format!("{login} [bot]")
    } else {
        login.to_string()
    }
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
        // The author is resolved back to `dim_entities` so callers get the login
        // GitHub uses. LEFT so the join can only add columns, never drop a row.
        let sql = format!(
            "SELECT DISTINCT p.pr_key, p.number, p.repo_key, p.author_key, p.state, p.title,
                    p.created_at, p.merged_at, p.comment_count, p.review_count,
                    p.additions, p.deletions, ae.login, ae.entity_type
             FROM fact_pull_requests p {joins}
             LEFT JOIN dim_entities ae ON ae.entity_key = p.author_key {}
             ORDER BY p.created_at DESC{}",
            Self::where_clause(&conditions),
            self.pagination_clause(),
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(parameters), |row| {
            let author_key: String = row.get(3)?;
            let login: Option<String> = row.get(12)?;
            let entity_type: Option<String> = row.get(13)?;
            Ok(PullRequestSummary {
                pr_key: row.get(0)?,
                number: row.get(1)?,
                repo_key: row.get(2)?,
                author: login.unwrap_or_else(|| to_bare_login(&author_key)),
                author_type: entity_type.unwrap_or_else(|| entity_namespace(&author_key)),
                author_key,
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
        let entity_join = if group_expression.contains("ae.") {
            " LEFT JOIN dim_entities ae ON ae.entity_key = p.author_key"
        } else {
            ""
        };
        let sql = format!(
            "SELECT {group_expression} AS grp, COUNT(DISTINCT p.pr_key) AS cnt
             FROM fact_pull_requests p {joins}{date_join}{entity_join} {}
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

    /// PR counts per author, labeled with the bare login.
    ///
    /// Grouping on the display label rather than the raw key keeps two
    /// identities that share a login — a person and a bot — as separate rows,
    /// because only one of them carries the `[bot]` marker.
    pub fn count_by_author(self) -> Result<Vec<(String, u64)>> {
        self.count_grouped(
            "COALESCE(ae.login, p.author_key) ||
             CASE WHEN ae.entity_type = 'bot' THEN ' [bot]' ELSE '' END",
        )
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
            "pr_key,number,repo,author,author_type,state,title,created_at,merged_at,comments,reviews,additions,deletions\n",
        );
        for row in rows {
            output.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                row.pr_key,
                row.number,
                row.repo_key,
                row.author,
                row.author_type,
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

/// Pull-request filters as the CLI and MCP surfaces receive them. Group names
/// and period expressions are validated against the warehouse when the
/// builder is made.
#[derive(Debug, Clone, Default)]
pub struct PrFilters {
    pub author: Option<String>,
    pub reviewer: Option<String>,
    pub repo: Option<String>,
    pub org: Option<String>,
    /// A user group filters by author; a repo group filters by repository.
    pub group: Option<String>,
    pub label: Option<String>,
    /// Applied after `merged`, so it wins. Parse text with [`PrState::parse`].
    pub state: Option<PrState>,
    pub merged: bool,
    /// Period expression; relative periods resolve against the warehouse's
    /// own calendar.
    pub period: Option<String>,
    pub since: Option<NaiveDate>,
    pub until: Option<NaiveDate>,
}

impl PrFilters {
    /// Validate the filters and compile them into a builder.
    pub fn builder<'a>(&self, connection: &'a Connection) -> Result<QueryBuilder<'a>> {
        let mut builder = QueryBuilder::new(connection);
        if let Some(author) = self.author.as_deref() {
            builder = builder.author(author);
        }
        if let Some(reviewer) = self.reviewer.as_deref() {
            builder = builder.reviewer(reviewer);
        }
        if let Some(repo) = self.repo.as_deref() {
            builder = builder.repo(repo);
        }
        if let Some(org) = self.org.as_deref() {
            builder = builder.org(org);
        }
        if let Some(name) = self.group.as_deref() {
            let kind = crate::groups::kind_of(connection, name)?;
            let members = crate::groups::members(connection, kind, name)?;
            builder = match kind {
                crate::groups::GroupKind::User => builder.authors(&members),
                crate::groups::GroupKind::Repo => builder.repos(&members),
            };
        }
        if let Some(label) = self.label.as_deref() {
            builder = builder.label(label);
        }
        if self.merged {
            builder = builder.merged();
        }
        if let Some(state) = self.state {
            builder = builder.state(state);
        }
        if let Some(text) = self.period.as_deref() {
            let today = crate::storage::time_dimension::today(connection)?;
            builder = match Period::parse_with_reference(text, today)? {
                rolling @ Period::Rolling(..) => {
                    let (start, end) = rolling.date_range();
                    builder.between(start, end)
                }
                other => builder.period(other),
            };
        }
        if let Some(since) = self.since {
            builder = builder.since(since);
        }
        if let Some(until) = self.until {
            builder = builder.until(until);
        }
        Ok(builder)
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
mod tests;
