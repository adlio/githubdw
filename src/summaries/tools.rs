//! Mixtape tools for agentic PR summarization (feature = "summaries").
//!
//! Tools provided to the agent:
//! - QueryDatabase: read-only SQL over the githubdw star schema
//! - WritePrSummary: persist a structured PR summary into `pr_summaries`
//!
//! This mirrors the CRUX warehouse's summary tools so PR and CR notability
//! scores stay comparable, but it reads and writes githubdw's own tables. Each
//! data warehouse owns the summaries of the data it holds.

use std::sync::Arc;

use mixtape_core::{Tool, ToolError, ToolResult};
use rusqlite::Connection;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// -----------------------------------------------------------------------------
// QueryDatabase Tool
// -----------------------------------------------------------------------------

/// Input for the QueryDatabase tool.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryDatabaseInput {
    /// SQL SELECT query to execute. Must be a read-only query (SELECT only).
    pub sql: String,

    /// Maximum number of rows to return. Defaults to 100.
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// Output row from a database query.
#[derive(Debug, Serialize)]
pub struct QueryRow {
    /// Column names
    pub columns: Vec<String>,
    /// Row values as strings
    pub values: Vec<Vec<String>>,
    /// True when the result was cut at the requested row limit
    pub truncated: bool,
}

/// Read-only SQL query tool for exploring the githubdw star schema.
pub struct QueryDatabaseTool {
    conn: Arc<Mutex<Connection>>,
}

impl QueryDatabaseTool {
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    fn is_read_only(sql: &str) -> bool {
        let trimmed = sql.trim().to_uppercase();
        // Only allow SELECT, WITH (CTE), and EXPLAIN
        trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("EXPLAIN")
    }
}

impl Tool for QueryDatabaseTool {
    type Input = QueryDatabaseInput;

    fn name(&self) -> &str {
        "query_database"
    }

    fn description(&self) -> &str {
        "Execute a read-only SQL query against the GitHub data warehouse. \
         Returns up to `limit` rows (default 100). Use this to explore PR data, \
         file diffs, reviews, comments, and check runs, and to gather context \
         for summaries. The database uses a star schema with fact tables \
         (fact_pull_requests, fact_file_diffs, fact_reviews, fact_review_comments, \
         fact_issue_comments, fact_check_runs) and dimension tables (dim_entities, \
         dim_repositories, dim_date, dim_time)."
    }

    fn execute(
        &self,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<ToolResult, ToolError>> + Send {
        let conn = self.conn.clone();
        async move {
            // Validate read-only
            if !QueryDatabaseTool::is_read_only(&input.sql) {
                return Err(ToolError::Custom(
                    "Only SELECT queries are allowed. Use write tools for modifications."
                        .to_string(),
                ));
            }

            // Execute query
            let conn = conn.lock().await;
            let mut stmt = conn
                .prepare(&input.sql)
                .map_err(|e| ToolError::Custom(format!("SQL prepare error: {}", e)))?;

            // Authoritative read-only enforcement. The prefix check above only
            // produces a friendly early error; SQLite itself classifies the
            // prepared statement (sqlite3_stmt_readonly), which is what stops
            // mutations hiding behind an allowed prefix, e.g.
            // `WITH x AS (SELECT 1) DELETE FROM ...`.
            if !stmt.readonly() {
                return Err(ToolError::Custom(
                    "Query rejected: statement is not read-only.".to_string(),
                ));
            }

            let column_names: Vec<String> = stmt
                .column_names()
                .iter()
                .map(|s: &&str| s.to_string())
                .collect();
            let column_count = column_names.len();

            // Cap rows while collecting -- never by rewriting the SQL text.
            // (Appending `LIMIT n` breaks queries with trailing semicolons,
            // and a substring check misfires on literals and subqueries.)
            let mapped = stmt
                .query_map([], |row: &rusqlite::Row| {
                    let mut values = Vec::with_capacity(column_count);
                    for i in 0..column_count {
                        let value: String = row
                            .get::<_, rusqlite::types::Value>(i)
                            .map(|v| match v {
                                rusqlite::types::Value::Null => "NULL".to_string(),
                                rusqlite::types::Value::Integer(i) => i.to_string(),
                                rusqlite::types::Value::Real(f) => f.to_string(),
                                rusqlite::types::Value::Text(s) => s,
                                rusqlite::types::Value::Blob(_) => "[BLOB]".to_string(),
                            })
                            .unwrap_or_else(|_| "NULL".to_string());
                        values.push(value);
                    }
                    Ok(values)
                })
                .map_err(|e| ToolError::Custom(format!("SQL query error: {}", e)))?;

            let mut rows: Vec<Vec<String>> = Vec::new();
            let mut truncated = false;
            for row in mapped {
                if rows.len() >= input.limit {
                    truncated = true;
                    break;
                }
                rows.push(row.map_err(|e| ToolError::Custom(format!("Row fetch error: {}", e)))?);
            }

            let result = QueryRow {
                columns: column_names,
                values: rows,
                truncated,
            };

            ToolResult::json(result).map_err(|e| ToolError::Custom(e.to_string()))
        }
    }
}

// -----------------------------------------------------------------------------
// WritePrSummary Tool
// -----------------------------------------------------------------------------

/// A notable aspect of a PR.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct NotableAspect {
    /// Type of aspect (e.g., "risk", "innovation", "collaboration")
    pub aspect_type: String,
    /// Description of this aspect
    pub text: String,
}

/// Input for writing a PR summary.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct WritePrSummaryInput {
    /// The PR key being summarized (format: "{owner}/{repo}#{number}", e.g. "octocat/hello#42")
    pub pr_key: String,

    /// One-line summary for lists and quick reference
    pub headline: String,

    /// Technical description of what changed
    pub what_changed: String,

    /// Business/impact framing of why this matters
    pub why_it_matters: String,

    /// Notability score from 0-10
    /// 0-2: Routine (typos, config tweaks)
    /// 3-4: Minor (small features, straightforward fixes)
    /// 5-6: Notable (meaningful features, important fixes)
    /// 7-8: Significant (major features, architectural changes)
    /// 9-10: Exceptional (transformational, company-wide impact)
    pub notability_score: u8,

    /// Justification for the notability score
    pub notability_justification: String,

    /// Types of changes (e.g., ["new_feature", "bugfix", "refactor"])
    pub change_types: Vec<String>,

    /// Areas impacted (e.g., ["user_facing", "api_surface", "internal"])
    pub impact_areas: Vec<String>,

    /// Complexity signal: "trivial", "straightforward", "involved", "substantial"
    pub complexity_signal: String,

    /// Notable aspects about this work (optional)
    #[serde(default)]
    pub notable_aspects: Option<Vec<NotableAspect>>,

    /// Technical keywords for search (optional)
    #[serde(default)]
    pub technical_keywords: Option<Vec<String>>,

    /// Domain/business keywords for search (optional)
    #[serde(default)]
    pub domain_keywords: Option<Vec<String>>,

    /// Context tags (e.g., ["greenfield", "tech_debt_paydown"]) (optional)
    #[serde(default)]
    pub context_tags: Option<Vec<String>>,

    /// Runtime in milliseconds (patched by the caller after the run, not by the agent)
    #[serde(default)]
    pub runtime_ms: Option<i64>,
}

/// Split a `pr_key` of the form "{owner}/{repo}#{number}" into (repo_key, number).
fn parse_pr_key(pr_key: &str) -> Option<(String, i64)> {
    let (repo_key, number) = pr_key.rsplit_once('#')?;
    let number: i64 = number.parse().ok()?;
    Some((repo_key.to_string(), number))
}

/// Write a PR summary to the database.
pub struct WritePrSummaryTool {
    conn: Arc<Mutex<Connection>>,
    prompt_version: i32,
}

impl WritePrSummaryTool {
    pub fn new(conn: Arc<Mutex<Connection>>, prompt_version: i32) -> Self {
        Self {
            conn,
            prompt_version,
        }
    }
}

impl Tool for WritePrSummaryTool {
    type Input = WritePrSummaryInput;

    fn name(&self) -> &str {
        "write_pr_summary"
    }

    fn description(&self) -> &str {
        "Save a structured summary for a pull request to the database. \
         Call this once you have analyzed the PR and determined its notability, \
         change types, and impact. Pass the pr_key; repo, number, author, and the \
         source timestamp are derived from the warehouse."
    }

    fn execute(
        &self,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<ToolResult, ToolError>> + Send {
        let conn = self.conn.clone();
        let prompt_version = self.prompt_version;
        async move {
            let now = chrono::Utc::now().to_rfc3339();
            let conn = conn.lock().await;

            // Derive repo_key, number, author_key, and the source timestamp from
            // the PR row itself so the caller only supplies the analysis fields.
            let pr_row: Option<(String, i64, Option<String>, Option<String>)> = conn
                .query_row(
                    "SELECT repo_key, number, author_key, updated_at
                     FROM fact_pull_requests WHERE pr_key = ?1",
                    rusqlite::params![input.pr_key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .ok();

            let (repo_key, number, author_key, source_updated_at) = match pr_row {
                Some((repo_key, number, author_key, updated_at)) => (
                    repo_key,
                    number,
                    author_key,
                    updated_at.unwrap_or_else(|| now.clone()),
                ),
                None => {
                    // PR not in the warehouse (yet): fall back to parsing the key.
                    let (repo_key, number) =
                        parse_pr_key(&input.pr_key).unwrap_or_else(|| (input.pr_key.clone(), 0));
                    (repo_key, number, None, now.clone())
                }
            };

            conn.execute(
                r#"INSERT INTO pr_summaries
                   (pr_key, repo_key, number, author_key, headline, what_changed, why_it_matters,
                    notability_score, notability_justification,
                    change_types, impact_areas, complexity_signal,
                    notable_aspects, technical_keywords, domain_keywords, context_tags,
                    prompt_version, source_updated_at, is_stale, runtime_ms, created_at, updated_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)
                   ON CONFLICT(pr_key) DO UPDATE SET
                     repo_key = excluded.repo_key,
                     number = excluded.number,
                     author_key = excluded.author_key,
                     headline = excluded.headline,
                     what_changed = excluded.what_changed,
                     why_it_matters = excluded.why_it_matters,
                     notability_score = excluded.notability_score,
                     notability_justification = excluded.notability_justification,
                     change_types = excluded.change_types,
                     impact_areas = excluded.impact_areas,
                     complexity_signal = excluded.complexity_signal,
                     notable_aspects = excluded.notable_aspects,
                     technical_keywords = excluded.technical_keywords,
                     domain_keywords = excluded.domain_keywords,
                     context_tags = excluded.context_tags,
                     prompt_version = excluded.prompt_version,
                     source_updated_at = excluded.source_updated_at,
                     is_stale = excluded.is_stale,
                     runtime_ms = excluded.runtime_ms,
                     updated_at = excluded.updated_at"#,
                rusqlite::params![
                    input.pr_key,
                    repo_key,
                    number,
                    author_key,
                    input.headline,
                    input.what_changed,
                    input.why_it_matters,
                    input.notability_score,
                    input.notability_justification,
                    serde_json::to_string(&input.change_types).unwrap_or_default(),
                    serde_json::to_string(&input.impact_areas).unwrap_or_default(),
                    input.complexity_signal,
                    input
                        .notable_aspects
                        .as_ref()
                        .and_then(|v| serde_json::to_string(v).ok()),
                    input
                        .technical_keywords
                        .as_ref()
                        .and_then(|v| serde_json::to_string(v).ok()),
                    input
                        .domain_keywords
                        .as_ref()
                        .and_then(|v| serde_json::to_string(v).ok()),
                    input
                        .context_tags
                        .as_ref()
                        .and_then(|v| serde_json::to_string(v).ok()),
                    prompt_version,
                    source_updated_at,
                    0_i32,
                    input.runtime_ms,
                    &now,
                    &now,
                ],
            )
            .map_err(|e| ToolError::Custom(format!("Failed to save PR summary: {}", e)))?;

            Ok(ToolResult::text(format!(
                "Saved summary for {} (notability: {})",
                input.pr_key, input.notability_score
            )))
        }
    }
}

// -----------------------------------------------------------------------------
// Tool Creation Helpers
// -----------------------------------------------------------------------------

/// Current prompt version for PR summaries.
pub const PROMPT_VERSION: i32 = 1;

/// Create all tools needed for PR summarization.
pub fn pr_summary_tools(conn: Arc<Mutex<Connection>>) -> Vec<Box<dyn mixtape_core::DynTool>> {
    vec![
        mixtape_core::box_tool(QueryDatabaseTool::new(conn.clone())),
        mixtape_core::box_tool(WritePrSummaryTool::new(conn.clone(), PROMPT_VERSION)),
    ]
}

#[cfg(test)]
mod tests;
