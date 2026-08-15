//! PR Summary Agent - analyzes a pull request and produces a structured summary.
//!
//! Mirrors the CRUX warehouse's CR summary agent (identical notability scale and
//! classification vocabulary, so PR and CR scores stay comparable) but reads
//! githubdw's own tables and writes into `pr_summaries`. Scope is single-PR
//! only; there are deliberately no period-level summary functions.

use std::sync::Arc;

use mixtape_core::{Agent, AgentResponse, ClaudeOpus4_5, DynTool};
use rusqlite::Connection;
use tokio::sync::Mutex;

use super::tools::pr_summary_tools;
use crate::Result;

/// System prompt for the PR Summary Agent.
const PR_SUMMARY_SYSTEM_PROMPT: &str = r##"You are a code review analyst. Your task is to analyze a GitHub pull request (PR) and produce a structured summary.

## Database Schema

You have access to a SQLite data warehouse of GitHub activity. Key tables:

### fact_pull_requests (one row per PR)
- pr_key: Primary key, format "{owner}/{repo}#{number}" (e.g. "octocat/hello#42")
- number: PR number within the repo
- repo_key: "{owner}/{repo}"
- author_key: Who opened it (e.g. "user:alice")
- state: OPEN, CLOSED, MERGED
- is_draft: 1 if a draft PR
- title: PR title
- body: PR description (Markdown)
- additions, deletions, changed_files: size of the change
- created_at, updated_at, merged_at: RFC3339 timestamps

### fact_file_diffs (one row per changed file)
- pr_key: Links to the PR
- file_path: Path of the changed file
- previous_path: Prior path for renames
- change_type: added, modified, removed, renamed, copied, changed
- patch: The real unified-diff hunks (the @@ ... @@ lines) - the actual code change
- additions, deletions: per-file line counts

### fact_reviews (one row per submitted review)
- pr_key: Links to the PR
- reviewer_key: Who reviewed (e.g. "user:bob")
- state: APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED
- body: The review's top-level comment

### fact_review_comments (inline code comments)
- pr_key: Links to the PR
- author_key: Who commented
- path, line: File and line the comment is anchored to
- body: The comment text

### fact_issue_comments (PR conversation comments)
- parent_type: filter to 'pull_request' for PR comments
- parent_key: the pr_key
- author_key: Who commented
- body: The comment text

### fact_check_runs (CI results)
- pr_key: Links to the PR
- name: Check name
- status: queued, in_progress, completed
- conclusion: success, failure, neutral, cancelled, timed_out, action_required

### dim_entities
- entity_key: "user:alice" or "team:backend"
- name: Display name

## Your Task

1. Use query_database to explore the PR:
   - Get PR metadata (title, body, author, state, additions/deletions/changed_files)
   - Get file changes and read the `patch` hunks in fact_file_diffs to see the actual code
   - Get reviews and their state (fact_reviews), inline comments (fact_review_comments),
     and conversation comments (fact_issue_comments WHERE parent_type = 'pull_request')
   - Check CI outcomes in fact_check_runs if relevant

2. Analyze and classify the work:
   - What type of change is this?
   - What is the scope and complexity?
   - What is the notability/impact?

3. Call write_pr_summary with your structured analysis, passing the pr_key.

## Notability Scale (0-10)

Use these anchors for consistent scoring:

- **0-2 (Routine)**: Typo fixes, config tweaks, trivial dependency bumps, auto-generated code, comment-only changes
- **3-4 (Minor)**: Small bug fixes, straightforward features, routine refactoring, adding tests for existing code
- **5-6 (Notable)**: Meaningful new features, important bug fixes, significant refactoring, new API endpoints
- **7-8 (Significant)**: Major features, architectural changes, cross-team integrations, performance improvements with measurable impact
- **9-10 (Exceptional)**: Transformational changes, new systems, company-wide infrastructure, security fixes for critical vulnerabilities

## Classification Values

### change_types (pick all that apply):
- new_feature: New functionality
- bugfix: Fixing broken behavior
- refactor: Code restructuring without behavior change
- performance: Speed/efficiency improvements
- testing: Adding or improving tests
- documentation: Docs, comments, READMEs
- infrastructure: Build, CI/CD, tooling
- dependency: Library/package updates
- security: Security fixes or hardening
- cleanup: Removing dead code, formatting

### impact_areas (pick all that apply):
- user_facing: Affects end users directly
- api_surface: Changes public APIs
- internal: Internal implementation only
- data_model: Database schema or data structures
- configuration: Config files, feature flags

### complexity_signal:
- trivial: One-liner, obvious change
- straightforward: Clear approach, limited scope
- involved: Multiple components, requires understanding
- substantial: Deep changes, significant planning

## Guidelines

- Be concise but informative in your summaries
- Focus on WHAT changed and WHY it matters
- Don't just describe the diff - explain the intent
- If the PR has interesting context (reverts something, addresses review feedback), mention it
- For large PRs, focus on the most important changes
- IMPORTANT: You MUST call write_pr_summary to save your analysis. Do not just describe the summary in text - actually call the tool.
"##;

/// A PR summary retrieved from the database.
#[derive(Debug, Clone)]
pub struct PrSummaryData {
    pub pr_key: String,
    pub headline: String,
    pub what_changed: String,
    pub why_it_matters: String,
    pub notability_score: i32,
    pub complexity_signal: String,
    pub runtime_ms: Option<i64>,
}

/// Result of running the PR Summary Agent.
pub struct PrSummaryResult {
    /// The PR key that was summarized.
    pub pr_key: String,
    /// Whether a summary was successfully written.
    pub success: bool,
    /// Whether the summary already existed (skipped).
    pub skipped: bool,
    /// The summary data (if successful or skipped).
    pub summary: Option<PrSummaryData>,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Check if a PR summary already exists.
pub fn has_pr_summary(conn: &Connection, pr_key: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM pr_summaries WHERE pr_key = ?1",
        rusqlite::params![pr_key],
        |_| Ok(true),
    )
    .unwrap_or(false)
}

/// Get a PR summary from the database.
pub fn get_pr_summary(conn: &Connection, pr_key: &str) -> Option<PrSummaryData> {
    conn.query_row(
        r#"SELECT pr_key, headline, what_changed, why_it_matters,
                  notability_score, complexity_signal, runtime_ms
           FROM pr_summaries WHERE pr_key = ?1"#,
        rusqlite::params![pr_key],
        |row| {
            Ok(PrSummaryData {
                pr_key: row.get(0)?,
                headline: row.get(1)?,
                what_changed: row.get(2)?,
                why_it_matters: row.get(3)?,
                notability_score: row.get(4)?,
                complexity_signal: row.get(5)?,
                runtime_ms: row.get(6)?,
            })
        },
    )
    .ok()
}

/// Run the PR Summary Agent for a single pull request.
///
/// If `force` is false and a summary already exists, returns the existing summary.
pub async fn summarize_pr(
    conn: Arc<Mutex<Connection>>,
    pr_key: &str,
    force: bool,
) -> Result<PrSummaryResult> {
    // Check if a summary already exists.
    {
        let conn_guard = conn.lock().await;
        if !force && has_pr_summary(&conn_guard, pr_key) {
            let summary = get_pr_summary(&conn_guard, pr_key);
            return Ok(PrSummaryResult {
                pr_key: pr_key.to_string(),
                success: true,
                skipped: true,
                summary,
                error: None,
            });
        }
    }

    // Keep a reference for checking success after the agent runs.
    let conn_for_check = conn.clone();
    let tools: Vec<Box<dyn DynTool>> = pr_summary_tools(conn);

    let agent = Agent::builder()
        .bedrock(ClaudeOpus4_5)
        .with_system_prompt(PR_SUMMARY_SYSTEM_PROMPT)
        .add_trusted_tools(tools)
        .build()
        .await
        .map_err(|e| crate::Error::Llm(format!("Failed to create agent: {}", e)))?;

    let prompt = format!(
        "Analyze and summarize pull request: {}\n\n\
         Start by querying the database to get the PR details, file diffs, reviews, \
         and comments. Then write a structured summary using write_pr_summary.",
        pr_key
    );

    // Track timing.
    let start = std::time::Instant::now();

    let response: AgentResponse = agent
        .run(&prompt)
        .await
        .map_err(|e| crate::Error::Llm(format!("Agent failed: {}", e)))?;

    let runtime_ms = start.elapsed().as_millis() as i64;

    // Check if a summary was actually written and patch in the runtime.
    let conn_guard = conn_for_check.lock().await;

    let success = has_pr_summary(&conn_guard, pr_key);

    if success {
        let _ = conn_guard.execute(
            "UPDATE pr_summaries SET runtime_ms = ?1 WHERE pr_key = ?2",
            rusqlite::params![runtime_ms, pr_key],
        );
    }

    let summary = if success {
        get_pr_summary(&conn_guard, pr_key)
    } else {
        None
    };

    let response_text = response.to_string();

    Ok(PrSummaryResult {
        pr_key: pr_key.to_string(),
        success,
        skipped: false,
        summary,
        error: if success { None } else { Some(response_text) },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[test]
    fn test_system_prompt_mentions_tools() {
        assert!(!PR_SUMMARY_SYSTEM_PROMPT.is_empty());
        assert!(PR_SUMMARY_SYSTEM_PROMPT.contains("query_database"));
        assert!(PR_SUMMARY_SYSTEM_PROMPT.contains("write_pr_summary"));
    }

    fn test_conn() -> Connection {
        let mut conn = Connection::open_in_memory().expect("in-memory db");
        crate::storage::schema::init(&mut conn).expect("schema init");
        conn
    }

    fn insert_summary(conn: &Connection, pr_key: &str) {
        conn.execute(
            r#"INSERT INTO pr_summaries
               (pr_key, repo_key, number, headline, what_changed, why_it_matters,
                notability_score, change_types, impact_areas, complexity_signal,
                prompt_version, source_updated_at, runtime_ms, created_at, updated_at)
               VALUES (?1, 'octo/hello', 42, 'A headline', 'What changed', 'Why it matters',
                       6, '["bugfix"]', '["internal"]', 'involved',
                       1, '2026-08-15T12:00:00Z', 777, '2026-08-15T13:00:00Z',
                       '2026-08-15T13:00:00Z')"#,
            rusqlite::params![pr_key],
        )
        .expect("insert summary");
    }

    #[test]
    fn has_and_get_pr_summary_round_trip() {
        let conn = test_conn();
        assert!(!has_pr_summary(&conn, "octo/hello#42"));
        assert!(get_pr_summary(&conn, "octo/hello#42").is_none());

        insert_summary(&conn, "octo/hello#42");

        assert!(has_pr_summary(&conn, "octo/hello#42"));
        let summary = get_pr_summary(&conn, "octo/hello#42").expect("summary");
        assert_eq!(summary.pr_key, "octo/hello#42");
        assert_eq!(summary.headline, "A headline");
        assert_eq!(summary.what_changed, "What changed");
        assert_eq!(summary.why_it_matters, "Why it matters");
        assert_eq!(summary.notability_score, 6);
        assert_eq!(summary.complexity_signal, "involved");
        assert_eq!(summary.runtime_ms, Some(777));
    }

    #[tokio::test]
    async fn summarize_pr_skips_when_summary_exists() {
        let conn = test_conn();
        insert_summary(&conn, "octo/hello#42");
        let conn = Arc::new(Mutex::new(conn));

        let result = summarize_pr(conn, "octo/hello#42", false)
            .await
            .expect("summarize_pr");
        assert!(result.skipped, "existing summary must short-circuit");
        assert!(result.success);
        assert!(result.error.is_none());
        let summary = result
            .summary
            .expect("skip path returns the stored summary");
        assert_eq!(summary.headline, "A headline");
    }
}
