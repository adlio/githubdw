//! Fulltext search over PRs, issues, and comments (FTS5 trigram).

use rusqlite::Connection;
use serde::Serialize;

use crate::error::Result;

/// Which corpora to search.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SearchScope {
    #[default]
    All,
    PullRequests,
    Issues,
    Comments,
}

/// One search hit, joined back to its fact row.
#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    /// "owner/repo#123" for PRs/issues; the comment node id for comments.
    pub key: String,
    /// 'pull_request' | 'issue' | 'review_comment' | 'issue_comment'
    pub kind: String,
    pub repository: Option<String>,
    pub title: Option<String>,
    pub author: Option<String>,
    pub state: Option<String>,
    /// Highlighted snippet with [ ] markers, ~64 chars of context.
    pub snippet: String,
    /// BM25 rank (lower = better).
    pub rank: f64,
}

/// Options for a search.
#[derive(Debug, Default)]
pub struct SearchOptions {
    pub scope: SearchScope,
    /// Restrict to one repository ("owner/name").
    pub repository: Option<String>,
    pub limit: u32,
}

impl SearchOptions {
    fn effective_limit(&self) -> u32 {
        if self.limit == 0 { 20 } else { self.limit }
    }
}

/// Escape a user query for FTS5 MATCH: wrap in double quotes so the
/// trigram tokenizer treats it as a literal substring.
fn fts_quote(query: &str) -> String {
    format!("\"{}\"", query.replace('"', "\"\""))
}

/// Search the warehouse. Results are merged across corpora and ordered by rank.
pub fn search(
    connection: &Connection,
    query: &str,
    options: &SearchOptions,
) -> Result<Vec<SearchHit>> {
    let quoted = fts_quote(query);
    let mut hits: Vec<SearchHit> = Vec::new();
    let limit = options.effective_limit();

    if matches!(options.scope, SearchScope::All | SearchScope::PullRequests) {
        let sql = "SELECT f.pr_key, p.repo_key, p.title, p.author_key, p.state,
                          snippet(pull_requests_fts, -1, '[', ']', '…', 12), rank
                   FROM pull_requests_fts f
                   JOIN fact_pull_requests p ON p.pr_key = f.pr_key
                   WHERE pull_requests_fts MATCH ?1
                     AND (?2 IS NULL OR p.repo_key = ?2)
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(
            rusqlite::params![quoted, options.repository, limit],
            |row| {
                Ok(SearchHit {
                    key: row.get(0)?,
                    kind: "pull_request".into(),
                    repository: row.get(1)?,
                    title: row.get(2)?,
                    author: row.get(3)?,
                    state: row.get(4)?,
                    snippet: row.get(5)?,
                    rank: row.get(6)?,
                })
            },
        )?;
        for row in rows {
            hits.push(row?);
        }
    }

    if matches!(options.scope, SearchScope::All | SearchScope::Issues) {
        let sql = "SELECT i.repo_key || '#' || i.number, i.repo_key, i.title, i.author_key,
                          i.state, snippet(issues_fts, -1, '[', ']', '…', 12), rank
                   FROM issues_fts f
                   JOIN issues i ON i.id = f.id
                   WHERE issues_fts MATCH ?1
                     AND (?2 IS NULL OR i.repo_key = ?2)
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(
            rusqlite::params![quoted, options.repository, limit],
            |row| {
                Ok(SearchHit {
                    key: row.get(0)?,
                    kind: "issue".into(),
                    repository: row.get(1)?,
                    title: row.get(2)?,
                    author: row.get(3)?,
                    state: row.get(4)?,
                    snippet: row.get(5)?,
                    rank: row.get(6)?,
                })
            },
        )?;
        for row in rows {
            hits.push(row?);
        }
    }

    if matches!(options.scope, SearchScope::All | SearchScope::Comments) {
        let sql = "SELECT c.comment_key, c.pr_key, p.repo_key, p.title, c.author_key,
                          snippet(review_comments_fts, -1, '[', ']', '…', 12), rank
                   FROM review_comments_fts f
                   JOIN fact_review_comments c ON c.comment_key = f.comment_key
                   JOIN fact_pull_requests p ON p.pr_key = c.pr_key
                   WHERE review_comments_fts MATCH ?1
                     AND (?2 IS NULL OR p.repo_key = ?2)
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(
            rusqlite::params![quoted, options.repository, limit],
            |row| {
                Ok(SearchHit {
                    key: row.get::<_, String>(1)?, // anchor to the PR
                    kind: "review_comment".into(),
                    repository: row.get(2)?,
                    title: row.get(3)?,
                    author: row.get(4)?,
                    state: None,
                    snippet: row.get(5)?,
                    rank: row.get(6)?,
                })
            },
        )?;
        for row in rows {
            hits.push(row?);
        }

        let sql = "SELECT c.comment_key, c.parent_key, c.author_key,
                          snippet(issue_comments_fts, -1, '[', ']', '…', 12), rank
                   FROM issue_comments_fts f
                   JOIN fact_issue_comments c ON c.comment_key = f.comment_key
                   WHERE issue_comments_fts MATCH ?1
                     AND (?2 IS NULL OR c.parent_key LIKE ?2 || '%'
                          OR c.parent_key IN (SELECT id FROM issues WHERE repo_key = ?2))
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(
            rusqlite::params![quoted, options.repository, limit],
            |row| {
                Ok(SearchHit {
                    key: row.get::<_, String>(1)?, // parent PR key or issue id
                    kind: "issue_comment".into(),
                    repository: None,
                    title: None,
                    author: row.get(2)?,
                    state: None,
                    snippet: row.get(3)?,
                    rank: row.get(4)?,
                })
            },
        )?;
        for row in rows {
            hits.push(row?);
        }
    }

    hits.sort_by(|a, b| {
        a.rank
            .partial_cmp(&b.rank)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    hits.truncate(limit as usize);
    Ok(hits)
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
                 VALUES ('user:alice', 'user', 'alice', 1, 0, 'alice');
                 INSERT INTO dim_repositories (repo_key, owner, name)
                 VALUES ('octo/alpha', 'octo', 'alpha'), ('octo/beta', 'octo', 'beta');
                 INSERT INTO dim_date (date_key, year, quarter, month, day_of_month, day_of_week,
                     is_weekend, week_of_year, week_key, month_key, quarter_key, year_key, half_key)
                 VALUES ('2026-01-10', 2026, 1, 1, 10, 6, 1, 2, '2026-W02', '2026-01', '2026-Q1', '2026', '2026-H1');
                 INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
                 VALUES ('10:00', 10, 10, 'AM', 'Morning', 1);
                 INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title, body,
                     created_at, created_date_key, created_time_key)
                 VALUES ('octo/alpha#1', 1, 'octo/alpha', 'user:alice', 'MERGED',
                         'Fix connection pool exhaustion', 'Pool ran dry under load',
                         '2026-01-10T18:00:00Z', '2026-01-10', '10:00'),
                        ('octo/beta#2', 2, 'octo/beta', 'user:alice', 'OPEN',
                         'Add retry logic', 'Retries with backoff',
                         '2026-01-10T19:00:00Z', '2026-01-10', '10:00');
                 INSERT INTO issues (id, repo_key, number, title, body, state, created_at, updated_at)
                 VALUES ('ISS1', 'octo/alpha', 5, 'Connection timeout on startup', 'Cold pool issue',
                         'open', '2026-01-10T18:00:00Z', '2026-01-10T18:00:00Z');
                 INSERT INTO fact_review_comments (comment_key, pr_key, author_key, body,
                     created_at, created_date_key, created_time_key)
                 VALUES ('RC1', 'octo/alpha#1', 'user:alice', 'The pool size should be configurable',
                         '2026-01-10T18:30:00Z', '2026-01-10', '10:00');",
            )
            .unwrap();
    }

    #[test]
    fn searches_across_corpora_with_snippets() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        seed(&warehouse);
        let hits = search(
            warehouse.connection(),
            "connection",
            &SearchOptions::default(),
        )
        .unwrap();
        assert_eq!(hits.len(), 2, "PR title + issue title");
        assert!(hits.iter().any(|hit| hit.kind == "pull_request"));
        assert!(hits.iter().any(|hit| hit.kind == "issue"));
        assert!(hits.iter().all(|hit| hit.snippet.contains('[')));
    }

    #[test]
    fn substring_search_hits_partial_words() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        seed(&warehouse);
        // 'onnection' is a partial word — trigram proof.
        let hits = search(
            warehouse.connection(),
            "onnection",
            &SearchOptions::default(),
        )
        .unwrap();
        assert!(!hits.is_empty());
    }

    #[test]
    fn scope_and_repository_filters() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        seed(&warehouse);
        let conn = warehouse.connection();

        let pr_only = search(
            conn,
            "pool",
            &SearchOptions {
                scope: SearchScope::PullRequests,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(pr_only.iter().all(|hit| hit.kind == "pull_request"));

        let comments = search(
            conn,
            "configurable",
            &SearchOptions {
                scope: SearchScope::Comments,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].kind, "review_comment");
        assert_eq!(comments[0].key, "octo/alpha#1");

        let beta_only = search(
            conn,
            "retry",
            &SearchOptions {
                repository: Some("octo/beta".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(beta_only.len(), 1);
        let alpha_scoped = search(
            conn,
            "retry",
            &SearchOptions {
                repository: Some("octo/alpha".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(alpha_scoped.is_empty());
    }
}
