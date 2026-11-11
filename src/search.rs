//! Fulltext search over PRs, issues, and comments (FTS5 trigram).

use rusqlite::Connection;
use serde::Serialize;

use crate::error::Result;
use crate::query::{entity_namespace, to_bare_login};

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
    /// The author's login, exactly as GitHub spells it — never namespaced.
    pub author: Option<String>,
    /// The namespace that login was stored under: `user` or `bot`.
    pub author_type: Option<String>,
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

/// The author of a hit, as a login plus its type.
///
/// The joined dimension row is authoritative; splitting the warehouse key is
/// the fallback, so a hit whose author dimension is missing still reports a
/// bare login rather than a raw key, and is never dropped from the results.
fn author_identity(
    author_key: Option<String>,
    login: Option<String>,
    entity_type: Option<String>,
) -> (Option<String>, Option<String>) {
    let author = login.or_else(|| author_key.as_deref().map(to_bare_login));
    let author_type = entity_type.or_else(|| author_key.as_deref().map(entity_namespace));
    (author, author_type)
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
    // repo_key is stored lowercased (see storage::repository::upsert_repository),
    // so the filter must lowercase its input once here rather than trust callers.
    let repository = options
        .repository
        .as_ref()
        .map(|value| value.to_lowercase());

    if matches!(options.scope, SearchScope::All | SearchScope::PullRequests) {
        let sql = "SELECT f.pr_key, p.repo_key, p.title, p.author_key, p.state,
                          snippet(pull_requests_fts, -1, '[', ']', '…', 12), rank,
                          ae.login, ae.entity_type
                   FROM pull_requests_fts f
                   JOIN fact_pull_requests p ON p.pr_key = f.pr_key
                   LEFT JOIN dim_entities ae ON ae.entity_key = p.author_key
                   WHERE pull_requests_fts MATCH ?1
                     AND (?2 IS NULL OR p.repo_key = ?2)
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params![quoted, repository, limit], |row| {
            let (author, author_type) = author_identity(row.get(3)?, row.get(7)?, row.get(8)?);
            Ok(SearchHit {
                key: row.get(0)?,
                kind: "pull_request".into(),
                repository: row.get(1)?,
                title: row.get(2)?,
                author,
                author_type,
                state: row.get(4)?,
                snippet: row.get(5)?,
                rank: row.get(6)?,
            })
        })?;
        for row in rows {
            hits.push(row?);
        }
    }

    if matches!(options.scope, SearchScope::All | SearchScope::Issues) {
        let sql = "SELECT i.repo_key || '#' || i.number, i.repo_key, i.title, i.author_key,
                          i.state, snippet(issues_fts, -1, '[', ']', '…', 12), rank,
                          ae.login, ae.entity_type
                   FROM issues_fts f
                   JOIN issues i ON i.id = f.id
                   LEFT JOIN dim_entities ae ON ae.entity_key = i.author_key
                   WHERE issues_fts MATCH ?1
                     AND (?2 IS NULL OR i.repo_key = ?2)
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params![quoted, repository, limit], |row| {
            let (author, author_type) = author_identity(row.get(3)?, row.get(7)?, row.get(8)?);
            Ok(SearchHit {
                key: row.get(0)?,
                kind: "issue".into(),
                repository: row.get(1)?,
                title: row.get(2)?,
                author,
                author_type,
                state: row.get(4)?,
                snippet: row.get(5)?,
                rank: row.get(6)?,
            })
        })?;
        for row in rows {
            hits.push(row?);
        }
    }

    if matches!(options.scope, SearchScope::All | SearchScope::Comments) {
        let sql = "SELECT c.comment_key, c.pr_key, p.repo_key, p.title, c.author_key,
                          snippet(review_comments_fts, -1, '[', ']', '…', 12), rank,
                          ae.login, ae.entity_type
                   FROM review_comments_fts f
                   JOIN fact_review_comments c ON c.comment_key = f.comment_key
                   JOIN fact_pull_requests p ON p.pr_key = c.pr_key
                   LEFT JOIN dim_entities ae ON ae.entity_key = c.author_key
                   WHERE review_comments_fts MATCH ?1
                     AND (?2 IS NULL OR p.repo_key = ?2)
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params![quoted, repository, limit], |row| {
            let (author, author_type) = author_identity(row.get(4)?, row.get(7)?, row.get(8)?);
            Ok(SearchHit {
                key: row.get::<_, String>(1)?, // anchor to the PR
                kind: "review_comment".into(),
                repository: row.get(2)?,
                title: row.get(3)?,
                author,
                author_type,
                state: None,
                snippet: row.get(5)?,
                rank: row.get(6)?,
            })
        })?;
        for row in rows {
            hits.push(row?);
        }

        // parent_key means two different things depending on parent_type: for a
        // pull-request comment it's the pr_key ("owner/repo#n"); for an issue
        // comment it's the issue's node id. A repo filter has to join to whichever
        // row parent_type names and compare that row's own repo_key, rather than
        // pattern-match parent_key itself (which both misses issue comments and
        // false-matches sibling repos like "octo/alpha-ng" when filtering for
        // "octo/alpha").
        let sql = "SELECT c.comment_key, c.parent_key, c.author_key,
                          snippet(issue_comments_fts, -1, '[', ']', '…', 12), rank,
                          ae.login, ae.entity_type
                   FROM issue_comments_fts f
                   JOIN fact_issue_comments c ON c.comment_key = f.comment_key
                   LEFT JOIN dim_entities ae ON ae.entity_key = c.author_key
                   LEFT JOIN fact_pull_requests pr
                     ON c.parent_type = 'pull_request' AND pr.pr_key = c.parent_key
                   LEFT JOIN issues iss
                     ON c.parent_type = 'issue' AND iss.id = c.parent_key
                   WHERE issue_comments_fts MATCH ?1
                     AND (?2 IS NULL OR pr.repo_key = ?2 OR iss.repo_key = ?2)
                   ORDER BY rank LIMIT ?3";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params![quoted, repository, limit], |row| {
            let (author, author_type) = author_identity(row.get(2)?, row.get(5)?, row.get(6)?);
            Ok(SearchHit {
                key: row.get::<_, String>(1)?, // parent PR key or issue id
                kind: "issue_comment".into(),
                repository: None,
                title: None,
                author,
                author_type,
                state: None,
                snippet: row.get(3)?,
                rank: row.get(4)?,
            })
        })?;
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

    /// A search hit names its author the way GitHub does. The warehouse's
    /// namespaced key is an internal join target, so the type it encodes is
    /// carried in its own field instead.
    #[test]
    fn hits_report_a_bare_author_and_its_type() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        seed(&warehouse);
        warehouse
            .connection()
            .execute_batch(
                "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
                 VALUES ('bot:builder', 'bot', 'builder', 0, 1, 'builder');
                 INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
                     title, body, created_at, created_date_key, created_time_key)
                 VALUES ('octo/beta#3', 3, 'octo/beta', 'bot:builder', 'OPEN',
                         'Bump the connection library', 'Automated dependency bump',
                         '2026-01-10T20:00:00Z', '2026-01-10', '10:00');",
            )
            .unwrap();

        let hits = search(
            warehouse.connection(),
            "connection",
            &SearchOptions::default(),
        )
        .unwrap();
        assert!(!hits.is_empty());
        for hit in &hits {
            if let Some(author) = hit.author.as_deref() {
                assert!(!author.contains(':'), "author '{author}' is bare");
            }
        }
        let bot_hit = hits
            .iter()
            .find(|hit| hit.author.as_deref() == Some("builder"))
            .expect("the bot-authored PR is found by its bare login");
        assert_eq!(bot_hit.author_type.as_deref(), Some("bot"));

        let human_hit = hits
            .iter()
            .find(|hit| hit.author.as_deref() == Some("alice"))
            .expect("the person-authored PR is found by its bare login");
        assert_eq!(human_hit.author_type.as_deref(), Some("user"));
    }
    /// Three repos chosen so a prefix/LIKE match would blur them:
    /// `octo/alpha` is a prefix of `octo/alpha-ng`, and `octo/al_ha` has a
    /// literal `_` that is also the SQL LIKE single-char wildcard. Each repo
    /// gets a PR, an issue, a PR comment and an issue comment containing the
    /// same search word ("widget"). The issue comment on `octo/al_ha` is
    /// stored with its parent as a node id (`"ISSNODE3"`), matching how a
    /// real issue-type `fact_issue_comments` row is written.
    fn seed_repo_filter_fixture(warehouse: &GithubDW) {
        warehouse
            .connection()
            .execute_batch(
                "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
                 VALUES ('user:alice', 'user', 'alice', 1, 0, 'alice');
                 INSERT INTO dim_repositories (repo_key, owner, name)
                 VALUES ('octo/alpha', 'octo', 'alpha'),
                        ('octo/alpha-ng', 'octo', 'alpha-ng'),
                        ('octo/al_ha', 'octo', 'al_ha');
                 INSERT INTO dim_date (date_key, year, quarter, month, day_of_month, day_of_week,
                     is_weekend, week_of_year, week_key, month_key, quarter_key, year_key, half_key)
                 VALUES ('2026-01-10', 2026, 1, 1, 10, 6, 1, 2, '2026-W02', '2026-01', '2026-Q1', '2026', '2026-H1');
                 INSERT INTO dim_time (time_key, hour, hour_12, am_pm, time_bucket, is_core_hours)
                 VALUES ('10:00', 10, 10, 'AM', 'Morning', 1);

                 INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state, title, body,
                     created_at, created_date_key, created_time_key)
                 VALUES ('octo/alpha#1', 1, 'octo/alpha', 'user:alice', 'OPEN', 'widget pr alpha', NULL,
                         '2026-01-10T18:00:00Z', '2026-01-10', '10:00'),
                        ('octo/alpha-ng#1', 1, 'octo/alpha-ng', 'user:alice', 'OPEN', 'widget pr alpha-ng', NULL,
                         '2026-01-10T18:00:00Z', '2026-01-10', '10:00'),
                        ('octo/al_ha#1', 1, 'octo/al_ha', 'user:alice', 'OPEN', 'widget pr al_ha', NULL,
                         '2026-01-10T18:00:00Z', '2026-01-10', '10:00');

                 INSERT INTO issues (id, repo_key, number, title, body, state, created_at, updated_at)
                 VALUES ('ISSNODE1', 'octo/alpha', 2, 'widget issue alpha', NULL, 'open',
                         '2026-01-10T18:00:00Z', '2026-01-10T18:00:00Z'),
                        ('ISSNODE2', 'octo/alpha-ng', 2, 'widget issue alpha-ng', NULL, 'open',
                         '2026-01-10T18:00:00Z', '2026-01-10T18:00:00Z'),
                        ('ISSNODE3', 'octo/al_ha', 2, 'widget issue al_ha', NULL, 'open',
                         '2026-01-10T18:00:00Z', '2026-01-10T18:00:00Z');

                 INSERT INTO fact_review_comments (comment_key, pr_key, author_key, body,
                     created_at, created_date_key, created_time_key)
                 VALUES ('RC-alpha', 'octo/alpha#1', 'user:alice', 'widget review comment alpha',
                         '2026-01-10T18:30:00Z', '2026-01-10', '10:00'),
                        ('RC-alpha-ng', 'octo/alpha-ng#1', 'user:alice', 'widget review comment alpha-ng',
                         '2026-01-10T18:30:00Z', '2026-01-10', '10:00'),
                        ('RC-al_ha', 'octo/al_ha#1', 'user:alice', 'widget review comment al_ha',
                         '2026-01-10T18:30:00Z', '2026-01-10', '10:00');

                 INSERT INTO fact_issue_comments (comment_key, parent_type, parent_key, author_key, body,
                     created_at, created_date_key, created_time_key)
                 VALUES ('IC-alpha', 'pull_request', 'octo/alpha#1', 'user:alice', 'widget issue comment alpha',
                         '2026-01-10T18:30:00Z', '2026-01-10', '10:00'),
                        ('IC-alpha-ng', 'pull_request', 'octo/alpha-ng#1', 'user:alice', 'widget issue comment alpha-ng',
                         '2026-01-10T18:30:00Z', '2026-01-10', '10:00'),
                        -- parent stored by node id, not 'owner/repo#n' -- the issue-type shape.
                        ('IC-al_ha', 'issue', 'ISSNODE3', 'user:alice', 'widget issue comment al_ha',
                         '2026-01-10T18:30:00Z', '2026-01-10', '10:00');",
            )
            .unwrap();
    }

    /// Hand-counted expected hit keys per repo, across all four kinds, for the
    /// "widget" search term in `seed_repo_filter_fixture`.
    fn expected_keys_for(repo: &str) -> Vec<&'static str> {
        match repo {
            "octo/alpha" => vec![
                "octo/alpha#1",
                "octo/alpha#2",
                "octo/alpha#1",
                "octo/alpha#1",
            ],
            "octo/alpha-ng" => vec![
                "octo/alpha-ng#1",
                "octo/alpha-ng#2",
                "octo/alpha-ng#1",
                "octo/alpha-ng#1",
            ],
            "octo/al_ha" => vec!["octo/al_ha#1", "octo/al_ha#2", "octo/al_ha#1", "ISSNODE3"],
            other => panic!("no expectations recorded for {other}"),
        }
    }

    /// `repository: Some("octo/alpha")` must return exactly octo/alpha's four
    /// rows (PR, issue, review comment, issue comment) and nothing from the
    /// sibling repos `octo/alpha-ng` or `octo/al_ha`, even though the old
    /// `LIKE ?2 || '%'` filter on `issue_comments.parent_key` would have let
    /// `octo/alpha-ng`'s PR-shaped parent key slip through a prefix match.
    #[test]
    fn repository_filter_matches_the_exact_repo_only() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        seed_repo_filter_fixture(&warehouse);
        let conn = warehouse.connection();

        for repo in ["octo/alpha", "octo/alpha-ng", "octo/al_ha"] {
            let hits = search(
                conn,
                "widget",
                &SearchOptions {
                    repository: Some(repo.into()),
                    limit: 20,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut keys: Vec<&str> = hits.iter().map(|hit| hit.key.as_str()).collect();
            keys.sort_unstable();
            let mut expected = expected_keys_for(repo);
            expected.sort_unstable();
            assert_eq!(
                keys, expected,
                "repository filter {repo:?} must return exactly its own rows"
            );
        }
    }

    /// A repository filter is case-insensitive the same way GitHub treats
    /// `owner/repo`: `repo_key` is stored lowercased at write time, so the
    /// filter has to lowercase its own input rather than rely on callers.
    #[test]
    fn repository_filter_is_case_insensitive() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        seed_repo_filter_fixture(&warehouse);
        let conn = warehouse.connection();

        let lower = search(
            conn,
            "widget",
            &SearchOptions {
                repository: Some("octo/alpha".into()),
                limit: 20,
                ..Default::default()
            },
        )
        .unwrap();
        let upper = search(
            conn,
            "widget",
            &SearchOptions {
                repository: Some("OCTO/Alpha".into()),
                limit: 20,
                ..Default::default()
            },
        )
        .unwrap();

        let mut lower_keys: Vec<&str> = lower.iter().map(|hit| hit.key.as_str()).collect();
        let mut upper_keys: Vec<&str> = upper.iter().map(|hit| hit.key.as_str()).collect();
        lower_keys.sort_unstable();
        upper_keys.sort_unstable();
        assert_eq!(lower_keys, upper_keys);
        let mut expected = expected_keys_for("octo/alpha");
        expected.sort_unstable();
        assert_eq!(lower_keys, expected);
    }

    /// An issue comment whose parent is stored by node id (`parent_type =
    /// "issue"`) is still found by a repository filter -- the join has to
    /// follow `parent_type` to the right table, not pattern-match
    /// `parent_key` itself.
    #[test]
    fn repository_filter_finds_issue_comment_stored_by_node_id() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        seed_repo_filter_fixture(&warehouse);
        let conn = warehouse.connection();

        let hits = search(
            conn,
            "widget",
            &SearchOptions {
                scope: SearchScope::Comments,
                repository: Some("octo/al_ha".into()),
                limit: 20,
            },
        )
        .unwrap();

        assert!(
            hits.iter()
                .any(|hit| hit.kind == "issue_comment" && hit.key == "ISSNODE3"),
            "the node-id-parented issue comment on octo/al_ha must be found"
        );
        assert!(
            hits.iter().all(|hit| hit.key != "ISSNODE2"),
            "octo/alpha-ng's issue comment must not leak into octo/al_ha's results"
        );
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
