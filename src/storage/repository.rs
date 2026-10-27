//! Core upsert/read repository for facts and dimensions.

use rusqlite::{Connection, OptionalExtension, params};

use crate::error::Result;
use crate::fetch::pull_requests::ActorReference;

/// Resolve an actor to a `dim_entities` row and return its entity key.
/// Bot detection: GraphQL `__typename == "Bot"` or login ending in the
/// configured bot suffix.
pub fn ensure_entity(
    conn: &Connection,
    actor: &ActorReference,
    bot_suffix: &str,
) -> Result<String> {
    let login = actor.login.to_lowercase();
    let is_bot = actor.type_name == "Bot" || login.ends_with(&bot_suffix.to_lowercase());
    let entity_type = if is_bot { "bot" } else { "user" };
    let entity_key = format!("{entity_type}:{login}");
    conn.execute(
        "INSERT INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (entity_key) DO UPDATE SET
             entity_type = excluded.entity_type,
             is_human = excluded.is_human,
             is_bot = excluded.is_bot",
        params![
            entity_key,
            entity_type,
            login,
            (!is_bot) as i64,
            is_bot as i64,
            actor.login,
        ],
    )?;
    Ok(entity_key)
}

/// The fallback entity for deleted/ghost accounts.
pub fn ensure_ghost_entity(conn: &Connection) -> Result<String> {
    conn.execute(
        "INSERT OR IGNORE INTO dim_entities (entity_key, entity_type, login, is_human, is_bot, name)
         VALUES ('user:ghost', 'user', 'ghost', 1, 0, 'ghost')",
        [],
    )?;
    Ok("user:ghost".to_string())
}

/// Upsert a repository dimension row; returns the repo key ("owner/name", lowercase).
#[allow(clippy::too_many_arguments)]
pub fn upsert_repository(
    conn: &Connection,
    name_with_owner: &str,
    primary_language: Option<&str>,
    is_fork: bool,
    is_private: bool,
    default_branch: Option<&str>,
    created_at: Option<&str>,
) -> Result<String> {
    let repo_key = name_with_owner.to_lowercase();
    let (owner, name) = repo_key.split_once('/').unwrap_or((repo_key.as_str(), ""));
    conn.execute(
        "INSERT INTO dim_repositories
             (repo_key, owner, name, primary_language, is_fork, is_private, default_branch, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT (repo_key) DO UPDATE SET
             primary_language = excluded.primary_language,
             is_fork = excluded.is_fork,
             is_private = excluded.is_private,
             default_branch = excluded.default_branch,
             created_at = COALESCE(excluded.created_at, dim_repositories.created_at)",
        params![
            repo_key,
            owner,
            name,
            primary_language,
            is_fork as i64,
            is_private as i64,
            default_branch,
            created_at,
        ],
    )?;
    Ok(repo_key)
}

/// Column values for one `fact_pull_requests` upsert.
pub struct PullRequestRow<'a> {
    pub pr_key: &'a str,
    pub number: i64,
    pub repo_key: &'a str,
    pub author_key: &'a str,
    pub state: &'a str,
    pub is_draft: bool,
    pub title: Option<&'a str>,
    pub body: Option<&'a str>,
    pub base_ref: Option<&'a str>,
    pub head_ref: Option<&'a str>,
    pub created_at: &'a str,
    pub updated_at: Option<&'a str>,
    pub merged_at: Option<&'a str>,
    pub closed_at: Option<&'a str>,
    pub merged_by_key: Option<&'a str>,
    pub created_date_key: &'a str,
    pub created_time_key: &'a str,
    pub updated_date_key: Option<&'a str>,
    pub updated_time_key: Option<&'a str>,
    pub merged_date_key: Option<&'a str>,
    pub comment_count: i64,
    pub review_count: i64,
    pub changed_files: i64,
    pub additions: i64,
    pub deletions: i64,
}

pub fn upsert_pull_request(conn: &Connection, row: &PullRequestRow<'_>) -> Result<()> {
    conn.execute(
        "INSERT INTO fact_pull_requests (
            pr_key, number, repo_key, author_key, state, is_draft, title, body,
            base_ref, head_ref, created_at, updated_at, merged_at, closed_at,
            merged_by_key, created_date_key, created_time_key, updated_date_key,
            updated_time_key, merged_date_key, comment_count, review_count,
            changed_files, additions, deletions
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                  ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25)
        ON CONFLICT (pr_key) DO UPDATE SET
            state = excluded.state,
            is_draft = excluded.is_draft,
            title = excluded.title,
            body = excluded.body,
            base_ref = excluded.base_ref,
            head_ref = excluded.head_ref,
            updated_at = excluded.updated_at,
            merged_at = excluded.merged_at,
            closed_at = excluded.closed_at,
            merged_by_key = excluded.merged_by_key,
            updated_date_key = excluded.updated_date_key,
            updated_time_key = excluded.updated_time_key,
            merged_date_key = excluded.merged_date_key,
            comment_count = excluded.comment_count,
            review_count = excluded.review_count,
            changed_files = excluded.changed_files,
            additions = excluded.additions,
            deletions = excluded.deletions",
        params![
            row.pr_key,
            row.number,
            row.repo_key,
            row.author_key,
            row.state,
            row.is_draft as i64,
            row.title,
            row.body,
            row.base_ref,
            row.head_ref,
            row.created_at,
            row.updated_at,
            row.merged_at,
            row.closed_at,
            row.merged_by_key,
            row.created_date_key,
            row.created_time_key,
            row.updated_date_key,
            row.updated_time_key,
            row.merged_date_key,
            row.comment_count,
            row.review_count,
            row.changed_files,
            row.additions,
            row.deletions,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn upsert_review(
    conn: &Connection,
    review_key: &str,
    pr_key: &str,
    reviewer_key: &str,
    state: &str,
    body: Option<&str>,
    submitted_at: &str,
    submitted_date_key: &str,
    submitted_time_key: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO fact_reviews (
            review_key, pr_key, reviewer_key, state, body, submitted_at,
            submitted_date_key, submitted_time_key
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT (review_key) DO UPDATE SET
            state = excluded.state,
            body = excluded.body,
            submitted_at = excluded.submitted_at,
            submitted_date_key = excluded.submitted_date_key,
            submitted_time_key = excluded.submitted_time_key",
        params![
            review_key,
            pr_key,
            reviewer_key,
            state,
            body,
            submitted_at,
            submitted_date_key,
            submitted_time_key,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn upsert_review_comment(
    conn: &Connection,
    comment_key: &str,
    pr_key: &str,
    author_key: &str,
    in_reply_to: Option<&str>,
    path: Option<&str>,
    line: Option<i64>,
    body: Option<&str>,
    created_at: &str,
    created_date_key: &str,
    created_time_key: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO fact_review_comments (
            comment_key, pr_key, review_key, author_key, in_reply_to, path, line,
            body, created_at, created_date_key, created_time_key
        ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT (comment_key) DO UPDATE SET
            in_reply_to = excluded.in_reply_to,
            path = excluded.path,
            line = excluded.line,
            body = excluded.body
        WHERE fact_review_comments.in_reply_to IS NOT excluded.in_reply_to
           OR fact_review_comments.path IS NOT excluded.path
           OR fact_review_comments.line IS NOT excluded.line
           OR fact_review_comments.body IS NOT excluded.body",
        params![
            comment_key,
            pr_key,
            author_key,
            in_reply_to,
            path,
            line,
            body,
            created_at,
            created_date_key,
            created_time_key,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn upsert_issue_comment(
    conn: &Connection,
    comment_key: &str,
    parent_type: &str,
    parent_key: &str,
    author_key: &str,
    in_reply_to: Option<&str>,
    body: Option<&str>,
    created_at: &str,
    created_date_key: &str,
    created_time_key: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO fact_issue_comments (
            comment_key, parent_type, parent_key, author_key, in_reply_to, body,
            created_at, created_date_key, created_time_key
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT (comment_key) DO UPDATE SET
            body = excluded.body,
            in_reply_to = excluded.in_reply_to
        WHERE fact_issue_comments.body IS NOT excluded.body
           OR fact_issue_comments.in_reply_to IS NOT excluded.in_reply_to",
        params![
            comment_key,
            parent_type,
            parent_key,
            author_key,
            in_reply_to,
            body,
            created_at,
            created_date_key,
            created_time_key,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn upsert_check_run(
    conn: &Connection,
    check_run_key: &str,
    pr_key: &str,
    head_sha: &str,
    name: &str,
    status: &str,
    conclusion: Option<&str>,
    started_at: Option<&str>,
    completed_at: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO fact_check_runs (
            check_run_key, pr_key, head_sha, name, status, conclusion, started_at, completed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT (check_run_key) DO UPDATE SET
            status = excluded.status,
            conclusion = excluded.conclusion,
            started_at = excluded.started_at,
            completed_at = excluded.completed_at
        WHERE fact_check_runs.pr_key = excluded.pr_key
          AND fact_check_runs.head_sha = excluded.head_sha",
        params![
            check_run_key,
            pr_key,
            head_sha,
            name,
            status,
            conclusion,
            started_at,
            completed_at,
        ],
    )?;
    Ok(())
}

/// What [`record_check_run`] did with one provider run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckRunWrite {
    Inserted,
    /// Same PR and commit; status, conclusion, or times changed.
    Updated,
    Unchanged,
    /// The key is already stored under another pull request. A check run
    /// row carries exactly one `pr_key`, so the row is left as it is.
    OwnedByOtherPullRequest(String),
    /// The key is stored for this pull request against another commit.
    /// Never rewritten silently; reported for investigation.
    HeadMismatch(String),
}

/// Store one check run without ever moving an existing row to another pull
/// request or commit. Only status, conclusion, and times of a row already
/// owned by `(pr_key, head_sha)` are updated.
#[allow(clippy::too_many_arguments)]
pub fn record_check_run(
    conn: &Connection,
    check_run_key: &str,
    pr_key: &str,
    head_sha: &str,
    name: &str,
    status: &str,
    conclusion: Option<&str>,
    started_at: Option<&str>,
    completed_at: Option<&str>,
) -> Result<CheckRunWrite> {
    type Stored = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let existing: Option<Stored> = conn
        .query_row(
            "SELECT pr_key, head_sha, status, conclusion, started_at, completed_at
             FROM fact_check_runs WHERE check_run_key = ?1",
            [check_run_key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((stored_pr, stored_sha, old_status, old_conclusion, old_started, old_completed)) =
        existing
    else {
        conn.execute(
            "INSERT INTO fact_check_runs (
                check_run_key, pr_key, head_sha, name, status, conclusion, started_at, completed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                check_run_key,
                pr_key,
                head_sha,
                name,
                status,
                conclusion,
                started_at,
                completed_at
            ],
        )?;
        return Ok(CheckRunWrite::Inserted);
    };
    if stored_pr != pr_key {
        return Ok(CheckRunWrite::OwnedByOtherPullRequest(stored_pr));
    }
    if !stored_sha.eq_ignore_ascii_case(head_sha) {
        return Ok(CheckRunWrite::HeadMismatch(stored_sha));
    }
    if old_status == status
        && old_conclusion.as_deref() == conclusion
        && old_started.as_deref() == started_at
        && old_completed.as_deref() == completed_at
    {
        return Ok(CheckRunWrite::Unchanged);
    }
    conn.execute(
        "UPDATE fact_check_runs
         SET status = ?2, conclusion = ?3, started_at = ?4, completed_at = ?5
         WHERE check_run_key = ?1",
        params![check_run_key, status, conclusion, started_at, completed_at],
    )?;
    Ok(CheckRunWrite::Updated)
}

#[allow(clippy::too_many_arguments)]
pub fn upsert_file_diff(
    conn: &Connection,
    pr_key: &str,
    repo_key: &str,
    file_path: &str,
    previous_path: Option<&str>,
    change_type: &str,
    patch: Option<&str>,
    additions: i64,
    deletions: i64,
) -> Result<()> {
    let file_diff_key = format!("{pr_key}:{file_path}");
    conn.execute(
        "INSERT INTO fact_file_diffs (
            file_diff_key, pr_key, repo_key, file_path, previous_path, change_type,
            patch, additions, deletions
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT (file_diff_key) DO UPDATE SET
            previous_path = excluded.previous_path,
            change_type = excluded.change_type,
            patch = COALESCE(excluded.patch, fact_file_diffs.patch),
            additions = excluded.additions,
            deletions = excluded.deletions",
        params![
            file_diff_key,
            pr_key,
            repo_key,
            file_path,
            previous_path,
            change_type,
            patch,
            additions,
            deletions,
        ],
    )?;
    Ok(())
}

/// Set the patch text for an existing file diff (REST backfill path).
pub fn set_file_diff_patch(
    conn: &Connection,
    pr_key: &str,
    file_path: &str,
    previous_path: Option<&str>,
    patch: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE fact_file_diffs SET patch = ?1, previous_path = COALESCE(?2, previous_path)
         WHERE file_diff_key = ?3",
        params![patch, previous_path, format!("{pr_key}:{file_path}")],
    )?;
    Ok(())
}

/// Stored file paths of one pull request that are absent from `keep`.
pub fn file_diffs_absent_from(
    conn: &Connection,
    pr_key: &str,
    keep: &std::collections::HashSet<&str>,
) -> Result<Vec<String>> {
    let mut statement =
        conn.prepare("SELECT file_path FROM fact_file_diffs WHERE pr_key = ?1 ORDER BY file_path")?;
    let paths = statement
        .query_map([pr_key], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(paths
        .into_iter()
        .filter(|path| !keep.contains(path.as_str()))
        .collect())
}

/// Delete the named file rows of one pull request. Only call this with paths
/// from [`file_diffs_absent_from`] against a provably complete file set.
pub fn delete_file_diffs(conn: &Connection, pr_key: &str, paths: &[String]) -> Result<usize> {
    let mut statement =
        conn.prepare("DELETE FROM fact_file_diffs WHERE pr_key = ?1 AND file_path = ?2")?;
    let mut deleted = 0;
    for path in paths {
        deleted += statement.execute(params![pr_key, path])?;
    }
    Ok(deleted)
}

/// The configured bot login suffix (default `[bot]`).
pub fn bot_login_suffix(conn: &Connection) -> String {
    conn.query_row(
        "SELECT value FROM config WHERE key = 'bot_login_suffix'",
        [],
        |row| row.get(0),
    )
    .unwrap_or_else(|_| "[bot]".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;

    fn comment_warehouse() -> (GithubDW, String, String) {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        ensure_entity(
            conn,
            &ActorReference {
                login: "octocat".into(),
                type_name: "User".into(),
            },
            "[bot]",
        )
        .unwrap();
        upsert_repository(conn, "octocat/hello", None, false, false, None, None).unwrap();
        let keys = crate::storage::time_dimension::ensure_keys_for_timestamp(
            conn,
            "2026-01-05T18:00:00Z",
            chrono_tz::UTC,
            (9, 17),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
             created_at, created_date_key, created_time_key)
             VALUES ('octocat/hello#1', 1, 'octocat/hello', 'user:octocat', 'OPEN',
             '2026-01-05T18:00:00Z', ?1, ?2)",
            params![keys.date_key, keys.time_key],
        )
        .unwrap();
        (warehouse, keys.date_key, keys.time_key)
    }

    #[test]
    fn unchanged_review_comment_does_not_rebuild_search_index() {
        let (warehouse, date, time) = comment_warehouse();
        let conn = warehouse.connection();
        let put =
            |reply: Option<&str>, path: Option<&str>, line: Option<i64>, body: Option<&str>| {
                upsert_review_comment(
                    conn,
                    "comment1",
                    "octocat/hello#1",
                    "user:octocat",
                    reply,
                    path,
                    line,
                    body,
                    "2026-01-05T18:00:00Z",
                    &date,
                    &time,
                )
                .unwrap();
            };
        put(None, None, None, None);
        let before = conn.total_changes();
        put(None, None, None, None);
        assert_eq!(
            conn.total_changes(),
            before,
            "unchanged NULL fields must be a no-op"
        );
        for (reply, path, line, body) in [
            (None, None, None, Some("original searchable text")),
            (Some("parent"), None, None, Some("original searchable text")),
            (
                Some("parent"),
                Some("src/lib.rs"),
                None,
                Some("original searchable text"),
            ),
            (
                Some("parent"),
                Some("src/lib.rs"),
                Some(7),
                Some("original searchable text"),
            ),
            (None, None, None, Some("replacement searchable text")),
        ] {
            let before = conn.total_changes();
            put(reply, path, line, body);
            assert!(
                conn.total_changes() > before,
                "changed fields must still be persisted"
            );
            let before = conn.total_changes();
            put(reply, path, line, body);
            assert_eq!(
                conn.total_changes(),
                before,
                "repeat must not touch the index"
            );
        }
        let hits: i64 = conn.query_row(
            "SELECT count(*) FROM review_comments_fts WHERE review_comments_fts MATCH 'replacement'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(hits, 1);
        put(None, None, None, None);
        let hits: i64 = conn.query_row(
            "SELECT count(*) FROM review_comments_fts WHERE review_comments_fts MATCH 'replacement'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(
            hits, 0,
            "clearing the body must remove stale search results"
        );
    }

    #[test]
    fn unchanged_issue_comment_does_not_rebuild_search_index() {
        let (warehouse, date, time) = comment_warehouse();
        let conn = warehouse.connection();
        let put = |reply: Option<&str>, body: Option<&str>| {
            upsert_issue_comment(
                conn,
                "comment1",
                "pull_request",
                "octocat/hello#1",
                "user:octocat",
                reply,
                body,
                "2026-01-05T18:00:00Z",
                &date,
                &time,
            )
            .unwrap();
        };
        for (reply, body) in [
            (None, None),
            (None, Some("original searchable text")),
            (Some("parent"), Some("original searchable text")),
            (None, Some("replacement searchable text")),
            (None, None),
        ] {
            let before = conn.total_changes();
            put(reply, body);
            assert!(
                conn.total_changes() > before,
                "changed fields must still be persisted"
            );
            let before = conn.total_changes();
            put(reply, body);
            assert_eq!(
                conn.total_changes(),
                before,
                "repeat must not touch the index"
            );
        }
        let hits: i64 = conn.query_row(
            "SELECT count(*) FROM issue_comments_fts WHERE issue_comments_fts MATCH 'replacement'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(
            hits, 0,
            "clearing the body must remove stale search results"
        );
    }

    #[test]
    fn entity_resolution_detects_bots() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();

        let human = ActorReference {
            login: "Octocat".into(),
            type_name: "User".into(),
        };
        let key = ensure_entity(conn, &human, "[bot]").unwrap();
        assert_eq!(key, "user:octocat");

        let bot = ActorReference {
            login: "dependabot[bot]".into(),
            type_name: "Bot".into(),
        };
        let key = ensure_entity(conn, &bot, "[bot]").unwrap();
        assert_eq!(key, "bot:dependabot[bot]");
        let is_bot: i64 = conn
            .query_row(
                "SELECT is_bot FROM dim_entities WHERE entity_key = ?1",
                [&key],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(is_bot, 1);
    }

    #[test]
    fn repository_upsert_is_idempotent() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let key = upsert_repository(
            conn,
            "Octocat/Hello",
            Some("Rust"),
            false,
            false,
            Some("main"),
            None,
        )
        .unwrap();
        assert_eq!(key, "octocat/hello");
        upsert_repository(
            conn,
            "Octocat/Hello",
            Some("Go"),
            false,
            false,
            Some("main"),
            None,
        )
        .unwrap();
        let (count, language): (i64, String) = conn
            .query_row(
                "SELECT COUNT(*), MAX(primary_language) FROM dim_repositories",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(language, "Go");
    }
}
