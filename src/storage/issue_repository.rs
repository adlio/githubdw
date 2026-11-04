//! Upserts for issues, labels, milestones, and assignee bridges.

use rusqlite::{Connection, params};

use crate::error::Result;
use crate::fetch::issues::{IssueData, LabelData, MilestoneData};

pub fn upsert_label(conn: &Connection, repo_key: &str, label: &LabelData) -> Result<()> {
    conn.execute(
        "INSERT INTO labels (id, repo_key, name, color, description)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (id) DO UPDATE SET
             name = excluded.name,
             color = excluded.color,
             description = excluded.description",
        params![
            label.id,
            repo_key,
            label.name,
            label.color,
            label.description
        ],
    )?;
    Ok(())
}

pub fn upsert_milestone(
    conn: &Connection,
    repo_key: &str,
    milestone: &MilestoneData,
) -> Result<()> {
    conn.execute(
        "INSERT INTO milestones (id, repo_key, number, title, description, state, due_on, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT (id) DO UPDATE SET
             title = excluded.title,
             description = excluded.description,
             state = excluded.state,
             due_on = excluded.due_on",
        params![
            milestone.id,
            repo_key,
            milestone.number,
            milestone.title,
            milestone.description,
            milestone.state.to_lowercase(),
            milestone.due_on,
            milestone.created_at,
        ],
    )?;
    Ok(())
}

/// Upsert the wide issue row. Label/assignee bridges are replaced separately.
pub fn upsert_issue(
    conn: &Connection,
    repo_key: &str,
    issue: &IssueData,
    author_key: Option<&str>,
    created_date_key: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO issues (
            id, repo_key, number, title, body, state, state_reason, author_key,
            milestone_id, comment_count, created_at, updated_at, closed_at, created_date_key
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
        ON CONFLICT (id) DO UPDATE SET
            title = excluded.title,
            body = excluded.body,
            state = excluded.state,
            state_reason = excluded.state_reason,
            author_key = excluded.author_key,
            milestone_id = excluded.milestone_id,
            comment_count = excluded.comment_count,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            created_date_key = excluded.created_date_key",
        params![
            issue.id,
            repo_key,
            issue.number,
            issue.title,
            issue.body,
            issue.state,
            issue.state_reason,
            author_key,
            issue.milestone.as_ref().map(|milestone| &milestone.id),
            issue.comments.len() as i64,
            issue.created_at,
            issue.updated_at,
            issue.closed_at,
            created_date_key,
        ],
    )?;
    Ok(())
}

/// App-mode issue upsert: a NULL or empty incoming title or body never
/// replaces a stored one.
pub fn upsert_issue_preserving(
    conn: &Connection,
    repo_key: &str,
    issue: &IssueData,
    author_key: Option<&str>,
    created_date_key: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO issues (
            id, repo_key, number, title, body, state, state_reason, author_key,
            milestone_id, comment_count, created_at, updated_at, closed_at, created_date_key
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
        ON CONFLICT (id) DO UPDATE SET
            title = COALESCE(NULLIF(excluded.title, ''), issues.title),
            body = COALESCE(NULLIF(excluded.body, ''), issues.body),
            state = excluded.state,
            state_reason = excluded.state_reason,
            author_key = COALESCE(excluded.author_key, issues.author_key),
            milestone_id = excluded.milestone_id,
            comment_count = excluded.comment_count,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            created_date_key = COALESCE(excluded.created_date_key, issues.created_date_key)",
        params![
            issue.id,
            repo_key,
            issue.number,
            issue.title,
            issue.body,
            issue.state,
            issue.state_reason,
            author_key,
            issue.milestone.as_ref().map(|milestone| &milestone.id),
            issue.comments.len() as i64,
            issue.created_at,
            issue.updated_at,
            issue.closed_at,
            created_date_key,
        ],
    )?;
    Ok(())
}

/// App-mode label bridge write: adds links, never deletes one.
pub fn add_issue_labels(conn: &Connection, issue_id: &str, label_ids: &[String]) -> Result<()> {
    for label_id in label_ids {
        conn.execute(
            "INSERT OR IGNORE INTO issue_labels (issue_id, label_id) VALUES (?1, ?2)",
            params![issue_id, label_id],
        )?;
    }
    Ok(())
}

/// App-mode assignee bridge write: adds links, never deletes one.
pub fn add_issue_assignees(
    conn: &Connection,
    issue_id: &str,
    entity_keys: &[String],
) -> Result<()> {
    for entity_key in entity_keys {
        conn.execute(
            "INSERT OR IGNORE INTO issue_assignees (issue_id, entity_key) VALUES (?1, ?2)",
            params![issue_id, entity_key],
        )?;
    }
    Ok(())
}

/// Replace the label set for an issue.
pub fn replace_issue_labels(conn: &Connection, issue_id: &str, label_ids: &[String]) -> Result<()> {
    conn.execute("DELETE FROM issue_labels WHERE issue_id = ?1", [issue_id])?;
    for label_id in label_ids {
        conn.execute(
            "INSERT OR IGNORE INTO issue_labels (issue_id, label_id) VALUES (?1, ?2)",
            params![issue_id, label_id],
        )?;
    }
    Ok(())
}

/// Replace the assignee set for an issue.
pub fn replace_issue_assignees(
    conn: &Connection,
    issue_id: &str,
    entity_keys: &[String],
) -> Result<()> {
    conn.execute(
        "DELETE FROM issue_assignees WHERE issue_id = ?1",
        [issue_id],
    )?;
    for entity_key in entity_keys {
        conn.execute(
            "INSERT OR IGNORE INTO issue_assignees (issue_id, entity_key) VALUES (?1, ?2)",
            params![issue_id, entity_key],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;
    use crate::fetch::issues::IssueData;
    use crate::storage::repository;

    fn sample_issue() -> IssueData {
        IssueData {
            id: "ISS1".into(),
            number: 42,
            title: "Pagination bug".into(),
            body: Some("List cuts off".into()),
            state: "open".into(),
            state_reason: None,
            created_at: "2026-02-01T12:00:00Z".into(),
            updated_at: "2026-02-01T12:00:00Z".into(),
            closed_at: None,
            author: None,
            milestone: None,
            labels: vec![],
            assignees: vec![],
            comments: vec![],
            pending_comments: None,
            pending_labels: None,
            pending_assignees: None,
        }
    }

    #[test]
    fn issue_upsert_and_label_replacement() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let repo_key =
            repository::upsert_repository(conn, "octocat/hello", None, false, false, None, None)
                .unwrap();

        let issue = sample_issue();
        upsert_issue(conn, &repo_key, &issue, None, None).unwrap();

        let label = LabelData {
            id: "LAB1".into(),
            name: "bug".into(),
            color: "d73a4a".into(),
            description: String::new(),
        };
        upsert_label(conn, &repo_key, &label).unwrap();
        replace_issue_labels(conn, &issue.id, &["LAB1".into()]).unwrap();
        replace_issue_labels(conn, &issue.id, &["LAB1".into()]).unwrap();

        let label_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM issue_labels", [], |row| row.get(0))
            .unwrap();
        assert_eq!(label_count, 1);

        // Re-upsert with new state; row count stays 1, FTS trigger keeps sync.
        let mut updated = sample_issue();
        updated.state = "closed".into();
        upsert_issue(conn, &repo_key, &updated, None, None).unwrap();
        let (count, state): (i64, String) = conn
            .query_row("SELECT COUNT(*), MAX(state) FROM issues", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(state, "closed");

        let fts_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM issues_fts WHERE issues_fts MATCH 'agination'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(fts_count, 1, "trigram substring match via trigger");
    }

    fn entity(conn: &Connection, login: &str) -> String {
        repository::ensure_entity(
            conn,
            &crate::fetch::pull_requests::ActorReference {
                login: login.into(),
                type_name: "User".into(),
            },
            "[bot]",
        )
        .unwrap()
    }

    fn bridge(conn: &Connection, sql: &str) -> Vec<String> {
        let mut statement = conn.prepare(sql).unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn preserving_upsert_keeps_stored_payload_and_updates_provider_facts() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let repo_key =
            repository::upsert_repository(conn, "acme/app", None, false, false, None, None)
                .unwrap();
        let alice = entity(conn, "alice");
        let milestone = MilestoneData {
            id: "MS1".into(),
            number: 1,
            title: "v1".into(),
            description: Some("first".into()),
            state: "OPEN".into(),
            due_on: None,
            created_at: Some("2026-01-01T00:00:00Z".into()),
        };
        upsert_milestone(conn, &repo_key, &milestone).unwrap();
        let mut closed = milestone.clone();
        closed.state = "CLOSED".into();
        closed.title = "v1.0".into();
        upsert_milestone(conn, &repo_key, &closed).unwrap();
        let (title, state): (String, String) = conn
            .query_row(
                "SELECT title, state FROM milestones WHERE id = 'MS1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((title.as_str(), state.as_str()), ("v1.0", "closed"));

        let mut issue = sample_issue();
        issue.milestone = Some(milestone);
        let keys = crate::storage::time_dimension::ensure_keys_for_timestamp(
            conn,
            &issue.created_at,
            chrono_tz::UTC,
            (9, 17),
        )
        .unwrap();
        upsert_issue_preserving(conn, &repo_key, &issue, Some(&alice), Some(&keys.date_key))
            .unwrap();

        // A sparse later read: empty title and body, no author or date key.
        let mut sparse = sample_issue();
        sparse.title = String::new();
        sparse.body = None;
        sparse.state = "closed".into();
        sparse.state_reason = Some("completed".into());
        sparse.closed_at = Some("2026-02-03T00:00:00Z".into());
        sparse.updated_at = "2026-02-03T00:00:00Z".into();
        upsert_issue_preserving(conn, &repo_key, &sparse, None, None).unwrap();

        let row: (
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            String,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT title, body, state, state_reason, author_key, milestone_id,
                        updated_at, created_date_key
                 FROM issues WHERE id = 'ISS1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row.0, "Pagination bug", "empty title never replaces");
        assert_eq!(row.1, "List cuts off", "missing body never replaces");
        assert_eq!(row.2, "closed");
        assert_eq!(row.3, "completed");
        assert_eq!(row.4, alice, "missing author never replaces");
        assert_eq!(row.5, None, "milestone follows the provider");
        assert_eq!(row.6, "2026-02-03T00:00:00Z");
        assert_eq!(
            row.7,
            Some(keys.date_key.clone()),
            "missing date key never replaces"
        );

        // A non-empty incoming value still replaces.
        let mut renamed = sample_issue();
        renamed.title = "Pagination bug (fixed)".into();
        upsert_issue_preserving(conn, &repo_key, &renamed, None, None).unwrap();
        let title: String = conn
            .query_row("SELECT title FROM issues WHERE id = 'ISS1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(title, "Pagination bug (fixed)");
    }

    #[test]
    fn add_bridges_never_delete_while_replace_bridges_do() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let repo_key =
            repository::upsert_repository(conn, "acme/app", None, false, false, None, None)
                .unwrap();
        upsert_issue(conn, &repo_key, &sample_issue(), None, None).unwrap();
        for (id, name) in [("LAB1", "bug"), ("LAB2", "docs")] {
            upsert_label(
                conn,
                &repo_key,
                &LabelData {
                    id: id.into(),
                    name: name.into(),
                    color: "ffffff".into(),
                    description: String::new(),
                },
            )
            .unwrap();
        }
        let alice = entity(conn, "alice");
        let bob = entity(conn, "bob");
        let labels = "SELECT label_id FROM issue_labels ORDER BY label_id";
        let assignees = "SELECT entity_key FROM issue_assignees ORDER BY entity_key";

        add_issue_labels(conn, "ISS1", &["LAB1".into()]).unwrap();
        add_issue_labels(conn, "ISS1", &["LAB2".into(), "LAB1".into()]).unwrap();
        add_issue_labels(conn, "ISS1", &[]).unwrap();
        assert_eq!(bridge(conn, labels), ["LAB1", "LAB2"]);

        add_issue_assignees(conn, "ISS1", std::slice::from_ref(&alice)).unwrap();
        add_issue_assignees(conn, "ISS1", std::slice::from_ref(&bob)).unwrap();
        add_issue_assignees(conn, "ISS1", &[]).unwrap();
        assert_eq!(bridge(conn, assignees), [alice.clone(), bob.clone()]);

        replace_issue_labels(conn, "ISS1", &["LAB2".into()]).unwrap();
        assert_eq!(bridge(conn, labels), ["LAB2"]);
        replace_issue_assignees(conn, "ISS1", std::slice::from_ref(&bob)).unwrap();
        assert_eq!(bridge(conn, assignees), [bob]);
        replace_issue_assignees(conn, "ISS1", &[]).unwrap();
        assert!(bridge(conn, assignees).is_empty());

        // Write failures surface instead of being dropped.
        conn.execute_batch("PRAGMA query_only = ON").unwrap();
        assert!(matches!(
            add_issue_labels(conn, "ISS1", &["LAB1".into()]),
            Err(crate::Error::Database(_))
        ));
        assert!(matches!(
            add_issue_assignees(conn, "ISS1", std::slice::from_ref(&alice)),
            Err(crate::Error::Database(_))
        ));
        assert!(matches!(
            upsert_issue_preserving(conn, &repo_key, &sample_issue(), None, None),
            Err(crate::Error::Database(_))
        ));
    }
}
