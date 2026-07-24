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
}
