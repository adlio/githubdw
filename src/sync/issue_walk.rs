//! Repository issue walk.

use super::*;

impl Syncer<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn sync_issues(
        &mut self,
        owner: &str,
        name: &str,
        repository_name: &str,
        entity_key: &str,
        updated_cursor: Option<&str>,
        summary: &mut SyncSummary,
    ) -> Result<()> {
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);

        // Ensure the repo dimension exists even in --issues-only mode.
        let repo_key = repository::upsert_repository(
            self.connection,
            repository_name,
            None,
            false,
            false,
            None,
            None,
        )?;

        let mut page_cursor: Option<String> = None;
        let mut watermark = WalkWatermark::default();
        let mut item_index: i64 = 0;

        'pages: loop {
            let mut variables: Vec<(&str, &str)> = vec![("owner", owner), ("name", name)];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = self
                .client
                .graphql(&issues::repository_issues_query(), &variables)?;
            let page = issues::parse_issue_page(&data)?;
            summary.pages_fetched += 1;

            let page_is_empty = page.issues.is_empty();
            for issue in &page.issues {
                // Strict comparison, so an item updated in the same second as
                // the cursor is re-ingested rather than skipped forever. See
                // the equivalent stop in `sync_pull_requests`.
                if let Some(cursor) = updated_cursor
                    && issue.updated_at.as_str() < cursor
                {
                    break 'pages;
                }

                item_index += 1;
                let item_id = format!("{repo_key}#issue-{}", issue.number);
                match self.upsert_one_issue(&repo_key, issue, timezone, core_hours, &bot_suffix) {
                    Ok(()) => {
                        summary.issues_synced += 1;
                        watermark.landed(Some(issue.updated_at.as_str()));
                    }
                    Err(error) => {
                        watermark.failed(Some(issue.updated_at.as_str()));
                        summary.failed.push((item_id.clone(), error.to_string()));
                    }
                }
                sync_state::update_lock_progress(
                    self.connection,
                    entity_key,
                    item_index,
                    &item_id,
                    summary.pull_requests_synced + summary.issues_synced,
                    summary.skipped,
                    summary.failed.len() as i64,
                )?;
            }

            if !page.has_next_page {
                break;
            }
            if page_is_empty {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: issue page reports more results but holds none"
                )));
            }
            if page.end_cursor == page_cursor {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: issue page cursor did not advance"
                )));
            }
            page_cursor = page.end_cursor;
        }

        sync_state::advance_cursor(
            self.connection,
            "repo_issues",
            repository_name,
            watermark.committable().as_deref(),
        )?;
        Ok(())
    }

    pub(super) fn upsert_one_issue(
        &mut self,
        repo_key: &str,
        issue: &issues::IssueData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
    ) -> Result<()> {
        let mut issue = issue.clone();
        issues::hydrate_issue(self.client, &mut issue)?;
        let issue = &issue;
        let conn = self.connection;

        let author_key = match issue.author.as_ref() {
            Some(actor) => Some(repository::ensure_entity(conn, actor, bot_suffix)?),
            None => Some(repository::ensure_ghost_entity(conn)?),
        };
        let created_keys = time_dimension::ensure_keys_for_timestamp(
            conn,
            &issue.created_at,
            timezone,
            core_hours,
        )?;

        if let Some(milestone) = issue.milestone.as_ref() {
            issue_repository::upsert_milestone(conn, repo_key, milestone)?;
        }

        issue_repository::upsert_issue(
            conn,
            repo_key,
            issue,
            author_key.as_deref(),
            Some(&created_keys.date_key),
        )?;

        let mut label_ids = Vec::new();
        for label in &issue.labels {
            issue_repository::upsert_label(conn, repo_key, label)?;
            label_ids.push(label.id.clone());
        }
        issue_repository::replace_issue_labels(conn, &issue.id, &label_ids)?;

        let mut assignee_keys = Vec::new();
        for assignee in &issue.assignees {
            assignee_keys.push(repository::ensure_entity(conn, assignee, bot_suffix)?);
        }
        issue_repository::replace_issue_assignees(conn, &issue.id, &assignee_keys)?;

        for comment in &issue.comments {
            let comment_author = match comment.author.as_ref() {
                Some(actor) => repository::ensure_entity(conn, actor, bot_suffix)?,
                None => repository::ensure_ghost_entity(conn)?,
            };
            let comment_keys = time_dimension::ensure_keys_for_timestamp(
                conn,
                &comment.created_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_issue_comment(
                conn,
                &comment.id,
                "issue",
                &issue.id,
                &comment_author,
                None,
                comment.body.as_deref(),
                &comment.created_at,
                &comment_keys.date_key,
                &comment_keys.time_key,
            )?;
        }
        Ok(())
    }
}
