//! Sync orchestration: fetch pages via `gh`, upsert into the warehouse,
//! stream progress, and record incremental coverage.

use chrono::{Duration, Utc};
use rusqlite::Connection;

use crate::error::Result;
use crate::fetch::GhClient;
use crate::fetch::issues::{self, REPOSITORY_ISSUES_QUERY};
use crate::fetch::pull_requests::{self, ActorReference, PullRequestData};
use crate::storage::issue_repository;
use crate::storage::monitor_repository;
use crate::storage::repository::{self, PullRequestRow};
use crate::storage::sync_state_repository as sync_state;
use crate::storage::time_dimension;

/// Options controlling one sync run.
#[derive(Debug, Clone, Default)]
pub struct SyncOptions {
    /// Restrict the window to the last N days (initial sync default: 90).
    pub days: Option<u32>,
    /// Skip fetching per-file patch text via REST (fast mode).
    pub skip_diffs: bool,
    /// Sync only pull requests (skip issues).
    pub pull_requests_only: bool,
    /// Sync only issues (skip pull requests).
    pub issues_only: bool,
}

/// Result summary of one sync run.
#[derive(Debug, Default)]
pub struct SyncSummary {
    pub pull_requests_synced: i64,
    pub issues_synced: i64,
    pub skipped: i64,
    pub failed: Vec<(String, String)>,
    pub pages_fetched: i64,
    /// True when the incremental cursor made the run a no-op.
    pub up_to_date: bool,
}

/// Default initial window when an entity has never been synced.
const INITIAL_WINDOW_DAYS: i64 = 90;

/// Orchestrates syncing one entity at a time.
pub struct Syncer<'a> {
    connection: &'a Connection,
    client: &'a mut GhClient,
}

impl<'a> Syncer<'a> {
    pub fn new(connection: &'a Connection, client: &'a mut GhClient) -> Self {
        Self { connection, client }
    }

    /// Sync a repository ("owner/name"): PRs (M1) and issues (M2).
    pub fn sync_repository(
        &mut self,
        repository_name: &str,
        options: &SyncOptions,
    ) -> Result<SyncSummary> {
        let repository_name = repository_name.to_lowercase();
        let entity_key = format!("repo:{repository_name}");

        sync_state::acquire_lock(self.connection, &entity_key)?;
        sync_state::start_job(self.connection, &entity_key)?;

        let outcome = self.sync_repository_locked(&repository_name, &entity_key, options);

        match &outcome {
            Ok(summary) => {
                sync_state::complete_job(
                    self.connection,
                    &entity_key,
                    summary.pull_requests_synced + summary.issues_synced,
                    summary.skipped,
                    &summary.failed,
                )?;
            }
            Err(error) => {
                sync_state::fail_job(self.connection, &entity_key, &error.to_string())?;
            }
        }
        sync_state::release_lock(self.connection, &entity_key)?;
        outcome
    }

    fn sync_repository_locked(
        &mut self,
        repository_name: &str,
        entity_key: &str,
        options: &SyncOptions,
    ) -> Result<SyncSummary> {
        let (owner, name) = repository_name.split_once('/').ok_or_else(|| {
            crate::Error::InvalidArgument(format!("expected owner/name, got '{repository_name}'"))
        })?;

        let mut summary = SyncSummary::default();
        let cursor_before =
            sync_state::last_updated_cursor(self.connection, "repo", repository_name)?;

        // Determine the work window from coverage + --days.
        let today = Utc::now().date_naive();
        let window_start = match options.days {
            Some(days) => today - Duration::days(days as i64),
            None => match sync_state::coverage_extent(self.connection, entity_key)? {
                Some(_) => today, // incremental: cursor governs, range extends to today
                None => today - Duration::days(INITIAL_WINDOW_DAYS),
            },
        };

        if !options.issues_only {
            self.sync_pull_requests(
                owner,
                name,
                repository_name,
                entity_key,
                cursor_before.as_deref(),
                options,
                &mut summary,
            )?;
        }

        if !options.pull_requests_only {
            let issue_cursor =
                sync_state::last_updated_cursor(self.connection, "repo_issues", repository_name)?;
            self.sync_issues(
                owner,
                name,
                repository_name,
                entity_key,
                issue_cursor.as_deref(),
                &mut summary,
            )?;
        }

        // Record coverage and advance the cursor.
        sync_state::record_range(
            self.connection,
            entity_key,
            &window_start.format("%Y-%m-%d").to_string(),
            &today.format("%Y-%m-%d").to_string(),
            summary.pull_requests_synced + summary.issues_synced,
        )?;
        monitor_repository::touch_repo(self.connection, repository_name)?;
        Ok(summary)
    }

    #[allow(clippy::too_many_arguments)]
    fn sync_pull_requests(
        &mut self,
        owner: &str,
        name: &str,
        repository_name: &str,
        entity_key: &str,
        updated_cursor: Option<&str>,
        options: &SyncOptions,
        summary: &mut SyncSummary,
    ) -> Result<()> {
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);

        let mut page_cursor: Option<String> = None;
        let mut max_updated_at: Option<String> = updated_cursor.map(str::to_string);
        let mut item_index: i64 = 0;
        // Adaptive page size: heavy PRs (many files/comments/checks) can
        // overflow GitHub's response stream; halve and retry on failure.
        let mut page_size: u32 = 25;

        'pages: loop {
            let query = pull_requests::repository_pull_requests_query(page_size);
            let mut variables: Vec<(&str, &str)> = vec![("owner", owner), ("name", name)];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = match self.client.graphql(&query, &variables) {
                Ok(data) => data,
                Err(error) if page_size > 1 => {
                    // Degrade and retry the same page with a smaller window.
                    page_size = (page_size / 2).max(1);
                    let _ = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let page = pull_requests::parse_pull_request_page(&data)?;
            summary.pages_fetched += 1;

            // Upsert the repository dimension from the page header.
            let repo_key = repository::upsert_repository(
                self.connection,
                &page.repository.name_with_owner,
                page.repository.primary_language.as_deref(),
                page.repository.is_fork,
                page.repository.is_private,
                page.repository.default_branch.as_deref(),
                page.repository.created_at.as_deref(),
            )?;

            let page_is_empty = page.pull_requests.is_empty();
            for pull_request in &page.pull_requests {
                // Incremental stop: results are updated-desc; once at/under
                // the cursor, everything further is already ingested.
                if let (Some(cursor), Some(updated)) =
                    (updated_cursor, pull_request.updated_at.as_deref())
                    && updated <= cursor
                {
                    summary.up_to_date = summary.pull_requests_synced == 0;
                    break 'pages;
                }

                item_index += 1;
                let pr_key = format!("{repo_key}#{}", pull_request.number);
                match self.upsert_one_pull_request(
                    &repo_key,
                    &pr_key,
                    pull_request,
                    timezone,
                    core_hours,
                    &bot_suffix,
                    options,
                ) {
                    Ok(()) => summary.pull_requests_synced += 1,
                    Err(error) => summary.failed.push((pr_key.clone(), error.to_string())),
                }
                if let Some(updated) = pull_request.updated_at.as_deref()
                    && max_updated_at.as_deref().is_none_or(|max| updated > max)
                {
                    max_updated_at = Some(updated.to_string());
                }
                sync_state::update_lock_progress(
                    self.connection,
                    entity_key,
                    item_index,
                    &pr_key,
                    summary.pull_requests_synced,
                    summary.skipped,
                    summary.failed.len() as i64,
                )?;
            }

            if !page.has_next_page || page_is_empty {
                break;
            }
            page_cursor = page.end_cursor;
            if page_cursor.is_none() {
                break;
            }
        }

        sync_state::advance_cursor(
            self.connection,
            "repo",
            repository_name,
            max_updated_at.as_deref(),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_one_pull_request(
        &mut self,
        repo_key: &str,
        pr_key: &str,
        pull_request: &PullRequestData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
        options: &SyncOptions,
    ) -> Result<()> {
        let conn = self.connection;

        let author_key = self.resolve_actor(pull_request.author.as_ref(), bot_suffix)?;
        let merged_by_key = match pull_request.merged_by.as_ref() {
            Some(actor) => Some(repository::ensure_entity(conn, actor, bot_suffix)?),
            None => None,
        };

        let created = time_dimension::ensure_keys_for_timestamp(
            conn,
            &pull_request.created_at,
            timezone,
            core_hours,
        )?;
        let updated = pull_request
            .updated_at
            .as_deref()
            .map(|ts| time_dimension::ensure_keys_for_timestamp(conn, ts, timezone, core_hours))
            .transpose()?;
        let merged = pull_request
            .merged_at
            .as_deref()
            .map(|ts| time_dimension::ensure_keys_for_timestamp(conn, ts, timezone, core_hours))
            .transpose()?;

        let comment_count =
            (pull_request.conversation_comments.len() + pull_request.review_comments.len()) as i64;

        repository::upsert_pull_request(
            conn,
            &PullRequestRow {
                pr_key,
                number: pull_request.number,
                repo_key,
                author_key: &author_key,
                state: &pull_request.state,
                is_draft: pull_request.is_draft,
                title: pull_request.title.as_deref(),
                body: pull_request.body.as_deref(),
                base_ref: pull_request.base_ref.as_deref(),
                head_ref: pull_request.head_ref.as_deref(),
                created_at: &pull_request.created_at,
                updated_at: pull_request.updated_at.as_deref(),
                merged_at: pull_request.merged_at.as_deref(),
                closed_at: pull_request.closed_at.as_deref(),
                merged_by_key: merged_by_key.as_deref(),
                created_date_key: &created.date_key,
                created_time_key: &created.time_key,
                updated_date_key: updated.as_ref().map(|keys| keys.date_key.as_str()),
                updated_time_key: updated.as_ref().map(|keys| keys.time_key.as_str()),
                merged_date_key: merged.as_ref().map(|keys| keys.date_key.as_str()),
                comment_count,
                review_count: pull_request.reviews.len() as i64,
                changed_files: pull_request.changed_files,
                additions: pull_request.additions,
                deletions: pull_request.deletions,
            },
        )?;

        for review in &pull_request.reviews {
            let Some(submitted_at) = review.submitted_at.as_deref() else {
                continue; // pending reviews have no timestamp
            };
            let reviewer_key = self.resolve_actor(review.author.as_ref(), bot_suffix)?;
            let submitted = time_dimension::ensure_keys_for_timestamp(
                conn,
                submitted_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_review(
                conn,
                &review.id,
                pr_key,
                &reviewer_key,
                &review.state,
                review.body.as_deref(),
                submitted_at,
                &submitted.date_key,
                &submitted.time_key,
            )?;
        }

        for comment in &pull_request.review_comments {
            let author = self.resolve_actor(comment.author.as_ref(), bot_suffix)?;
            let created_keys = time_dimension::ensure_keys_for_timestamp(
                conn,
                &comment.created_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_review_comment(
                conn,
                &comment.id,
                pr_key,
                &author,
                comment.in_reply_to.as_deref(),
                comment.path.as_deref(),
                comment.line,
                comment.body.as_deref(),
                &comment.created_at,
                &created_keys.date_key,
                &created_keys.time_key,
            )?;
        }

        for comment in &pull_request.conversation_comments {
            let author = self.resolve_actor(comment.author.as_ref(), bot_suffix)?;
            let created_keys = time_dimension::ensure_keys_for_timestamp(
                conn,
                &comment.created_at,
                timezone,
                core_hours,
            )?;
            repository::upsert_issue_comment(
                conn,
                &comment.id,
                "pull_request",
                pr_key,
                &author,
                None,
                comment.body.as_deref(),
                &comment.created_at,
                &created_keys.date_key,
                &created_keys.time_key,
            )?;
        }

        if let Some(head_sha) = pull_request.head_sha.as_deref() {
            for check in &pull_request.check_runs {
                repository::upsert_check_run(
                    conn,
                    &check.id,
                    pr_key,
                    head_sha,
                    &check.name,
                    &check.status,
                    check.conclusion.as_deref(),
                    check.started_at.as_deref(),
                    check.completed_at.as_deref(),
                )?;
            }
        }

        for file in &pull_request.files {
            repository::upsert_file_diff(
                conn,
                pr_key,
                repo_key,
                &file.path,
                None,
                &file.change_type,
                None,
                file.additions,
                file.deletions,
            )?;
        }

        // Patch text backfill via REST (skipped in fast mode).
        if !options.skip_diffs && !pull_request.files.is_empty() {
            let path = format!(
                "repos/{repo_key}/pulls/{}/files?per_page=100",
                pull_request.number
            );
            if let Ok(response) = self.client.rest(&path) {
                for (file_path, previous_path, patch) in
                    pull_requests::parse_rest_file_patches(&response)
                {
                    repository::set_file_diff_patch(
                        conn,
                        pr_key,
                        &file_path,
                        previous_path.as_deref(),
                        patch.as_deref(),
                    )?;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn sync_issues(
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
        let mut max_updated_at: Option<String> = updated_cursor.map(str::to_string);
        let mut item_index: i64 = 0;

        'pages: loop {
            let mut variables: Vec<(&str, &str)> = vec![("owner", owner), ("name", name)];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = self.client.graphql(REPOSITORY_ISSUES_QUERY, &variables)?;
            let page = issues::parse_issue_page(&data)?;
            summary.pages_fetched += 1;

            let page_is_empty = page.issues.is_empty();
            for issue in &page.issues {
                if let Some(cursor) = updated_cursor
                    && issue.updated_at.as_str() <= cursor
                {
                    break 'pages;
                }

                item_index += 1;
                let item_id = format!("{repo_key}#issue-{}", issue.number);
                match self.upsert_one_issue(&repo_key, issue, timezone, core_hours, &bot_suffix) {
                    Ok(()) => summary.issues_synced += 1,
                    Err(error) => summary.failed.push((item_id.clone(), error.to_string())),
                }
                if max_updated_at
                    .as_deref()
                    .is_none_or(|max| issue.updated_at.as_str() > max)
                {
                    max_updated_at = Some(issue.updated_at.clone());
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

            if !page.has_next_page || page_is_empty {
                break;
            }
            page_cursor = page.end_cursor;
            if page_cursor.is_none() {
                break;
            }
        }

        sync_state::advance_cursor(
            self.connection,
            "repo_issues",
            repository_name,
            max_updated_at.as_deref(),
        )?;
        Ok(())
    }

    fn upsert_one_issue(
        &mut self,
        repo_key: &str,
        issue: &issues::IssueData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
    ) -> Result<()> {
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

    fn resolve_actor(&self, actor: Option<&ActorReference>, bot_suffix: &str) -> Result<String> {
        match actor {
            Some(actor) => repository::ensure_entity(self.connection, actor, bot_suffix),
            None => repository::ensure_ghost_entity(self.connection),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GithubDW;
    use crate::fetch::test_support::FixtureTransport;
    use serde_json::json;

    fn page_response(has_next: bool, cursor: Option<&str>, numbers: &[i64]) -> String {
        let nodes: Vec<_> = numbers
            .iter()
            .map(|number| {
                json!({
                    "number": number,
                    "title": format!("PR {number}"),
                    "body": "body text",
                    "state": "MERGED",
                    "isDraft": false,
                    "createdAt": "2026-01-05T18:00:00Z",
                    "updatedAt": format!("2026-01-{:02}T09:00:00Z", 5 + number),
                    "mergedAt": "2026-01-06T09:00:00Z",
                    "closedAt": "2026-01-06T09:00:00Z",
                    "baseRefName": "main",
                    "headRefName": format!("feature/{number}"),
                    "additions": 10,
                    "deletions": 2,
                    "changedFiles": 1,
                    "author": {"login": "octocat", "__typename": "User"},
                    "mergedBy": {"login": "hubot", "__typename": "User"},
                    "reviews": {"nodes": [{
                        "id": format!("REV{number}"), "state": "APPROVED", "body": "",
                        "submittedAt": "2026-01-06T08:00:00Z",
                        "author": {"login": "hubot", "__typename": "User"}
                    }]},
                    "reviewThreads": {"nodes": []},
                    "comments": {"nodes": []},
                    "files": {"nodes": [{
                        "path": "src/lib.rs", "changeType": "MODIFIED",
                        "additions": 10, "deletions": 2
                    }]},
                    "commits": {"nodes": []}
                })
            })
            .collect();
        json!({
            "data": {
                "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
                "repository": {
                    "nameWithOwner": "octocat/hello",
                    "primaryLanguage": {"name": "Rust"},
                    "isFork": false,
                    "isPrivate": false,
                    "defaultBranchRef": {"name": "main"},
                    "createdAt": "2020-01-01T00:00:00Z",
                    "pullRequests": {
                        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                        "nodes": nodes
                    }
                }
            }
        })
        .to_string()
    }

    #[test]
    fn syncs_two_pages_and_records_state() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let transport = FixtureTransport::new(vec![
            Ok(page_response(true, Some("C1"), &[1, 2])),
            Ok(page_response(false, None, &[3])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = SyncOptions {
            skip_diffs: true,
            pull_requests_only: true,
            ..Default::default()
        };
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

        assert_eq!(summary.pull_requests_synced, 3);
        assert_eq!(summary.pages_fetched, 2);
        assert!(summary.failed.is_empty());

        let conn = warehouse.connection();
        let pr_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_pull_requests", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pr_count, 3);
        let review_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_reviews", [], |r| r.get(0))
            .unwrap();
        assert_eq!(review_count, 3);
        let diff_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM fact_file_diffs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(diff_count, 3);

        // Job completed, lock released, range recorded, cursor advanced.
        let status: String = conn
            .query_row(
                "SELECT status FROM sync_jobs WHERE entity_key = 'repo:octocat/hello'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
        let locks: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(locks, 0);
        let cursor = sync_state::last_updated_cursor(conn, "repo", "octocat/hello").unwrap();
        assert_eq!(cursor.as_deref(), Some("2026-01-08T09:00:00Z"));
    }

    #[test]
    fn incremental_rerun_stops_at_cursor() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let transport = FixtureTransport::new(vec![
            Ok(page_response(false, None, &[1, 2])),
            // Second run returns the same (unchanged) PRs.
            Ok(page_response(false, None, &[1, 2])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = SyncOptions {
            skip_diffs: true,
            pull_requests_only: true,
            ..Default::default()
        };

        {
            let mut syncer = Syncer::new(warehouse.connection(), &mut client);
            let first = syncer.sync_repository("octocat/hello", &options).unwrap();
            assert_eq!(first.pull_requests_synced, 2);
        }
        {
            let mut syncer = Syncer::new(warehouse.connection(), &mut client);
            let second = syncer.sync_repository("octocat/hello", &options).unwrap();
            assert_eq!(second.pull_requests_synced, 0, "no-op on unchanged data");
            assert!(second.up_to_date);
        }
    }

    fn issue_page_response(has_next: bool, cursor: Option<&str>, numbers: &[i64]) -> String {
        let nodes: Vec<_> = numbers
            .iter()
            .map(|number| {
                json!({
                    "id": format!("ISS{number}"),
                    "number": number,
                    "title": format!("Issue {number}"),
                    "body": "issue body",
                    "state": "OPEN",
                    "stateReason": null,
                    "createdAt": "2026-02-01T12:00:00Z",
                    "updatedAt": format!("2026-02-{:02}T12:00:00Z", number),
                    "closedAt": null,
                    "author": {"login": "octocat", "__typename": "User"},
                    "milestone": {
                        "id": "MILE1", "number": 1, "title": "v1.0",
                        "description": null, "state": "OPEN",
                        "dueOn": null, "createdAt": "2026-01-01T00:00:00Z"
                    },
                    "labels": {"nodes": [{
                        "id": "LAB1", "name": "bug", "color": "d73a4a", "description": ""
                    }]},
                    "assignees": {"nodes": [{"login": "hubot", "__typename": "User"}]},
                    "comments": {"nodes": [{
                        "id": format!("ICOM{number}"), "body": "on it",
                        "createdAt": "2026-02-02T08:00:00Z",
                        "author": {"login": "hubot", "__typename": "User"}
                    }]}
                })
            })
            .collect();
        json!({
            "data": {
                "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999, "resetAt": "2099-01-01T00:00:00Z"},
                "repository": {
                    "nameWithOwner": "octocat/hello",
                    "issues": {
                        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                        "nodes": nodes
                    }
                }
            }
        })
        .to_string()
    }

    #[test]
    fn syncs_issues_with_labels_and_comments() {
        let warehouse = GithubDW::open_in_memory().unwrap();
        let transport = FixtureTransport::new(vec![
            Ok(issue_page_response(true, Some("IC1"), &[1, 2])),
            Ok(issue_page_response(false, None, &[3])),
        ]);
        let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
        let options = SyncOptions {
            issues_only: true,
            ..Default::default()
        };
        let mut syncer = Syncer::new(warehouse.connection(), &mut client);
        let summary = syncer.sync_repository("octocat/hello", &options).unwrap();

        assert_eq!(summary.issues_synced, 3);
        assert_eq!(summary.pages_fetched, 2);

        let conn = warehouse.connection();
        let issue_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM issues", [], |r| r.get(0))
            .unwrap();
        assert_eq!(issue_count, 3);
        let label_links: i64 = conn
            .query_row("SELECT COUNT(*) FROM issue_labels", [], |r| r.get(0))
            .unwrap();
        assert_eq!(label_links, 3);
        let assignees: i64 = conn
            .query_row("SELECT COUNT(*) FROM issue_assignees", [], |r| r.get(0))
            .unwrap();
        assert_eq!(assignees, 3);
        let comments: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fact_issue_comments WHERE parent_type = 'issue'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(comments, 3);
        let milestones: i64 = conn
            .query_row("SELECT COUNT(*) FROM milestones", [], |r| r.get(0))
            .unwrap();
        assert_eq!(milestones, 1);
        let fts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM issues_fts WHERE issues_fts MATCH 'ssue'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(fts, 3, "issues_fts populated via trigger");
    }
}
