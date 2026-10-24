//! Repository pull-request walk: page, hydrate, recover files, and store.

use super::*;

/// Outcome of git-backed recovery for one pull request: `None` when it did
/// not run, `Some(Err(reason))` when it ran and could not prove a set.
pub(super) type Recovery = Option<std::result::Result<RecoveredFileSet, String>>;

/// Run git-backed recovery when the API listing cannot be trusted. All
/// network and git work happens here, before any savepoint opens.
pub(super) fn recover_files(
    client: &mut GhClient,
    config: Option<&GitRecoveryConfig>,
    repo_key: &str,
    pull_request: &PullRequestData,
    patches: Option<&Result<pull_requests::PatchListing>>,
    want_patches: bool,
) -> Recovery {
    let config = config?;
    let listing = patches.and_then(|listing| listing.as_ref().ok());
    if !git_files::needs_recovery(pull_request, listing) {
        return None;
    }
    // The REST walk carries GitHub's rename pairs; fetch it if fast mode
    // skipped it. It is used for validation only, never stored here.
    let fetched;
    let listing = match patches {
        Some(Ok(listing)) => listing,
        Some(Err(error)) => return Some(Err(format!("REST file listing failed: {error}"))),
        None => {
            fetched = pull_requests::fetch_file_patches(client, repo_key, pull_request.number);
            match &fetched {
                Ok(listing) => listing,
                Err(error) => return Some(Err(format!("REST file listing failed: {error}"))),
            }
        }
    };
    Some(
        git_files::recover_pull_request_files(
            client,
            config,
            repo_key,
            pull_request,
            listing,
            want_patches,
        )
        .map_err(|error| error.to_string()),
    )
}

/// Drop the hydrator's truncation note once git proved the complete set.
pub(super) fn without_superseded_notes(note: Option<String>, recovered: &Recovery) -> Vec<String> {
    let proven = matches!(recovered, Some(Ok(_)));
    note.map(|note| {
        note.split("; ")
            // The hydrator's truncation caveat itself contains "; ", so both
            // halves are recognised.
            .filter(|part| {
                !(proven
                    && (*part == "file coverage is truncated"
                        || (part.contains("provider listed") && part.ends_with("changed files"))))
            })
            .map(str::to_string)
            .collect()
    })
    .unwrap_or_default()
}

impl Syncer<'_> {
    /// Walk a repository's pull requests newest-first, resumably.
    ///
    /// The walk is checkpointed after every fully processed page, so a run
    /// that is killed, fails mid-walk, or yields at its item budget leaves a
    /// durable position and the next run continues from it instead of
    /// re-walking from the newest page. The real high-water cursor moves only
    /// once the walk reaches its stop, and then only as far as
    /// [`WalkWatermark::committable`] allows.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn sync_pull_requests(
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

        // Resume only a checkpoint that belongs to this walk: same format,
        // started from the cursor that is still stored. Anything else (corrupt
        // text, a walk begun from an older cursor) is dropped — it can only
        // cost re-reading pages, never skipping one.
        let checkpoint: Option<PullRequestWalkCheckpoint> =
            sync_state::load_checkpoint(self.connection, REPO_PR_CHECKPOINT, repository_name)?
                .and_then(|text| serde_json::from_str::<PullRequestWalkCheckpoint>(&text).ok())
                .filter(|saved| {
                    saved.version == CHECKPOINT_VERSION
                        && saved.base.as_deref() == updated_cursor
                        && !saved.after.is_empty()
                });
        if checkpoint.is_none() {
            sync_state::clear_checkpoint(self.connection, REPO_PR_CHECKPOINT, repository_name)?;
        }
        let mut watermark = checkpoint
            .as_ref()
            .map(|saved| saved.watermark.clone())
            .unwrap_or_default();
        let mut page_cursor: Option<String> = checkpoint.as_ref().map(|saved| saved.after.clone());
        let mut walk_pages = checkpoint.as_ref().map_or(0, |saved| saved.pages);
        let mut walk_items = checkpoint.as_ref().map_or(0, |saved| saved.items);
        let mut restarts = checkpoint.as_ref().map_or(0, |saved| saved.restarts);
        if let Some(saved) = &checkpoint {
            summary.notes.push(format!(
                "{repository_name}: resumed pull request walk after {} page(s), {} item(s)",
                saved.pages, saved.items
            ));
        }
        let mut restarted_this_run = false;
        // Page cursors served this run: a provider that repeats one is cycling.
        let mut seen_page_cursors: std::collections::HashSet<String> =
            page_cursor.iter().cloned().collect();

        let mut item_index: i64 = 0;
        let mut processed_this_run: i64 = 0;
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
                // A saved cursor GitHub no longer accepts: restart this walk
                // from the newest page, keeping its base and watermark. At most
                // once per run, so a persistent rejection cannot loop.
                Err(error)
                    if is_invalid_cursor_error(&error)
                        && page_cursor.is_some()
                        && !restarted_this_run =>
                {
                    restarted_this_run = true;
                    restarts += 1;
                    page_cursor = None;
                    summary.notes.push(format!(
                        "{repository_name}: saved page cursor was rejected; restarted the \
                         walk from the newest page"
                    ));
                    continue;
                }
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
                // Incremental stop: results are updated-desc, so once an item
                // is *strictly* older than the cursor everything after it is
                // already ingested.
                //
                // The comparison must be strict. `updated_at` is
                // second-granular, so an item updated in the same second as
                // the cursor — after the previous run had already served its
                // page — sits exactly at the boundary. Stopping there would
                // skip it forever, since only a further update (which moves
                // its timestamp) could bring it back. Re-ingesting the
                // boundary second instead is free: the upserts are
                // `ON CONFLICT` idempotent.
                if let (Some(cursor), Some(updated)) =
                    (updated_cursor, pull_request.updated_at.as_deref())
                    && updated < cursor
                {
                    summary.up_to_date = summary.pull_requests_synced == 0;
                    break 'pages;
                }

                item_index += 1;
                processed_this_run += 1;
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
                    Ok(note) => {
                        summary.notes.extend(note);
                        summary.pull_requests_synced += 1;
                        watermark.landed(pull_request.updated_at.as_deref());
                    }
                    Err(error) => {
                        // A failed item caps the committable watermark at its
                        // own timestamp, wherever it sits in the walk, so the
                        // next walk reaches it again.
                        watermark.failed(pull_request.updated_at.as_deref());
                        summary.failed.push((pr_key.clone(), error.to_string()));
                    }
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
            walk_pages += 1;
            walk_items += page.pull_requests.len() as i64;

            if !page.has_next_page {
                break;
            }
            if page_is_empty {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: pull request page reports more results but holds none"
                )));
            }
            if let Some(next) = page.end_cursor.as_deref()
                && !seen_page_cursors.insert(next.to_string())
            {
                return Err(crate::Error::GitHubApi(format!(
                    "{repository_name}: pull request page cursor repeated"
                )));
            }
            page_cursor = page.end_cursor;

            // Every item on this page is now processed (landed or recorded as
            // failed in the watermark), so the position after it is durable.
            let saved = PullRequestWalkCheckpoint {
                version: CHECKPOINT_VERSION,
                base: updated_cursor.map(str::to_string),
                after: page_cursor.clone().unwrap_or_default(),
                watermark: watermark.clone(),
                pages: walk_pages,
                items: walk_items,
                restarts,
            };
            sync_state::save_checkpoint(
                self.connection,
                REPO_PR_CHECKPOINT,
                repository_name,
                &serde_json::to_string(&saved)?,
            )?;

            if let Some(budget) = self.item_budget
                && processed_this_run >= budget
            {
                summary.partial = true;
                summary.up_to_date = false;
                summary.notes.push(format!(
                    "{repository_name}: pull request walk paused at its budget of {budget} \
                     item(s); the next run resumes from page {}",
                    walk_pages + 1
                ));
                return Ok(());
            }
        }

        sync_state::complete_walk(
            self.connection,
            "repo",
            REPO_PR_CHECKPOINT,
            repository_name,
            watermark.committable().as_deref().or(updated_cursor),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn upsert_one_pull_request(
        &mut self,
        repo_key: &str,
        pr_key: &str,
        pull_request: &PullRequestData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
        options: &SyncOptions,
    ) -> Result<Option<String>> {
        let mut pull_request = pull_request.clone();
        let note = pull_requests::hydrate_pull_request(self.client, &mut pull_request)?;
        // Fetch patch text BEFORE any write, then land every write for this
        // item in one savepoint: a failure part-way (including a patch write)
        // leaves the previously stored rows exactly as they were.
        let patches = (!options.skip_diffs && !pull_request.files.is_empty())
            .then(|| pull_requests::fetch_file_patches(self.client, repo_key, pull_request.number));
        let recovered = recover_files(
            self.client,
            self.git_recovery.as_ref(),
            repo_key,
            &pull_request,
            patches.as_ref(),
            !options.skip_diffs,
        );
        let connection = self.connection;
        let mut caveats = without_superseded_notes(note, &recovered);
        caveats.extend(within_savepoint(connection, || {
            self.store_pull_request(
                repo_key,
                pr_key,
                &pull_request,
                timezone,
                core_hours,
                bot_suffix,
                patches,
                recovered,
            )
        })?);
        Ok((!caveats.is_empty()).then(|| caveats.join("; ")))
    }

    /// Write one hydrated pull request and everything nested under it.
    ///
    /// Performs no network calls; callers wrap it in a savepoint. When the
    /// incoming file list is provably complete
    /// ([`pull_requests::authoritative_file_paths`]) it is authoritative and
    /// stored file rows absent from it are deleted; otherwise every stored row
    /// is kept and a caveat says so. Patch text, when supplied, is written
    /// after the file set settles. Returns the caveats.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn store_pull_request(
        &self,
        repo_key: &str,
        pr_key: &str,
        pull_request: &PullRequestData,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
        patches: Option<Result<pull_requests::PatchListing>>,
        recovered: Recovery,
    ) -> Result<Vec<String>> {
        let conn = self.connection;
        let (changed_files, additions, deletions) = match &recovered {
            Some(Ok(set)) => (set.changed_files, set.additions, set.deletions),
            Some(Err(reason)) => return Err(crate::Error::GitRecovery(reason.clone())),
            None => (
                pull_request.changed_files,
                pull_request.additions,
                pull_request.deletions,
            ),
        };

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
                changed_files,
                additions,
                deletions,
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

        match recovered {
            Some(Ok(set)) => {
                return Self::store_recovered_files(
                    conn,
                    repo_key,
                    pr_key,
                    pull_request.number,
                    &set,
                    patches.is_some(),
                );
            }
            Some(Err(reason)) => return Err(crate::Error::GitRecovery(reason)),
            None => {}
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

        let mut caveats = Vec::new();
        match pull_requests::authoritative_file_paths(pull_request) {
            Ok(current) => {
                let stale = repository::file_diffs_absent_from(conn, pr_key, &current)?;
                repository::delete_file_diffs(conn, pr_key, &stale)?;
            }
            Err(gap) => {
                let kept = repository::file_diffs_absent_from(
                    conn,
                    pr_key,
                    &pull_request
                        .files
                        .iter()
                        .map(|file| file.path.as_str())
                        .collect(),
                )?;
                if !kept.is_empty() || gap.anomalous {
                    caveats.push(format!(
                        "PR #{}: file list is not provably complete ({}); kept {} stored \
                         file row(s) absent from this response",
                        pull_request.number,
                        gap.reason,
                        kept.len()
                    ));
                }
            }
        }

        // Patch text backfill via REST (absent in fast mode).
        if let Some(listing) = patches {
            caveats.extend(Self::store_patch_listing(
                conn,
                pr_key,
                pull_request,
                listing,
            )?);
        }
        Ok(caveats)
    }

    /// Land a git-proven complete file set: every record (with GitHub's served
    /// rename pairs and patches, git's tail), then prune rows outside the set.
    /// Runs inside the caller's savepoint, so any failure rolls everything back.
    pub(super) fn store_recovered_files(
        conn: &Connection,
        repo_key: &str,
        pr_key: &str,
        number: i64,
        set: &RecoveredFileSet,
        write_patches: bool,
    ) -> Result<Vec<String>> {
        for file in &set.files {
            repository::upsert_file_diff(
                conn,
                pr_key,
                repo_key,
                &file.path,
                file.previous_path.as_deref(),
                &file.change_type,
                None,
                file.additions,
                file.deletions,
            )?;
            if write_patches {
                repository::set_file_diff_patch(
                    conn,
                    pr_key,
                    &file.path,
                    file.previous_path.as_deref(),
                    file.patch.as_deref(),
                )?;
            }
        }
        let keep: std::collections::HashSet<&str> = set.keep.iter().map(String::as_str).collect();
        let stale = repository::file_diffs_absent_from(conn, pr_key, &keep)?;
        repository::delete_file_diffs(conn, pr_key, &stale)?;
        let records: std::collections::HashSet<&str> =
            set.files.iter().map(|file| file.path.as_str()).collect();
        let empty = std::collections::HashSet::new();
        let stored = repository::file_diffs_absent_from(conn, pr_key, &empty)?;
        let landed = stored
            .iter()
            .filter(|path| records.contains(path.as_str()))
            .count();
        if landed != set.files.len()
            || stored.len() != set.files.len()
            || set.files.len() as i64 != set.changed_files
        {
            return Err(crate::Error::GitRecovery(format!(
                "PR #{number}: {landed} of {} recovered records landed for {} changed files",
                set.files.len(),
                set.changed_files
            )));
        }
        let mut caveats = Vec::new();
        if write_patches && set.oversized_patches > 0 {
            caveats.push(format!(
                "PR #{number}: {} recovered patch(es) exceeded the size bound and were not stored",
                set.oversized_patches
            ));
        }
        Ok(caveats)
    }

    /// Store patch text for every file the REST files endpoint served.
    ///
    /// The endpoint pages at 100 files; every page is walked, and a failed or
    /// capped walk is returned as a caveat rather than passed off as the
    /// complete listing. Only UPDATEs existing file rows.
    pub(super) fn store_patch_listing(
        conn: &Connection,
        pr_key: &str,
        pull_request: &PullRequestData,
        listing: Result<pull_requests::PatchListing>,
    ) -> Result<Vec<String>> {
        let mut caveats = Vec::new();
        match listing {
            Ok(listing) => {
                for (file_path, previous_path, patch) in &listing.files {
                    repository::set_file_diff_patch(
                        conn,
                        pr_key,
                        file_path,
                        previous_path.as_deref(),
                        patch.as_deref(),
                    )?;
                }
                let served = listing.files.len();
                if listing.provider_capped
                    && pull_request.changed_files > pull_requests::REST_FILE_LIMIT as i64
                {
                    caveats.push(format!(
                        "PR #{}: provider serves patches for at most {} of {} changed files",
                        pull_request.number,
                        pull_requests::REST_FILE_LIMIT,
                        pull_request.changed_files
                    ));
                } else if served < pull_request.files.len() {
                    caveats.push(format!(
                        "PR #{}: provider served patches for {served} of {} listed files",
                        pull_request.number,
                        pull_request.files.len()
                    ));
                }
            }
            Err(error) => caveats.push(format!(
                "PR #{}: patch text not retrieved: {error}",
                pull_request.number
            )),
        }
        Ok(caveats)
    }
}
