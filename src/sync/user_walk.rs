//! User-scoped walks over search: authored and reviewed pull requests.

use super::*;

impl Syncer<'_> {
    /// Sync one monitored user: pull requests they authored and reviews they
    /// gave, across every repository GitHub will show us.
    ///
    /// The repository path can walk one repo's `pullRequests` connection to
    /// exhaustion. A user's work has no such connection — it is spread across
    /// repositories, so search is the only endpoint that can enumerate it. Two
    /// searches run per user (`author:` and `reviewed-by:`), each with its own
    /// watermark, and both feed the same entity/fact pipeline the repository
    /// path uses.
    ///
    /// Repositories discovered this way get their dimension row so the facts
    /// have somewhere to hang, but they are *not* added to `monitored_repos`:
    /// syncing a person is a statement about that person, not an instruction to
    /// start tracking every repo they contributed to.
    pub fn sync_user(&mut self, login: &str, options: &SyncOptions) -> Result<SyncSummary> {
        let login = crate::query::to_bare_login(login);
        let entity_key = format!("user:{login}");

        sync_state::acquire_lock(self.connection, &entity_key)?;
        sync_state::start_job(self.connection, &entity_key)?;

        let outcome = self.sync_user_locked(&login, &entity_key, options);

        match &outcome {
            Ok(summary) => {
                sync_state::complete_job_outcome(
                    self.connection,
                    &entity_key,
                    summary.pull_requests_synced,
                    summary.skipped,
                    &summary.failed,
                    summary.partial,
                )?;
            }
            Err(error) => {
                sync_state::fail_job(self.connection, &entity_key, &error.to_string())?;
            }
        }
        sync_state::release_lock(self.connection, &entity_key)?;
        outcome
    }

    pub(super) fn sync_user_locked(
        &mut self,
        login: &str,
        entity_key: &str,
        options: &SyncOptions,
    ) -> Result<SyncSummary> {
        let mut summary = SyncSummary::default();
        if options.issues_only {
            // Issues are fetched per repository; there is no user-scoped issue
            // search in this release. Saying so beats returning zero silently.
            summary.notes.push(format!(
                "{login}: --issues-only has no user-scoped equivalent; user sync covers pull \
                 requests and reviews"
            ));
            return Ok(summary);
        }

        let today = time_dimension::today_as_of(self.connection, self.now)?;
        // Same rule as the repository path: never claim the day still running
        // in the warehouse's own calendar.
        let coverage_end = today - Duration::days(1);
        let coverage_before =
            sync_state::coverage_extent_as_of(self.connection, entity_key, self.now)?;
        let window_start = match options.days {
            Some(days) => today - Duration::days(days as i64),
            None => match coverage_before.as_ref() {
                Some((_, end)) => {
                    NaiveDate::parse_from_str(end, "%Y-%m-%d").map_err(|e| {
                        crate::error::Error::InvalidArgument(format!("bad coverage date: {e}"))
                    })? + Duration::days(1)
                }
                None => today - Duration::days(INITIAL_WINDOW_DAYS),
            },
        }
        .min(coverage_end);

        for role in UserRole::all() {
            let cursor =
                sync_state::last_updated_cursor(self.connection, role.cursor_source_type(), login)?;
            let mut watermark = WalkWatermark::default();

            // Incremental when there is both prior coverage and a watermark to
            // stop on. Otherwise this is a backfill, and a backfill must be
            // windowed: `sort:updated-desc` plus a watermark stop cannot reach
            // an old PR whose `updated_at` already sits below the cursor.
            let incremental = options.days.is_none() && coverage_before.is_some();
            if incremental && cursor.is_some() {
                self.walk_user_search(
                    login,
                    role,
                    None,
                    cursor.as_deref(),
                    entity_key,
                    options,
                    &mut summary,
                    &mut watermark,
                    0,
                )?;
            } else {
                for (start, end) in month_windows(window_start, coverage_end) {
                    self.walk_user_search(
                        login,
                        role,
                        Some((start, end)),
                        None,
                        entity_key,
                        options,
                        &mut summary,
                        &mut watermark,
                        0,
                    )?;
                }
            }

            if !summary.partial {
                sync_state::advance_cursor(
                    self.connection,
                    role.cursor_source_type(),
                    login,
                    watermark.committable().as_deref(),
                )?;
            }
        }

        summary.partial |= !summary.failed.is_empty();
        if summary.partial {
            return Ok(summary);
        }
        summary.up_to_date = summary.pull_requests_synced == 0 && summary.failed.is_empty();
        sync_state::record_range_as_of(
            self.connection,
            entity_key,
            &window_start.format("%Y-%m-%d").to_string(),
            &coverage_end.format("%Y-%m-%d").to_string(),
            summary.pull_requests_synced,
            self.now,
        )?;
        monitor_repository::touch_user(self.connection, login)?;
        Ok(summary)
    }

    /// Walk one search query to exhaustion, splitting the created-date window
    /// when GitHub reports more matches than search will ever serve.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn walk_user_search(
        &mut self,
        login: &str,
        role: UserRole,
        window: Option<(NaiveDate, NaiveDate)>,
        cursor_stop: Option<&str>,
        entity_key: &str,
        options: &SyncOptions,
        summary: &mut SyncSummary,
        watermark: &mut WalkWatermark,
        depth: u32,
    ) -> Result<()> {
        if let Some(base) = cursor_stop
            && sync_state::load_checkpoint(
                self.connection,
                "user_update_checkpoint",
                &format!("{}:{login}", role.qualifier()),
            )?
            .is_some()
        {
            return self.walk_user_update_windows(login, role, base, entity_key, options, summary);
        }
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);

        let window_text = window.map(|(start, end)| {
            (
                start.format("%Y-%m-%d").to_string(),
                end.format("%Y-%m-%d").to_string(),
            )
        });
        let expression = user_search::user_search_expression(
            login,
            role,
            window_text
                .as_ref()
                .map(|(start, end)| (start.as_str(), end.as_str())),
        );

        let mut page_cursor: Option<String> = None;
        let mut page_size = SEARCH_PAGE_SIZE;
        let mut first_page = true;
        let mut reported_total: i64 = 0;
        let mut stopped_at_watermark = false;

        'pages: loop {
            let query = user_search::user_pull_requests_query(page_size);
            let mut variables: Vec<(&str, &str)> = vec![("q", expression.as_str())];
            if let Some(cursor) = page_cursor.as_deref() {
                variables.push(("cursor", cursor));
            }
            let data = match self.client.graphql_search(&query, &variables) {
                Ok(data) => data,
                Err(error) if page_size > 1 => {
                    page_size = (page_size / 2).max(1);
                    let _ = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let page = user_search::parse_user_search_page(&data)?;
            summary.pages_fetched += 1;

            if first_page {
                reported_total = page.total_count;
                if let Some(base) = cursor_stop
                    && page.exceeds_result_cap()
                    && page.items.first().is_none_or(|item| {
                        item.pull_request
                            .updated_at
                            .as_deref()
                            .is_none_or(|at| at >= base)
                    })
                {
                    return self
                        .walk_user_update_windows(login, role, base, entity_key, options, summary);
                }
                // A windowed walk intends to paginate to exhaustion, so a match
                // count over the cap is decided here — before any of this
                // window's work is spent on a query that cannot serve it all.
                //
                // An incremental walk is bounded by its watermark instead, and a
                // user with thousands of lifetime matches is the normal case
                // there. Whether the cap actually cost anything is only knowable
                // once the walk ends, so it is checked after the loop.
                if cursor_stop.is_none() && page.exceeds_result_cap() {
                    if let Some((start, end)) = window
                        && start < end
                        && depth < MAX_WINDOW_SPLIT_DEPTH
                    {
                        let midpoint = start + Duration::days((end - start).num_days() / 2);
                        for half in [(start, midpoint), (midpoint + Duration::days(1), end)] {
                            self.walk_user_search(
                                login,
                                role,
                                Some(half),
                                cursor_stop,
                                entity_key,
                                options,
                                summary,
                                watermark,
                                depth + 1,
                            )?;
                        }
                        return Ok(());
                    }
                    // Nothing left to narrow: report the shortfall instead of
                    // recording coverage that was never fetched. Keep both the
                    // date range and the role cursor open for a future backfill.
                    summary.partial = true;
                    watermark.unbounded_failure = true;
                    summary.notes.push(format!(
                        "{login} ({}): '{expression}' matches {} items but GitHub search serves \
                         at most {}; results beyond the cap in this window were not fetched",
                        role.qualifier(),
                        page.total_count,
                        user_search::SEARCH_RESULT_CAP
                    ));
                }
            }
            first_page = false;

            let page_is_empty = page.items.is_empty();
            for item in &page.items {
                // Strict comparison, exactly as the repository walk does it: an
                // item updated in the same second as the cursor may have missed
                // the previous run's page, and re-ingesting it is a free
                // idempotent upsert where skipping it is permanent.
                if let (Some(cursor), Some(updated)) =
                    (cursor_stop, item.pull_request.updated_at.as_deref())
                    && updated < cursor
                {
                    stopped_at_watermark = true;
                    break 'pages;
                }

                let repo_key = repository::upsert_repository(
                    self.connection,
                    &item.repository.name_with_owner,
                    item.repository.primary_language.as_deref(),
                    item.repository.is_fork,
                    item.repository.is_private,
                    item.repository.default_branch.as_deref(),
                    item.repository.created_at.as_deref(),
                )?;
                let pr_key = format!("{repo_key}#{}", item.pull_request.number);
                match self.upsert_one_pull_request(
                    &repo_key,
                    &pr_key,
                    &item.pull_request,
                    timezone,
                    core_hours,
                    &bot_suffix,
                    options,
                ) {
                    Ok(note) => {
                        summary.notes.extend(note);
                        summary.pull_requests_synced += 1;
                        watermark.landed(item.pull_request.updated_at.as_deref());
                    }
                    Err(error) => {
                        // Caps the committable watermark at this item, so a
                        // failure in the middle of the walk is retried rather
                        // than stranded below a newer item's timestamp.
                        watermark.failed(item.pull_request.updated_at.as_deref());
                        summary.failed.push((pr_key.clone(), error.to_string()));
                    }
                }
                sync_state::update_lock_progress(
                    self.connection,
                    entity_key,
                    summary.pull_requests_synced + summary.failed.len() as i64,
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

        // An incremental walk that ran out of pages *without* reaching its
        // watermark did not converge: search stopped serving results before the
        // gap closed, so items between the cap and the watermark are still
        // missing. A walk that reached the watermark is complete no matter how
        // many lifetime matches the query reports, which is why this cannot be
        // decided from the match count alone.
        if cursor_stop.is_some()
            && !stopped_at_watermark
            && reported_total > user_search::SEARCH_RESULT_CAP
        {
            summary.partial = true;
            watermark.unbounded_failure = true;
            summary.notes.push(format!(
                "{login} ({}): incremental walk ran out of search results before reaching its \
                 watermark ({} matches, {} served per query); backfill the gap with `githubdw \
                 sync user {login} --days N`",
                role.qualifier(),
                reported_total,
                user_search::SEARCH_RESULT_CAP
            ));
        }
        Ok(())
    }

    /// Resume a large incremental user search without persisting unstable search
    /// offsets. A leaf is committed only when its complete result fits one page.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn walk_user_update_windows(
        &mut self,
        login: &str,
        role: UserRole,
        base: &str,
        entity_key: &str,
        options: &SyncOptions,
        summary: &mut SyncSummary,
    ) -> Result<()> {
        const KIND: &str = "user_update_checkpoint";
        let key = format!("{}:{login}", role.qualifier());
        let lower = DateTime::parse_from_rfc3339(base)
            .map_err(|e| crate::error::Error::InvalidArgument(format!("bad user cursor: {e}")))?
            .timestamp();
        let upper = self.now.timestamp().max(lower);
        let mut saved = sync_state::load_checkpoint(self.connection, KIND, &key)?
            .and_then(|text| serde_json::from_str::<UserUpdateCheckpoint>(&text).ok())
            .filter(|c| {
                c.version == CHECKPOINT_VERSION
                    && c.base == base
                    && c.upper >= lower
                    && c.upper <= upper
                    && c.pending.first().is_none_or(|(_, end)| *end == c.upper)
                    && c.pending
                        .windows(2)
                        .all(|pair| pair[1].1.checked_add(1) == Some(pair[0].0))
                    && c.pending
                        .iter()
                        .all(|(a, b)| lower <= *a && a <= b && *b <= c.upper)
            })
            .unwrap_or_else(|| UserUpdateCheckpoint {
                version: CHECKPOINT_VERSION,
                base: base.to_string(),
                upper,
                pending: vec![(lower, upper)],
            });
        sync_state::save_checkpoint(self.connection, KIND, &key, &serde_json::to_string(&saved)?)?;
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);
        let mut page_size = SEARCH_PAGE_SIZE;

        while let Some(&(start, end)) = saved.pending.last() {
            if summary.pull_requests_synced + summary.failed.len() as i64
                >= self.item_budget.unwrap_or(500)
            {
                summary.partial = true;
                summary.notes.push(format!(
                    "{login} ({}): catch-up paused with {} update windows remaining",
                    role.qualifier(),
                    saved.pending.len()
                ));
                return Ok(());
            }
            let expression = format!(
                "is:pr {}:{login} updated:{}..{} sort:updated-desc",
                role.qualifier(),
                search_timestamp(start),
                search_timestamp(end),
            );
            let data = loop {
                match self.client.graphql_search(
                    &user_search::user_pull_requests_query(page_size),
                    &[("q", expression.as_str())],
                ) {
                    Ok(data) => break data,
                    Err(_) if page_size > 1 => page_size = (page_size / 2).max(1),
                    Err(error) => return Err(error),
                }
            };
            let page = user_search::parse_user_search_page(&data)?;
            summary.pages_fetched += 1;
            if page.total_count > i64::from(page_size) || page.has_next_page {
                if start == end {
                    summary.partial = true;
                    summary.notes.push(format!("{login} ({}): {} matches share one second; cannot safely complete this update window", role.qualifier(), page.total_count));
                    return Ok(());
                }
                let middle = start + (end - start) / 2;
                saved.pending.pop();
                // Stack: process oldest first, with disjoint inclusive seconds.
                saved.pending.push((middle + 1, end));
                saved.pending.push((start, middle));
            } else {
                // A sparse or malformed response must not close a coverage gap.
                // GitHub's search-index date and the returned PR updatedAt can
                // differ. Ingest every returned node from a complete page, even
                // if newer than the query window; only the frozen search bound
                // is committed, so later updates remain eligible next time.
                if page.total_count != page.items.len() as i64
                    || page.items.iter().any(|item| {
                        item.pull_request
                            .updated_at
                            .as_deref()
                            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                            .is_none()
                    })
                {
                    summary.partial = true;
                    summary.notes.push(format!(
                        "{login}: incomplete or malformed search response; window retained"
                    ));
                    return Ok(());
                }
                let failures_before = summary.failed.len();
                for item in &page.items {
                    let repo_key = repository::upsert_repository(
                        self.connection,
                        &item.repository.name_with_owner,
                        item.repository.primary_language.as_deref(),
                        item.repository.is_fork,
                        item.repository.is_private,
                        item.repository.default_branch.as_deref(),
                        item.repository.created_at.as_deref(),
                    )?;
                    let pr_key = format!("{repo_key}#{}", item.pull_request.number);
                    match self.upsert_one_pull_request(
                        &repo_key,
                        &pr_key,
                        &item.pull_request,
                        timezone,
                        core_hours,
                        &bot_suffix,
                        options,
                    ) {
                        Ok(note) => {
                            summary.notes.extend(note);
                            summary.pull_requests_synced += 1;
                        }
                        Err(error) => summary.failed.push((pr_key.clone(), error.to_string())),
                    }
                    sync_state::update_lock_progress(
                        self.connection,
                        entity_key,
                        summary.pull_requests_synced + summary.failed.len() as i64,
                        &pr_key,
                        summary.pull_requests_synced,
                        summary.skipped,
                        summary.failed.len() as i64,
                    )?;
                }
                if summary.failed.len() != failures_before {
                    summary.partial = true;
                    return Ok(());
                }
                saved.pending.pop();
            }
            sync_state::save_checkpoint(
                self.connection,
                KIND,
                &key,
                &serde_json::to_string(&saved)?,
            )?;
        }
        // The frozen bound, not the newest observed item, closes this walk.
        // Future updates are fetched by the next incremental walk. Keep the
        // freshness stamp at that bound when resuming a walk from a prior run.
        let boundary = search_timestamp(saved.upper);
        let transaction = self.connection.unchecked_transaction()?;
        sync_state::advance_cursor(
            &transaction,
            role.cursor_source_type(),
            login,
            Some(&boundary),
        )?;
        transaction.execute(
            "UPDATE sync_metadata SET last_sync_at=?3 WHERE source_type=?1 AND source_id=?2",
            rusqlite::params![role.cursor_source_type(), login, boundary],
        )?;
        sync_state::clear_checkpoint(&transaction, KIND, &key)?;
        transaction.commit()?;
        if saved.upper < self.now.timestamp() {
            summary.partial = true;
            summary.notes.push(format!(
                "{login} ({}): caught up through {boundary}; newer updates remain for the next run",
                role.qualifier()
            ));
        }
        Ok(())
    }
}
