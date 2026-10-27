//! Targeted refresh of named pull requests and issues.

use super::*;

impl Syncer<'_> {
    /// Re-fetch a bounded set of exact pull requests and issues and store them
    /// through the same hydrate and upsert path the walks use.
    ///
    /// Targets are validated (1-64, deduplicated, no number named as both
    /// kinds) and grouped by repository. Each repository's ordinary sync lock is
    /// taken for the duration of its targets; when another sync holds it, those
    /// targets fail with that reason and the other run's lock is left alone.
    ///
    /// Nothing outside the items changes: no sync job is started or finished,
    /// no cursor, checkpoint, or coverage range moves, and nothing is enrolled
    /// for monitoring. Each target lands inside its own savepoint, so a target
    /// that fails part-way leaves its previously stored rows exactly as they
    /// were.
    pub fn refresh_targets(
        &mut self,
        targets: &[RefreshTarget],
        options: &RefreshOptions,
    ) -> Result<RefreshReport> {
        let targets = refresh::validate_targets(targets.to_vec())?;
        let mut repositories: Vec<String> = Vec::new();
        for target in &targets {
            if !repositories.contains(&target.repository) {
                repositories.push(target.repository.clone());
            }
        }
        let mut report = RefreshReport::default();
        for repository_name in repositories {
            let group: Vec<&RefreshTarget> = targets
                .iter()
                .filter(|target| target.repository == repository_name)
                .collect();
            let entity_key = format!("repo:{repository_name}");
            match sync_state::acquire_lock(self.connection, &entity_key) {
                Ok(()) => {}
                Err(crate::Error::SyncInProgress(_)) => {
                    for target in group {
                        let counts = refresh::stored_counts(self.connection, target)?;
                        report.outcomes.push(TargetOutcome {
                            target: target.clone(),
                            status: TargetStatus::Failed,
                            error: Some(format!(
                                "a sync of {repository_name} is in progress; retry after it finishes"
                            )),
                            caveats: Vec::new(),
                            before: counts.clone(),
                            after: counts,
                        });
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
            let outcome = self.refresh_group(&group, &entity_key, options, &mut report);
            sync_state::release_lock(self.connection, &entity_key)?;
            outcome?;
        }
        Ok(report)
    }

    pub(super) fn refresh_group(
        &mut self,
        group: &[&RefreshTarget],
        entity_key: &str,
        options: &RefreshOptions,
        report: &mut RefreshReport,
    ) -> Result<()> {
        let timezone = time_dimension::configured_timezone(self.connection)?;
        let core_hours = time_dimension::configured_core_hours(self.connection);
        let bot_suffix = repository::bot_login_suffix(self.connection);
        let mut failed = 0_i64;
        for (index, target) in group.iter().enumerate() {
            let before = refresh::stored_counts(self.connection, target)?;
            let landed = self.refresh_one(target, timezone, core_hours, &bot_suffix, options);
            let (status, error, caveats) = match landed {
                Ok(caveats) => (TargetStatus::Refreshed, None, caveats),
                Err(error) => {
                    failed += 1;
                    (TargetStatus::Failed, Some(error.to_string()), Vec::new())
                }
            };
            let after = refresh::stored_counts(self.connection, target)?;
            sync_state::update_lock_progress(
                self.connection,
                entity_key,
                index as i64 + 1,
                &target.reference(),
                index as i64 + 1 - failed,
                0,
                failed,
            )?;
            report.outcomes.push(TargetOutcome {
                target: (*target).clone(),
                status,
                error,
                caveats,
                before,
                after,
            });
        }
        Ok(())
    }

    /// Fetch, hydrate, and store one target. Returns the provider caveats.
    pub(super) fn refresh_one(
        &mut self,
        target: &RefreshTarget,
        timezone: chrono_tz::Tz,
        core_hours: (u32, u32),
        bot_suffix: &str,
        options: &RefreshOptions,
    ) -> Result<Vec<String>> {
        let (owner, name) = target
            .repository
            .split_once('/')
            .ok_or_else(|| crate::Error::InvalidArgument(target.reference()))?;
        let number = target.number.to_string();
        let variables = [
            ("owner", owner),
            ("name", name),
            ("number", number.as_str()),
        ];
        let check_identity = |metadata: &pull_requests::RepositoryMetadata, found: i64| {
            let resolved = metadata.name_with_owner.to_lowercase();
            if resolved != target.repository {
                return Err(crate::Error::GitHubApi(format!(
                    "{} now resolves to {resolved}; refresh {resolved}#{} instead",
                    target.repository, target.number
                )));
            }
            if found != target.number {
                return Err(crate::Error::GitHubApi(format!(
                    "asked for #{} but the provider returned #{found}",
                    target.number
                )));
            }
            Ok(())
        };
        let upsert_repo = |conn: &Connection, metadata: &pull_requests::RepositoryMetadata| {
            repository::upsert_repository(
                conn,
                &metadata.name_with_owner,
                metadata.primary_language.as_deref(),
                metadata.is_fork,
                metadata.is_private,
                metadata.default_branch.as_deref(),
                metadata.created_at.as_deref(),
            )
        };
        // Everything is fetched and hydrated BEFORE the savepoint opens, so the
        // write lock is never held across network calls, and a fetch failure
        // writes nothing at all.
        match target.kind {
            TargetKind::PullRequest => {
                let data = self
                    .client
                    .graphql(&pull_requests::single_pull_request_query(), &variables)?;
                let (metadata, mut pull_request) = pull_requests::parse_single_pull_request(&data)?;
                check_identity(&metadata, pull_request.number)?;
                let note = pull_requests::hydrate_pull_request(self.client, &mut pull_request)?;
                let repo_key = metadata.name_with_owner.to_lowercase();
                let pr_key = format!("{repo_key}#{}", pull_request.number);
                // Fetch patches before the savepoint, then land all database
                // writes together. A patch-write failure rolls back the PR,
                // its file-set prune, and the repository row too.
                let patches = (!options.skip_diffs && !pull_request.files.is_empty()).then(|| {
                    pull_requests::fetch_file_patches(self.client, &repo_key, pull_request.number)
                });
                let recovered = recover_files(
                    self.client,
                    self.git_recovery.as_ref(),
                    &repo_key,
                    &pull_request,
                    patches.as_ref(),
                    !options.skip_diffs,
                );
                let connection = self.connection;
                within_savepoint(connection, || {
                    upsert_repo(connection, &metadata)?;
                    let mut caveats = without_superseded_notes(note, &recovered);
                    caveats.extend(self.store_pull_request(
                        &repo_key,
                        &pr_key,
                        &pull_request,
                        timezone,
                        core_hours,
                        bot_suffix,
                        patches,
                        recovered,
                    )?);
                    Ok(caveats)
                })
            }
            TargetKind::Issue => {
                let data = self
                    .client
                    .graphql(&issues::single_issue_query(), &variables)?;
                let (metadata, mut issue) = issues::parse_single_issue(&data)?;
                check_identity(&metadata, issue.number)?;
                issues::hydrate_issue(self.client, &mut issue)?;
                let repo_key = metadata.name_with_owner.to_lowercase();
                let connection = self.connection;
                within_savepoint(connection, || {
                    upsert_repo(connection, &metadata)?;
                    self.upsert_one_issue(&repo_key, &issue, timezone, core_hours, bot_suffix)
                })?;
                Ok(Vec::new())
            }
        }
    }
}
