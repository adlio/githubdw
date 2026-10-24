//! Repairing one target: fetch every head, then write in one transaction.

use super::*;

pub(super) fn repair_one(
    conn: &Connection,
    client: &mut GhClient,
    target: &RefreshTarget,
    options: &CheckHistoryOptions,
    budget: Option<&QuotaBudget>,
    lease: &mut Lease,
) -> std::result::Result<HistoryOutcome, RepairError> {
    let pr_key = target.reference();
    let repository = target.repository.as_str();
    let present: i64 = conn.query_row(
        "SELECT COUNT(*) FROM fact_pull_requests WHERE pr_key = ?1",
        [&pr_key],
        |row| row.get(0),
    )?;
    if present == 0 {
        return Err(Error::InvalidArgument(format!(
            "{pr_key} is not in the warehouse; run `sync targets --pr {pr_key}` first"
        ))
        .into());
    }
    let before = stored_runs(conn, &pr_key, None)?;
    let mut caveats = Vec::new();

    // ---- discover commits (network only) ----
    let mut sources: BTreeMap<String, Vec<HeadSource>> = BTreeMap::new();
    let mut add = |sha: &str, source: HeadSource| {
        let entry = sources.entry(sha.to_ascii_lowercase()).or_default();
        if !entry.contains(&source) {
            entry.push(source);
        }
    };
    for sha in stored_heads(conn, &pr_key)? {
        add(&sha, HeadSource::StoredHead);
    }
    let listed = check_history::fetch_pull_request_commits(client, repository, target.number)?;
    add(&listed.head_sha, HeadSource::CurrentHead);
    if options.observed_only {
        caveats
            .push("observed-only: commit list and force-push history were not walked".to_string());
    } else {
        if (listed.commits.len() as i64) < listed.commit_count {
            caveats.push(format!(
                "provider listed {} of {} commits (GitHub lists at most {}); runs on unlisted \
                 commits that were never a stored head are not recoverable here",
                listed.commits.len(),
                listed.commit_count,
                check_history::PR_COMMIT_LIST_LIMIT
            ));
        }
        for sha in &listed.commits {
            add(sha, HeadSource::CommitList);
        }
        let pushes = check_history::fetch_force_pushes(client, repository, target.number)?;
        let unresolved = pushes.iter().filter(|push| push.before.is_none()).count();
        if unresolved > 0 {
            caveats.push(format!(
                "{unresolved} force push(es) replaced a commit the provider no longer resolves; \
                 runs on those heads are unavailable unless already stored"
            ));
        }
        for push in &pushes {
            for sha in [&push.before, &push.after].into_iter().flatten() {
                add(sha, HeadSource::ForcePush);
            }
        }
    }
    let closed_at = parse_time(listed.closed_at.as_deref());
    let context = Context {
        repository,
        number: target.number,
        head_ref: listed.head_ref.as_deref(),
        head_repository: listed.head_repository.as_deref(),
        created_at: parse_time(listed.created_at.as_deref()),
        closed_at,
        closed_unknown: listed.closed_at.is_some() && closed_at.is_none(),
    };

    // ---- fetch runs and suites per commit (network only) ----
    let mut fetched: Vec<Fetched> = Vec::new();
    for (sha, mut head_sources) in sources {
        lease.renew(conn)?;
        // Every newly discovered commit costs requests: stop before one the
        // observed quota (or the batch cap) could not cover.
        if budget.is_some() && client_allowance_low(client) {
            return Err(Error::QuotaReserve(
                "quota headroom or request cap cannot cover another commit".into(),
            )
            .into());
        }
        head_sources.sort();
        let strong = head_sources
            .iter()
            .any(|source| *source != HeadSource::CommitList);
        let mut head = HeadOutcome {
            sha: sha.clone(),
            sources: head_sources,
            stored_before: stored_runs(conn, &pr_key, Some(&sha))?,
            ..Default::default()
        };
        let mut runs = match check_history::fetch_commit_check_runs(client, repository, &sha)? {
            CommitCheckRuns::Complete(runs) => runs,
            CommitCheckRuns::Unavailable(reason) => {
                head.unavailable = Some(reason);
                fetched.push(Fetched {
                    head,
                    runs: Vec::new(),
                });
                continue;
            }
        };
        head.upstream_runs = Some(runs.len());
        if runs.is_empty() {
            fetched.push(Fetched { head, runs });
            continue;
        }
        let suites: BTreeMap<i64, CheckSuite> =
            check_history::fetch_commit_check_suites(client, repository, &sha)?
                .into_iter()
                .map(|suite| (suite.id, suite))
                .collect();
        let others_stored: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT pr_key) FROM fact_check_runs
             WHERE lower(head_sha) = ?1 AND pr_key <> ?2",
            [&sha, &pr_key],
            |row| row.get(0),
        )?;
        let mut shared = others_stored > 0
            || runs
                .iter()
                .any(|run| run.pull_requests.iter().any(|n| *n != target.number));
        // Pull requests the provider says contain the commit. Asked for every
        // commit, stored heads included: a stored head is discovery, not
        // proof, so it never exempts a commit from the shared-commit check.
        let owners = check_history::fetch_commit_pull_requests(client, repository, &sha)?;
        if !strong && !owners.is_empty() && !owners.contains(&target.number) {
            // A listed commit can belong to another pull request (a stacked
            // or merged branch). A positive provider answer naming only other
            // pull requests is enough to leave it out.
            head.skipped = Some(format!(
                "commit belongs to pull request(s) {owners:?} per the provider"
            ));
            head.attributed_elsewhere = runs.len();
            runs.clear();
            fetched.push(Fetched { head, runs });
            continue;
        }
        shared |= owners.iter().any(|n| *n != target.number);
        // Workflow runs carry the identity a suite lacks (event, head
        // repository, pull requests), keyed by suite id. Read only when a
        // run here is a GitHub Actions run of this repository.
        let workflows = if runs.iter().any(|run| {
            run.app_slug.as_deref() == Some(check_history::ACTIONS_APP)
                && run
                    .suite_id
                    .and_then(|id| suites.get(&id))
                    .is_some_and(|suite| {
                        suite.app_slug.as_deref() == Some(check_history::ACTIONS_APP)
                    })
        }) {
            check_history::fetch_commit_workflow_runs(client, repository, &sha)?
        } else {
            Vec::new()
        };
        head.shared_head = shared;
        let mut own = Vec::new();
        for run in runs {
            let attribution = attribute(&run, &suites, &workflows, &context, shared);
            if attribution == Attribution::Own {
                own.push(run);
                continue;
            }
            match existing_owner(conn, &run.node_id)? {
                Some(owner) if owner == pr_key => head.stored_unconfirmed.push(run.node_id.clone()),
                Some(owner) => {
                    head.owned_by_other_pr.insert(run.node_id.clone(), owner);
                }
                None => {}
            }
            match attribution {
                Attribution::Elsewhere(_) => head.attributed_elsewhere += 1,
                Attribution::Ambiguous(reason) => {
                    head.ambiguous.insert(run.node_id.clone(), reason);
                }
                Attribution::Own => unreachable!(),
            }
        }
        fetched.push(Fetched { head, runs: own });
    }
    lease.renew(conn)?;

    // ---- write everything together; final counts read before commit ----
    conn.execute_batch("SAVEPOINT githubdw_check_history")?;
    let written = (|| -> Result<i64> {
        for item in &mut fetched {
            for run in &item.runs {
                let write = repository::record_check_run(
                    conn,
                    &run.node_id,
                    &pr_key,
                    &run.head_sha,
                    &run.name,
                    &run.status,
                    run.conclusion.as_deref(),
                    run.started_at.as_deref(),
                    run.completed_at.as_deref(),
                )?;
                match write {
                    CheckRunWrite::Inserted => item.head.inserted += 1,
                    CheckRunWrite::Updated => item.head.updated += 1,
                    CheckRunWrite::Unchanged => item.head.unchanged += 1,
                    CheckRunWrite::OwnedByOtherPullRequest(owner) => {
                        // Provider evidence proves this pull request, the
                        // row says another: a conflict, never moved here.
                        item.head
                            .ownership_conflicts
                            .insert(run.node_id.clone(), owner);
                    }
                    CheckRunWrite::HeadMismatch(stored) => {
                        item.head.head_mismatch.insert(run.node_id.clone(), stored);
                    }
                }
            }
            item.head.stored_after = stored_runs(conn, &pr_key, Some(&item.head.sha))?;
        }
        stored_runs(conn, &pr_key, None)
    })();
    let after = match written {
        Ok(after) => match (
            run_before_release_hook(),
            sql(
                conn,
                Fault::Release,
                "RELEASE SAVEPOINT githubdw_check_history",
            ),
        ) {
            ((), Ok(())) => after,
            ((), Err(error)) => {
                // The commit is unconfirmed. Try not to leave the savepoint
                // open; whether rows changed stays unknown either way.
                let _ = conn.execute_batch(
                    "ROLLBACK TO SAVEPOINT githubdw_check_history;
                     RELEASE SAVEPOINT githubdw_check_history",
                );
                return Err(RepairError {
                    error,
                    rows_unchanged: None,
                });
            }
        },
        Err(error) => {
            let rolled_back = sql(
                conn,
                Fault::Rollback,
                "ROLLBACK TO SAVEPOINT githubdw_check_history;
                 RELEASE SAVEPOINT githubdw_check_history",
            )
            .is_ok();
            return Err(RepairError {
                error,
                rows_unchanged: rolled_back.then_some(true),
            });
        }
    };

    // ---- account for what could not be completed ----
    let heads: Vec<HeadOutcome> = fetched.into_iter().map(|item| item.head).collect();
    let unavailable: Vec<&str> = heads
        .iter()
        .filter(|head| head.unavailable.is_some())
        .map(|head| head.sha.as_str())
        .collect();
    if !unavailable.is_empty() {
        caveats.push(format!(
            "provider no longer serves {} commit(s): {}",
            unavailable.len(),
            unavailable.join(", ")
        ));
    }
    let ambiguous: usize = heads.iter().map(|head| head.ambiguous.len()).sum();
    if ambiguous > 0 {
        caveats.push(format!(
            "{ambiguous} run(s) could not be attributed to this pull request from provider \
             suite evidence; not written"
        ));
    }
    let elsewhere: usize = heads
        .iter()
        .filter(|head| head.skipped.is_none())
        .map(|head| head.attributed_elsewhere)
        .sum();
    if elsewhere > 0 {
        caveats.push(format!(
            "{elsewhere} run(s) on this pull request's commits are attributed by their suite to \
             another pull request; a shared commit cannot prove they are not this one's"
        ));
    }
    let unconfirmed: usize = heads.iter().map(|head| head.stored_unconfirmed.len()).sum();
    if unconfirmed > 0 {
        caveats.push(format!(
            "{unconfirmed} run(s) stored under this pull request are not confirmed by provider \
             evidence; left unchanged for investigation"
        ));
    }
    let shared: usize = heads.iter().map(|head| head.owned_by_other_pr.len()).sum();
    if shared > 0 {
        caveats.push(format!(
            "{shared} run(s) are already stored under another pull request; a check run row \
             holds one pull request, so they were left there"
        ));
    }
    let conflicts: usize = heads
        .iter()
        .map(|head| head.ownership_conflicts.len())
        .sum();
    if conflicts > 0 {
        caveats.push(format!(
            "CONFLICT: {conflicts} run(s) the provider's workflow-run evidence attributes to this \
             pull request are stored under another pull request; not moved, needs review"
        ));
    }
    let mismatched: usize = heads.iter().map(|head| head.head_mismatch.len()).sum();
    if mismatched > 0 {
        caveats.push(format!(
            "{mismatched} stored run(s) record a different commit than the provider; left \
             unchanged for investigation"
        ));
    }
    let status = if caveats.is_empty() {
        HistoryStatus::Complete
    } else {
        HistoryStatus::Incomplete
    };
    Ok(HistoryOutcome {
        target: target.clone(),
        status,
        error: None,
        caveats,
        rows_unchanged: None,
        lock_release: None,
        check_runs_before: before,
        check_runs_after: after,
        heads,
    })
}
