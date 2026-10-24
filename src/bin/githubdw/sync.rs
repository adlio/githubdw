//! `githubdw sync ...`: run walks, targeted refreshes, and repair plans.

use std::path::PathBuf;

use githubdw::GithubDW;
use githubdw::fetch::git_files::GitRecoveryConfig;
use githubdw::storage::sync_state_repository as sync_state;

use crate::cli::SyncCommand;
use crate::open_warehouse;

/// Pull requests one repository may process per `sync all` run before it
/// checkpoints and yields to the next source.
const SYNC_ALL_REPO_ITEM_BUDGET: i64 = 500;

/// Git-backed completion of capped pull-request file listings, cached next to
/// the warehouse database (never under a temp directory).
fn git_recovery_for(warehouse: &GithubDW) -> Option<GitRecoveryConfig> {
    let parent = warehouse.database_path().parent()?;
    if parent.as_os_str().is_empty() {
        return None;
    }
    Some(GitRecoveryConfig::github(parent.join("git-cache")))
}

pub fn run(db: Option<PathBuf>, command: SyncCommand) -> githubdw::Result<()> {
    // Plans only read: open without migrating or taking write locks.
    let warehouse = if matches!(
        &command,
        SyncCommand::RepairPlan { .. } | SyncCommand::CheckHistoryPlan { .. }
    ) {
        let path = db.map(Ok).unwrap_or_else(GithubDW::default_database_path)?;
        GithubDW::open_read_only(path)?
    } else {
        open_warehouse(db)?
    };
    match command {
        SyncCommand::Repo {
            repository,
            days,
            skip_diffs,
            pull_requests_only,
            issues_only,
        } => {
            let mut client = githubdw::fetch::GhClient::new();
            client.preflight()?;
            let options = githubdw::sync::SyncOptions {
                days,
                skip_diffs,
                pull_requests_only,
                issues_only,
            };
            let mut syncer = githubdw::sync::Syncer::new(warehouse.connection(), &mut client);
            if let Some(config) = git_recovery_for(&warehouse) {
                syncer = syncer.with_git_recovery(config);
            }
            let summary = syncer.sync_repository(&repository, &options)?;
            if summary.up_to_date && summary.issues_synced == 0 {
                println!("{repository}: already up to date");
            } else {
                println!(
                    "{repository}: synced {} pull requests, {} issues ({} pages, {} failed)",
                    summary.pull_requests_synced,
                    summary.issues_synced,
                    summary.pages_fetched,
                    summary.failed.len()
                );
            }
            for (item, error) in &summary.failed {
                eprintln!("failed {item}: {error}");
            }
        }
        SyncCommand::User {
            login,
            days,
            skip_diffs,
        } => {
            let mut client = githubdw::fetch::GhClient::new();
            client.preflight()?;
            let options = githubdw::sync::SyncOptions {
                days,
                skip_diffs,
                pull_requests_only: true,
                issues_only: false,
            };
            let mut syncer = githubdw::sync::Syncer::new(warehouse.connection(), &mut client);
            if let Some(config) = git_recovery_for(&warehouse) {
                syncer = syncer.with_git_recovery(config);
            }
            let summary = syncer.sync_user(&login, &options)?;
            if summary.up_to_date {
                println!("{login}: already up to date");
            } else {
                println!(
                    "{login}: synced {} pull requests authored or reviewed ({} pages, {} failed)",
                    summary.pull_requests_synced,
                    summary.pages_fetched,
                    summary.failed.len()
                );
            }
            for note in &summary.notes {
                println!("{note}");
            }
            for (item, error) in &summary.failed {
                eprintln!("failed {item}: {error}");
            }
        }
        SyncCommand::All => {
            let mut client = githubdw::fetch::GhClient::new();
            client.preflight()?;
            let sources = githubdw::storage::monitor_repository::list(warehouse.connection())?;
            let plan = githubdw::sync::plan_all(&sources);
            // Printed before any work, so a source this build cannot
            // serve is visible even if the run is interrupted.
            for notice in &plan.notices {
                println!("{notice}");
            }
            let sources = plan.fair_sources(warehouse.connection())?;
            for (index, source) in sources.iter().enumerate() {
                // Record the successor first: a killed run resumes at
                // another source rather than starving it indefinitely.
                githubdw::sync::SyncPlan::checkpoint_next_source(
                    warehouse.connection(),
                    &sources[(index + 1) % sources.len()],
                )?;
                let options = githubdw::sync::SyncOptions::default();
                let mut syncer = githubdw::sync::Syncer::new(warehouse.connection(), &mut client)
                    .with_item_budget(SYNC_ALL_REPO_ITEM_BUDGET);
                if let Some(config) = git_recovery_for(&warehouse) {
                    syncer = syncer.with_git_recovery(config);
                }
                let (label, outcome) = match source {
                    githubdw::sync::SyncSource::User(login) => {
                        (format!("user {login}"), syncer.sync_user(login, &options))
                    }
                    githubdw::sync::SyncSource::Repository(repo) => {
                        (repo.clone(), syncer.sync_repository(repo, &options))
                    }
                };
                match outcome {
                    Ok(summary) => {
                        println!(
                            "{label}: {} PRs, {} issues{}",
                            summary.pull_requests_synced,
                            summary.issues_synced,
                            if summary.partial {
                                " (partial, resumes next run)"
                            } else {
                                ""
                            }
                        );
                        for note in &summary.notes {
                            println!("  {note}");
                        }
                        for (item, error) in &summary.failed {
                            eprintln!("failed {item}: {error}");
                        }
                    }
                    Err(error) => eprintln!("{label}: {error}"),
                }
            }
        }
        SyncCommand::Targets {
            pull_requests,
            issues,
            skip_diffs,
            json,
        } => {
            use githubdw::refresh::{RefreshTarget, TargetKind, TargetStatus};
            let mut targets = Vec::new();
            for reference in &pull_requests {
                targets.push(RefreshTarget::parse(TargetKind::PullRequest, reference)?);
            }
            for reference in &issues {
                targets.push(RefreshTarget::parse(TargetKind::Issue, reference)?);
            }
            // Validate before touching the network, so a bad list fails fast.
            let targets = githubdw::refresh::validate_targets(targets)?;
            let mut client = githubdw::fetch::GhClient::new();
            client.preflight()?;
            let mut syncer = githubdw::sync::Syncer::new(warehouse.connection(), &mut client);
            if let Some(config) = git_recovery_for(&warehouse) {
                syncer = syncer.with_git_recovery(config);
            }
            let report = syncer
                .refresh_targets(&targets, &githubdw::refresh::RefreshOptions { skip_diffs })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                for outcome in &report.outcomes {
                    let reference = outcome.target.reference();
                    match outcome.status {
                        TargetStatus::Refreshed => {
                            let (before, after) = (&outcome.before, &outcome.after);
                            println!(
                                "{reference}: refreshed{} (comments {}->{}, reviews {}->{}, \
                                 review comments {}->{}, files {}->{}, labels {}->{}, \
                                 assignees {}->{})",
                                if outcome.caveats.is_empty() {
                                    ""
                                } else {
                                    " INCOMPLETE"
                                },
                                before.comments,
                                after.comments,
                                before.reviews,
                                after.reviews,
                                before.review_comments,
                                after.review_comments,
                                before.files,
                                after.files,
                                before.labels,
                                after.labels,
                                before.assignees,
                                after.assignees,
                            );
                            for caveat in &outcome.caveats {
                                println!("  caveat: {caveat}");
                            }
                        }
                        TargetStatus::Failed => eprintln!(
                            "{reference}: FAILED, stored rows unchanged: {}",
                            outcome.error.as_deref().unwrap_or("unknown error")
                        ),
                    }
                }
            }
            if report.failed() > 0 {
                return Err(githubdw::Error::GitHubApi(format!(
                    "{} of {} target(s) failed",
                    report.failed(),
                    report.outcomes.len()
                )));
            }
        }
        SyncCommand::CheckHistory {
            pull_requests,
            observed_only,
            max_requests,
            json,
        } => {
            use githubdw::check_history::{CheckHistoryOptions, HistoryStatus};
            use githubdw::refresh::{RefreshTarget, TargetKind};
            let mut targets = Vec::new();
            for reference in &pull_requests {
                targets.push(RefreshTarget::parse(TargetKind::PullRequest, reference)?);
            }
            let targets = githubdw::refresh::validate_targets(targets)?;
            let mut client = githubdw::fetch::GhClient::new();
            client.preflight()?;
            let report = githubdw::check_history::repair_check_history(
                warehouse.connection(),
                &mut client,
                &targets,
                &CheckHistoryOptions {
                    observed_only,
                    // Keep headroom for the scheduled sync's REST use.
                    rest_reserve: Some(500),
                    max_requests,
                },
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                for outcome in &report.outcomes {
                    let reference = outcome.target.reference();
                    match outcome.status {
                        HistoryStatus::Failed => eprintln!(
                            "{reference}: FAILED, {}: {}",
                            match outcome.rows_unchanged {
                                Some(true) => "stored rows unchanged",
                                _ => "stored rows UNKNOWN (commit not confirmed)",
                            },
                            outcome.error.as_deref().unwrap_or("unknown error")
                        ),
                        HistoryStatus::NotAttempted => eprintln!(
                            "{reference}: NOT ATTEMPTED, stored rows unchanged: {}",
                            outcome.error.as_deref().unwrap_or("batch stopped")
                        ),
                        status => {
                            println!(
                                "{reference}: {} (check runs {}->{}, {} commits read)",
                                if status == HistoryStatus::Complete {
                                    "complete"
                                } else {
                                    "INCOMPLETE"
                                },
                                outcome.check_runs_before,
                                outcome.check_runs_after,
                                outcome.heads.len()
                            );
                            for head in outcome.heads.iter().filter(|head| {
                                head.upstream_runs.unwrap_or(0) > 0
                                    || head.stored_before > 0
                                    || head.unavailable.is_some()
                            }) {
                                println!(
                                    "  {} upstream {} stored {}->{} (+{} ~{}){}",
                                    &head.sha[..head.sha.len().min(12)],
                                    head.upstream_runs
                                        .map(|n| n.to_string())
                                        .unwrap_or_else(|| "unavailable".into()),
                                    head.stored_before,
                                    head.stored_after,
                                    head.inserted,
                                    head.updated,
                                    head.skipped
                                        .as_deref()
                                        .map(|why| format!(" skipped: {why}"))
                                        .unwrap_or_default()
                                );
                            }
                            for caveat in &outcome.caveats {
                                println!("  caveat: {caveat}");
                            }
                        }
                    }
                }
            }
            if !json {
                for outcome in &report.outcomes {
                    if let Some(note) = &outcome.lock_release {
                        eprintln!("{}: {note}", outcome.target.reference());
                    }
                }
                if let Some(stopped) = &report.stopped {
                    eprintln!("batch {stopped}");
                }
            }
            // Anything short of complete is a failure for the caller,
            // reported after the full report (JSON included) is out.
            if report.not_complete() > 0 {
                return Err(githubdw::Error::GitHubApi(format!(
                    "{} incomplete, {} failed, {} not attempted of {} target(s)",
                    report.count(HistoryStatus::Incomplete),
                    report.count(HistoryStatus::Failed),
                    report.count(HistoryStatus::NotAttempted),
                    report.outcomes.len()
                )));
            }
        }
        SyncCommand::CheckHistoryPlan {
            repo,
            min_heads,
            limit,
            json,
        } => {
            let candidates = githubdw::check_history::history_candidates(
                warehouse.connection(),
                repo.as_deref(),
                min_heads,
                limit,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&candidates)?);
            } else if candidates.is_empty() {
                println!("no check-history candidates");
            } else {
                for candidate in &candidates {
                    println!(
                        "--pr {}\t# {} stored heads, {} stored runs",
                        candidate.target.reference(),
                        candidate.stored_heads,
                        candidate.stored_runs
                    );
                }
            }
        }
        SyncCommand::RepairPlan { repo, limit, json } => {
            let candidates = githubdw::refresh::repair_candidates(
                warehouse.connection(),
                repo.as_deref(),
                limit,
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&candidates)?);
            } else if candidates.is_empty() {
                println!("no repair candidates");
            } else {
                for candidate in &candidates {
                    let flag = match candidate.target.kind {
                        githubdw::refresh::TargetKind::PullRequest => "--pr",
                        githubdw::refresh::TargetKind::Issue => "--issue",
                    };
                    println!(
                        "{flag} {}\t# {}",
                        candidate.target.reference(),
                        candidate.reasons.join(", ")
                    );
                }
            }
        }
        SyncCommand::Watch => {
            let connection = warehouse.connection();
            let active = sync_state::active_syncs(connection)?;
            if active.is_empty() {
                println!("no sync in progress");
                for job in sync_state::recent_jobs(connection, 5)? {
                    println!(
                        "{}: {} (started {}, finished {}, {} synced)",
                        job.entity_key,
                        job.status,
                        job.started_at,
                        job.completed_at.unwrap_or_else(|| "-".into()),
                        job.synced
                    );
                }
            }
            for sync in active {
                println!(
                    "{}: item {} ({}) since {} — {} synced, {} skipped, {} failed",
                    sync.entity_key,
                    sync.current_item,
                    sync.current_item_id.unwrap_or_else(|| "-".into()),
                    sync.started_at,
                    sync.synced,
                    sync.skipped,
                    sync.failed
                );
            }
        }
    }
    Ok(())
}
