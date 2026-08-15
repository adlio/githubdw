//! githubdw command-line interface.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use githubdw::GithubDW;

#[derive(Parser)]
#[command(
    name = "githubdw",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_SHA"), ")"),
    about = "Local SQLite data warehouse for GitHub: sync PRs and issues, query, metrics, search, MCP"
)]
struct CommandLine {
    /// Path to the SQLite database (default: ~/.githubdw/githubdw.db)
    #[arg(long, global = true)]
    db: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sync data from GitHub into the warehouse
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    /// Fulltext search over PR/issue titles, bodies, and comments
    Search {
        /// Search terms (trigram fulltext; 3+ character substrings match)
        query: String,
        /// Search only pull requests
        #[arg(long)]
        pull_requests: bool,
        /// Search only issues
        #[arg(long)]
        issues: bool,
        /// Search only comments
        #[arg(long)]
        comments: bool,
        /// Restrict to one repository ("owner/name")
        #[arg(long)]
        repo: Option<String>,
        /// Maximum results
        #[arg(long, default_value_t = 20)]
        limit: u32,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
    /// Query pull requests with filters
    Query(Box<QueryArguments>),
    /// Metrics with period-over-period deltas
    Metrics {
        #[command(subcommand)]
        command: MetricsCommand,
    },
    /// Manage monitored users, repositories, and organizations
    Monitor {
        #[command(subcommand)]
        command: MonitorCommand,
    },
    /// Manage custom user and repository groups
    Group {
        #[command(subcommand)]
        command: GroupCommand,
    },
    /// Read or write configuration values
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Run the MCP (Model Context Protocol) stdio server
    Mcp,
}

#[derive(clap::Args)]
struct QueryArguments {
    /// Filter by author login
    #[arg(long)]
    author: Option<String>,
    /// Filter by reviewer login
    #[arg(long)]
    reviewer: Option<String>,
    /// Filter by repository ("owner/name")
    #[arg(long)]
    repo: Option<String>,
    /// Filter by organization (repo owner)
    #[arg(long)]
    org: Option<String>,
    /// Filter by group (user- or repo-group)
    #[arg(long)]
    group: Option<String>,
    /// Filter by label name
    #[arg(long)]
    label: Option<String>,
    /// Filter by state (open|merged|closed)
    #[arg(long)]
    state: Option<String>,
    /// Shorthand for --state merged
    #[arg(long)]
    merged: bool,
    /// Period filter (2026-Q1, 2026-01, 2026-W02, last-30, this-quarter, ...)
    #[arg(long)]
    period: Option<String>,
    /// Inclusive start date (YYYY-MM-DD)
    #[arg(long)]
    since: Option<String>,
    /// Inclusive end date (YYYY-MM-DD)
    #[arg(long)]
    until: Option<String>,
    /// Maximum rows
    #[arg(long)]
    limit: Option<u32>,
    /// Row offset
    #[arg(long)]
    offset: Option<u32>,
    /// Output format
    #[arg(long, default_value = "table")]
    output: String,
}

#[derive(Subcommand)]
enum SyncCommand {
    /// Sync a single repository ("owner/name")
    Repo {
        /// Repository in "owner/name" form
        repository: String,
        /// Limit the sync window to the last N days
        #[arg(long)]
        days: Option<u32>,
        /// Skip fetching per-file patch text (fast mode)
        #[arg(long)]
        skip_diffs: bool,
        /// Sync only pull requests
        #[arg(long)]
        pull_requests_only: bool,
        /// Sync only issues
        #[arg(long)]
        issues_only: bool,
    },
    /// Sync every enabled monitored source
    All,
    /// Watch live progress of a running sync
    Watch,
}

#[derive(Subcommand)]
enum MetricsCommand {
    /// Metrics for a single user
    User {
        /// GitHub login
        login: String,
        /// Period (default: this-quarter)
        #[arg(long, default_value = "this-quarter")]
        period: String,
        /// Leaderboard size
        #[arg(long, default_value_t = 5)]
        top: u32,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
    /// Metrics for a single repository
    Repo {
        /// Repository in "owner/name" form
        repository: String,
        /// Period (default: this-quarter)
        #[arg(long, default_value = "this-quarter")]
        period: String,
        /// Leaderboard size
        #[arg(long, default_value_t = 5)]
        top: u32,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
    /// Metrics for a custom group
    Group {
        /// Group name
        name: String,
        /// Period (default: this-quarter)
        #[arg(long, default_value = "this-quarter")]
        period: String,
        /// Emit JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum GroupCommand {
    /// Manage user groups
    User {
        #[command(subcommand)]
        action: GroupAction,
    },
    /// Manage repository groups
    Repo {
        #[command(subcommand)]
        action: GroupAction,
    },
}

#[derive(Subcommand)]
enum GroupAction {
    /// Create a group
    Create {
        name: String,
        /// Optional description
        #[arg(long)]
        description: Option<String>,
    },
    /// Delete a group
    Delete { name: String },
    /// Show a group's members
    Show { name: String },
    /// List all groups
    List,
    /// Add members to a group
    Add {
        name: String,
        /// Members (logins or owner/name repos)
        members: Vec<String>,
    },
    /// Remove members from a group
    Remove { name: String, members: Vec<String> },
    /// Replace a group's member set
    Set { name: String, members: Vec<String> },
}

#[derive(Subcommand)]
enum MonitorCommand {
    /// Add a repository to the monitored set
    AddRepo {
        /// Repository in "owner/name" form
        repository: String,
    },
    /// Add a user to the monitored set
    AddUser {
        /// GitHub login
        login: String,
    },
    /// Add an organization to the monitored set
    AddOrg {
        /// Organization login
        org: String,
    },
    /// List monitored sources
    List,
    /// Remove a monitored source
    Remove {
        /// Source identifier (login or "owner/name")
        source: String,
    },
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Get a configuration value
    Get {
        /// Configuration key
        key: String,
    },
    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,
        /// New value
        value: String,
    },
    /// List all configuration values
    List,
}

fn parse_date_argument(text: &str) -> githubdw::Result<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|error| githubdw::Error::InvalidArgument(format!("bad date '{text}': {error}")))
}

fn print_grouped(rows: Vec<(String, u64)>) {
    for (group, count) in rows {
        println!("{group}\t{count}");
    }
}

fn print_leaderboard(title: &str, rows: &[githubdw::metrics::EntityMetric]) {
    if rows.is_empty() {
        return;
    }
    println!("  {title}:");
    for row in rows {
        println!("    {}", row.render());
    }
}

fn open_warehouse(db: Option<PathBuf>) -> githubdw::Result<GithubDW> {
    match db {
        Some(path) => GithubDW::open(path),
        None => GithubDW::open_default(),
    }
}

fn run(command_line: CommandLine) -> githubdw::Result<()> {
    match command_line.command {
        Command::Config { command } => {
            let warehouse = open_warehouse(command_line.db)?;
            match command {
                ConfigCommand::Get { key } => {
                    let value: Option<String> = warehouse
                        .connection()
                        .query_row("SELECT value FROM config WHERE key = ?1", [&key], |row| {
                            row.get(0)
                        })
                        .map(Some)
                        .or_else(|error| match error {
                            rusqlite::Error::QueryReturnedNoRows => Ok(None),
                            other => Err(other),
                        })?;
                    match value {
                        Some(value) => println!("{value}"),
                        None => {
                            return Err(githubdw::Error::NotFound(format!("config key '{key}'")));
                        }
                    }
                }
                ConfigCommand::Set { key, value } => {
                    warehouse.connection().execute(
                        "INSERT INTO config (key, value) VALUES (?1, ?2)
                         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                        [&key, &value],
                    )?;
                    println!("{key} = {value}");
                }
                ConfigCommand::List => {
                    let connection = warehouse.connection();
                    let mut statement =
                        connection.prepare("SELECT key, value FROM config ORDER BY key")?;
                    let rows = statement.query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?;
                    for row in rows {
                        let (key, value) = row?;
                        println!("{key} = {value}");
                    }
                }
            }
            Ok(())
        }
        Command::Sync { command } => {
            let warehouse = open_warehouse(command_line.db)?;
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
                    let mut syncer =
                        githubdw::sync::Syncer::new(warehouse.connection(), &mut client);
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
                SyncCommand::All => {
                    let mut client = githubdw::fetch::GhClient::new();
                    client.preflight()?;
                    let sources =
                        githubdw::storage::monitor_repository::list(warehouse.connection())?;
                    let repos: Vec<_> = sources
                        .into_iter()
                        .filter(|source| source.sync_enabled && source.source_type == "repo")
                        .collect();
                    if repos.is_empty() {
                        println!(
                            "no enabled monitored repositories — use `githubdw monitor add-repo`"
                        );
                    }
                    for source in repos {
                        let options = githubdw::sync::SyncOptions::default();
                        let mut syncer =
                            githubdw::sync::Syncer::new(warehouse.connection(), &mut client);
                        match syncer.sync_repository(&source.identifier, &options) {
                            Ok(summary) => println!(
                                "{}: {} PRs, {} issues",
                                source.identifier,
                                summary.pull_requests_synced,
                                summary.issues_synced
                            ),
                            Err(error) => eprintln!("{}: {error}", source.identifier),
                        }
                    }
                }
                SyncCommand::Watch => {
                    /// One live lock row: entity, started, current index/id, counters.
                    type LockRow = (String, String, i64, Option<String>, i64, i64, i64);
                    /// One recent job row: entity, status, started, completed, synced.
                    type JobRow = (String, String, String, Option<String>, i64);
                    let connection = warehouse.connection();
                    let mut statement = connection.prepare(
                        "SELECT entity_key, started_at, current_item, current_item_id,
                                synced, skipped, failed
                         FROM sync_locks ORDER BY started_at",
                    )?;
                    let rows: Vec<LockRow> = statement
                        .query_map([], |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                                row.get(6)?,
                            ))
                        })?
                        .collect::<std::result::Result<_, _>>()?;
                    if rows.is_empty() {
                        println!("no sync in progress");
                        let mut jobs = connection.prepare(
                            "SELECT entity_key, status, started_at, completed_at, synced
                             FROM sync_jobs ORDER BY started_at DESC LIMIT 5",
                        )?;
                        let job_rows: Vec<JobRow> = jobs
                            .query_map([], |row| {
                                Ok((
                                    row.get(0)?,
                                    row.get(1)?,
                                    row.get(2)?,
                                    row.get(3)?,
                                    row.get(4)?,
                                ))
                            })?
                            .collect::<std::result::Result<_, _>>()?;
                        for (entity, status, started, completed, synced) in job_rows {
                            println!(
                                "{entity}: {status} (started {started}, finished {}, {synced} synced)",
                                completed.unwrap_or_else(|| "-".into())
                            );
                        }
                    }
                    for (entity, started, current, current_id, synced, skipped, failed) in rows {
                        println!(
                            "{entity}: item {current} ({}) since {started} — {synced} synced, {skipped} skipped, {failed} failed",
                            current_id.unwrap_or_else(|| "-".into())
                        );
                    }
                }
            }
            Ok(())
        }
        Command::Monitor { command } => {
            let warehouse = open_warehouse(command_line.db)?;
            let connection = warehouse.connection();
            match command {
                MonitorCommand::AddRepo { repository } => {
                    githubdw::storage::monitor_repository::add_repo(connection, &repository)?;
                    println!("monitoring repo {}", repository.to_lowercase());
                }
                MonitorCommand::AddUser { login } => {
                    githubdw::storage::monitor_repository::add_user(connection, &login)?;
                    println!("monitoring user {}", login.to_lowercase());
                }
                MonitorCommand::AddOrg { org } => {
                    githubdw::storage::monitor_repository::add_org(connection, &org)?;
                    println!("monitoring org {}", org.to_lowercase());
                }
                MonitorCommand::List => {
                    let sources = githubdw::storage::monitor_repository::list(connection)?;
                    if sources.is_empty() {
                        println!("nothing monitored yet");
                    }
                    for source in sources {
                        println!(
                            "{}\t{}\t{}\tlast sync: {}",
                            source.source_type,
                            source.identifier,
                            if source.sync_enabled {
                                "enabled"
                            } else {
                                "disabled"
                            },
                            source.last_sync_at.unwrap_or_else(|| "never".into())
                        );
                    }
                }
                MonitorCommand::Remove { source } => {
                    let removed =
                        githubdw::storage::monitor_repository::remove(connection, &source)?;
                    if removed == 0 {
                        return Err(githubdw::Error::NotFound(format!(
                            "monitored source '{source}'"
                        )));
                    }
                    println!("removed {source}");
                }
            }
            Ok(())
        }
        Command::Query(arguments) => {
            let QueryArguments {
                author,
                reviewer,
                repo,
                org,
                group,
                label,
                state,
                merged,
                period,
                since,
                until,
                limit,
                offset,
                output,
            } = *arguments;
            let warehouse = open_warehouse(command_line.db)?;
            let connection = warehouse.connection();
            let mut builder = githubdw::QueryBuilder::new(connection);
            if let Some(author) = author.as_deref() {
                builder = builder.author(author);
            }
            if let Some(reviewer) = reviewer.as_deref() {
                builder = builder.reviewer(reviewer);
            }
            if let Some(repo) = repo.as_deref() {
                builder = builder.repo(repo);
            }
            if let Some(org) = org.as_deref() {
                builder = builder.org(org);
            }
            if let Some(group_name) = group.as_deref() {
                match githubdw::groups::kind_of(connection, group_name)? {
                    githubdw::groups::GroupKind::User => {
                        let members = githubdw::groups::members(
                            connection,
                            githubdw::groups::GroupKind::User,
                            group_name,
                        )?;
                        builder = builder.authors(&members);
                    }
                    githubdw::groups::GroupKind::Repo => {
                        let members = githubdw::groups::members(
                            connection,
                            githubdw::groups::GroupKind::Repo,
                            group_name,
                        )?;
                        builder = builder.repos(&members);
                    }
                }
            }
            if let Some(label) = label.as_deref() {
                builder = builder.label(label);
            }
            if merged {
                builder = builder.merged();
            }
            if let Some(state) = state.as_deref() {
                let parsed = match state.to_lowercase().as_str() {
                    "open" => githubdw::PrState::Open,
                    "merged" => githubdw::PrState::Merged,
                    "closed" => githubdw::PrState::Closed,
                    other => {
                        return Err(githubdw::Error::InvalidArgument(format!(
                            "unknown state '{other}'"
                        )));
                    }
                };
                builder = builder.state(parsed);
            }
            if let Some(period_text) = period.as_deref() {
                let parsed = githubdw::Period::parse(period_text)?;
                match parsed {
                    githubdw::Period::Rolling(..) => {
                        let (start, end) = parsed.date_range();
                        builder = builder.between(start, end);
                    }
                    other => builder = builder.period(other),
                }
            }
            if let Some(since) = since.as_deref() {
                builder = builder.since(parse_date_argument(since)?);
            }
            if let Some(until) = until.as_deref() {
                builder = builder.until(parse_date_argument(until)?);
            }
            if let Some(limit) = limit {
                builder = builder.limit(limit);
            }
            if let Some(offset) = offset {
                builder = builder.offset(offset);
            }

            match output.as_str() {
                "count" => println!("{}", builder.count()?),
                "count-by-author" => print_grouped(builder.count_by_author()?),
                "count-by-repo" => print_grouped(builder.count_by_repo()?),
                "count-by-state" => print_grouped(builder.count_by_state()?),
                "count-by-period" => print_grouped(builder.count_by_period()?),
                "json" => println!("{}", builder.to_json()?),
                "csv" => print!("{}", builder.to_csv()?),
                "table" => {
                    let rows = builder.pull_requests()?;
                    let total = rows.len();
                    for row in rows.iter().take(25) {
                        println!(
                            "{}\t{}\t{}\t{}\t{}",
                            row.pr_key,
                            row.state,
                            row.author,
                            row.created_at,
                            row.title.as_deref().unwrap_or("")
                        );
                    }
                    if total > 25 {
                        println!("… and {} more", total - 25);
                    }
                }
                other => {
                    return Err(githubdw::Error::InvalidArgument(format!(
                        "unknown output format '{other}'"
                    )));
                }
            }
            Ok(())
        }
        Command::Metrics { command } => {
            let warehouse = open_warehouse(command_line.db)?;
            let connection = warehouse.connection();
            let engine = githubdw::metrics::MetricsEngine::new(connection);
            match command {
                MetricsCommand::User {
                    login,
                    period,
                    top,
                    json,
                } => {
                    let period = githubdw::Period::parse(&period)?;
                    let metrics = engine.user_metrics(&login, &period)?;
                    let aggregations = engine.user_aggregations(&login, &period, top)?;
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "metrics": metrics,
                                "aggregations": aggregations,
                            }))?
                        );
                    } else {
                        println!(
                            "User {} — {} (vs {})",
                            metrics.login, metrics.period_key, metrics.previous_period_key
                        );
                        println!("  PRs opened:       {}", metrics.prs_opened.render());
                        println!("  PRs merged:       {}", metrics.prs_merged.render());
                        println!("  Reviews given:    {}", metrics.reviews_given.render());
                        println!("  Reviews received: {}", metrics.reviews_received.render());
                        println!("  Comments given:   {}", metrics.comments_given.render());
                        println!("  Lines added:      {}", metrics.lines_added.render());
                        println!("  Lines removed:    {}", metrics.lines_removed.render());
                        print_leaderboard("Top repos", &aggregations.top_repos);
                        print_leaderboard("Top reviewers", &aggregations.top_reviewers);
                        print_leaderboard(
                            "Top reviewed authors",
                            &aggregations.top_reviewed_authors,
                        );
                    }
                }
                MetricsCommand::Repo {
                    repository,
                    period,
                    top,
                    json,
                } => {
                    let period = githubdw::Period::parse(&period)?;
                    let metrics = engine.repo_metrics(&repository, &period)?;
                    let aggregations = engine.repo_aggregations(&repository, &period, top)?;
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&serde_json::json!({
                                "metrics": metrics,
                                "aggregations": aggregations,
                            }))?
                        );
                    } else {
                        println!(
                            "Repo {} — {} (vs {})",
                            metrics.repository, metrics.period_key, metrics.previous_period_key
                        );
                        println!("  PRs opened:  {}", metrics.prs_opened.render());
                        println!("  PRs merged:  {}", metrics.prs_merged.render());
                        println!("  Reviews:     {}", metrics.total_reviews.render());
                        println!("  Comments:    {}", metrics.total_comments.render());
                        if let Some(rate) = metrics.check_failure_rate {
                            println!("  Check failure rate: {:.1}%", rate * 100.0);
                        }
                        print_leaderboard("Top contributors", &aggregations.top_contributors);
                        print_leaderboard("Top mergers", &aggregations.top_mergers);
                        print_leaderboard("Top reviewers", &aggregations.top_reviewers);
                        print_leaderboard("Top commenters", &aggregations.top_commenters);
                    }
                }
                MetricsCommand::Group { name, period, json } => {
                    let period = githubdw::Period::parse(&period)?;
                    let kind = githubdw::groups::kind_of(connection, &name)?;
                    let group_metrics = match kind {
                        githubdw::groups::GroupKind::User => {
                            let members = githubdw::groups::members(
                                connection,
                                githubdw::groups::GroupKind::User,
                                &name,
                            )?;
                            engine.user_group_metrics(&name, &members, &period)?
                        }
                        githubdw::groups::GroupKind::Repo => {
                            let members = githubdw::groups::members(
                                connection,
                                githubdw::groups::GroupKind::Repo,
                                &name,
                            )?;
                            engine.repo_group_metrics(&name, &members, &period)?
                        }
                    };
                    if json {
                        println!("{}", serde_json::to_string_pretty(&group_metrics)?);
                    } else {
                        println!(
                            "Group {} ({} group, {} members) — {} (vs {})",
                            group_metrics.group_name,
                            group_metrics.kind,
                            group_metrics.member_count,
                            group_metrics.period_key,
                            group_metrics.previous_period_key
                        );
                        println!("  PRs opened: {}", group_metrics.prs_opened.render());
                        println!("  PRs merged: {}", group_metrics.prs_merged.render());
                        if let Some(reviews) = &group_metrics.reviews_given {
                            println!("  Reviews given (to non-members): {}", reviews.render());
                        }
                        if let Some(reviews) = &group_metrics.total_reviews {
                            println!("  Reviews: {}", reviews.render());
                        }
                        if let Some(comments) = &group_metrics.total_comments {
                            println!("  Comments: {}", comments.render());
                        }
                    }
                }
            }
            Ok(())
        }
        Command::Group { command } => {
            let warehouse = open_warehouse(command_line.db)?;
            let connection = warehouse.connection();
            let (kind, action) = match command {
                GroupCommand::User { action } => (githubdw::groups::GroupKind::User, action),
                GroupCommand::Repo { action } => (githubdw::groups::GroupKind::Repo, action),
            };
            match action {
                GroupAction::Create { name, description } => {
                    githubdw::groups::create_group(
                        connection,
                        kind,
                        &name,
                        description.as_deref(),
                    )?;
                    println!("created group {name}");
                }
                GroupAction::Delete { name } => {
                    githubdw::groups::delete_group(connection, kind, &name)?;
                    println!("deleted group {name}");
                }
                GroupAction::Show { name } => {
                    for member in githubdw::groups::members(connection, kind, &name)? {
                        println!("{member}");
                    }
                }
                GroupAction::List => {
                    for (name, description, count) in
                        githubdw::groups::list_groups(connection, kind)?
                    {
                        println!(
                            "{name}\t{count} members\t{}",
                            description.unwrap_or_default()
                        );
                    }
                }
                GroupAction::Add { name, members } => {
                    githubdw::groups::add_members(connection, kind, &name, &members)?;
                    println!("added {} member(s) to {name}", members.len());
                }
                GroupAction::Remove { name, members } => {
                    githubdw::groups::remove_members(connection, kind, &name, &members)?;
                    println!("removed {} member(s) from {name}", members.len());
                }
                GroupAction::Set { name, members } => {
                    githubdw::groups::set_members(connection, kind, &name, &members)?;
                    println!("set {name} to {} member(s)", members.len());
                }
            }
            Ok(())
        }
        Command::Search {
            query,
            pull_requests,
            issues,
            comments,
            repo,
            limit,
            json,
        } => {
            let warehouse = open_warehouse(command_line.db)?;
            let scope = match (pull_requests, issues, comments) {
                (true, false, false) => githubdw::search::SearchScope::PullRequests,
                (false, true, false) => githubdw::search::SearchScope::Issues,
                (false, false, true) => githubdw::search::SearchScope::Comments,
                (false, false, false) => githubdw::search::SearchScope::All,
                _ => {
                    return Err(githubdw::Error::InvalidArgument(
                        "pick at most one of --pull-requests / --issues / --comments".into(),
                    ));
                }
            };
            let options = githubdw::search::SearchOptions {
                scope,
                repository: repo.map(|value| value.to_lowercase()),
                limit,
            };
            let hits = githubdw::search::search(warehouse.connection(), &query, &options)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&hits)?);
            } else if hits.is_empty() {
                println!("no matches");
            } else {
                for hit in hits {
                    println!(
                        "{}\t{}\t{}\t{}",
                        hit.key,
                        hit.kind,
                        hit.title.as_deref().unwrap_or("-"),
                        hit.snippet.replace('\n', " ")
                    );
                }
            }
            Ok(())
        }
        Command::Mcp => {
            let warehouse = open_warehouse(command_line.db)?;
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(githubdw::mcp::serve_stdio(warehouse))
        }
    }
}

fn main() -> ExitCode {
    let command_line = CommandLine::parse();
    match run(command_line) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
