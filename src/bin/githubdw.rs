//! githubdw command-line interface.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use githubdw::GithubDW;

#[derive(Parser)]
#[command(
    name = "githubdw",
    version,
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
    },
    /// Query pull requests with filters
    Query,
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
    Group,
    /// Read or write configuration values
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Run the MCP (Model Context Protocol) stdio server
    Mcp,
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
    },
    /// Metrics for a single repository
    Repo {
        /// Repository in "owner/name" form
        repository: String,
    },
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
        Command::Search { .. }
        | Command::Query
        | Command::Metrics { .. }
        | Command::Group
        | Command::Mcp => {
            // Ensure the database exists/migrates even for stub commands.
            let _warehouse = open_warehouse(command_line.db)?;
            eprintln!("this command is not implemented yet");
            Err(githubdw::Error::InvalidArgument(
                "not implemented in this milestone".into(),
            ))
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
