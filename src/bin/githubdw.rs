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
        Command::Sync { .. }
        | Command::Search { .. }
        | Command::Query
        | Command::Metrics { .. }
        | Command::Monitor { .. }
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
