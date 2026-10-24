//! githubdw command-line interface.
//!
//! Each command module parses nothing and decides nothing beyond wiring
//! arguments to the library and printing its results.

mod admin;
mod cli;
mod report;
mod sync;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use cli::{Command, CommandLine};
use githubdw::GithubDW;

fn open_warehouse(db: Option<PathBuf>) -> githubdw::Result<GithubDW> {
    match db {
        Some(path) => GithubDW::open(path),
        None => GithubDW::open_default(),
    }
}

fn run(command_line: CommandLine) -> githubdw::Result<()> {
    let db = command_line.db;
    match command_line.command {
        Command::Config { command } => admin::config(open_warehouse(db)?, command),
        Command::Monitor { command } => admin::monitor(open_warehouse(db)?, command),
        Command::Group { command } => admin::group(open_warehouse(db)?, command),
        Command::Sync { command } => sync::run(db, command),
        Command::Query(arguments) => report::query(open_warehouse(db)?, *arguments),
        Command::Metrics { command } => report::metrics(open_warehouse(db)?, command),
        Command::Search(arguments) => report::search(open_warehouse(db)?, arguments),
        Command::Mcp => {
            let warehouse = open_warehouse(db)?;
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
