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

/// Exit code for "the run finished, but some source or item in it failed".
/// Distinct from [`ExitCode::FAILURE`] (1), which means the run never got to
/// do its work (bad arguments, `gh` missing or unauthenticated, lock held).
/// A cron job can therefore tell "retry later" (3) from "fix the invocation"
/// (1) apart.
pub(crate) const EXIT_PARTIAL_FAILURE: u8 = 3;

fn run(command_line: CommandLine) -> githubdw::Result<ExitCode> {
    let db = command_line.db;
    match command_line.command {
        Command::Config { command } => admin::config(open_warehouse(db)?, command).map(ok),
        Command::Monitor { command } => admin::monitor(open_warehouse(db)?, command).map(ok),
        Command::Group { command } => admin::group(open_warehouse(db)?, command).map(ok),
        Command::Sync { command } => sync::run(db, command),
        Command::Query(arguments) => report::query(open_warehouse(db)?, *arguments).map(ok),
        Command::Metrics { command } => report::metrics(open_warehouse(db)?, command).map(ok),
        Command::Search(arguments) => report::search(open_warehouse(db)?, arguments).map(ok),
        Command::Mcp => {
            let warehouse = open_warehouse(db)?;
            let runtime = tokio::runtime::Runtime::new()?;
            runtime
                .block_on(githubdw::mcp::serve_stdio(warehouse))
                .map(ok)
        }
    }
}

fn ok(_: ()) -> ExitCode {
    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    let command_line = CommandLine::parse();
    match run(command_line) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
