//! githubdw command-line interface.
//!
//! Each command module parses nothing and decides nothing beyond wiring
//! arguments to the library and printing its results.

mod admin;
mod cli;
mod notes;
mod report;
mod sync;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use cli::{Command, CommandLine};
use githubdw::GithubDW;

fn open_warehouse(db: Option<PathBuf>) -> githubdw::Result<GithubDW> {
    let warehouse = match db {
        Some(path) => GithubDW::open(path),
        None => GithubDW::open_default(),
    }?;
    release_locks_on_signal(warehouse.database_path().to_path_buf());
    Ok(warehouse)
}

/// On SIGTERM, SIGINT or SIGHUP (a systemd timeout, Ctrl-C, a closed
/// terminal), delete the sync locks this process holds, then exit with
/// 128 + the signal number. Without it a killed run leaves its lock rows
/// behind and blocks those entities until the rows age out. Started once.
fn release_locks_on_signal(path: PathBuf) {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            let code = runtime.block_on(async {
                use tokio::signal::unix::{SignalKind, signal};
                let (Ok(mut term), Ok(mut int), Ok(mut hup)) = (
                    signal(SignalKind::terminate()),
                    signal(SignalKind::interrupt()),
                    signal(SignalKind::hangup()),
                ) else {
                    return None;
                };
                Some(tokio::select! {
                    _ = term.recv() => 143,
                    _ = int.recv() => 130,
                    _ = hup.recv() => 129,
                })
            });
            let Some(code) = code else { return };
            match GithubDW::release_locks_held_by(&path, std::process::id()) {
                Ok(n) => eprintln!("stopped by signal; released {n} sync lock(s)"),
                Err(e) => eprintln!("stopped by signal; could not release sync locks: {e}"),
            }
            std::process::exit(code);
        });
    });
}

/// Exit code for "the run finished, but some source or item in it failed".
/// Distinct from [`ExitCode::FAILURE`] (1), which means the run never got to
/// do its work (bad arguments, `gh` missing or unauthenticated, lock held).
/// A cron job can therefore tell "retry later" (3) from "fix the invocation"
/// (1) apart.
pub(crate) const EXIT_PARTIAL_FAILURE: u8 = 3;

fn run(command_line: CommandLine) -> githubdw::Result<ExitCode> {
    let db = command_line.db;
    let max_rate_use = command_line.max_rate_use;
    let diff_retention = command_line.diff_retention;
    match command_line.command {
        Command::Config { command } => admin::config(open_warehouse(db)?, command).map(ok),
        Command::Monitor { command } => admin::monitor(open_warehouse(db)?, command).map(ok),
        Command::Group { command } => admin::group(open_warehouse(db)?, command).map(ok),
        Command::Sync { command } => sync::run(
            db,
            max_rate_use.as_deref(),
            diff_retention.as_deref(),
            command,
        ),
        Command::Diffs {
            command: cli::DiffsCommand::Purge { dry_run, vacuum },
        } => admin::purge_diffs(open_warehouse(db)?, dry_run, vacuum).map(ok),
        Command::Notes { command } => notes::run(open_warehouse(db)?, command).map(ok),
        Command::Query(arguments) => report::query(open_warehouse(db)?, *arguments).map(ok),
        Command::Metrics { command } => report::metrics(open_warehouse(db)?, command).map(ok),
        Command::Search(arguments) => report::search(open_warehouse(db)?, arguments).map(ok),
        Command::Mcp { read_only } => {
            let warehouse = open_warehouse(db)?;
            let retention = sync::resolve_retention(&warehouse, diff_retention.as_deref())?;
            let runtime = tokio::runtime::Runtime::new()?;
            runtime
                .block_on(githubdw::mcp::serve_stdio(warehouse, read_only, retention))
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
