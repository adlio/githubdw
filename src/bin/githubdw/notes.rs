//! `githubdw notes`: write and print change notes.

use githubdw::GithubDW;

use crate::cli::NotesCommand;

pub fn run(warehouse: GithubDW, command: NotesCommand) -> githubdw::Result<()> {
    match command {
        NotesCommand::Show { pr_keys } => {
            let notes = githubdw::notes::change_notes(warehouse.connection(), &pr_keys)?;
            for key in &pr_keys {
                match notes.get(key) {
                    Some(n) => println!(
                        "{key}\t{}{}",
                        n.note,
                        if n.current {
                            ""
                        } else {
                            "\t(written before the PR changed)"
                        }
                    ),
                    None => println!("{key}\t(no note)"),
                }
            }
            Ok(())
        }
        NotesCommand::Summarize {
            pr_keys,
            author,
            repo,
            since,
            model,
            aws_profile,
            no_diffs,
        } => {
            let mut keys = pr_keys;
            keys.extend(select(
                &warehouse,
                author.as_deref(),
                repo.as_deref(),
                since.as_deref(),
            )?);
            if keys.is_empty() {
                return Err(githubdw::Error::InvalidArgument(
                    "name pull requests, or pass --author or --repo".into(),
                ));
            }
            summarize(warehouse, keys, model, aws_profile, no_diffs)
        }
    }
}

/// Stored pull requests by author and/or repository, oldest first.
fn select(
    warehouse: &GithubDW,
    author: Option<&str>,
    repo: Option<&str>,
    since: Option<&str>,
) -> githubdw::Result<Vec<String>> {
    if author.is_none() && repo.is_none() {
        return Ok(vec![]);
    }
    let mut stmt = warehouse.connection().prepare(
        "SELECT pr_key FROM fact_pull_requests
         WHERE (?1 IS NULL OR author_key = 'user:' || lower(?1))
           AND (?2 IS NULL OR repo_key = lower(?2))
           AND (?3 IS NULL OR created_date_key >= ?3)
         ORDER BY created_at, pr_key",
    )?;
    let keys = stmt
        .query_map(rusqlite::params![author, repo, since], |r| r.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(keys)
}

#[cfg(feature = "summaries")]
fn summarize(
    warehouse: GithubDW,
    keys: Vec<String>,
    model: Option<String>,
    aws_profile: Option<String>,
    no_diffs: bool,
) -> githubdw::Result<()> {
    use githubdw::storage::config_repository::get;
    use githubdw::summaries::{
        BedrockNoteModel, CONFIG_AWS_PROFILE, CONFIG_MODEL, CONFIG_REGION, SummarizeOptions,
    };
    let conn = warehouse.connection();
    let model = match model {
        Some(m) => m,
        None => get(conn, CONFIG_MODEL)?.ok_or_else(|| {
            githubdw::Error::InvalidArgument(format!(
                "no model: pass --model or run `githubdw config set {CONFIG_MODEL} <model id>`"
            ))
        })?,
    };
    let profile = match aws_profile {
        Some(p) => Some(p),
        None => get(conn, CONFIG_AWS_PROFILE)?,
    };
    let region = get(conn, CONFIG_REGION)?;
    let path = warehouse.database_path().to_path_buf();
    drop(warehouse);
    let runtime = tokio::runtime::Runtime::new()?;
    let report = runtime.block_on(async {
        let model = BedrockNoteModel::new(profile.as_deref(), &model, region.as_deref()).await?;
        let mut options = SummarizeOptions::default();
        if no_diffs {
            options.diff_client = None;
        }
        githubdw::summaries::summarize_changes(&path, &keys, std::sync::Arc::new(model), &options)
            .await
    })?;
    println!(
        "{} written ({} unclear), {} already current, {} unknown, {} live diffs, {} missing",
        report.written,
        report.unclear,
        report.already_current,
        report.unknown.len(),
        report.diffs_fetched,
        report.missing.len()
    );
    for error in &report.errors {
        eprintln!("  {error}");
    }
    Ok(())
}

#[cfg(not(feature = "summaries"))]
fn summarize(
    _: GithubDW,
    _: Vec<String>,
    _: Option<String>,
    _: Option<String>,
    _: bool,
) -> githubdw::Result<()> {
    Err(githubdw::Error::InvalidArgument(
        "this githubdw was built without the `summaries` feature; reinstall with \
         `cargo install githubdw --features summaries`"
            .into(),
    ))
}
