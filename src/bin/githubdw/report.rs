//! Read-only reporting: query, metrics, and fulltext search.

use githubdw::GithubDW;
use githubdw::query::{PrFilters, PrState};

use crate::cli::{MetricsCommand, QueryArguments, SearchArguments};

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

pub fn query(warehouse: GithubDW, arguments: QueryArguments) -> githubdw::Result<()> {
    let filters = PrFilters {
        author: arguments.author,
        reviewer: arguments.reviewer,
        repo: arguments.repo,
        org: arguments.org,
        group: arguments.group,
        label: arguments.label,
        state: arguments.state.as_deref().map(PrState::parse).transpose()?,
        merged: arguments.merged,
        period: arguments.period,
        since: arguments
            .since
            .as_deref()
            .map(parse_date_argument)
            .transpose()?,
        until: arguments
            .until
            .as_deref()
            .map(parse_date_argument)
            .transpose()?,
    };
    let mut builder = filters.builder(warehouse.connection())?;
    if let Some(limit) = arguments.limit {
        builder = builder.limit(limit);
    }
    if let Some(offset) = arguments.offset {
        builder = builder.offset(offset);
    }

    match arguments.output.as_str() {
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
                    row.author_display(),
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

pub fn metrics(warehouse: GithubDW, command: MetricsCommand) -> githubdw::Result<()> {
    let connection = warehouse.connection();
    // Relative periods resolve in the warehouse's own calendar, the
    // same one every stored date key is built in.
    let reference_date = githubdw::storage::time_dimension::today(connection)?;
    let engine = githubdw::metrics::MetricsEngine::new(connection);
    match command {
        MetricsCommand::User {
            login,
            period,
            top,
            json,
        } => {
            let period = githubdw::Period::parse_with_reference(&period, reference_date)?;
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
                    metrics.display_login(),
                    metrics.period_key,
                    metrics.previous_period_key
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
                print_leaderboard("Top reviewed authors", &aggregations.top_reviewed_authors);
            }
        }
        MetricsCommand::Repo {
            repository,
            period,
            top,
            json,
        } => {
            let period = githubdw::Period::parse_with_reference(&period, reference_date)?;
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
            let period = githubdw::Period::parse_with_reference(&period, reference_date)?;
            let group_metrics = engine.group_metrics(&name, &period)?;
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

pub fn search(warehouse: GithubDW, arguments: SearchArguments) -> githubdw::Result<()> {
    let scope = match (
        arguments.pull_requests,
        arguments.issues,
        arguments.comments,
    ) {
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
        repository: arguments.repo.map(|value| value.to_lowercase()),
        limit: arguments.limit,
    };
    let hits = githubdw::search::search(warehouse.connection(), &arguments.query, &options)?;
    if arguments.json {
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
