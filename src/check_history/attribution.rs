//! Attributing fetched check runs to the pull request from provider evidence.

use super::*;

/// One commit's fetched answer, held until the write phase.
pub(super) struct Fetched {
    pub(super) head: HeadOutcome,
    pub(super) runs: Vec<CommitCheckRun>,
}

/// Per-run attribution from provider suite evidence.
#[derive(Debug, PartialEq)]
pub(super) enum Attribution {
    Own,
    Elsewhere(String),
    Ambiguous(String),
}

pub(super) struct Context<'a> {
    pub(super) repository: &'a str,
    pub(super) number: i64,
    pub(super) head_ref: Option<&'a str>,
    /// Lower-cased `owner/name` of the pull request's head repository.
    pub(super) head_repository: Option<&'a str>,
    pub(super) created_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    pub(super) closed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    /// The pull request is closed but the provider gave no close time.
    pub(super) closed_unknown: bool,
}

/// Events whose workflow runs a pull request triggers.
pub(super) const PULL_REQUEST_EVENTS: [&str; 2] = ["pull_request", "pull_request_target"];

pub(super) fn parse_time(value: Option<&str>) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    value.and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
}

/// The single pull request a workflow run names, validated against this
/// repository: its API URL is this repository's `pulls/{n}` and its base is
/// this repository. `Err` with the reason when the identity is missing,
/// conflicting, or does not validate.
pub(super) fn workflow_pull_request<'r>(
    workflow: &'r WorkflowRun,
    repository: &str,
) -> std::result::Result<&'r RunPullRequest, String> {
    let numbers: std::collections::BTreeSet<i64> =
        workflow.pull_requests.iter().map(|pr| pr.number).collect();
    let pull = match (numbers.len(), workflow.pull_requests.first()) {
        (0, _) | (_, None) => {
            return Err(
                "workflow run names no pull request (fork or closed pull requests are \
                        not listed), and a branch name is not proof"
                    .into(),
            );
        }
        (1, Some(pull)) => pull,
        _ => {
            return Err(format!(
                "workflow run names several pull requests: {numbers:?}"
            ));
        }
    };
    for entry in &workflow.pull_requests {
        let suffix = format!("/pulls/{}", entry.number);
        if !entry
            .url
            .as_deref()
            .is_some_and(|url| check_history::api_url_names(url, repository, &suffix))
        {
            return Err(format!(
                "workflow run's pull request #{} is not identified as this repository's",
                entry.number
            ));
        }
        if !entry
            .base_repo_url
            .as_deref()
            .is_some_and(|url| check_history::api_url_names(url, repository, ""))
        {
            return Err(format!(
                "workflow run's pull request #{} does not name this repository as its base",
                entry.number
            ));
        }
    }
    Ok(pull)
}

/// Attribute one run from provider evidence only (see the module docs).
/// Anything short of proof is `Ambiguous`; `Elsewhere` is reported only when
/// validated evidence names something other than this pull request.
pub(super) fn attribute(
    run: &CommitCheckRun,
    suites: &BTreeMap<i64, CheckSuite>,
    workflows: &[WorkflowRun],
    context: &Context,
    shared_head: bool,
) -> Attribution {
    let Some(suite) = run.suite_id.and_then(|id| suites.get(&id)) else {
        return Attribution::Ambiguous("run's check suite is not listed for the commit".into());
    };
    let app = match (run.app_slug.as_deref(), suite.app_slug.as_deref()) {
        (Some(run_app), Some(suite_app)) if run_app == suite_app => run_app,
        (None, _) | (_, None) => return Attribution::Ambiguous("unknown app".into()),
        (Some(run_app), Some(suite_app)) => {
            return Attribution::Ambiguous(format!(
                "run app {run_app} differs from its suite's app {suite_app}"
            ));
        }
    };
    match suite.repository.as_deref() {
        Some(name) if name.eq_ignore_ascii_case(context.repository) => {}
        Some(_) => return Attribution::Ambiguous("suite belongs to another repository".into()),
        None => return Attribution::Ambiguous("suite does not name its repository".into()),
    }
    let Some(branch) = suite.head_branch.as_deref() else {
        return Attribution::Ambiguous("suite has no head branch".into());
    };
    // Only a GitHub Actions suite belongs to one workflow run. Every other
    // app keeps one suite per commit, whose ref -- branch OR refs/pull/N --
    // names only the first push, so nothing about it proves an owner.
    if app != check_history::ACTIONS_APP {
        return Attribution::Ambiguous(format!(
            "{app} keeps one suite per commit; its ref {branch} names the first push, not the \
             pull request a run served"
        ));
    }
    let suite_id = suite.id;
    let mut keyed = workflows
        .iter()
        .filter(|workflow| workflow.check_suite_id == Some(suite_id));
    let workflow = match (keyed.next(), keyed.next()) {
        (Some(workflow), None) => workflow,
        (None, _) => {
            return Attribution::Ambiguous(format!("no workflow run is keyed by suite {suite_id}"));
        }
        (Some(_), Some(_)) => {
            return Attribution::Ambiguous(format!("several workflow runs claim suite {suite_id}"));
        }
    };
    if !workflow
        .repository
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case(context.repository))
    {
        return Attribution::Ambiguous("workflow run does not name this repository".into());
    }
    if workflow.head_branch.as_deref() != Some(branch) {
        return Attribution::Ambiguous(format!(
            "workflow run branch {:?} differs from its suite's {branch}",
            workflow.head_branch
        ));
    }
    // Check-run tags that contradict the workflow run's identity.
    let named: std::collections::BTreeSet<i64> =
        workflow.pull_requests.iter().map(|pr| pr.number).collect();
    if !run.pull_requests.is_empty()
        && run
            .pull_requests
            .iter()
            .any(|number| !named.contains(number))
    {
        return Attribution::Ambiguous(format!(
            "check run tags {:?} conflict with its workflow run's pull requests {named:?}",
            run.pull_requests
        ));
    }
    if let Some(rest) = branch.strip_prefix("refs/pull/") {
        let number = rest
            .strip_suffix("/head")
            .or_else(|| rest.strip_suffix("/merge"))
            .and_then(|n| n.parse::<i64>().ok());
        let Some(number) = number else {
            return Attribution::Ambiguous(format!("unrecognized pull ref {branch}"));
        };
        if !workflow
            .head_repository
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(context.repository))
        {
            return Attribution::Ambiguous(format!(
                "workflow run on {branch} does not name this repository as its head"
            ));
        }
        if number != context.number {
            return Attribution::Elsewhere(format!("workflow run ref names #{number}"));
        }
        if !named.is_empty() && named.iter().any(|n| *n != context.number) {
            return Attribution::Ambiguous(format!(
                "workflow run on {branch} also names {named:?}"
            ));
        }
        if shared_head && named.is_empty() {
            return Attribution::Ambiguous(format!(
                "commit is shared with other pull request(s) and the workflow run on {branch} \
                 names no pull request"
            ));
        }
        return Attribution::Own;
    }
    let event = workflow.event.as_deref().unwrap_or("unknown");
    if !PULL_REQUEST_EVENTS.contains(&event) {
        return Attribution::Ambiguous(format!(
            "workflow run event {event} was not triggered by a pull request"
        ));
    }
    let pull = match workflow_pull_request(workflow, context.repository) {
        Ok(pull) => pull,
        Err(reason) => return Attribution::Ambiguous(reason),
    };
    if pull.number != context.number {
        return Attribution::Elsewhere(format!("workflow run names #{}", pull.number));
    }
    // The named pull request is ours: every identity field must agree.
    let Some(head_ref) = context.head_ref else {
        return Attribution::Ambiguous("pull request head branch unknown".into());
    };
    if pull.head_ref.as_deref() != Some(branch) || branch != head_ref {
        return Attribution::Ambiguous(format!(
            "workflow run branch {branch} does not match the pull request's head {head_ref}"
        ));
    }
    let Some(head_repository) = context.head_repository else {
        return Attribution::Ambiguous("pull request head repository unknown".into());
    };
    if !workflow
        .head_repository
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case(head_repository))
        || !pull
            .head_repo_url
            .as_deref()
            .is_some_and(|url| check_history::api_url_names(url, head_repository, ""))
    {
        return Attribution::Ambiguous(
            "workflow run head repository does not match the pull request's".into(),
        );
    }
    // `pull_requests` reflects open pull requests matching the run, not
    // necessarily the trigger; a run outside the pull request's lifetime
    // could not have been triggered by it.
    let Some(created) = parse_time(workflow.created_at.as_deref()) else {
        return Attribution::Ambiguous("workflow run has no creation time".into());
    };
    let Some(opened) = context.created_at else {
        return Attribution::Ambiguous("pull request creation time unknown".into());
    };
    if created < opened
        || context.closed_unknown
        || context.closed_at.is_some_and(|closed| created > closed)
    {
        return Attribution::Ambiguous(
            "workflow run falls outside the pull request's open lifetime".into(),
        );
    }
    Attribution::Own
}

/// True when observed headroom could not cover one more commit.
pub(super) fn client_allowance_low(client: &GhClient) -> bool {
    client
        .request_allowance()
        .is_some_and(|left| left < MIN_COMMIT_REQUESTS)
        || ["core", "graphql"].iter().any(|resource| {
            client
                .quota_headroom(resource)
                .is_some_and(|left| left < MIN_COMMIT_REQUESTS as i64)
        })
}

pub(super) fn existing_owner(conn: &Connection, key: &str) -> Result<Option<String>> {
    use rusqlite::OptionalExtension;
    Ok(conn
        .query_row(
            "SELECT pr_key FROM fact_check_runs WHERE check_run_key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()?)
}
