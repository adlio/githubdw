use super::*;

#[test]
fn interrupted_sync_all_moves_on_and_eventually_revisits_every_source() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let plan = SyncPlan {
        users: vec!["one".into(), "two".into()],
        repos: vec!["owner/a".into(), "owner/b".into()],
        notices: vec![],
    };
    let initial = plan.fair_sources(conn).unwrap();
    assert_eq!(
        initial.iter().map(SyncSource::key).collect::<Vec<_>>(),
        vec!["user:one", "repo:owner/a", "user:two", "repo:owner/b"]
    );
    let mut visited = Vec::new();
    for _ in 0..initial.len() {
        let run = plan.fair_sources(conn).unwrap();
        visited.push(run[0].key());
        SyncPlan::checkpoint_next_source(conn, &run[1]).unwrap();
        // Simulate interruption during the first source, before it finishes.
    }
    assert_eq!(
        visited,
        initial.iter().map(SyncSource::key).collect::<Vec<_>>()
    );
    assert_eq!(plan.fair_sources(conn).unwrap(), initial);
    SyncPlan::checkpoint_next_source(conn, &SyncSource::User("removed".into())).unwrap();
    assert_eq!(
        plan.fair_sources(conn).unwrap(),
        initial,
        "removed sources do not break scheduling"
    );
    assert!(SyncPlan::default().fair_sources(conn).unwrap().is_empty());
}

#[test]
fn budget_yield_is_persisted_as_partial_not_completed() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    sync_state::start_job(conn, "repo:octocat/hello").unwrap();
    sync_state::complete_job_outcome(conn, "repo:octocat/hello", 500, 0, &[], true).unwrap();
    let (status, count, note): (String, i64, String) = conn
        .query_row(
            "SELECT status,synced,error FROM sync_jobs WHERE entity_key='repo:octocat/hello'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "partial");
    assert_eq!(count, 500);
    assert!(note.contains("paused"));
    sync_state::complete_job(conn, "repo:octocat/hello", 25, 0, &[]).unwrap();
    let status: String = conn
        .query_row(
            "SELECT status FROM sync_jobs WHERE entity_key='repo:octocat/hello'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "completed");
}

/// `sync all` covers users as well as repositories.
#[test]
fn the_plan_covers_both_supported_source_types() {
    let plan = plan_all(&[
        source("repo", "acme/widgets", true),
        source("user", "octocat", true),
    ]);
    assert_eq!(plan.repos, vec!["acme/widgets"]);
    assert_eq!(plan.users, vec!["octocat"]);
    assert!(plan.notices.is_empty());
}

/// The invariant that keeps the plan honest: every enabled source is either
/// in a bucket this build serves or named in a notice the caller prints. A
/// source type nobody handles must never just vanish from the run.
#[test]
fn no_enabled_source_is_dropped_without_a_word() {
    let sources = [
        source("repo", "acme/widgets", true),
        source("user", "octocat", true),
        source("org", "acme", true),
        source("repo", "acme/legacy", false),
    ];
    let plan = plan_all(&sources);
    let notices = plan.notices.join("\n");

    for entry in &sources {
        let accounted = plan.repos.contains(&entry.identifier)
            || plan.users.contains(&entry.identifier)
            || notices.contains(&entry.identifier);
        assert!(
            accounted,
            "{} ({}) is neither synced nor mentioned",
            entry.identifier, entry.source_type
        );
    }
    assert!(
        notices.contains("org") && notices.contains("not implemented"),
        "the unsupported type says so: {notices}"
    );
    assert!(
        notices.contains("disabled"),
        "a deliberately disabled source is still accounted for: {notices}"
    );
}

#[test]
fn an_empty_monitored_set_says_what_to_do() {
    let plan = plan_all(&[]);
    assert!(plan.is_empty());
    assert_eq!(plan.notices.len(), 1);
    assert!(plan.notices[0].contains("monitor add-user"));
}
