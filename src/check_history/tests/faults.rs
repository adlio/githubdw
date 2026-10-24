use super::*;

fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    pub(super) static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[test]
fn an_unconfirmed_commit_stops_the_batch_and_reports_rows_unknown() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    arm_fault(Fault::Release);
    let (mut client, transport) = shared_client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed);
    assert_eq!(
        first.rows_unchanged, None,
        "a failed commit is never 'unchanged'"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("could not be confirmed")
    );
    let used = transport.calls.lock().unwrap().len();
    assert_eq!(used, 11, "the second target made no request");
    assert!(conn.is_autocommit(), "no savepoint left open");
    assert_eq!(lock_count(&conn), 0);
    // The JSON a caller emits carries every outcome and the stop.
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["outcomes"].as_array().unwrap().len(), 2);
    assert!(json["outcomes"][0]["rows_unchanged"].is_null());
    assert_eq!(json["outcomes"][1]["status"], "not_attempted");
    assert!(json["stopped"].is_string());
}

#[test]
fn a_failed_rollback_stops_the_batch_and_never_claims_rows_unchanged() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    conn.execute_batch(
        "CREATE TRIGGER refuse_c BEFORE INSERT ON fact_check_runs
         WHEN NEW.check_run_key = 'CR_c' BEGIN SELECT RAISE(ABORT, 'refused'); END;",
    )
    .unwrap();
    arm_fault(Fault::Rollback);
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed);
    assert_eq!(first.rows_unchanged, None);
    assert!(
        first.lock_release.is_some(),
        "an open transaction cannot release the lock: {first:?}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(report.stopped.is_some());
    // Leave the connection clean for the drop.
    conn.execute_batch(
        "ROLLBACK TO SAVEPOINT githubdw_check_history;
         RELEASE SAVEPOINT githubdw_check_history",
    )
    .unwrap();
    assert_eq!(status_of(&conn, "CR_a"), "IN_PROGRESS");
}

#[test]
fn a_lock_release_failure_keeps_the_outcome_and_stops_the_batch() {
    let conn = warehouse();
    arm_fault(Fault::LockRelease);
    let mut client = client(current_head_only(vec![run("CR_v", H2)]));
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(
        first.status,
        HistoryStatus::Incomplete,
        "a lease left behind is never complete: {first:?}"
    );
    assert_eq!((first.check_runs_before, first.check_runs_after), (0, 1));
    assert!(
        first
            .caveats
            .iter()
            .any(|c| c.contains("lease not released"))
    );
    assert!(
        first
            .lock_release
            .as_deref()
            .unwrap()
            .contains("could not be released")
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("could not be released")
    );
    assert_eq!(report.not_complete(), 2, "the CLI exits nonzero on this");
    // The lease is left behind for the stale window, never silently dropped.
    assert_eq!(lock_count(&conn), 1);
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["outcomes"][0]["status"], "incomplete");
    assert!(json["outcomes"][0]["lock_release"].is_string());
}

#[test]
fn the_request_cap_stops_a_long_page_walk_without_calling_the_provider() {
    let conn = warehouse();
    let page = || {
        let list: Vec<Value> = (0..100)
            .map(|i| run(&format!("CR_{i}_{}", rand_suffix()), H2))
            .collect();
        Ok(json!({"total_count": 1000, "check_runs": list}).to_string())
    };
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    for _ in 0..5 {
        responses.push(page());
    }
    let (mut client, transport) = shared_client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions {
            observed_only: false,
            rest_reserve: None,
            max_requests: Some(8),
        },
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Failed);
    assert!(
        outcome.error.as_deref().unwrap().contains("allowance"),
        "the sixth page is refused locally: {outcome:?}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert_eq!(transport.calls.lock().unwrap().len(), 8);
    assert!(heads_of(&conn, "o/r#5").is_empty());
    assert_eq!(lock_count(&conn), 0);
}

#[test]
fn a_reader_lock_that_makes_the_real_release_busy_never_claims_rows_unchanged() {
    // File-backed, rollback-journal mode: a second connection holding a
    // SHARED lock (an open read transaction) makes the outermost RELEASE
    // (the commit) fail with SQLITE_BUSY. Nothing is injected; the hook only
    // times the reader to start after the writes and before the commit.
    let dir = std::env::temp_dir().join(format!(
        "githubdw-busy-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("warehouse.db");
    let mut conn = Connection::open(&path).unwrap();
    schema::init(&mut conn).unwrap();
    // The warehouse defaults to WAL; force a rollback journal so a reader's
    // SHARED lock blocks the writer's commit.
    let _: String = conn
        .query_row("PRAGMA journal_mode = DELETE", [], |r| r.get(0))
        .unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "delete");
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
             head_ref, created_at, created_date_key, created_time_key)
         VALUES ('o/r#5', 5, 'o/r', 'user:a', 'MERGED', 'feat/five', '2026-09-01T00:00:00Z',
                 '2026-09-01', '00:00'),
                ('o/r#9', 9, 'o/r', 'user:a', 'MERGED', 'feat/nine', '2026-09-01T00:00:00Z',
                 '2026-09-01', '00:00');",
    )
    .unwrap();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");

    let reader = std::rc::Rc::new(Connection::open(&path).unwrap());
    let held = reader.clone();
    before_release(Box::new(move || {
        held.execute_batch("BEGIN").unwrap();
        let _: i64 = held
            .query_row("SELECT COUNT(*) FROM fact_check_runs", [], |r| r.get(0))
            .unwrap();
    }));
    let (mut client, transport) = shared_client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target(), nine()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let first = &report.outcomes[0];
    assert_eq!(first.status, HistoryStatus::Failed, "{first:?}");
    assert_eq!(
        first.rows_unchanged, None,
        "a commit that hit SQLITE_BUSY is never reported unchanged"
    );
    let error = first.error.as_deref().unwrap().to_lowercase();
    assert!(
        error.contains("locked") || error.contains("busy"),
        "{error}"
    );
    assert_eq!(report.outcomes[1].status, HistoryStatus::NotAttempted);
    assert!(
        report
            .stopped
            .as_deref()
            .unwrap()
            .contains("could not be confirmed")
    );
    let used = transport.calls.lock().unwrap().len();
    assert_eq!(used, 11, "the second target made no request");
    // The full report still serializes for the caller.
    let json = serde_json::to_value(&report).unwrap();
    assert!(json["outcomes"][0]["rows_unchanged"].is_null());

    // Let the reader go, then show what actually happened on disk.
    reader.execute_batch("COMMIT").unwrap();
    if !conn.is_autocommit() {
        conn.execute_batch(
            "ROLLBACK TO SAVEPOINT githubdw_check_history;
             RELEASE SAVEPOINT githubdw_check_history",
        )
        .unwrap();
    }
    assert_eq!(status_of(&conn, "CR_a"), "IN_PROGRESS", "the rollback held");
    drop(conn);
    drop(reader);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn stack_branch_runs_on_a_shared_head_import_only_with_workflow_identity() {
    // Same commit is the head of #5 (same-repo stack branch) and of a fork's
    // #9; suite 1 is our branch with the workflow run naming #5, suite 2 is a
    // fork pull_request_target run on a same-named branch with no pull
    // request listed, suite 3 is refs/pull/5/head, suite 7 is code scanning.
    let conn = warehouse();
    store(&conn, "CR_wrong", "o/r#9", H2, "COMPLETED");
    set_workflows(
        H2,
        vec![
            workflow(
                1,
                H2,
                "pull_request_target",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            ),
            workflow(
                2,
                H2,
                "pull_request_target",
                json!("feat/nine"),
                "fork/r",
                vec![],
            ),
            workflow(3, H2, "dynamic", json!("refs/pull/5/head"), "o/r", vec![]),
        ],
    );
    let outcome = repair(
        &conn,
        current_head_extended(vec![
            run_in("CR_stack", H2, 1),
            run_in("CR_wrong", H2, 1),
            run_in("CR_fork", H2, 2),
            run_in("CR_dyn", H2, 3),
            run_app("CR_codeql", H2, 7, "github-advanced-security"),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert!(h2.shared_head);
    assert!(h2.ambiguous.contains_key("CR_fork"), "{h2:?}");
    assert!(h2.ambiguous.contains_key("CR_dyn"), "{h2:?}");
    assert!(h2.ambiguous.contains_key("CR_codeql"), "{h2:?}");
    assert_eq!(
        h2.ownership_conflicts.get("CR_wrong").map(String::as_str),
        Some("o/r#9"),
        "the existing wrong owner is reported, never moved"
    );
    assert_eq!(
        heads_of(&conn, "o/r#5"),
        vec![("CR_stack".into(), H2.into())]
    );
    assert_eq!(
        heads_of(&conn, "o/r#9"),
        vec![("CR_wrong".into(), H2.into())]
    );
}

#[test]
fn known_defect_normal_collection_stores_a_shared_head_run_under_whichever_pr_is_visited() {
    // sync.rs stores every check run on a pull request's CURRENT head under
    // that pull request via upsert_check_run, with no suite/app/workflow
    // attribution (fetch/pull_requests.rs parse_head_commit flattens all
    // suites). When two pull requests share the head, the first one visited
    // takes the row and the guarded upsert then keeps it there forever. This
    // characterizes today's behavior so a fix must flip it deliberately; it
    // is the likely origin of the eight c590f74a rows held by #2916/#2905
    // that workflow-run evidence attributes to #3004.
    let conn = warehouse();
    // Sync visits #9 first: the run triggered for #5 lands under #9.
    repository::upsert_check_run(
        &conn,
        "CR_for5",
        "o/r#9",
        H2,
        "job",
        "COMPLETED",
        None,
        None,
        None,
    )
    .unwrap();
    // Visiting #5 later cannot correct it.
    repository::upsert_check_run(
        &conn,
        "CR_for5",
        "o/r#5",
        H2,
        "job",
        "COMPLETED",
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        heads_of(&conn, "o/r#9"),
        vec![("CR_for5".into(), H2.into())]
    );
    assert!(heads_of(&conn, "o/r#5").is_empty());
}
