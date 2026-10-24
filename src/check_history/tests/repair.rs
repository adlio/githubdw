use super::*;

#[test]
fn stores_each_run_under_the_commit_the_provider_reports() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(
        outcome.status,
        HistoryStatus::Complete,
        "{:?}",
        outcome.caveats
    );
    assert_eq!(
        (outcome.check_runs_before, outcome.check_runs_after),
        (1, 3)
    );
    assert_eq!(
        heads_of(&conn, "o/r#5"),
        vec![
            ("CR_a".into(), H1.into()),
            ("CR_b".into(), H1.into()),
            ("CR_c".into(), H2.into())
        ]
    );
    let h1 = outcome.heads.iter().find(|head| head.sha == H1).unwrap();
    assert_eq!(
        h1.sources,
        vec![HeadSource::StoredHead, HeadSource::CommitList]
    );
    assert_eq!(
        (h1.upstream_runs, h1.stored_before, h1.stored_after),
        (Some(2), 1, 2)
    );
    assert_eq!((h1.inserted, h1.updated), (1, 1));
}

#[test]
fn repeated_runs_are_idempotent() {
    let conn = warehouse();
    let options = CheckHistoryOptions::default();
    let mut first = client(two_head_responses());
    repair_check_history(&conn, &mut first, &[target()], &options).unwrap();
    let mut second = client(two_head_responses());
    let report = repair_check_history(&conn, &mut second, &[target()], &options).unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(
        outcome.status,
        HistoryStatus::Complete,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        (outcome.check_runs_before, outcome.check_runs_after),
        (3, 3)
    );
    assert!(
        outcome
            .heads
            .iter()
            .all(|head| head.inserted == 0 && head.updated == 0)
    );
}

#[test]
fn a_fetch_failure_leaves_stored_rows_unchanged() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    let mut responses = pull(H2, &[H1, H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_a", H1), run("CR_b", H1)]));
    responses.push(suites());
    responses.push(owners(&[]));
    responses.push(Err(Error::GitHubApi("boom".into())));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_a".into(), H1.into())]);
    let status: String = conn
        .query_row(
            "SELECT status FROM fact_check_runs WHERE check_run_key = 'CR_a'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        status, "IN_PROGRESS",
        "the earlier head's update must roll back too"
    );
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 0);
}

#[test]
fn a_run_stored_under_another_pull_request_is_never_moved() {
    // Like the eight c590f74a rows held under #2916/#2905 while the
    // provider's workflow runs name #3004: proof for us, a conflict, unmoved.
    let conn = warehouse();
    store(&conn, "CR_c", "o/r#9", H2, "COMPLETED");
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(heads_of(&conn, "o/r#9"), vec![("CR_c".into(), H2.into())]);
    let h2 = outcome.heads.iter().find(|head| head.sha == H2).unwrap();
    assert!(h2.shared_head);
    assert_eq!(
        h2.ownership_conflicts.get("CR_c").map(String::as_str),
        Some("o/r#9")
    );
    assert!(
        outcome.caveats.iter().any(|c| c.starts_with("CONFLICT")),
        "{:?}",
        outcome.caveats
    );
}

#[test]
fn a_stored_run_on_a_different_commit_is_reported_not_rewritten() {
    let conn = warehouse();
    store(&conn, "CR_b", "o/r#5", H2, "COMPLETED");
    let mut client = client(two_head_responses());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Incomplete);
    let sha: String = conn
        .query_row(
            "SELECT head_sha FROM fact_check_runs WHERE check_run_key = 'CR_b'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sha, H2);
}

#[test]
fn a_listed_commit_the_provider_attributes_elsewhere_is_skipped() {
    let conn = warehouse();
    let mut responses = pull(H2, &[C3, H2]);
    responses.push(no_force_pushes());
    // BTreeMap order: H2 ("222...") before C3 ("333...").
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[5]));
    responses.push(runs(vec![run("CR_x", C3)]));
    responses.push(suites());
    responses.push(owners(&[9]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Complete);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_c".into(), H2.into())]);
    let c3 = outcome.heads.iter().find(|head| head.sha == C3).unwrap();
    assert_eq!(c3.attributed_elsewhere, 1);
    assert!(c3.skipped.is_some());
}

#[test]
fn an_unavailable_stored_head_makes_the_result_incomplete() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(Err(Error::GhCommand {
        exit_code: Some(1),
        stderr: "No commit found for SHA (HTTP 422)".into(),
    }));
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(
        outcome.check_runs_after, 2,
        "the stored run is kept, never pruned"
    );
}

#[test]
fn a_capped_commit_list_is_never_reported_complete() {
    let conn = warehouse();
    let mut responses = vec![
        Ok(pull_json(H2, 300, "o/r")),
        Ok(Value::Array(vec![json!({"sha": H2})]).to_string()),
    ];
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Incomplete);
}

#[test]
fn observed_only_reads_stored_and_current_heads_and_is_not_complete() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut responses = pull(H2, &[H1, H2]);
    responses.push(runs(vec![run("CR_a", H1), run("CR_b", H1)]));
    responses.push(suites());
    responses.push(owners(&[]));
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let mut client = client(responses);
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions {
            observed_only: true,
            rest_reserve: None,
            max_requests: None,
        },
    )
    .unwrap();
    let outcome = &report.outcomes[0];
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(outcome.check_runs_after, 3);
}

#[test]
fn a_running_sync_fails_the_target_and_keeps_its_lock() {
    let conn = warehouse();
    sync_state::acquire_lock(&conn, "repo:o/r").unwrap();
    let mut client = client(Vec::new());
    let report = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_locks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(locks, 1);
}

#[test]
fn a_pull_request_missing_from_the_warehouse_fails_without_network() {
    let conn = warehouse();
    let mut client = client(Vec::new());
    let other = RefreshTarget::parse(TargetKind::PullRequest, "o/r#77").unwrap();
    let report = repair_check_history(
        &conn,
        &mut client,
        &[other],
        &CheckHistoryOptions::default(),
    )
    .unwrap();
    assert_eq!(report.outcomes[0].status, HistoryStatus::Failed);
}

#[test]
fn candidates_rank_pull_requests_by_stored_heads() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    store(&conn, "CR_b", "o/r#5", H2, "COMPLETED");
    store(&conn, "CR_c", "o/r#9", H1, "COMPLETED");
    let candidates = history_candidates(&conn, Some("O/R"), 2, 10).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].target.reference(), "o/r#5");
    assert_eq!(candidates[0].stored_heads, 2);
}

#[test]
fn the_guarded_upsert_never_rewrites_another_pull_requests_row() {
    let conn = warehouse();
    store(&conn, "CR_c", "o/r#9", H2, "IN_PROGRESS");
    store(&conn, "CR_c", "o/r#5", H2, "COMPLETED");
    let (pr, status): (String, String) = conn
        .query_row(
            "SELECT pr_key, status FROM fact_check_runs WHERE check_run_key = 'CR_c'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((pr.as_str(), status.as_str()), ("o/r#9", "IN_PROGRESS"));
}

#[test]
fn check_run_continuations_keep_every_rerun() {
    let query = crate::fetch::continuation::continuation_query("CheckSuite", "checkRuns", "id");
    assert!(query.contains("filterBy: {checkType: ALL}"));
    let other = crate::fetch::continuation::continuation_query("PullRequest", "files", "path");
    assert!(!other.contains("filterBy"));
}

#[test]
fn a_shared_head_is_resolved_only_by_workflow_run_identity() {
    // Like c590f74a: several pull requests share one head. The suite's
    // branch names whichever pushed first; only the workflow run keyed by the
    // suite says which pull request triggered a run.
    let conn = warehouse();
    store(&conn, "CR_other", "o/r#9", H2, "COMPLETED");
    let outcome = repair(
        &conn,
        current_head_only(vec![
            run_in("CR_q", H2, 1),
            run_in("CR_r", H2, 3),
            run_in("CR_s", H2, 2),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert!(h2.shared_head);
    assert!(
        h2.ambiguous.get("CR_r").unwrap().contains("shared"),
        "a bare pull ref on a shared commit is not proof: {h2:?}"
    );
    assert_eq!(h2.attributed_elsewhere, 1);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_q".into(), H2.into())]);
    assert_eq!(
        heads_of(&conn, "o/r#9"),
        vec![("CR_other".into(), H2.into())]
    );
}

#[test]
fn a_shared_head_without_workflow_identity_is_ambiguous() {
    // Fork pull requests: the provider's workflow runs name no pull request,
    // and a unique-looking branch is not proof.
    let conn = warehouse();
    store(&conn, "CR_other", "o/r#9", H2, "COMPLETED");
    set_workflows(
        H2,
        vec![workflow(
            1,
            H2,
            "pull_request",
            json!("feat/five"),
            "o/r",
            vec![],
        )],
    );
    let outcome = repair(&conn, current_head_only(vec![run_in("CR_q", H2, 1)]));
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_q").unwrap();
    assert!(reason.contains("names no pull request"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn provider_pull_request_tags_are_neither_proof_nor_exclusion() {
    let conn = warehouse();
    // A check-run tag contradicting the workflow run's identity is a
    // conflict; an absent tag excludes nothing.
    let mut tagged = run_in("CR_t", H2, 1);
    tagged["pull_requests"] = json!([{"number": 9}]);
    let outcome = repair(
        &conn,
        current_head_only(vec![tagged, run_in("CR_u", H2, 1)]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert!(head(&outcome, H2).ambiguous.contains_key("CR_t"));
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_u".into(), H2.into())]);
}

#[test]
fn absent_tags_with_our_branch_on_an_unshared_head_are_complete() {
    let conn = warehouse();
    let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
    assert_eq!(
        outcome.status,
        HistoryStatus::Complete,
        "{:?}",
        outcome.caveats
    );
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_v".into(), H2.into())]);
}

#[test]
fn a_stack_parent_commit_is_never_imported() {
    let conn = warehouse();
    let mut responses = pull(H2, &[C3, H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_c", H2)]));
    responses.push(suites());
    responses.push(owners(&[]));
    responses.push(runs(vec![run_in("CR_p", C3, 2)]));
    responses.push(suites());
    responses.push(owners(&[])); // closed parents give no owner answer
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(head(&outcome, C3).attributed_elsewhere, 1);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_c".into(), H2.into())]);
}

#[test]
fn a_reused_branch_name_neither_proves_nor_blocks_workflow_identity() {
    let conn = warehouse();
    conn.execute_batch(
        "INSERT INTO fact_pull_requests (pr_key, number, repo_key, author_key, state,
             head_ref, created_at, created_date_key, created_time_key)
         VALUES ('o/r#7', 7, 'o/r', 'user:a', 'CLOSED', 'feat/five', '2026-08-01T00:00:00Z',
                 '2026-08-01', '00:00');",
    )
    .unwrap();
    // Suite 1's workflow run names #7 (the earlier user of the branch).
    set_workflows(
        H2,
        vec![
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(7, "feat/five", "o/r")],
            ),
            workflow(3, H2, "dynamic", json!("refs/pull/5/head"), "o/r", vec![]),
        ],
    );
    let outcome = repair(
        &conn,
        current_head_only(vec![run("CR_w", H2), run_in("CR_x", H2, 3)]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert_eq!(head(&outcome, H2).attributed_elsewhere, 1);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_x".into(), H2.into())]);
}

#[test]
fn a_fork_branch_name_is_ambiguous() {
    let conn = warehouse();
    let mut responses = vec![
        Ok(pull_json(H2, 1, "someone/r")),
        Ok(json!([{"sha": H2}]).to_string()),
    ];
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_f", H2), run_in("CR_g", H2, 3)]));
    responses.push(suites());
    responses.push(owners(&[]));
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    assert!(head(&outcome, H2).ambiguous.contains_key("CR_f"));
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_g".into(), H2.into())]);
}

#[test]
fn unknown_app_and_branchless_suites_are_ambiguous() {
    let conn = warehouse();
    let mut appless = run_in("CR_app", H2, 6);
    appless.as_object_mut().unwrap().remove("app");
    let outcome = repair(
        &conn,
        current_head_only(vec![
            appless,
            run_in("CR_nob", H2, 5),
            run_in("CR_nos", H2, 99),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert_eq!(h2.ambiguous.len(), 3, "{h2:?}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn imported_rows_are_revalidated_on_every_repeat() {
    let conn = warehouse();
    let first = repair(&conn, current_head_only(vec![run("CR_i", H2)]));
    assert_eq!(first.status, HistoryStatus::Complete);
    // Second run: the stored row makes H2 a stored head, which must not
    // stand in for proof. The suite evidence no longer names our branch.
    let second = repair(&conn, current_head_only(vec![run_in("CR_i", H2, 5)]));
    assert_eq!(second.status, HistoryStatus::Incomplete);
    let h2 = head(&second, H2);
    assert!(h2.sources.contains(&HeadSource::StoredHead));
    assert_eq!(h2.stored_unconfirmed, vec!["CR_i".to_string()]);
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_i".into(), H2.into())]);
}

#[test]
fn a_write_error_rolls_back_and_reports_rows_unchanged() {
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "IN_PROGRESS");
    conn.execute_batch(
        "CREATE TRIGGER refuse_c BEFORE INSERT ON fact_check_runs
         WHEN NEW.check_run_key = 'CR_c' BEGIN SELECT RAISE(ABORT, 'refused'); END;",
    )
    .unwrap();
    let outcome = repair(&conn, two_head_responses());
    assert_eq!(outcome.status, HistoryStatus::Failed);
    assert_eq!(outcome.rows_unchanged, Some(true));
    assert_eq!(heads_of(&conn, "o/r#5"), vec![("CR_a".into(), H1.into())]);
    let status: String = conn
        .query_row(
            "SELECT status FROM fact_check_runs WHERE check_run_key = 'CR_a'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "IN_PROGRESS");
}
