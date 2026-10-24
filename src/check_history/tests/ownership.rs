use super::*;

#[test]
fn a_workflow_run_naming_several_pull_requests_is_ambiguous() {
    let conn = warehouse();
    set_workflows(
        H2,
        vec![workflow(
            1,
            H2,
            "pull_request",
            json!("feat/five"),
            "o/r",
            vec![
                pr_entry(5, "feat/five", "o/r"),
                pr_entry(3, "feat/five", "o/r"),
            ],
        )],
    );
    let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_v").unwrap();
    assert!(reason.contains("several pull requests"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn unvalidated_or_mismatched_workflow_identity_is_ambiguous() {
    // Each case breaks exactly one link of the identity chain.
    let cases: Vec<(&str, Value)> = vec![
        ("another repository's pull URL", {
            let mut pr = pr_entry(5, "feat/five", "o/r");
            pr["url"] = json!("https://api.github.com/repos/x/r/pulls/5");
            workflow(1, H2, "pull_request", json!("feat/five"), "o/r", vec![pr])
        }),
        ("a base in another repository", {
            let mut pr = pr_entry(5, "feat/five", "o/r");
            pr["base"]["repo"]["url"] = json!("https://api.github.com/repos/x/r");
            workflow(1, H2, "pull_request", json!("feat/five"), "o/r", vec![pr])
        }),
        ("an arbitrary body URL", {
            let mut pr = pr_entry(5, "feat/five", "o/r");
            pr["url"] = json!("see https://api.github.com/repos/o/r/pulls/5");
            workflow(1, H2, "pull_request", json!("feat/five"), "o/r", vec![pr])
        }),
        ("a same-named branch in a fork", {
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "someone/r",
                vec![pr_entry(5, "feat/five", "someone/r")],
            )
        }),
        ("a push event", {
            workflow(
                1,
                H2,
                "push",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            )
        }),
        ("a run before the pull request opened", {
            let mut wf = workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            );
            wf["created_at"] = json!("2026-08-01T00:00:00Z");
            wf
        }),
        ("a workflow branch differing from the suite", {
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/other"),
                "o/r",
                vec![pr_entry(5, "feat/other", "o/r")],
            )
        }),
    ];
    for (what, wf) in cases {
        let conn = warehouse();
        set_workflows(H2, vec![wf]);
        let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
        assert_eq!(outcome.status, HistoryStatus::Incomplete, "{what}");
        assert!(
            head(&outcome, H2).ambiguous.contains_key("CR_v"),
            "{what}: {:?}",
            head(&outcome, H2)
        );
        assert!(heads_of(&conn, "o/r#5").is_empty(), "{what}");
    }
    // No workflow run keyed by the suite, or two claiming it.
    for list in [
        vec![],
        vec![
            workflow(
                1,
                H2,
                "pull_request",
                json!("feat/five"),
                "o/r",
                vec![pr_entry(5, "feat/five", "o/r")],
            ),
            {
                let mut twin = workflow(
                    1,
                    H2,
                    "pull_request",
                    json!("feat/five"),
                    "o/r",
                    vec![pr_entry(5, "feat/five", "o/r")],
                );
                twin["id"] = json!(99);
                twin
            },
        ],
    ] {
        let conn = warehouse();
        set_workflows(H2, list);
        let outcome = repair(&conn, current_head_only(vec![run("CR_v", H2)]));
        assert!(head(&outcome, H2).ambiguous.contains_key("CR_v"));
        assert!(heads_of(&conn, "o/r#5").is_empty());
    }
}

#[test]
fn a_per_commit_app_pull_ref_is_never_proof() {
    // Reviewer finding: a refs/pull/N suite from a per-commit app named its
    // first creator; the early return accepted it before the app check.
    let conn = warehouse();
    let suites = || {
        Ok(json!({"total_count": 1, "check_suites": [
            {"id": 3, "head_branch": "refs/pull/5/head",
             "app": {"slug": "github-advanced-security"},
             "repository": {"full_name": "o/r"}}]})
        .to_string())
    };
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run_app(
        "CR_cs",
        H2,
        3,
        "github-advanced-security",
    )]));
    responses.push(suites());
    responses.push(owners(&[9]));
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_cs").unwrap();
    assert!(reason.contains("one suite per commit"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn a_per_commit_app_branch_is_never_proof_even_when_it_is_ours() {
    // Code scanning keeps one suite per commit: its head branch is the
    // first push, as in the CodeQL run whose suite named #2916's branch while
    // its output referred to #3004 and the warehouse held it under #2905.
    let conn = warehouse();
    let (mut client, transport) = shared_client(current_head_extended(vec![
        run_app("CR_cs", H2, 7, "github-advanced-security"),
        run_app("CR_cq", H2, 9, "github-advanced-security"),
    ]));
    let outcome = repair_check_history(
        &conn,
        &mut client,
        &[target()],
        &CheckHistoryOptions::default(),
    )
    .unwrap()
    .outcomes
    .remove(0);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert!(h2.ambiguous.contains_key("CR_cs"), "{h2:?}");
    assert!(
        h2.ambiguous.contains_key("CR_cq"),
        "another PR's branch from a per-commit app is not proof of elsewhere either"
    );
    assert_eq!(h2.attributed_elsewhere, 0);
    assert!(heads_of(&conn, "o/r#5").is_empty());
    let calls = transport.calls.lock().unwrap();
    assert!(
        calls
            .iter()
            .all(|call| !call[1].contains("pulls?state=all")),
        "branch owners are not read when no run could rest on them"
    );
}

#[test]
fn a_suite_without_a_repository_is_ambiguous() {
    let conn = warehouse();
    let outcome = repair(&conn, current_head_extended(vec![run_in("CR_nr", H2, 8)]));
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let reason = head(&outcome, H2).ambiguous.get("CR_nr").unwrap();
    assert!(reason.contains("repository"), "{reason}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn a_run_whose_app_differs_from_its_suite_is_ambiguous_even_on_a_pull_ref() {
    let conn = warehouse();
    let outcome = repair(
        &conn,
        current_head_only(vec![
            run_app("CR_m1", H2, 1, "some-other-app"),
            run_app("CR_m3", H2, 3, "some-other-app"),
        ]),
    );
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h2 = head(&outcome, H2);
    assert_eq!(h2.ambiguous.len(), 2, "{h2:?}");
    assert!(heads_of(&conn, "o/r#5").is_empty());
}

#[test]
fn a_stored_head_the_provider_shares_with_another_pull_request_is_not_ours_by_branch() {
    // H1 is a stored head of #5 (discovery only). The provider says #9 also
    // contains it, so our branch's suite cannot prove the run is #5's.
    let conn = warehouse();
    store(&conn, "CR_a", "o/r#5", H1, "COMPLETED");
    let mut responses = pull(H2, &[H2]);
    responses.push(no_force_pushes());
    responses.push(runs(vec![run("CR_a", H1), run_in("CR_r", H1, 3)]));
    responses.push(suites());
    responses.push(owners(&[5, 9]));
    responses.push(runs(vec![run_in("CR_c", H2, 3)]));
    responses.push(suites());
    responses.push(owners(&[5]));
    // The workflow run behind suite 1 names no pull request (fork-like).
    set_workflows(
        H1,
        vec![
            workflow(1, H1, "pull_request", json!("feat/five"), "o/r", vec![]),
            workflow(3, H1, "dynamic", json!("refs/pull/5/head"), "o/r", vec![]),
        ],
    );
    let outcome = repair(&conn, responses);
    assert_eq!(outcome.status, HistoryStatus::Incomplete);
    let h1 = head(&outcome, H1);
    assert!(h1.shared_head);
    assert!(h1.ambiguous.contains_key("CR_a"));
    assert!(
        h1.ambiguous.contains_key("CR_r"),
        "a bare pull ref on a shared commit is not proof: {h1:?}"
    );
    assert_eq!(h1.stored_unconfirmed, vec!["CR_a".to_string()]);
    // The unshared pull-ref run is proof; the stored row is left alone.
    assert_eq!(
        heads_of(&conn, "o/r#5"),
        vec![("CR_a".into(), H1.into()), ("CR_c".into(), H2.into())]
    );
}
