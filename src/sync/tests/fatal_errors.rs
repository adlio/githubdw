use super::*;

fn fatal_errors() -> Vec<crate::Error> {
    vec![
        crate::Error::Cancelled,
        crate::Error::QuotaReserve("reserve reached".into()),
        crate::Error::RateLimited {
            resource: "graphql".into(),
            wake_at: chrono::DateTime::from_timestamp(4_000_000_000, 0).unwrap(),
            reason: "spent".into(),
        },
    ]
}

/// Five PRs, newest first; the second needs a continuation request.
fn five_prs_second_is_heavy() -> String {
    let light = |number: i64| {
        pr_node(
            number,
            "2026-01-05T18:00:00Z",
            &format!("2026-01-{:02}T09:00:00Z", 10 + number),
        )
    };
    let mut second = heavy_pr_node(4);
    second["updatedAt"] = json!("2026-01-14T09:00:00Z");
    page_of(
        false,
        None,
        vec![light(5), second, light(3), light(2), light(1)],
    )
}

fn five_light_prs() -> String {
    page_response(false, None, &[5, 4, 3, 2, 1])
}

/// A run-level error on item 2 of 5 ends the walk: one PR landed, none recorded
/// as failed, nothing after the error is requested, no high-water claim. A
/// second run then lands all five.
#[test]
fn a_run_level_error_on_item_two_stops_the_repo_walk_and_resumes() {
    for error in fatal_errors() {
        let label = error.to_string();
        let warehouse = GithubDW::open_in_memory().unwrap();
        let conn = warehouse.connection();
        let options = fast_pr_options();

        let (mut client, transport) =
            shared_client(vec![Ok(five_prs_second_is_heavy()), Err(error)]);
        let result = Syncer::new(conn, &mut client).sync_repository("octocat/hello", &options);
        let failure = result.expect_err(&label);
        assert!(failure.is_run_fatal(), "{label}: {failure}");
        assert_eq!(
            transport.calls.lock().unwrap().len(),
            2,
            "{label}: one page request, one failing continuation, nothing else"
        );
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM fact_pull_requests"),
            1,
            "{label}: only PR 5 landed before the error"
        );
        assert_eq!(pr_cursor(conn), None, "{label}: no high-water claim");
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM synced_ranges"),
            0,
            "{label}: no coverage claim"
        );

        let (mut client, _) = shared_client(vec![Ok(five_light_prs())]);
        let summary = Syncer::new(conn, &mut client)
            .sync_repository("octocat/hello", &options)
            .unwrap();
        assert!(summary.failed.is_empty(), "{label}: {:?}", summary.failed);
        assert_eq!(
            count(conn, "SELECT COUNT(*) FROM fact_pull_requests"),
            5,
            "{label}: the resumed run lands the rest"
        );
    }
}

/// After a completed page, the checkpoint stays at that page when the next
/// page dies with a run-level error, so the resumed run starts there.
#[test]
fn a_run_level_error_on_page_two_keeps_the_page_one_checkpoint() {
    let warehouse = GithubDW::open_in_memory().unwrap();
    let conn = warehouse.connection();
    let (mut client, _) = shared_client(vec![
        Ok(page_response(true, Some("C1"), &[10, 9])),
        Err(crate::Error::Cancelled),
    ]);
    let error = Syncer::new(conn, &mut client)
        .sync_repository("octocat/hello", &fast_pr_options())
        .unwrap_err();
    assert!(matches!(error, crate::Error::Cancelled), "{error}");
    assert_eq!(count(conn, "SELECT COUNT(*) FROM fact_pull_requests"), 2);
    assert_eq!(saved_checkpoint(conn).expect("durable").after, "C1");
}

/// The user search path stops the same way.
#[test]
fn a_run_level_error_stops_the_user_walk() {
    for error in fatal_errors() {
        let label = error.to_string();
        let warehouse = los_angeles_warehouse();
        monitor_repository::add_user(warehouse.connection(), "octocat").unwrap();
        let mut heavy = heavy_pr_node(5);
        heavy["updatedAt"] = json!("2026-08-15T09:00:00Z");
        heavy["createdAt"] = json!("2026-08-14T18:00:00Z");
        heavy["repository"] = json!({
            "nameWithOwner": "acme/widgets", "primaryLanguage": {"name": "Rust"},
            "isFork": false, "isPrivate": false, "defaultBranchRef": {"name": "main"},
            "createdAt": "2020-01-01T00:00:00Z"
        });
        let (mut client, transport) =
            shared_client(vec![Ok(search_response(1, vec![heavy])), Err(error)]);
        let result = Syncer::new(warehouse.connection(), &mut client)
            .as_of(evening_instant())
            .sync_user("octocat", &fast_user_options(Some(1)));
        let failure = result.expect_err(&label);
        assert!(failure.is_run_fatal(), "{label}: {failure}");
        assert_eq!(transport.calls.lock().unwrap().len(), 2, "{label}");
    }
}

fn gh(stderr: &str) -> crate::Error {
    crate::Error::GhCommand {
        exit_code: Some(1),
        stderr: stderr.into(),
    }
}

fn clone_error(error: &crate::Error) -> crate::Error {
    match error {
        crate::Error::GhCommand { exit_code, stderr } => crate::Error::GhCommand {
            exit_code: *exit_code,
            stderr: stderr.clone(),
        },
        crate::Error::GitHubApi(message) => crate::Error::GitHubApi(message.clone()),
        other => unreachable!("{other}"),
    }
}

fn page_query_sizes(transport: &FixtureTransport) -> Vec<String> {
    transport
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter_map(|call| {
            call.iter()
                .find_map(|argument| argument.strip_prefix("query="))
                .and_then(|query| query.split("pullRequests(first: ").nth(1))
                .map(|rest| rest.split(',').next().unwrap().to_string())
        })
        .collect()
}

/// Only size and timeout symptoms halve the page; each other error returns
/// at once instead of costing five more requests.
#[test]
fn page_size_halves_only_for_size_symptoms() {
    // (error, requests the client itself spends retrying it before the walk
    // sees it: gh failures with "HTTP 5xx" or "timeout" are retried 7 times).
    let size_errors = [
        (gh("HTTP 502"), 7),
        (gh("HTTP 504: gateway timeout"), 7),
        (
            crate::Error::GitHubApi("Something went wrong while executing your query".into()),
            1,
        ),
        (
            crate::Error::GitHubApi("RESOURCE_LIMITS_EXCEEDED".into()),
            1,
        ),
    ];
    for (error, attempts) in size_errors {
        let label = error.to_string();
        let warehouse = GithubDW::open_in_memory().unwrap();
        let mut responses: Vec<_> = (0..attempts).map(|_| Err(clone_error(&error))).collect();
        responses.push(Ok(page_response(false, None, &[1])));
        let (mut client, transport) = shared_client(responses);
        let summary = Syncer::new(warehouse.connection(), &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .unwrap();
        assert_eq!(summary.pull_requests_synced, 1, "{label}");
        let mut sizes = page_query_sizes(&transport);
        sizes.dedup();
        assert_eq!(sizes, ["25", "12"], "{label}");
    }

    let not_size_errors = fatal_errors().into_iter().chain([
        crate::Error::GitHubApi("Could not resolve to a Repository".into()),
        gh("HTTP 401: Bad credentials"),
    ]);
    for error in not_size_errors {
        let label = error.to_string();
        let warehouse = GithubDW::open_in_memory().unwrap();
        let (mut client, transport) = shared_client(vec![Err(error)]);
        Syncer::new(warehouse.connection(), &mut client)
            .sync_repository("octocat/hello", &fast_pr_options())
            .expect_err(&label);
        assert_eq!(page_query_sizes(&transport), ["25"], "{label}: no halving");
    }
}
