use super::*;
use serde_json::json;

fn sample_page() -> Value {
    json!({
        "repository": {
            "nameWithOwner": "octocat/hello",
            "primaryLanguage": {"name": "Rust"},
            "isFork": false,
            "isPrivate": false,
            "defaultBranchRef": {"name": "main"},
            "createdAt": "2020-01-01T00:00:00Z",
            "pullRequests": {
                "pageInfo": {"hasNextPage": true, "endCursor": "CURSOR1"},
                "nodes": [{
                    "number": 7,
                    "title": "Add rate limiter",
                    "body": "Implements client-side pacing",
                    "state": "MERGED",
                    "isDraft": false,
                    "createdAt": "2026-01-05T18:00:00Z",
                    "updatedAt": "2026-01-06T09:00:00Z",
                    "mergedAt": "2026-01-06T09:00:00Z",
                    "closedAt": "2026-01-06T09:00:00Z",
                    "baseRefName": "main",
                    "headRefName": "feature/rate-limit",
                    "additions": 120,
                    "deletions": 4,
                    "changedFiles": 3,
                    "author": {"login": "octocat", "__typename": "User"},
                    "mergedBy": {"login": "hubot", "__typename": "User"},
                    "reviews": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                        "id": "REV1", "state": "APPROVED", "body": "LGTM",
                        "submittedAt": "2026-01-06T08:00:00Z",
                        "author": {"login": "hubot", "__typename": "User"}
                    }]},
                    "reviewThreads": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                        "id": "THREAD1",
                        "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                            "id": "RC1", "body": "nit: rename",
                            "path": "src/lib.rs", "line": 10,
                            "createdAt": "2026-01-05T20:00:00Z",
                            "author": {"login": "hubot", "__typename": "User"},
                            "replyTo": null
                        }]}
                    }]},
                    "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                        "id": "IC1", "body": "Looks good overall",
                        "createdAt": "2026-01-05T19:00:00Z",
                        "author": {"login": "dependabot[bot]", "__typename": "Bot"}
                    }]},
                    "files": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                        "path": "src/lib.rs", "changeType": "MODIFIED",
                        "additions": 100, "deletions": 2
                    }]},
                    "commits": {"nodes": [{
                        "commit": {
                            "id": "COMMIT_1",
                            "oid": "abc123",
                            "checkSuites": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                                "id": "SUITE_1",
                                "checkRuns": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                                    "id": "CHK1", "name": "build",
                                    "status": "COMPLETED", "conclusion": "SUCCESS",
                                    "startedAt": "2026-01-05T18:05:00Z",
                                    "completedAt": "2026-01-05T18:10:00Z"
                                }]}
                            }]}
                        }
                    }]}
                }]
            }
        }
    })
}

#[test]
fn parses_full_page() {
    let page = parse_pull_request_page(&sample_page()).expect("parse page");
    assert_eq!(page.repository.name_with_owner, "octocat/hello");
    assert_eq!(page.repository.primary_language.as_deref(), Some("Rust"));
    assert!(page.has_next_page);
    assert_eq!(page.end_cursor.as_deref(), Some("CURSOR1"));
    assert_eq!(page.pull_requests.len(), 1);

    let pull_request = &page.pull_requests[0];
    assert_eq!(pull_request.number, 7);
    assert_eq!(pull_request.state, "MERGED");
    assert_eq!(pull_request.author.as_ref().unwrap().login, "octocat");
    assert_eq!(pull_request.reviews.len(), 1);
    assert_eq!(pull_request.reviews[0].state, "APPROVED");
    assert_eq!(pull_request.review_comments.len(), 1);
    assert_eq!(
        pull_request.review_comments[0].path.as_deref(),
        Some("src/lib.rs")
    );
    assert_eq!(pull_request.conversation_comments.len(), 1);
    assert_eq!(
        pull_request.conversation_comments[0]
            .author
            .as_ref()
            .unwrap()
            .type_name,
        "Bot"
    );
    assert_eq!(pull_request.files.len(), 1);
    assert_eq!(pull_request.head_sha.as_deref(), Some("abc123"));
    assert_eq!(pull_request.check_runs.len(), 1);
    assert_eq!(
        pull_request.check_runs[0].conclusion.as_deref(),
        Some("SUCCESS")
    );
}

fn rest_page(start: usize, count: usize) -> Result<String> {
    let files: Vec<Value> = (start..start + count)
        .map(|index| json!({"filename": format!("f{index}.rs"), "patch": "@@"}))
        .collect();
    Ok(Value::Array(files).to_string())
}

fn rest_client(responses: Vec<Result<String>>) -> GhClient {
    let transport = crate::fetch::test_support::FixtureTransport::new(responses);
    GhClient::with_transport(Box::new(transport)).without_sleeping()
}

#[test]
fn patch_walk_follows_pages_until_a_short_page() {
    let mut client = rest_client(vec![
        rest_page(0, 100),
        rest_page(100, 100),
        rest_page(200, 7),
    ]);
    let listing = fetch_file_patches(&mut client, "o/r", 1).unwrap();
    assert_eq!(listing.files.len(), 207);
    assert_eq!(listing.pages, 3);
    assert!(!listing.provider_capped);
}

#[test]
fn patch_walk_ends_on_an_empty_page_for_exact_multiples() {
    let mut client = rest_client(vec![rest_page(0, 100), rest_page(100, 0)]);
    let listing = fetch_file_patches(&mut client, "o/r", 1).unwrap();
    assert_eq!(listing.files.len(), 100);
    assert_eq!(listing.pages, 2);
}

#[test]
fn patch_walk_stops_at_the_provider_ceiling() {
    let pages = (0..30).map(|page| rest_page(page * 100, 100)).collect();
    let mut client = rest_client(pages);
    let listing = fetch_file_patches(&mut client, "o/r", 1).unwrap();
    assert_eq!(listing.files.len(), REST_FILE_LIMIT);
    assert_eq!(listing.pages, 30);
    assert!(listing.provider_capped);
}

#[test]
fn patch_walk_fails_on_a_failed_or_malformed_page() {
    let mut client = rest_client(vec![
        rest_page(0, 100),
        Err(Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 404: Not Found".into(),
        }),
    ]);
    assert!(fetch_file_patches(&mut client, "o/r", 1).is_err());
    let mut client = rest_client(vec![Ok(r#"{"message": "oops"}"#.into())]);
    assert!(fetch_file_patches(&mut client, "o/r", 1).is_err());
}

#[test]
fn page_parse_fails_closed_on_malformed_metadata() {
    let mut page = sample_page();
    page["repository"]["pullRequests"]
        .as_object_mut()
        .unwrap()
        .remove("pageInfo");
    assert!(parse_pull_request_page(&page).is_err(), "missing pageInfo");
    let mut page = sample_page();
    page["repository"]["pullRequests"]["pageInfo"] =
        json!({"hasNextPage": true, "endCursor": null});
    assert!(
        parse_pull_request_page(&page).is_err(),
        "next page without cursor"
    );
    let mut page = sample_page();
    page["repository"]["pullRequests"]["nodes"] = Value::Null;
    assert!(parse_pull_request_page(&page).is_err(), "null nodes");
    let mut page = sample_page();
    page["repository"]["pullRequests"]["nodes"][0]["comments"] = Value::Null;
    assert!(
        parse_pull_request_page(&page).is_err(),
        "null nested connection"
    );
    let mut page = sample_page();
    page["repository"]["pullRequests"]["nodes"][0]["files"]["nodes"] = json!([null]);
    assert!(parse_pull_request_page(&page).is_err(), "null nested node");
}

#[test]
fn single_pull_request_parses_and_refuses_null() {
    let page = sample_page();
    let mut data = json!({"repository": page["repository"].clone()});
    data["repository"]["pullRequest"] = page["repository"]["pullRequests"]["nodes"][0].clone();
    let (metadata, pull_request) = parse_single_pull_request(&data).unwrap();
    assert_eq!(metadata.name_with_owner, "octocat/hello");
    assert_eq!(pull_request.number, 7);
    data["repository"]["pullRequest"] = Value::Null;
    assert!(parse_single_pull_request(&data).is_err());
}

#[test]
fn check_run_truncation_is_detected() {
    let mut page = sample_page();
    page["repository"]["pullRequests"]["nodes"][0]["commits"]["nodes"][0]["commit"]["checkSuites"]
        ["nodes"][0]["checkRuns"]["pageInfo"] = json!({"hasNextPage": true});
    let parsed = parse_pull_request_page(&page).unwrap();
    assert!(parsed.pull_requests[0].check_runs_truncated);
    assert!(
        !parse_pull_request_page(&sample_page())
            .unwrap()
            .pull_requests[0]
            .check_runs_truncated
    );
}

fn pr_with_more_check_suites() -> PullRequestData {
    let mut page = sample_page();
    let commit =
        &mut page["repository"]["pullRequests"]["nodes"][0]["commits"]["nodes"][0]["commit"];
    commit["id"] = json!("COMMIT_1");
    commit["checkSuites"]["pageInfo"] = json!({"hasNextPage": true, "endCursor": "SUITE_PAGE_1"});
    parse_pull_request_page(&page)
        .unwrap()
        .pull_requests
        .remove(0)
}

#[test]
fn hydrates_check_suites_and_their_nested_check_run_pages() {
    let mut pr = pr_with_more_check_suites();
    let mut client = rest_client(vec![
        Ok(json!({"data": {"node": {"checkSuites": {
            "pageInfo": {"hasNextPage": false}, "nodes": [{
                "id": "SUITE_2", "checkRuns": {
                    "pageInfo": {"hasNextPage": true, "endCursor": "RUN_PAGE_1"},
                    "nodes": [{"id": "CHK2", "name": "test", "status": "IN_PROGRESS"}]
                }
            }]
        }}}}).to_string()),
        Ok(json!({"data": {"node": {"checkRuns": {
            "pageInfo": {"hasNextPage": false},
            "nodes": [{"id": "CHK3", "name": "lint", "status": "COMPLETED", "conclusion": "SUCCESS"}]
        }}}}).to_string()),
    ]);
    hydrate_pull_request(&mut client, &mut pr).unwrap();
    assert_eq!(
        pr.check_runs
            .iter()
            .map(|check| check.id.as_str())
            .collect::<Vec<_>>(),
        vec!["CHK1", "CHK2", "CHK3"]
    );
    assert_eq!(pr.check_runs[1].status, "IN_PROGRESS");
    assert_eq!(pr.head_sha.as_deref(), Some("abc123"));
    assert!(!pr.check_runs_truncated);
    assert!(pr.pending.is_empty());
    let query = single_pull_request_query();
    assert!(query.contains("id oid"));
    assert!(
        query.contains(
            "checkSuites(first: 10) {\n                pageInfo { hasNextPage endCursor }"
        )
    );
}

#[test]
fn malformed_check_run_continuation_fails_instead_of_skipping_a_run() {
    let mut pr = pr_with_more_check_suites();
    let mut client = rest_client(vec![Ok(json!({"data": {"node": {"checkSuites": {
        "pageInfo": {"hasNextPage": false},
        "nodes": [{"id": "SUITE_2", "checkRuns": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{"name": "missing id"}]}}]
    }}}})
    .to_string())]);
    let error = hydrate_pull_request(&mut client, &mut pr).unwrap_err();
    assert!(error.to_string().contains("check run has no id"));
}

/// Mutate the sample's only PR node and expect the page to fail closed.
fn assert_node_rejected(name: &str, mutate: impl FnOnce(&mut Value)) {
    let mut page = sample_page();
    mutate(&mut page["repository"]["pullRequests"]["nodes"][0]);
    assert!(parse_pull_request_page(&page).is_err(), "{name}");
}

fn pointer_mut<'a>(node: &'a mut Value, path: &str) -> &'a mut Value {
    node.pointer_mut(path)
        .unwrap_or_else(|| panic!("fixture path {path}"))
}

const NESTED_CONNECTIONS: [&str; 7] = [
    "/reviews",
    "/reviewThreads",
    "/reviewThreads/nodes/0/comments",
    "/comments",
    "/files",
    "/commits/nodes/0/commit/checkSuites",
    "/commits/nodes/0/commit/checkSuites/nodes/0/checkRuns",
];

#[test]
fn every_selected_nested_connection_fails_closed_when_incomplete() {
    for path in NESTED_CONNECTIONS {
        let (parent, field) = path.rsplit_once('/').unwrap();
        assert_node_rejected(&format!("{path} absent"), |node| {
            pointer_mut(node, if parent.is_empty() { "" } else { parent })
                .as_object_mut()
                .unwrap()
                .remove(field);
        });
        assert_node_rejected(&format!("{path} null"), |node| {
            *pointer_mut(node, path) = Value::Null;
        });
        assert_node_rejected(&format!("{path} without pageInfo"), |node| {
            pointer_mut(node, path)
                .as_object_mut()
                .unwrap()
                .remove("pageInfo");
        });
        assert_node_rejected(&format!("{path} null pageInfo"), |node| {
            pointer_mut(node, path)["pageInfo"] = Value::Null;
        });
        assert_node_rejected(&format!("{path} without hasNextPage"), |node| {
            pointer_mut(node, path)["pageInfo"] = json!({"endCursor": "X"});
        });
        assert_node_rejected(&format!("{path} without nodes"), |node| {
            pointer_mut(node, path)
                .as_object_mut()
                .unwrap()
                .remove("nodes");
        });
        assert_node_rejected(&format!("{path} with a null node"), |node| {
            pointer_mut(node, path)["nodes"] = json!([null]);
        });
    }
}

#[test]
fn head_commit_selection_is_validated_without_inventing_history_pagination() {
    // `commits(last: 1)` selects no pageInfo: the sample (which has none)
    // parses, and an empty connection is a PR with no head commit.
    let parsed = parse_pull_request_page(&sample_page()).unwrap();
    assert_eq!(parsed.pull_requests[0].head_sha.as_deref(), Some("abc123"));
    let mut page = sample_page();
    page["repository"]["pullRequests"]["nodes"][0]["commits"] = json!({"nodes": []});
    assert!(
        parse_pull_request_page(&page).unwrap().pull_requests[0]
            .head_sha
            .is_none()
    );
    assert_node_rejected("commits absent", |node| {
        node.as_object_mut().unwrap().remove("commits");
    });
    assert_node_rejected("commits null", |node| node["commits"] = Value::Null);
    assert_node_rejected("commits without nodes", |node| {
        node["commits"] = json!({});
    });
    assert_node_rejected("null commit node", |node| {
        node["commits"]["nodes"] = json!([null]);
    });
    assert_node_rejected("commit node without commit", |node| {
        node["commits"]["nodes"][0]["commit"] = Value::Null;
    });
    for field in ["id", "oid"] {
        assert_node_rejected(&format!("commit without {field}"), |node| {
            node["commits"]["nodes"][0]["commit"]
                .as_object_mut()
                .unwrap()
                .remove(field);
        });
    }
}

#[test]
fn malformed_first_slice_records_fail_instead_of_being_dropped() {
    for (path, field) in [
        ("/reviews/nodes/0", "id"),
        ("/reviewThreads/nodes/0", "id"),
        ("/reviewThreads/nodes/0/comments/nodes/0", "id"),
        ("/reviewThreads/nodes/0/comments/nodes/0", "createdAt"),
        ("/reviewThreads/nodes/0/comments/nodes/0", "path"),
        ("/comments/nodes/0", "id"),
        ("/comments/nodes/0", "createdAt"),
        ("/files/nodes/0", "path"),
        ("/commits/nodes/0/commit/checkSuites/nodes/0", "id"),
        (
            "/commits/nodes/0/commit/checkSuites/nodes/0/checkRuns/nodes/0",
            "id",
        ),
    ] {
        assert_node_rejected(&format!("{path} without {field}"), |node| {
            pointer_mut(node, path)
                .as_object_mut()
                .unwrap()
                .remove(field);
        });
    }
}

#[test]
fn legitimately_nullable_fields_still_parse() {
    let mut page = sample_page();
    let node = &mut page["repository"]["pullRequests"]["nodes"][0];
    node["author"] = Value::Null;
    node["mergedBy"] = Value::Null;
    node["reviews"]["nodes"][0]["author"] = Value::Null;
    node["reviews"]["nodes"][0]["submittedAt"] = Value::Null;
    let comment = &mut node["reviewThreads"]["nodes"][0]["comments"]["nodes"][0];
    comment["line"] = Value::Null;
    comment["author"] = Value::Null;
    node["comments"]["nodes"][0]["author"] = Value::Null;
    node["commits"]["nodes"][0]["commit"]["checkSuites"]["nodes"][0]["checkRuns"]["nodes"][0]["conclusion"] =
        Value::Null;
    let pull_request = parse_pull_request_page(&page)
        .unwrap()
        .pull_requests
        .remove(0);
    assert!(pull_request.author.is_none() && pull_request.merged_by.is_none());
    assert!(pull_request.reviews[0].submitted_at.is_none());
    assert!(pull_request.review_comments[0].line.is_none());
    assert_eq!(pull_request.conversation_comments.len(), 1);
    assert!(pull_request.check_runs[0].conclusion.is_none());
}

/// One pull request whose `field` first slice reports a second page.
fn pr_continuing(path: &str) -> PullRequestData {
    let mut page = sample_page();
    let node = &mut page["repository"]["pullRequests"]["nodes"][0];
    node["id"] = json!("PR_7");
    pointer_mut(node, path)["pageInfo"] = json!({"hasNextPage": true, "endCursor": "P1"});
    let pull_request = parse_pull_request_page(&page)
        .unwrap()
        .pull_requests
        .remove(0);
    assert_eq!(pull_request.pending.len(), 1, "{path}");
    pull_request
}

fn continuation_response(field: &str, nodes: Value) -> Result<String> {
    Ok(json!({"data": {"node": {field: {
        "pageInfo": {"hasNextPage": false, "endCursor": null},
        "nodes": nodes
    }}}})
    .to_string())
}

#[test]
fn malformed_continuation_records_fail_instead_of_being_dropped() {
    for (path, field, record) in [
        ("/reviews", "reviews", json!({"state": "APPROVED"})),
        (
            "/reviewThreads",
            "reviewThreads",
            json!({"comments": {
            "pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []}}),
        ),
        ("/reviewThreads", "reviewThreads", json!({"id": "THREAD2"})),
        (
            "/reviewThreads/nodes/0/comments",
            "comments",
            json!({
            "id": "RC2", "path": "a.rs"}),
        ),
        ("/comments", "comments", json!({"id": "IC2"})),
        ("/files", "files", json!({"changeType": "ADDED"})),
        (
            "/commits/nodes/0/commit/checkSuites",
            "checkSuites",
            json!({"checkRuns": {
            "pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []}}),
        ),
        (
            "/commits/nodes/0/commit/checkSuites",
            "checkSuites",
            json!({
            "id": "SUITE_2", "checkRuns": {"nodes": []}}),
        ),
    ] {
        let mut pull_request = pr_continuing(path);
        let mut client = rest_client(vec![continuation_response(field, json!([record]))]);
        assert!(
            hydrate_pull_request(&mut client, &mut pull_request).is_err(),
            "{path}: {record}"
        );
    }
}

#[test]
fn well_formed_continuations_land_every_record() {
    let mut pull_request = pr_continuing("/files");
    let mut client = rest_client(vec![continuation_response(
        "files",
        json!([{"path": "b.rs", "changeType": "ADDED", "additions": 1, "deletions": 0}]),
    )]);
    hydrate_pull_request(&mut client, &mut pull_request).unwrap();
    assert_eq!(pull_request.files.len(), 2);
    assert!(pull_request.pending.is_empty());
}

#[test]
fn query_texts_share_the_node_selection() {
    for query in [
        repository_pull_requests_query(25),
        single_pull_request_query(),
    ] {
        assert!(query.contains("files(first: 100)"));
        // ALL keeps superseded re-runs; the default (LATEST) drops them.
        assert!(query.contains("checkRuns(first: 20, filterBy: {checkType: ALL})"));
        assert!(!query.contains("{NODE}") && !query.contains("{REPOSITORY}"));
    }
    assert!(single_pull_request_query().contains("pullRequest(number: $number)"));
}

#[test]
fn parses_rest_file_patches() {
    let value = json!([
        {"filename": "src/lib.rs", "patch": "@@ -1 +1 @@"},
        {"filename": "renamed.rs", "previous_filename": "old.rs", "patch": null}
    ]);
    let patches = parse_rest_file_patches(&value);
    assert_eq!(patches.len(), 2);
    assert_eq!(patches[0].0, "src/lib.rs");
    assert_eq!(patches[0].2.as_deref(), Some("@@ -1 +1 @@"));
    assert_eq!(patches[1].1.as_deref(), Some("old.rs"));
}
