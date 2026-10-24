//! Cursor continuation for nested GraphQL connections.
//!
//! The page queries select the first slice of each nested collection (reviews,
//! review threads, comments, files). When GitHub reports `hasNextPage` on one of
//! those slices, the rest is fetched here by addressing the owning object with
//! `node(id:)` and walking the same connection from its `endCursor`, so a heavy
//! pull request or issue lands complete instead of silently truncated.

use serde_json::Value;

use crate::error::{Error, Result};
use crate::fetch::GhClient;

/// Page size for continuation requests (GitHub's maximum for these connections).
pub const CONTINUATION_PAGE_SIZE: u32 = 100;

/// Upper bound on continuation pages per connection, so a misbehaving cursor
/// cannot loop forever. 100 pages × 100 nodes covers GitHub's own 3,000-file cap
/// and any realistic comment thread with ample room.
pub const MAX_CONTINUATION_PAGES: u32 = 100;

/// A nested connection that still has unfetched nodes.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingConnection {
    /// GraphQL node id of the object that owns the connection.
    pub owner_id: Option<String>,
    /// GraphQL type of the owner (`PullRequest`, `PullRequestReviewThread`, `Issue`).
    pub owner_type: &'static str,
    /// Connection field name on the owner.
    pub field: &'static str,
    /// Cursor to resume after. `None` with a pending page is a provider fault.
    pub cursor: Option<String>,
}

/// Read a SELECTED nested connection strictly: its first-slice nodes plus the
/// continuation when GitHub reports more.
///
/// Every connection the page queries select is required, so an absent or null
/// connection, a missing `pageInfo`/`hasNextPage`, a missing `nodes` array, or
/// a null node fails the owning item instead of reading as an empty, complete
/// collection.
pub fn required_connection<'a>(
    value: &'a Value,
    collection: &'static str,
    label: &str,
    owner_id: Option<&str>,
    owner_type: &'static str,
) -> Result<(Vec<&'a Value>, Option<PendingConnection>)> {
    let connection = value
        .get(collection)
        .filter(|connection| !connection.is_null())
        .ok_or_else(|| Error::GitHubApi(format!("{label} connection is missing or null")))?;
    let page_info = connection
        .get("pageInfo")
        .filter(|info| !info.is_null())
        .ok_or_else(|| Error::GitHubApi(format!("{label} has no pageInfo")))?;
    let has_next = page_info
        .get("hasNextPage")
        .and_then(Value::as_bool)
        .ok_or_else(|| Error::GitHubApi(format!("{label} has no hasNextPage")))?;
    let nodes = connection
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::GitHubApi(format!("{label} has no nodes array")))?;
    if nodes.iter().any(Value::is_null) {
        return Err(Error::GitHubApi(format!("{label} contains a null node")));
    }
    let nodes: Vec<&Value> = nodes.iter().collect();
    if !has_next {
        return Ok((nodes, None));
    }
    // A successor without a cursor or owner id is recorded as pending, not
    // rejected here: `fetch_remaining` refuses it, which fails just this item
    // (recorded and retried) rather than the whole page around it.
    Ok((
        nodes,
        Some(PendingConnection {
            owner_id: owner_id.map(str::to_string),
            owner_type,
            field: collection,
            cursor: page_info
                .get("endCursor")
                .and_then(Value::as_str)
                .map(str::to_string),
        }),
    ))
}

/// A required string field of a nested record, failing closed so a malformed
/// record fails its owning item instead of being dropped from the collection.
pub fn required_str(value: &Value, field: &str, label: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Error::GitHubApi(format!("{label} has no {field}")))
}

/// Build the continuation query for one connection.
pub fn continuation_query(owner_type: &str, field: &str, selection: &str) -> String {
    // Check runs default to LATEST (one run per name); the page queries ask
    // for ALL so re-runs are kept, and every continuation page must too.
    let filter = if field == "checkRuns" {
        ", filterBy: {checkType: ALL}"
    } else {
        ""
    };
    format!(
        "query($id: ID!, $cursor: String) {{\n  rateLimit {{ limit cost remaining resetAt }}\n  \
         node(id: $id) {{\n    ... on {owner_type} {{\n      {field}(first: {CONTINUATION_PAGE_SIZE}, \
         after: $cursor{filter}) {{\n        pageInfo {{ hasNextPage endCursor }}\n        nodes {{ {selection} }}\n      \
         }}\n    }}\n  }}\n}}\n"
    )
}

/// Fetch every remaining node of `pending`, in provider order.
///
/// Fails (so the caller records the item as failed and retries it later) when
/// the owner id or a cursor is missing, the owner vanished, a cursor does not
/// advance, or the page bound is exceeded — never returns a short list as if it
/// were complete.
pub fn fetch_remaining(
    client: &mut GhClient,
    pending: &PendingConnection,
    selection: &str,
) -> Result<Vec<Value>> {
    let describe = || format!("{}.{}", pending.owner_type, pending.field);
    let owner_id = pending.owner_id.as_deref().ok_or_else(|| {
        Error::GitHubApi(format!(
            "{} has more pages but its owner id is missing",
            describe()
        ))
    })?;
    let mut cursor = pending.cursor.clone().ok_or_else(|| {
        Error::GitHubApi(format!(
            "{} reports another page but no endCursor",
            describe()
        ))
    })?;
    let query = continuation_query(pending.owner_type, pending.field, selection);
    let mut collected = Vec::new();
    // Every cursor this walk has requested. A provider that hands back any of
    // them again (not only the immediately previous one) is cycling, and
    // following it would re-store the same nodes until the page bound.
    let mut seen = std::collections::HashSet::new();
    seen.insert(cursor.clone());
    for _ in 0..MAX_CONTINUATION_PAGES {
        let data = client.graphql(&query, &[("id", owner_id), ("cursor", cursor.as_str())])?;
        let connection = data
            .get("node")
            .filter(|node| !node.is_null())
            .and_then(|node| node.get(pending.field))
            .filter(|connection| !connection.is_null())
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "{} continuation returned no connection",
                    describe()
                ))
            })?;
        let nodes = connection
            .get("nodes")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "{} continuation returned no nodes array",
                    describe()
                ))
            })?;
        if nodes.iter().any(Value::is_null) {
            return Err(Error::GitHubApi(format!(
                "{} continuation returned a null node",
                describe()
            )));
        }
        collected.extend(nodes.iter().cloned());
        let page_info = connection.get("pageInfo");
        let has_next = page_info
            .and_then(|info| info.get("hasNextPage"))
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "{} continuation returned no page status",
                    describe()
                ))
            })?;
        if !has_next {
            return Ok(collected);
        }
        if nodes.is_empty() {
            return Err(Error::GitHubApi(format!(
                "{} continuation returned an empty page with a successor",
                describe()
            )));
        }
        let next = page_info
            .and_then(|info| info.get("endCursor"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::GitHubApi(format!(
                    "{} reports another page but no endCursor",
                    describe()
                ))
            })?;
        if !seen.insert(next.to_string()) {
            return Err(Error::GitHubApi(format!(
                "{} cursor did not advance (repeated cursor)",
                describe()
            )));
        }
        cursor = next.to_string();
    }
    Err(Error::GitHubApi(format!(
        "{} exceeded {MAX_CONTINUATION_PAGES} continuation pages",
        describe()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetch::test_support::FixtureTransport;

    fn pending(cursor: Option<&str>) -> PendingConnection {
        PendingConnection {
            owner_id: Some("PR_1".into()),
            owner_type: "PullRequest",
            field: "comments",
            cursor: cursor.map(str::to_string),
        }
    }

    fn page(has_next: bool, cursor: Option<&str>, ids: &[&str]) -> Result<String> {
        let nodes: Vec<Value> = ids.iter().map(|id| serde_json::json!({"id": id})).collect();
        Ok(serde_json::json!({"data": {"node": {"comments": {
            "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
            "nodes": nodes
        }}}})
        .to_string())
    }

    fn client(responses: Vec<Result<String>>) -> GhClient {
        GhClient::with_transport(Box::new(FixtureTransport::new(responses))).without_sleeping()
    }

    #[test]
    fn walks_every_remaining_page() {
        let mut client = client(vec![
            page(true, Some("B"), &["2"]),
            page(false, None, &["3"]),
        ]);
        let nodes = fetch_remaining(&mut client, &pending(Some("A")), "id").unwrap();
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn refuses_a_pending_page_without_cursor() {
        let mut client = client(vec![]);
        let error = fetch_remaining(&mut client, &pending(None), "id").unwrap_err();
        assert!(error.to_string().contains("no endCursor"));
    }

    #[test]
    fn refuses_a_cursor_that_does_not_advance() {
        let mut client = client(vec![page(true, Some("A"), &["2"])]);
        let error = fetch_remaining(&mut client, &pending(Some("A")), "id").unwrap_err();
        assert!(error.to_string().contains("did not advance"));
    }

    #[test]
    fn refuses_a_cursor_cycle_longer_than_one_page() {
        let mut client = client(vec![
            page(true, Some("B"), &["2"]),
            page(true, Some("A"), &["3"]),
        ]);
        let error = fetch_remaining(&mut client, &pending(Some("A")), "id").unwrap_err();
        assert!(error.to_string().contains("did not advance"), "{error}");
    }

    #[test]
    fn transport_failure_mid_walk_is_an_error_not_a_short_list() {
        let mut client = client(vec![
            page(true, Some("B"), &["2"]),
            Err(Error::GhCommand {
                exit_code: Some(1),
                stderr: "HTTP 404: Not Found".into(),
            }),
        ]);
        assert!(fetch_remaining(&mut client, &pending(Some("A")), "id").is_err());
    }

    #[test]
    fn empty_final_page_completes() {
        let mut client = client(vec![page(false, None, &[])]);
        let nodes = fetch_remaining(&mut client, &pending(Some("A")), "id").unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn refuses_an_empty_continuation_page_with_a_successor() {
        let mut client = client(vec![page(true, Some("B"), &[])]);
        let error = fetch_remaining(&mut client, &pending(Some("A")), "id").unwrap_err();
        assert!(error.to_string().contains("empty page with a successor"));
    }

    #[test]
    fn refuses_a_missing_owner() {
        let mut client = client(vec![Ok(r#"{"data": {"node": null}}"#.into())]);
        let error = fetch_remaining(&mut client, &pending(Some("A")), "id").unwrap_err();
        assert!(error.to_string().contains("no connection"));
    }

    #[test]
    fn refuses_incomplete_continuation_payloads() {
        for connection in [
            serde_json::json!({"pageInfo": {"hasNextPage": false}}),
            serde_json::json!({"nodes": []}),
            serde_json::json!({"nodes": [null], "pageInfo": {"hasNextPage": false}}),
        ] {
            let response = serde_json::json!({"data": {"node": {"comments": connection}}});
            let mut client = client(vec![Ok(response.to_string())]);
            assert!(fetch_remaining(&mut client, &pending(Some("A")), "id").is_err());
        }
    }

    fn connection(page_info: Value, nodes: Value) -> Value {
        serde_json::json!({"comments": {"pageInfo": page_info, "nodes": nodes}})
    }

    #[test]
    fn required_connection_reports_continuations_only_when_more_exist() {
        let more = connection(
            serde_json::json!({"hasNextPage": true, "endCursor": "X"}),
            serde_json::json!([{"id": "1"}]),
        );
        let (nodes, pending) =
            required_connection(&more, "comments", "c", Some("PR_1"), "PullRequest").unwrap();
        assert_eq!(nodes.len(), 1);
        let pending = pending.unwrap();
        assert_eq!(pending.cursor.as_deref(), Some("X"));
        assert_eq!(pending.owner_id.as_deref(), Some("PR_1"));
        let done = connection(
            serde_json::json!({"hasNextPage": false, "endCursor": null}),
            serde_json::json!([]),
        );
        let (nodes, pending) =
            required_connection(&done, "comments", "c", None, "PullRequest").unwrap();
        assert!(nodes.is_empty() && pending.is_none());
    }

    #[test]
    fn required_connection_fails_closed_on_every_incomplete_shape() {
        let cases = [
            ("absent", serde_json::json!({})),
            ("null", serde_json::json!({"comments": null})),
            (
                "no pageInfo",
                serde_json::json!({"comments": {"nodes": []}}),
            ),
            (
                "null pageInfo",
                serde_json::json!({"comments": {"pageInfo": null, "nodes": []}}),
            ),
            (
                "no hasNextPage",
                connection(serde_json::json!({"endCursor": "X"}), serde_json::json!([])),
            ),
            (
                "non-bool hasNextPage",
                connection(
                    serde_json::json!({"hasNextPage": "no"}),
                    serde_json::json!([]),
                ),
            ),
            (
                "no nodes",
                serde_json::json!({"comments": {"pageInfo": {"hasNextPage": false}}}),
            ),
            (
                "null node",
                connection(
                    serde_json::json!({"hasNextPage": false}),
                    serde_json::json!([null]),
                ),
            ),
        ];
        for (name, value) in cases {
            assert!(
                required_connection(&value, "comments", "c", Some("PR_1"), "PullRequest").is_err(),
                "{name}"
            );
        }
        // A successor missing its cursor or owner id parses as pending, and the
        // continuation walk refuses it, failing only the owning item.
        let more = connection(
            serde_json::json!({"hasNextPage": true, "endCursor": null}),
            serde_json::json!([{"id": "1"}]),
        );
        let (_, pending) =
            required_connection(&more, "comments", "c", None, "PullRequest").unwrap();
        let pending = pending.unwrap();
        let mut client = client(vec![]);
        assert!(fetch_remaining(&mut client, &pending, "id").is_err());
    }
}
