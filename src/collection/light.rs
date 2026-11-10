//! The light search node: identifying metadata, no comment or diff history.

use serde_json::Value;

use crate::fetch::pull_requests::{
    ActorReference, REPOSITORY_HEADER_SELECTION, RepositoryMetadata, ReviewData,
    parse_repository_metadata,
};
use crate::fetch::{is_inaccessible, node_index};

/// Search query for the counts stage. Per node: identifying metadata, title
/// and body (always retained when fetched), sizes, connection totals with no
/// nodes, and the searched login's own reviews (id, state, submittedAt; no
/// bodies). No review threads, comments, files, commits, or patches.
pub fn light_search_query() -> String {
    LIGHT_QUERY_TEMPLATE.replace("{REPOSITORY}", REPOSITORY_HEADER_SELECTION)
}

const LIGHT_QUERY_TEMPLATE: &str = r#"
query($q: String!, $login: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  search(query: $q, type: ISSUE, first: 100, after: $cursor) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        id number title body state isDraft
        createdAt updatedAt mergedAt closedAt
        baseRefName headRefName baseRefOid headRefOid
        additions deletions changedFiles
        author { login __typename }
        mergedBy { login __typename }
        comments { totalCount }
        reviews { totalCount }
        loginReviews: reviews(author: $login, first: 100) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes { id state submittedAt author { login __typename } }
        }
        repository {
          {REPOSITORY}
        }
      }
    }
  }
}
"#;

/// One light pull request.
#[derive(Debug, Clone)]
pub struct LightPullRequest {
    pub repository: RepositoryMetadata,
    pub number: i64,
    pub title: Option<String>,
    pub body: Option<String>,
    pub state: String,
    pub is_draft: bool,
    pub created_at: String,
    pub updated_at: Option<String>,
    pub merged_at: Option<String>,
    pub closed_at: Option<String>,
    pub base_ref: Option<String>,
    pub head_ref: Option<String>,
    pub base_oid: Option<String>,
    pub head_oid: Option<String>,
    pub additions: i64,
    pub deletions: i64,
    pub changed_files: i64,
    pub author: Option<ActorReference>,
    pub merged_by: Option<ActorReference>,
    /// Conversation comments (`comments.totalCount`).
    pub comment_total: i64,
    /// All reviews on the pull request (`reviews.totalCount`).
    pub review_total: i64,
    pub login_reviews: Vec<ReviewData>,
    /// The login's review list has pages not yet read. Cleared once
    /// `complete_login_reviews` reads them all.
    pub login_reviews_truncated: bool,
    /// GraphQL node id, used to continue the review list.
    pub node_id: Option<String>,
    /// Cursor after the last review read.
    pub login_reviews_cursor: Option<String>,
}

/// Continuation of one pull request's `loginReviews` connection.
pub const LOGIN_REVIEWS_CONTINUATION_QUERY: &str = r#"
query($id: ID!, $login: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  node(id: $id) {
    ... on PullRequest {
      loginReviews: reviews(author: $login, first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { id state submittedAt author { login __typename } }
      }
    }
  }
}
"#;

/// Page through the rest of the login's reviews on `item`. On success the
/// list is complete and `login_reviews_truncated` is cleared. Any error is
/// returned with the item left marked truncated.
pub fn complete_login_reviews(
    client: &mut crate::fetch::GhClient,
    item: &mut LightPullRequest,
    login: &str,
) -> crate::Result<()> {
    let id = item.node_id.clone().ok_or_else(|| {
        crate::Error::GitHubApi(format!(
            "PR #{} has no node id to continue reviews",
            item.number
        ))
    })?;
    let mut cursor = item.login_reviews_cursor.clone();
    let mut seen: std::collections::HashSet<String> =
        item.login_reviews.iter().map(|r| r.id.clone()).collect();
    // A provider that kept answering would otherwise loop forever.
    for _ in 0..1000 {
        let Some(after) = cursor.clone() else {
            return Err(crate::Error::GitHubApi(format!(
                "PR #{}: review list reports more pages but no cursor",
                item.number
            )));
        };
        let data = client.graphql(
            LOGIN_REVIEWS_CONTINUATION_QUERY,
            &[
                ("id", id.as_str()),
                ("login", login),
                ("cursor", after.as_str()),
            ],
        )?;
        let connection = data
            .get("node")
            .and_then(|n| n.get("loginReviews"))
            .filter(|c| !c.is_null())
            .ok_or_else(|| {
                crate::Error::GitHubApi(format!("PR #{}: review continuation missing", item.number))
            })?;
        let (reviews, has_next, end) =
            parse_review_connection(connection, item.number).map_err(crate::Error::GitHubApi)?;
        for review in reviews {
            if seen.insert(review.id.clone()) {
                item.login_reviews.push(review);
            }
        }
        if !has_next {
            item.login_reviews_truncated = false;
            item.login_reviews_cursor = None;
            return Ok(());
        }
        if end == cursor {
            return Err(crate::Error::GitHubApi(format!(
                "PR #{}: review cursor repeated",
                item.number
            )));
        }
        cursor = end;
    }
    Err(crate::Error::GitHubApi(format!(
        "PR #{}: review list did not end after 1000 pages",
        item.number
    )))
}

type ReviewPage = (Vec<ReviewData>, bool, Option<String>);

fn parse_review_connection(connection: &Value, number: i64) -> Result<ReviewPage, String> {
    let info = connection
        .get("pageInfo")
        .filter(|v| !v.is_null())
        .ok_or(format!("PR #{number} loginReviews has no pageInfo"))?;
    let has_next = info
        .get("hasNextPage")
        .and_then(Value::as_bool)
        .ok_or(format!("PR #{number} loginReviews has no hasNextPage"))?;
    let end = text(info, "endCursor");
    let mut reviews = Vec::new();
    for review in connection
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or(format!("PR #{number} loginReviews has no nodes"))?
    {
        if review.is_null() {
            return Err(format!("PR #{number} loginReviews contains a null node"));
        }
        reviews.push(ReviewData {
            id: text(review, "id").ok_or(format!("PR #{number} review has no id"))?,
            state: text(review, "state").unwrap_or_else(|| "COMMENTED".into()),
            body: None,
            submitted_at: text(review, "submittedAt"),
            author: actor(review, "author"),
        });
    }
    Ok((reviews, has_next, end))
}

pub(super) struct LightPage {
    pub total_count: i64,
    pub items: Vec<LightPullRequest>,
    pub inaccessible: i64,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

pub(super) enum PageError {
    RateLimited(String),
    Failed(String),
}

fn text(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn integer(value: &Value, field: &str) -> Option<i64> {
    value.get(field).and_then(Value::as_i64)
}

fn actor(value: &Value, field: &str) -> Option<ActorReference> {
    let inner = value.get(field)?;
    Some(ActorReference {
        login: inner.get("login")?.as_str()?.to_string(),
        type_name: inner
            .get("__typename")
            .and_then(Value::as_str)
            .unwrap_or("User")
            .to_string(),
    })
}

pub(super) fn parse_light_page(data: &Value, errors: &[Value]) -> Result<LightPage, PageError> {
    let mut skipped_nodes = std::collections::HashSet::new();
    for error in errors {
        let kind = error
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown GraphQL error")
            .to_string();
        if kind == "RATE_LIMITED" || message.to_lowercase().contains("rate limit") {
            return Err(PageError::RateLimited(message));
        }
        match node_index(error) {
            Some(index) if is_inaccessible(error) => {
                skipped_nodes.insert(index);
            }
            _ => return Err(PageError::Failed(message)),
        }
    }
    let fail = |m: &str| PageError::Failed(m.to_string());
    let search = data
        .get("search")
        .filter(|v| !v.is_null())
        .ok_or_else(|| fail("search block missing from response"))?;
    let total_count =
        integer(search, "issueCount").ok_or_else(|| fail("search.issueCount missing"))?;
    let info = search
        .get("pageInfo")
        .filter(|v| !v.is_null())
        .ok_or_else(|| fail("search has no pageInfo"))?;
    let has_next_page = info
        .get("hasNextPage")
        .and_then(Value::as_bool)
        .ok_or_else(|| fail("search has no hasNextPage"))?;
    let end_cursor = text(info, "endCursor");
    if has_next_page && end_cursor.is_none() {
        return Err(fail("search reports another page but no endCursor"));
    }
    let nodes = search
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| fail("search has no nodes array"))?;
    let mut items = Vec::new();
    let mut inaccessible = 0;
    for (index, node) in nodes.iter().enumerate() {
        if skipped_nodes.contains(&index) {
            inaccessible += 1;
            continue;
        }
        if node.is_null() {
            return Err(fail("search contains a null node with no error"));
        }
        // An issue that slips past `is:pr` answers the fragment with `{}`.
        if node.as_object().is_some_and(serde_json::Map::is_empty) {
            continue;
        }
        items.push(parse_light_node(node).map_err(PageError::Failed)?);
    }
    // A skipped node outside the served nodes is still a node the provider
    // counted; it is accounted for once here.
    inaccessible += skipped_nodes.iter().filter(|i| **i >= nodes.len()).count() as i64;
    Ok(LightPage {
        total_count,
        items,
        inaccessible,
        has_next_page,
        end_cursor,
    })
}

fn parse_light_node(node: &Value) -> Result<LightPullRequest, String> {
    let number = integer(node, "number").ok_or("pull request number missing")?;
    let created_at = text(node, "createdAt").ok_or(format!("PR #{number} has no createdAt"))?;
    let repository = node
        .get("repository")
        .filter(|v| !v.is_null())
        .ok_or(format!("PR #{number} has no repository"))?;
    let repository = parse_repository_metadata(repository).map_err(|e| e.to_string())?;
    let total = |field: &str| -> Result<i64, String> {
        node.get(field)
            .and_then(|c| c.get("totalCount"))
            .and_then(Value::as_i64)
            .ok_or(format!("PR #{number} has no {field}.totalCount"))
    };
    let login_reviews_value = node
        .get("loginReviews")
        .filter(|v| !v.is_null())
        .ok_or(format!("PR #{number} has no loginReviews"))?;
    let (login_reviews, truncated, review_cursor) =
        parse_review_connection(login_reviews_value, number)?;
    Ok(LightPullRequest {
        repository,
        number,
        title: text(node, "title"),
        body: text(node, "body"),
        state: text(node, "state").unwrap_or_else(|| "OPEN".into()),
        is_draft: node
            .get("isDraft")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at,
        updated_at: text(node, "updatedAt"),
        merged_at: text(node, "mergedAt"),
        closed_at: text(node, "closedAt"),
        base_ref: text(node, "baseRefName"),
        head_ref: text(node, "headRefName"),
        base_oid: text(node, "baseRefOid"),
        head_oid: text(node, "headRefOid"),
        additions: integer(node, "additions").unwrap_or(0),
        deletions: integer(node, "deletions").unwrap_or(0),
        changed_files: integer(node, "changedFiles").unwrap_or(0),
        author: actor(node, "author"),
        merged_by: actor(node, "mergedBy"),
        comment_total: total("comments")?,
        review_total: total("reviews")?,
        login_reviews,
        login_reviews_truncated: truncated,
        node_id: text(node, "id"),
        login_reviews_cursor: review_cursor,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::fetch::test_support::FixtureTransport;
    use crate::fetch::{GhClient, GhTransport};

    fn review_page(ids: &[&str], has_next: bool, end: Option<&str>) -> Value {
        let nodes: Vec<Value> = ids
            .iter()
            .map(|id| {
                json!({"id": id, "state": "APPROVED", "submittedAt": "2026-11-02T10:00:00Z",
                       "author": {"login": "alice", "__typename": "User"}})
            })
            .collect();
        json!({"totalCount": ids.len(), "pageInfo": {"hasNextPage": has_next, "endCursor": end},
               "nodes": nodes})
    }

    fn node(number: i64, reviews: Value) -> Value {
        json!({
            "id": format!("PR_{number}"), "number": number, "title": "t", "body": "b",
            "state": "OPEN", "isDraft": false, "createdAt": "2026-11-02T09:00:00Z",
            "updatedAt": "2026-11-02T10:00:00Z", "mergedAt": null, "closedAt": null,
            "additions": 1, "deletions": 0, "changedFiles": 1,
            "author": {"login": "bob"},
            "comments": {"totalCount": 0}, "reviews": {"totalCount": 1},
            "loginReviews": reviews,
            "repository": {"nameWithOwner": "acme/app", "primaryLanguage": null,
                           "isFork": false, "isPrivate": false,
                           "defaultBranchRef": {"name": "main"},
                           "createdAt": "2020-01-01T00:00:00Z"}
        })
    }

    fn search(nodes: Vec<Value>, total: i64, has_next: bool, end: Option<&str>) -> Value {
        json!({"search": {"issueCount": total,
                          "pageInfo": {"hasNextPage": has_next, "endCursor": end},
                          "nodes": nodes}})
    }

    fn page(data: &Value, errors: &[Value]) -> Result<LightPage, String> {
        parse_light_page(data, errors).map_err(|error| match error {
            PageError::RateLimited(reason) => format!("rate-limited: {reason}"),
            PageError::Failed(reason) => format!("failed: {reason}"),
        })
    }

    fn truncated_item() -> LightPullRequest {
        let parsed = parse_light_node(&node(7, review_page(&["R1"], true, Some("c1")))).unwrap();
        assert!(parsed.login_reviews_truncated);
        parsed
    }

    fn continuation(page: Value) -> crate::Result<String> {
        Ok(json!({"data": {"node": {"loginReviews": page}}}).to_string())
    }

    fn client(responses: Vec<crate::Result<String>>) -> GhClient {
        GhClient::with_transport(Box::new(FixtureTransport::new(responses))).without_sleeping()
    }

    #[test]
    fn the_light_node_parses_identity_sizes_and_the_login_reviews() {
        let item = parse_light_node(&node(7, review_page(&["R1", "R2"], false, None))).unwrap();
        assert_eq!(item.number, 7);
        assert_eq!(item.repository.name_with_owner, "acme/app");
        assert_eq!(item.author.as_ref().unwrap().type_name, "User");
        assert_eq!(item.login_reviews.len(), 2);
        assert!(
            item.login_reviews[0].body.is_none(),
            "light reads carry no bodies"
        );
        assert!(!item.login_reviews_truncated);
        assert!(light_search_query().contains("nameWithOwner"));
    }

    #[test]
    fn malformed_review_connections_are_rejected() {
        let mut no_flag = review_page(&["R1"], false, None);
        no_flag["pageInfo"] = json!({"endCursor": null});
        let error = parse_light_node(&node(7, no_flag)).unwrap_err();
        assert_eq!(error, "PR #7 loginReviews has no hasNextPage");
        let mut null_node = review_page(&[], false, None);
        null_node["nodes"] = json!([null]);
        let error = parse_light_node(&node(7, null_node)).unwrap_err();
        assert_eq!(error, "PR #7 loginReviews contains a null node");
    }

    #[test]
    fn continuing_reviews_reads_every_page_once() {
        let mut item = truncated_item();
        let mut client = client(vec![
            continuation(review_page(&["R1", "R2"], true, Some("c2"))),
            continuation(review_page(&["R3"], false, None)),
        ]);
        complete_login_reviews(&mut client, &mut item, "alice").unwrap();
        let ids: Vec<&str> = item.login_reviews.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            ["R1", "R2", "R3"],
            "R1 repeated on page two is kept once"
        );
        assert!(!item.login_reviews_truncated);
        assert_eq!(item.login_reviews_cursor, None);
    }

    #[test]
    fn continuing_reviews_fails_without_an_id_or_cursor_and_on_a_missing_node() {
        let mut item = truncated_item();
        item.node_id = None;
        let error = complete_login_reviews(&mut client(vec![]), &mut item, "alice").unwrap_err();
        assert!(error.to_string().contains("has no node id"), "{error}");

        let mut item = truncated_item();
        item.login_reviews_cursor = None;
        let error = complete_login_reviews(&mut client(vec![]), &mut item, "alice").unwrap_err();
        assert!(
            error.to_string().contains("more pages but no cursor"),
            "{error}"
        );

        let mut item = truncated_item();
        let mut missing = client(vec![Ok(json!({"data": {"node": null}}).to_string())]);
        let error = complete_login_reviews(&mut missing, &mut item, "alice").unwrap_err();
        assert!(
            error.to_string().contains("review continuation missing"),
            "{error}"
        );
        assert!(
            item.login_reviews_truncated,
            "a failure leaves the item truncated"
        );
    }

    #[test]
    fn a_repeated_cursor_or_an_endless_list_is_an_error() {
        let mut item = truncated_item();
        let mut repeating = client(vec![continuation(review_page(&["R2"], true, Some("c1")))]);
        let error = complete_login_reviews(&mut repeating, &mut item, "alice").unwrap_err();
        assert!(
            error.to_string().contains("review cursor repeated"),
            "{error}"
        );

        struct Endless(Mutex<usize>);
        impl GhTransport for Endless {
            fn run(&self, _arguments: &[String]) -> crate::Result<String> {
                let mut calls = self.0.lock().unwrap();
                *calls += 1;
                let id = format!("E{calls}");
                let cursor = format!("e{calls}");
                continuation(review_page(&[id.as_str()], true, Some(cursor.as_str())))
            }
        }
        let mut item = truncated_item();
        let mut endless =
            GhClient::with_transport(Box::new(Endless(Mutex::new(0)))).without_sleeping();
        let error = complete_login_reviews(&mut endless, &mut item, "alice").unwrap_err();
        assert!(
            error.to_string().contains("did not end after 1000 pages"),
            "{error}"
        );
        assert_eq!(endless.requests_issued(), 1000);
    }

    #[test]
    fn page_errors_are_classified() {
        let data = search(vec![node(1, review_page(&[], false, None))], 1, false, None);
        let limited = page(
            &data,
            &[json!({"type": "RATE_LIMITED", "message": "slow down"})],
        );
        assert_eq!(limited.err().unwrap(), "rate-limited: slow down");
        let limited = page(&data, &[json!({"message": "API rate limit exceeded"})]);
        assert!(limited.err().unwrap().starts_with("rate-limited"));
        let other = page(
            &data,
            &[json!({"type": "INTERNAL", "path": ["search", "nodes", 0]})],
        );
        assert_eq!(other.err().unwrap(), "failed: unknown GraphQL error");
        let outside = page(
            &data,
            &[json!({"type": "FORBIDDEN", "message": "no", "path": ["viewer"]})],
        );
        assert_eq!(
            outside.err().unwrap(),
            "failed: no",
            "a non-node error is not a skip"
        );
    }

    #[test]
    fn inaccessible_nodes_are_recognised_by_type_or_message() {
        for error in [
            json!({"type": "NOT_FOUND", "message": "gone"}),
            json!({"message": "Resource protected by organization SAML enforcement."}),
            json!({"message": "Blocked by the IP allow list"}),
            json!({"message": "Resource not accessible by integration"}),
        ] {
            let mut error = error;
            error["path"] = json!(["search", "nodes", 0]);
            let data = search(
                vec![Value::Null, node(2, review_page(&[], false, None))],
                2,
                false,
                None,
            );
            let parsed = page(&data, &[error]).unwrap();
            assert_eq!((parsed.items.len(), parsed.inaccessible), (1, 1));
        }
        // A skipped node index past the served nodes is still counted once.
        let data = search(vec![], 1, false, None);
        let error = json!({"type": "FORBIDDEN", "path": ["search", "nodes", 3]});
        assert_eq!(page(&data, &[error]).unwrap().inaccessible, 1);
    }

    #[test]
    fn malformed_pages_fail_and_issue_nodes_are_skipped() {
        let no_cursor = search(vec![], 5, true, None);
        assert_eq!(
            page(&no_cursor, &[]).err().unwrap(),
            "failed: search reports another page but no endCursor"
        );
        let null_node = search(vec![Value::Null], 1, false, None);
        assert_eq!(
            page(&null_node, &[]).err().unwrap(),
            "failed: search contains a null node with no error"
        );
        let with_issue = search(
            vec![json!({}), node(3, review_page(&[], false, None))],
            2,
            true,
            Some("n2"),
        );
        let parsed = page(&with_issue, &[]).unwrap();
        assert_eq!(
            parsed.items.len(),
            1,
            "an issue node answers the fragment with {{}}"
        );
        assert_eq!(parsed.items[0].number, 3);
        assert_eq!(parsed.end_cursor.as_deref(), Some("n2"));
        assert!(parsed.has_next_page);
    }
}
