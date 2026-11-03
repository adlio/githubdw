//! The light search node: identifying metadata, no comment or diff history.

use serde_json::Value;

use crate::fetch::pull_requests::{
    ActorReference, REPOSITORY_HEADER_SELECTION, RepositoryMetadata, ReviewData,
    parse_repository_metadata,
};

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
        number title body state isDraft
        createdAt updatedAt mergedAt closedAt
        baseRefName headRefName baseRefOid headRefOid
        additions deletions changedFiles
        author { login __typename }
        mergedBy { login __typename }
        comments { totalCount }
        reviews { totalCount }
        loginReviews: reviews(author: $login, first: 100) {
          totalCount
          pageInfo { hasNextPage }
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
    /// More than 100 reviews by the login; the rest were not read.
    pub login_reviews_truncated: bool,
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

/// An error the provider raises for a node the viewer may not see. These are
/// expected skips (counted, never stored, never failures).
fn is_inaccessible(error: &Value) -> bool {
    let kind = error
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_lowercase();
    matches!(kind, "FORBIDDEN" | "NOT_FOUND")
        || message.contains("saml")
        || message.contains("ip allow list")
        || message.contains("resource not accessible")
}

/// The search node index an error points at (`["search","nodes",i,...]`).
fn node_index(error: &Value) -> Option<usize> {
    let path = error.get("path")?.as_array()?;
    if path.first()?.as_str()? != "search" || path.get(1)?.as_str()? != "nodes" {
        return None;
    }
    path.get(2)?.as_u64().map(|i| i as usize)
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
    let truncated = login_reviews_value
        .get("pageInfo")
        .and_then(|p| p.get("hasNextPage"))
        .and_then(Value::as_bool)
        .ok_or(format!("PR #{number} loginReviews has no pageInfo"))?;
    let mut login_reviews = Vec::new();
    for review in login_reviews_value
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or(format!("PR #{number} loginReviews has no nodes"))?
    {
        if review.is_null() {
            return Err(format!("PR #{number} loginReviews contains a null node"));
        }
        login_reviews.push(ReviewData {
            id: text(review, "id").ok_or(format!("PR #{number} review has no id"))?,
            state: text(review, "state").unwrap_or_else(|| "COMMENTED".into()),
            body: None,
            submitted_at: text(review, "submittedAt"),
            author: actor(review, "author"),
        });
    }
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
    })
}
