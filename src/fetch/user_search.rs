//! User-scoped pull-request fetching via GitHub's search API.
//!
//! The repository path walks `repository.pullRequests`, which is bounded by one
//! repo and paginates without limit. A user's work is not bounded that way:
//! their PRs and reviews are spread across every repository they touched,
//! including ones nobody monitors. Search is the only endpoint that answers
//! "everything by this person", so this module wraps it — and carries its two
//! constraints honestly:
//!
//! * **1,000 results per query.** `search` stops paginating there no matter how
//!   many matches `issueCount` reports. The caller partitions by `created:` date
//!   window and splits any window that reports more, so no result set is
//!   silently truncated at the cap.
//! * **A separate, much smaller budget.** Search is 30 requests/minute for an
//!   authenticated user, where the GraphQL budget the repository path spends is
//!   5,000 points/hour. [`GhClient::graphql_search`](crate::fetch::GhClient::graphql_search)
//!   paces on that tighter clock.
//!
//! Every node is parsed by the same
//! [`parse_pull_request_node`]
//! used for repository pages, so a PR ingested through either path lands
//! identically in the warehouse.

use serde_json::Value;

use super::pull_requests::{PullRequestData, RepositoryMetadata, parse_pull_request_node};
use crate::error::{Error, Result};

/// GitHub's hard ceiling on results per search query, regardless of match count.
pub const SEARCH_RESULT_CAP: i64 = 1000;

/// Which side of a pull request a monitored user is being matched on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserRole {
    /// PRs the user opened (`author:`).
    Author,
    /// PRs the user reviewed (`reviewed-by:`).
    Reviewer,
}

impl UserRole {
    /// The GitHub search qualifier for this role.
    pub fn qualifier(self) -> &'static str {
        match self {
            UserRole::Author => "author",
            UserRole::Reviewer => "reviewed-by",
        }
    }

    /// The `sync_metadata.source_type` this role's cursor is stored under.
    ///
    /// The two roles walk disjoint result sets, so folding them into one
    /// watermark would let the faster-moving side advance the cursor past
    /// unfetched items on the slower one.
    pub fn cursor_source_type(self) -> &'static str {
        match self {
            UserRole::Author => "user",
            UserRole::Reviewer => "user_reviews",
        }
    }

    /// Every role a user sync covers.
    pub fn all() -> [UserRole; 2] {
        [UserRole::Author, UserRole::Reviewer]
    }
}

/// Build the search query string for one user, role, and optional created-date
/// window.
///
/// `sort:updated-desc` makes the walk newest-first, which is what lets the
/// incremental cursor stop the page loop the same way the repository path does.
pub fn user_search_expression(login: &str, role: UserRole, window: Option<(&str, &str)>) -> String {
    let mut expression = format!("is:pr {}:{login}", role.qualifier());
    if let Some((start, end)) = window {
        expression.push_str(&format!(" created:{start}..{end}"));
    }
    expression.push_str(" sort:updated-desc");
    expression
}

/// GraphQL search query. The node selection mirrors the repository query's PR
/// node so both paths feed the same parser, with `repository` added because a
/// search page spans repositories.
pub fn user_pull_requests_query(page_size: u32) -> String {
    USER_PULL_REQUESTS_QUERY_TEMPLATE.replace("{PAGE_SIZE}", &page_size.to_string())
}

const USER_PULL_REQUESTS_QUERY_TEMPLATE: &str = r#"
query($q: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  search(query: $q, type: ISSUE, first: {PAGE_SIZE}, after: $cursor) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        number title body state isDraft
        createdAt updatedAt mergedAt closedAt
        baseRefName headRefName
        additions deletions changedFiles
        author { login __typename }
        mergedBy { login __typename }
        repository {
          nameWithOwner
          primaryLanguage { name }
          isFork
          isPrivate
          defaultBranchRef { name }
          createdAt
        }
        reviews(first: 50) {
          nodes {
            id state body submittedAt
            author { login __typename }
          }
        }
        reviewThreads(first: 50) {
          nodes {
            comments(first: 50) {
              nodes {
                id body path line createdAt
                author { login __typename }
                replyTo { id }
              }
            }
          }
        }
        comments(first: 50) {
          nodes {
            id body createdAt
            author { login __typename }
          }
        }
        files(first: 100) {
          nodes { path changeType additions deletions }
        }
        commits(last: 1) {
          nodes {
            commit {
              oid
              checkSuites(first: 10) {
                nodes {
                  checkRuns(first: 20) {
                    nodes { id name status conclusion startedAt completedAt }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"#;

/// One pull request from a search page, with the repository it lives in.
#[derive(Debug, Clone)]
pub struct SearchedPullRequest {
    pub repository: RepositoryMetadata,
    pub pull_request: PullRequestData,
}

/// One page of search results.
#[derive(Debug)]
pub struct UserSearchPage {
    /// Total matches GitHub reports for the query — may exceed
    /// [`SEARCH_RESULT_CAP`], which is exactly the case the caller must split.
    pub total_count: i64,
    pub items: Vec<SearchedPullRequest>,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

impl UserSearchPage {
    /// True when GitHub reports more matches than it will ever serve, so
    /// paginating this query to exhaustion still leaves results behind.
    pub fn exceeds_result_cap(&self) -> bool {
        self.total_count > SEARCH_RESULT_CAP
    }
}

/// Parse one page of the user search response (the `data` value).
pub fn parse_user_search_page(data: &Value) -> Result<UserSearchPage> {
    let search = data
        .get("search")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("search block missing from response".into()))?;

    let total_count = search
        .get("issueCount")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::GitHubApi("search.issueCount missing".into()))?;
    let page_info = search.get("pageInfo");
    let has_next_page = page_info
        .and_then(|info| info.get("hasNextPage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let end_cursor = page_info
        .and_then(|info| info.get("endCursor"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let mut items = Vec::new();
    for node in search
        .get("nodes")
        .and_then(Value::as_array)
        .map(|array| array.iter())
        .into_iter()
        .flatten()
    {
        // `type: ISSUE` also matches issues; `is:pr` filters them out, but a
        // node with no PR fields would otherwise fail the parse. Skipping the
        // empty inline-fragment case keeps the walk robust.
        if node.is_null() || node.get("number").is_none() {
            continue;
        }
        let repository = node
            .get("repository")
            .filter(|value| !value.is_null())
            .ok_or_else(|| Error::GitHubApi("search node has no repository".into()))?;
        items.push(SearchedPullRequest {
            repository: super::pull_requests::parse_repository_metadata(repository)?,
            pull_request: parse_pull_request_node(node)?,
        });
    }

    Ok(UserSearchPage {
        total_count,
        items,
        has_next_page,
        end_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn search_response(total: i64, numbers: &[i64]) -> Value {
        let nodes: Vec<Value> = numbers
            .iter()
            .map(|number| {
                json!({
                    "number": number,
                    "title": format!("PR {number}"),
                    "body": "body",
                    "state": "MERGED",
                    "isDraft": false,
                    "createdAt": "2026-01-05T18:00:00Z",
                    "updatedAt": "2026-01-06T09:00:00Z",
                    "mergedAt": "2026-01-06T09:00:00Z",
                    "closedAt": "2026-01-06T09:00:00Z",
                    "baseRefName": "main",
                    "headRefName": "feature",
                    "additions": 1,
                    "deletions": 0,
                    "changedFiles": 1,
                    "author": {"login": "octocat", "__typename": "User"},
                    "mergedBy": null,
                    "repository": {
                        "nameWithOwner": "acme/widgets",
                        "primaryLanguage": {"name": "Rust"},
                        "isFork": false,
                        "isPrivate": true,
                        "defaultBranchRef": {"name": "main"},
                        "createdAt": "2020-01-01T00:00:00Z"
                    },
                    "reviews": {"nodes": []},
                    "reviewThreads": {"nodes": []},
                    "comments": {"nodes": []},
                    "files": {"nodes": []},
                    "commits": {"nodes": []}
                })
            })
            .collect();
        json!({
            "search": {
                "issueCount": total,
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": nodes
            }
        })
    }

    #[test]
    fn builds_expressions_for_both_roles() {
        assert_eq!(
            user_search_expression("octocat", UserRole::Author, None),
            "is:pr author:octocat sort:updated-desc"
        );
        assert_eq!(
            user_search_expression(
                "octocat",
                UserRole::Reviewer,
                Some(("2026-01-01", "2026-01-31"))
            ),
            "is:pr reviewed-by:octocat created:2026-01-01..2026-01-31 sort:updated-desc"
        );
    }

    #[test]
    fn roles_keep_separate_cursors() {
        assert_eq!(UserRole::Author.cursor_source_type(), "user");
        assert_eq!(UserRole::Reviewer.cursor_source_type(), "user_reviews");
    }

    #[test]
    fn parses_page_with_per_node_repository() {
        let page = parse_user_search_page(&search_response(2, &[7, 8])).unwrap();
        assert_eq!(page.total_count, 2);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].repository.name_with_owner, "acme/widgets");
        assert!(page.items[0].repository.is_private);
        assert_eq!(page.items[0].pull_request.number, 7);
        assert!(!page.exceeds_result_cap());
    }

    /// A match count above the cap is the signal that pagination alone cannot
    /// reach every result — the caller has to narrow the window.
    #[test]
    fn reports_when_matches_exceed_the_cap() {
        let page = parse_user_search_page(&search_response(4200, &[1])).unwrap();
        assert!(page.exceeds_result_cap());
    }

    /// `type: ISSUE` can return issue nodes, which have none of the PR fields.
    /// They are skipped rather than failing the page.
    #[test]
    fn skips_non_pull_request_nodes() {
        let mut response = search_response(1, &[7]);
        response["search"]["nodes"]
            .as_array_mut()
            .unwrap()
            .push(json!({}));
        let page = parse_user_search_page(&response).unwrap();
        assert_eq!(page.items.len(), 1);
    }
}
