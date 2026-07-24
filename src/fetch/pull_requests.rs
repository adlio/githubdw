//! Pull-request fetching: GraphQL query text and typed parsing of responses.

use serde_json::Value;

use crate::error::{Error, Result};

/// GraphQL query for a repository's pull requests with nested reviews,
/// review-thread comments, conversation comments, files, and check runs.
/// `page_size` tunes the outer page: large repos with heavy PRs can overflow
/// GitHub's response stream at 25, so the syncer degrades adaptively.
pub fn repository_pull_requests_query(page_size: u32) -> String {
    REPOSITORY_PULL_REQUESTS_QUERY_TEMPLATE.replace("{PAGE_SIZE}", &page_size.to_string())
}

const REPOSITORY_PULL_REQUESTS_QUERY_TEMPLATE: &str = r#"
query($owner: String!, $name: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    nameWithOwner
    primaryLanguage { name }
    isFork
    isPrivate
    defaultBranchRef { name }
    createdAt
    pullRequests(first: {PAGE_SIZE}, after: $cursor,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number title body state isDraft
        createdAt updatedAt mergedAt closedAt
        baseRefName headRefName
        additions deletions changedFiles
        author { login __typename }
        mergedBy { login __typename }
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

/// An actor reference from GraphQL (`author { login __typename }`).
#[derive(Debug, Clone, PartialEq)]
pub struct ActorReference {
    pub login: String,
    pub type_name: String,
}

impl ActorReference {
    fn from_value(value: &Value) -> Option<Self> {
        let login = value.get("login")?.as_str()?.to_string();
        let type_name = value
            .get("__typename")
            .and_then(Value::as_str)
            .unwrap_or("User")
            .to_string();
        Some(Self { login, type_name })
    }
}

/// Repository metadata from the query header.
#[derive(Debug, Clone)]
pub struct RepositoryMetadata {
    pub name_with_owner: String,
    pub primary_language: Option<String>,
    pub is_fork: bool,
    pub is_private: bool,
    pub default_branch: Option<String>,
    pub created_at: Option<String>,
}

/// One parsed pull request with all nested collections.
#[derive(Debug, Clone)]
pub struct PullRequestData {
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
    pub additions: i64,
    pub deletions: i64,
    pub changed_files: i64,
    pub author: Option<ActorReference>,
    pub merged_by: Option<ActorReference>,
    pub reviews: Vec<ReviewData>,
    pub review_comments: Vec<ReviewCommentData>,
    pub conversation_comments: Vec<ConversationCommentData>,
    pub files: Vec<FileDiffData>,
    pub head_sha: Option<String>,
    pub check_runs: Vec<CheckRunData>,
}

#[derive(Debug, Clone)]
pub struct ReviewData {
    pub id: String,
    pub state: String,
    pub body: Option<String>,
    pub submitted_at: Option<String>,
    pub author: Option<ActorReference>,
}

#[derive(Debug, Clone)]
pub struct ReviewCommentData {
    pub id: String,
    pub body: Option<String>,
    pub path: Option<String>,
    pub line: Option<i64>,
    pub created_at: String,
    pub author: Option<ActorReference>,
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConversationCommentData {
    pub id: String,
    pub body: Option<String>,
    pub created_at: String,
    pub author: Option<ActorReference>,
}

#[derive(Debug, Clone)]
pub struct FileDiffData {
    pub path: String,
    pub change_type: String,
    pub additions: i64,
    pub deletions: i64,
}

#[derive(Debug, Clone)]
pub struct CheckRunData {
    pub id: String,
    pub name: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

/// One page of PR results.
#[derive(Debug)]
pub struct PullRequestPage {
    pub repository: RepositoryMetadata,
    pub pull_requests: Vec<PullRequestData>,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn integer_field(value: &Value, field: &str) -> i64 {
    value.get(field).and_then(Value::as_i64).unwrap_or(0)
}

fn nodes<'a>(value: &'a Value, collection: &str) -> Vec<&'a Value> {
    value
        .get(collection)
        .and_then(|c| c.get("nodes"))
        .and_then(Value::as_array)
        .map(|array| array.iter().collect())
        .unwrap_or_default()
}

/// Parse one page of the repository pull-requests query response (`data` value).
pub fn parse_pull_request_page(data: &Value) -> Result<PullRequestPage> {
    let repository = data
        .get("repository")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("repository not found in response".into()))?;

    let metadata = RepositoryMetadata {
        name_with_owner: string_field(repository, "nameWithOwner")
            .ok_or_else(|| Error::GitHubApi("repository.nameWithOwner missing".into()))?,
        primary_language: repository
            .get("primaryLanguage")
            .and_then(|language| language.get("name"))
            .and_then(Value::as_str)
            .map(str::to_string),
        is_fork: repository
            .get("isFork")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        is_private: repository
            .get("isPrivate")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        default_branch: repository
            .get("defaultBranchRef")
            .and_then(|reference| reference.get("name"))
            .and_then(Value::as_str)
            .map(str::to_string),
        created_at: string_field(repository, "createdAt"),
    };

    let pull_requests_value = repository
        .get("pullRequests")
        .ok_or_else(|| Error::GitHubApi("repository.pullRequests missing".into()))?;

    let page_info = pull_requests_value.get("pageInfo");
    let has_next_page = page_info
        .and_then(|info| info.get("hasNextPage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let end_cursor = page_info
        .and_then(|info| info.get("endCursor"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let mut pull_requests = Vec::new();
    for node in nodes(repository, "pullRequests") {
        pull_requests.push(parse_pull_request(node)?);
    }

    Ok(PullRequestPage {
        repository: metadata,
        pull_requests,
        has_next_page,
        end_cursor,
    })
}

fn parse_pull_request(node: &Value) -> Result<PullRequestData> {
    let number = node
        .get("number")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::GitHubApi("pull request number missing".into()))?;
    let created_at = string_field(node, "createdAt")
        .ok_or_else(|| Error::GitHubApi(format!("PR #{number} has no createdAt")))?;

    let reviews = nodes(node, "reviews")
        .into_iter()
        .filter_map(|review| {
            Some(ReviewData {
                id: string_field(review, "id")?,
                state: string_field(review, "state").unwrap_or_else(|| "COMMENTED".into()),
                body: string_field(review, "body").filter(|body| !body.is_empty()),
                submitted_at: string_field(review, "submittedAt"),
                author: review.get("author").and_then(ActorReference::from_value),
            })
        })
        .collect();

    let mut review_comments = Vec::new();
    for thread in nodes(node, "reviewThreads") {
        for comment in nodes(thread, "comments") {
            let Some(id) = string_field(comment, "id") else {
                continue;
            };
            let Some(created) = string_field(comment, "createdAt") else {
                continue;
            };
            review_comments.push(ReviewCommentData {
                id,
                body: string_field(comment, "body"),
                path: string_field(comment, "path"),
                line: comment.get("line").and_then(Value::as_i64),
                created_at: created,
                author: comment.get("author").and_then(ActorReference::from_value),
                in_reply_to: comment
                    .get("replyTo")
                    .and_then(|reply| reply.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        }
    }

    let conversation_comments = nodes(node, "comments")
        .into_iter()
        .filter_map(|comment| {
            Some(ConversationCommentData {
                id: string_field(comment, "id")?,
                body: string_field(comment, "body"),
                created_at: string_field(comment, "createdAt")?,
                author: comment.get("author").and_then(ActorReference::from_value),
            })
        })
        .collect();

    let files = nodes(node, "files")
        .into_iter()
        .filter_map(|file| {
            Some(FileDiffData {
                path: string_field(file, "path")?,
                change_type: string_field(file, "changeType").unwrap_or_else(|| "MODIFIED".into()),
                additions: integer_field(file, "additions"),
                deletions: integer_field(file, "deletions"),
            })
        })
        .collect();

    let mut head_sha = None;
    let mut check_runs = Vec::new();
    for commit_node in nodes(node, "commits") {
        let Some(commit) = commit_node.get("commit") else {
            continue;
        };
        head_sha = string_field(commit, "oid");
        for suite in nodes(commit, "checkSuites") {
            for check in nodes(suite, "checkRuns") {
                let Some(id) = string_field(check, "id") else {
                    continue;
                };
                check_runs.push(CheckRunData {
                    id,
                    name: string_field(check, "name").unwrap_or_default(),
                    status: string_field(check, "status").unwrap_or_else(|| "COMPLETED".into()),
                    conclusion: string_field(check, "conclusion"),
                    started_at: string_field(check, "startedAt"),
                    completed_at: string_field(check, "completedAt"),
                });
            }
        }
    }

    Ok(PullRequestData {
        number,
        title: string_field(node, "title"),
        body: string_field(node, "body"),
        state: string_field(node, "state").unwrap_or_else(|| "OPEN".into()),
        is_draft: node
            .get("isDraft")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at,
        updated_at: string_field(node, "updatedAt"),
        merged_at: string_field(node, "mergedAt"),
        closed_at: string_field(node, "closedAt"),
        base_ref: string_field(node, "baseRefName"),
        head_ref: string_field(node, "headRefName"),
        additions: integer_field(node, "additions"),
        deletions: integer_field(node, "deletions"),
        changed_files: integer_field(node, "changedFiles"),
        author: node.get("author").and_then(ActorReference::from_value),
        merged_by: node.get("mergedBy").and_then(ActorReference::from_value),
        reviews,
        review_comments,
        conversation_comments,
        files,
        head_sha,
        check_runs,
    })
}

/// Parse the REST `/repos/{owner}/{name}/pulls/{number}/files` response into
/// (path, previous_path, patch) tuples for patch backfill.
pub fn parse_rest_file_patches(value: &Value) -> Vec<(String, Option<String>, Option<String>)> {
    let Some(files) = value.as_array() else {
        return Vec::new();
    };
    files
        .iter()
        .filter_map(|file| {
            let path = string_field(file, "filename")?;
            let previous_path = string_field(file, "previous_filename");
            let patch = string_field(file, "patch");
            Some((path, previous_path, patch))
        })
        .collect()
}

#[cfg(test)]
mod tests {
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
                        "reviews": {"nodes": [{
                            "id": "REV1", "state": "APPROVED", "body": "LGTM",
                            "submittedAt": "2026-01-06T08:00:00Z",
                            "author": {"login": "hubot", "__typename": "User"}
                        }]},
                        "reviewThreads": {"nodes": [{
                            "comments": {"nodes": [{
                                "id": "RC1", "body": "nit: rename",
                                "path": "src/lib.rs", "line": 10,
                                "createdAt": "2026-01-05T20:00:00Z",
                                "author": {"login": "hubot", "__typename": "User"},
                                "replyTo": null
                            }]}
                        }]},
                        "comments": {"nodes": [{
                            "id": "IC1", "body": "Looks good overall",
                            "createdAt": "2026-01-05T19:00:00Z",
                            "author": {"login": "dependabot[bot]", "__typename": "Bot"}
                        }]},
                        "files": {"nodes": [{
                            "path": "src/lib.rs", "changeType": "MODIFIED",
                            "additions": 100, "deletions": 2
                        }]},
                        "commits": {"nodes": [{
                            "commit": {
                                "oid": "abc123",
                                "checkSuites": {"nodes": [{
                                    "checkRuns": {"nodes": [{
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
}
