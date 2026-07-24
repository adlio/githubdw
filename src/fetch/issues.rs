//! Issue fetching: GraphQL query text and typed parsing of responses.

use serde_json::Value;

use crate::error::{Error, Result};
use crate::fetch::pull_requests::ActorReference;

/// GraphQL query for a repository's issues with labels, milestone,
/// assignees, and comments.
pub const REPOSITORY_ISSUES_QUERY: &str = r#"
query($owner: String!, $name: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    nameWithOwner
    issues(first: 50, after: $cursor,
           orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id number title body state stateReason
        createdAt updatedAt closedAt
        author { login __typename }
        milestone { id number title description state dueOn createdAt }
        labels(first: 20) { nodes { id name color description } }
        assignees(first: 10) { nodes { login __typename } }
        comments(first: 50) {
          nodes {
            id body createdAt
            author { login __typename }
          }
        }
      }
    }
  }
}
"#;

#[derive(Debug, Clone)]
pub struct LabelData {
    pub id: String,
    pub name: String,
    pub color: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct MilestoneData {
    pub id: String,
    pub number: i64,
    pub title: String,
    pub description: Option<String>,
    pub state: String,
    pub due_on: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IssueCommentData {
    pub id: String,
    pub body: Option<String>,
    pub created_at: String,
    pub author: Option<ActorReference>,
}

#[derive(Debug, Clone)]
pub struct IssueData {
    pub id: String,
    pub number: i64,
    pub title: String,
    pub body: Option<String>,
    pub state: String,
    pub state_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
    pub author: Option<ActorReference>,
    pub milestone: Option<MilestoneData>,
    pub labels: Vec<LabelData>,
    pub assignees: Vec<ActorReference>,
    pub comments: Vec<IssueCommentData>,
}

/// One page of issue results.
#[derive(Debug)]
pub struct IssuePage {
    pub issues: Vec<IssueData>,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn nodes<'a>(value: &'a Value, collection: &str) -> Vec<&'a Value> {
    value
        .get(collection)
        .and_then(|c| c.get("nodes"))
        .and_then(Value::as_array)
        .map(|array| array.iter().collect())
        .unwrap_or_default()
}

fn actor(value: &Value, field: &str) -> Option<ActorReference> {
    let actor_value = value.get(field)?;
    let login = actor_value.get("login")?.as_str()?.to_string();
    let type_name = actor_value
        .get("__typename")
        .and_then(Value::as_str)
        .unwrap_or("User")
        .to_string();
    Some(ActorReference { login, type_name })
}

/// Parse one page of the repository issues query response (`data` value).
pub fn parse_issue_page(data: &Value) -> Result<IssuePage> {
    let repository = data
        .get("repository")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("repository not found in response".into()))?;
    let issues_value = repository
        .get("issues")
        .ok_or_else(|| Error::GitHubApi("repository.issues missing".into()))?;

    let page_info = issues_value.get("pageInfo");
    let has_next_page = page_info
        .and_then(|info| info.get("hasNextPage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let end_cursor = page_info
        .and_then(|info| info.get("endCursor"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let mut issues = Vec::new();
    for node in nodes(repository, "issues") {
        let id =
            string_field(node, "id").ok_or_else(|| Error::GitHubApi("issue id missing".into()))?;
        let number = node
            .get("number")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::GitHubApi("issue number missing".into()))?;
        let created_at = string_field(node, "createdAt")
            .ok_or_else(|| Error::GitHubApi(format!("issue #{number} has no createdAt")))?;
        let updated_at = string_field(node, "updatedAt").unwrap_or_else(|| created_at.clone());

        let milestone = node
            .get("milestone")
            .filter(|value| !value.is_null())
            .and_then(|value| {
                Some(MilestoneData {
                    id: string_field(value, "id")?,
                    number: value.get("number").and_then(Value::as_i64).unwrap_or(0),
                    title: string_field(value, "title").unwrap_or_default(),
                    description: string_field(value, "description"),
                    state: string_field(value, "state").unwrap_or_else(|| "OPEN".into()),
                    due_on: string_field(value, "dueOn"),
                    created_at: string_field(value, "createdAt"),
                })
            });

        let labels = nodes(node, "labels")
            .into_iter()
            .filter_map(|label| {
                Some(LabelData {
                    id: string_field(label, "id")?,
                    name: string_field(label, "name").unwrap_or_default(),
                    color: string_field(label, "color").unwrap_or_default(),
                    description: string_field(label, "description").unwrap_or_default(),
                })
            })
            .collect();

        let assignees = nodes(node, "assignees")
            .into_iter()
            .filter_map(|assignee| {
                Some(ActorReference {
                    login: string_field(assignee, "login")?,
                    type_name: string_field(assignee, "__typename")
                        .unwrap_or_else(|| "User".into()),
                })
            })
            .collect();

        let comments = nodes(node, "comments")
            .into_iter()
            .filter_map(|comment| {
                Some(IssueCommentData {
                    id: string_field(comment, "id")?,
                    body: string_field(comment, "body"),
                    created_at: string_field(comment, "createdAt")?,
                    author: actor(comment, "author"),
                })
            })
            .collect();

        issues.push(IssueData {
            id,
            number,
            title: string_field(node, "title").unwrap_or_default(),
            body: string_field(node, "body"),
            state: string_field(node, "state")
                .unwrap_or_else(|| "OPEN".into())
                .to_lowercase(),
            state_reason: string_field(node, "stateReason").map(|reason| reason.to_lowercase()),
            created_at,
            updated_at,
            closed_at: string_field(node, "closedAt"),
            author: actor(node, "author"),
            milestone,
            labels,
            assignees,
            comments,
        });
    }

    Ok(IssuePage {
        issues,
        has_next_page,
        end_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_issue_page() {
        let data = json!({
            "repository": {
                "nameWithOwner": "octocat/hello",
                "issues": {
                    "pageInfo": {"hasNextPage": false, "endCursor": null},
                    "nodes": [{
                        "id": "ISS1",
                        "number": 42,
                        "title": "Pagination bug",
                        "body": "List cuts off at page two",
                        "state": "CLOSED",
                        "stateReason": "COMPLETED",
                        "createdAt": "2026-02-01T12:00:00Z",
                        "updatedAt": "2026-02-03T12:00:00Z",
                        "closedAt": "2026-02-03T12:00:00Z",
                        "author": {"login": "octocat", "__typename": "User"},
                        "milestone": {
                            "id": "MILE1", "number": 1, "title": "v1.0",
                            "description": null, "state": "OPEN",
                            "dueOn": null, "createdAt": "2026-01-01T00:00:00Z"
                        },
                        "labels": {"nodes": [{
                            "id": "LAB1", "name": "bug", "color": "d73a4a", "description": "Something broken"
                        }]},
                        "assignees": {"nodes": [{"login": "hubot", "__typename": "User"}]},
                        "comments": {"nodes": [{
                            "id": "COM1", "body": "Reproduced",
                            "createdAt": "2026-02-02T08:00:00Z",
                            "author": {"login": "hubot", "__typename": "User"}
                        }]}
                    }]
                }
            }
        });
        let page = parse_issue_page(&data).expect("parse issues");
        assert_eq!(page.issues.len(), 1);
        assert!(!page.has_next_page);
        let issue = &page.issues[0];
        assert_eq!(issue.number, 42);
        assert_eq!(issue.state, "closed");
        assert_eq!(issue.state_reason.as_deref(), Some("completed"));
        assert_eq!(issue.labels.len(), 1);
        assert_eq!(issue.labels[0].name, "bug");
        assert_eq!(issue.assignees.len(), 1);
        assert_eq!(issue.comments.len(), 1);
        assert_eq!(issue.milestone.as_ref().unwrap().title, "v1.0");
    }
}
