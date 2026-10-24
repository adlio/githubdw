//! Issue fetching: GraphQL query text and typed parsing of responses.

use serde_json::Value;

use crate::error::{Error, Result};
use crate::fetch::GhClient;
use crate::fetch::continuation::{
    PendingConnection, fetch_remaining, required_connection, required_str,
};
use crate::fetch::pull_requests::ActorReference;

/// GraphQL query for a repository's issues with labels, milestone,
/// assignees, and comments.
pub fn repository_issues_query() -> String {
    REPOSITORY_ISSUES_QUERY_TEMPLATE.replace("{NODE}", ISSUE_NODE_SELECTION)
}

/// GraphQL query for ONE issue addressed by number, with the same node
/// selection as the repository walk.
pub fn single_issue_query() -> String {
    SINGLE_ISSUE_QUERY_TEMPLATE.replace("{NODE}", ISSUE_NODE_SELECTION)
}

/// Issue node selection shared by the walk and the targeted refresh.
const ISSUE_NODE_SELECTION: &str = r#"id number title body state stateReason
        createdAt updatedAt closedAt
        author { login __typename }
        milestone { id number title description state dueOn createdAt }
        labels(first: 20) {
          pageInfo { hasNextPage endCursor }
          nodes { id name color description }
        }
        assignees(first: 10) {
          pageInfo { hasNextPage endCursor }
          nodes { login __typename }
        }
        comments(first: 50) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id body createdAt
            author { login __typename }
          }
        }"#;

const REPOSITORY_ISSUES_QUERY_TEMPLATE: &str = r#"
query($owner: String!, $name: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    nameWithOwner
    issues(first: 50, after: $cursor,
           orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        {NODE}
      }
    }
  }
}
"#;

const SINGLE_ISSUE_QUERY_TEMPLATE: &str = r#"
query($owner: String!, $name: String!, $number: Int!) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    nameWithOwner
    primaryLanguage { name }
    isFork
    isPrivate
    defaultBranchRef { name }
    createdAt
    issue(number: $number) {
      {NODE}
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
    /// Comment continuation when the first slice did not hold every comment.
    pub pending_comments: Option<PendingConnection>,
    /// Label and assignee continuations. Both are stored as REPLACED sets, so
    /// a short first slice would delete real rows rather than merely miss some.
    pub pending_labels: Option<PendingConnection>,
    pub pending_assignees: Option<PendingConnection>,
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

    let (has_next_page, end_cursor) =
        crate::fetch::pull_requests::page_status(issues_value, "repository.issues")?;

    let mut issues = Vec::new();
    for node in
        crate::fetch::pull_requests::strict_nodes(repository, "issues", "repository.issues")?
    {
        issues.push(parse_issue_node(node)?);
    }

    Ok(IssuePage {
        issues,
        has_next_page,
        end_cursor,
    })
}

/// Parse the single-issue query response (`data` value) into the repository
/// metadata and the issue. A `null` issue (deleted, transferred, or the number
/// names a pull request) is an error, never an empty success.
pub fn parse_single_issue(
    data: &Value,
) -> Result<(crate::fetch::pull_requests::RepositoryMetadata, IssueData)> {
    let repository = data
        .get("repository")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("repository not found in response".into()))?;
    let metadata = crate::fetch::pull_requests::parse_repository_metadata(repository)?;
    let node = repository
        .get("issue")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("issue not found".into()))?;
    Ok((metadata, parse_issue_node(node)?))
}

/// Parse one issue node (shared by the walk and the targeted refresh).
pub fn parse_issue_node(node: &Value) -> Result<IssueData> {
    let id = string_field(node, "id").ok_or_else(|| Error::GitHubApi("issue id missing".into()))?;
    let number = node
        .get("number")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::GitHubApi("issue number missing".into()))?;
    let created_at = string_field(node, "createdAt")
        .ok_or_else(|| Error::GitHubApi(format!("issue #{number} has no createdAt")))?;
    let updated_at = string_field(node, "updatedAt").unwrap_or_else(|| created_at.clone());

    // Null when the issue has no milestone: legitimately nullable. A present
    // milestone must carry its id, which keys the dimension row.
    let milestone = match node.get("milestone").filter(|value| !value.is_null()) {
        None => None,
        Some(value) => Some(MilestoneData {
            id: required_str(value, "id", &format!("issue #{number} milestone"))?,
            number: value.get("number").and_then(Value::as_i64).unwrap_or(0),
            title: string_field(value, "title").unwrap_or_default(),
            description: string_field(value, "description"),
            state: string_field(value, "state").unwrap_or_else(|| "OPEN".into()),
            due_on: string_field(value, "dueOn"),
            created_at: string_field(value, "createdAt"),
        }),
    };

    let label = format!("issue #{number}");
    let owner = Some(id.as_str());
    let (label_nodes, pending_labels) =
        required_connection(node, "labels", &format!("{label} labels"), owner, "Issue")?;
    let labels = label_nodes
        .into_iter()
        .map(parse_label)
        .collect::<Result<Vec<_>>>()?;
    let (assignee_nodes, pending_assignees) = required_connection(
        node,
        "assignees",
        &format!("{label} assignees"),
        owner,
        "Issue",
    )?;
    let assignees = assignee_nodes
        .into_iter()
        .map(parse_assignee)
        .collect::<Result<Vec<_>>>()?;
    let (comment_nodes, pending_comments) = required_connection(
        node,
        "comments",
        &format!("{label} comments"),
        owner,
        "Issue",
    )?;
    let comments = comment_nodes
        .into_iter()
        .map(parse_issue_comment)
        .collect::<Result<Vec<_>>>()?;

    Ok(IssueData {
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
        pending_comments,
        pending_labels,
        pending_assignees,
    })
}

fn parse_label(label: &Value) -> Result<LabelData> {
    Ok(LabelData {
        id: required_str(label, "id", "label")?,
        name: string_field(label, "name").unwrap_or_default(),
        color: string_field(label, "color").unwrap_or_default(),
        description: string_field(label, "description").unwrap_or_default(),
    })
}

fn parse_assignee(assignee: &Value) -> Result<ActorReference> {
    Ok(ActorReference {
        login: required_str(assignee, "login", "assignee")?,
        type_name: string_field(assignee, "__typename").unwrap_or_else(|| "User".into()),
    })
}

fn parse_issue_comment(comment: &Value) -> Result<IssueCommentData> {
    let id = required_str(comment, "id", "issue comment")?;
    Ok(IssueCommentData {
        body: string_field(comment, "body"),
        created_at: required_str(comment, "createdAt", &format!("issue comment {id}"))?,
        // Null for a deleted ("ghost") account: legitimately nullable.
        author: actor(comment, "author"),
        id,
    })
}

const ISSUE_COMMENT_SELECTION: &str = "id body createdAt author { login __typename }";
const LABEL_SELECTION: &str = "id name color description";
const ASSIGNEE_SELECTION: &str = "login __typename";

/// Fetch the comments beyond the page query's first slice. Errors leave the
/// issue to the caller's failed-record handling rather than storing a short list.
pub fn hydrate_issue(client: &mut GhClient, issue: &mut IssueData) -> Result<()> {
    if let Some(pending) = issue.pending_comments.take() {
        let more = fetch_remaining(client, &pending, ISSUE_COMMENT_SELECTION)?;
        for comment in &more {
            issue.comments.push(parse_issue_comment(comment)?);
        }
    }
    if let Some(pending) = issue.pending_labels.take() {
        let more = fetch_remaining(client, &pending, LABEL_SELECTION)?;
        for label in &more {
            issue.labels.push(parse_label(label)?);
        }
    }
    if let Some(pending) = issue.pending_assignees.take() {
        let more = fetch_remaining(client, &pending, ASSIGNEE_SELECTION)?;
        for assignee in &more {
            issue.assignees.push(parse_assignee(assignee)?);
        }
    }
    Ok(())
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
                        "labels": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
                            "id": "LAB1", "name": "bug", "color": "d73a4a", "description": "Something broken"
                        }]},
                        "assignees": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{"login": "hubot", "__typename": "User"}]},
                        "comments": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{
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

    fn issue_node() -> Value {
        let complete = |nodes: Value| json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": nodes});
        json!({
            "id": "ISS1", "number": 42, "title": "t", "body": null, "state": "OPEN",
            "stateReason": null, "createdAt": "2026-02-01T12:00:00Z",
            "updatedAt": "2026-02-01T12:00:00Z", "closedAt": null,
            "author": {"login": "octocat", "__typename": "User"},
            "milestone": {"id": "MILE1", "number": 1, "title": "v1", "state": "OPEN"},
            "labels": complete(json!([{"id": "LAB1", "name": "bug", "color": "d73a4a",
                                       "description": null}])),
            "assignees": complete(json!([{"login": "hubot", "__typename": "User"}])),
            "comments": complete(json!([{"id": "COM1", "body": "x",
                                         "createdAt": "2026-02-02T08:00:00Z",
                                         "author": {"login": "hubot", "__typename": "User"}}]))
        })
    }

    type Mutation = fn(&mut Value, &str);

    #[test]
    fn selected_issue_connections_fail_closed_when_incomplete() {
        assert!(parse_issue_node(&issue_node()).is_ok());
        for field in ["labels", "assignees", "comments"] {
            let mutations: [(&str, Mutation); 6] = [
                ("absent", |node, field| {
                    node.as_object_mut().unwrap().remove(field);
                }),
                ("null", |node, field| node[field] = Value::Null),
                ("without pageInfo", |node, field| {
                    node[field].as_object_mut().unwrap().remove("pageInfo");
                }),
                ("without hasNextPage", |node, field| {
                    node[field]["pageInfo"] = json!({"endCursor": null});
                }),
                ("without nodes", |node, field| {
                    node[field].as_object_mut().unwrap().remove("nodes");
                }),
                ("with a null node", |node, field| {
                    node[field]["nodes"] = json!([null]);
                }),
            ];
            for (name, mutate) in mutations {
                let mut node = issue_node();
                mutate(&mut node, field);
                assert!(parse_issue_node(&node).is_err(), "{field} {name}");
            }
        }
    }

    #[test]
    fn malformed_issue_records_fail_and_nullable_fields_parse() {
        for (field, key) in [
            ("labels", "id"),
            ("assignees", "login"),
            ("comments", "id"),
            ("comments", "createdAt"),
        ] {
            let mut node = issue_node();
            node[field]["nodes"][0].as_object_mut().unwrap().remove(key);
            assert!(parse_issue_node(&node).is_err(), "{field} without {key}");
        }
        let mut node = issue_node();
        node["milestone"].as_object_mut().unwrap().remove("id");
        assert!(parse_issue_node(&node).is_err(), "milestone without id");

        let mut node = issue_node();
        node["milestone"] = Value::Null;
        node["author"] = Value::Null;
        node["comments"]["nodes"][0]["author"] = Value::Null;
        let issue = parse_issue_node(&node).unwrap();
        assert!(issue.milestone.is_none() && issue.author.is_none());
        assert!(issue.comments[0].author.is_none());
    }

    #[test]
    fn malformed_issue_continuation_records_fail() {
        for (field, record) in [
            ("labels", json!({"name": "no id"})),
            ("assignees", json!({"__typename": "User"})),
            ("comments", json!({"id": "COM2"})),
        ] {
            let mut node = issue_node();
            node[field]["pageInfo"] = json!({"hasNextPage": true, "endCursor": "P1"});
            let mut issue = parse_issue_node(&node).unwrap();
            let response = json!({"data": {"node": {field: {
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": [record]
            }}}});
            let transport =
                crate::fetch::test_support::FixtureTransport::new(vec![Ok(response.to_string())]);
            let mut client = GhClient::with_transport(Box::new(transport)).without_sleeping();
            assert!(hydrate_issue(&mut client, &mut issue).is_err(), "{field}");
        }
    }
}
