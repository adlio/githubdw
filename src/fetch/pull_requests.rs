//! Pull-request fetching: GraphQL query text and typed parsing of responses.

use serde_json::Value;

use crate::error::{Error, Result};
use crate::fetch::GhClient;
use crate::fetch::continuation::{
    PendingConnection, fetch_remaining, required_connection, required_str,
};

/// GraphQL query for a repository's pull requests with nested reviews,
/// review-thread comments, conversation comments, files, and check runs.
/// `page_size` tunes the outer page: large repos with heavy PRs can overflow
/// GitHub's response stream at 25, so the syncer degrades adaptively.
pub fn repository_pull_requests_query(page_size: u32) -> String {
    REPOSITORY_PULL_REQUESTS_QUERY_TEMPLATE
        .replace("{PAGE_SIZE}", &page_size.to_string())
        .replace("{REPOSITORY}", REPOSITORY_HEADER_SELECTION)
        .replace("{NODE}", PULL_REQUEST_NODE_SELECTION)
}

/// GraphQL query for ONE pull request addressed by number, with the same node
/// selection as the repository walk, so a targeted refresh parses, hydrates,
/// and lands through exactly the same code as a bulk sync.
pub fn single_pull_request_query() -> String {
    SINGLE_PULL_REQUEST_QUERY_TEMPLATE
        .replace("{REPOSITORY}", REPOSITORY_HEADER_SELECTION)
        .replace("{NODE}", PULL_REQUEST_NODE_SELECTION)
}

/// Repository dimension fields selected alongside pull requests.
pub const REPOSITORY_HEADER_SELECTION: &str = r#"nameWithOwner
    primaryLanguage { name }
    isFork
    isPrivate
    defaultBranchRef { name }
    createdAt"#;

/// The pull-request node selection shared by the repository walk, the
/// user-search walk, and the targeted refresh. Keep the continuation
/// selections below in step with it.
pub const PULL_REQUEST_NODE_SELECTION: &str = r#"id number title body state isDraft
        createdAt updatedAt mergedAt closedAt
        baseRefName headRefName
        additions deletions changedFiles
        author { login __typename }
        mergedBy { login __typename }
        reviews(first: 50) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id state body submittedAt
            author { login __typename }
          }
        }
        reviewThreads(first: 50) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            comments(first: 50) {
              pageInfo { hasNextPage endCursor }
              nodes {
                id body path line createdAt
                author { login __typename }
                replyTo { id }
              }
            }
          }
        }
        comments(first: 50) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id body createdAt
            author { login __typename }
          }
        }
        files(first: 100) {
          pageInfo { hasNextPage endCursor }
          nodes { path changeType additions deletions }
        }
        commits(last: 1) {
          nodes {
            commit {
              id oid
              checkSuites(first: 10) {
                pageInfo { hasNextPage endCursor }
                nodes {
                  id
                  checkRuns(first: 20, filterBy: {checkType: ALL}) {
                    pageInfo { hasNextPage endCursor }
                    nodes { id name status conclusion startedAt completedAt }
                  }
                }
              }
            }
          }
        }"#;

const REPOSITORY_PULL_REQUESTS_QUERY_TEMPLATE: &str = r#"
query($owner: String!, $name: String!, $cursor: String) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    {REPOSITORY}
    pullRequests(first: {PAGE_SIZE}, after: $cursor,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        {NODE}
      }
    }
  }
}
"#;

const SINGLE_PULL_REQUEST_QUERY_TEMPLATE: &str = r#"
query($owner: String!, $name: String!, $number: Int!) {
  rateLimit { limit cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    {REPOSITORY}
    pullRequest(number: $number) {
      {NODE}
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
    /// True when the provider actually reported `changedFiles`. A missing
    /// field reads as 0 in `changed_files`, which must never be mistaken for
    /// an authoritative empty file set.
    pub changed_files_reported: bool,
    pub author: Option<ActorReference>,
    pub merged_by: Option<ActorReference>,
    pub reviews: Vec<ReviewData>,
    pub review_comments: Vec<ReviewCommentData>,
    pub conversation_comments: Vec<ConversationCommentData>,
    pub files: Vec<FileDiffData>,
    pub head_sha: Option<String>,
    pub check_runs: Vec<CheckRunData>,
    /// GraphQL node id, used to continue nested connections.
    pub node_id: Option<String>,
    /// Nested connections whose first slice did not hold every node.
    pub pending: Vec<PendingConnection>,
    /// True until pending head-commit check suites and runs are hydrated.
    pub check_runs_truncated: bool,
}

/// Node selections reused by the continuation queries; they must match the
/// fields the page queries select for each connection.
const REVIEW_SELECTION: &str = "id state body submittedAt author { login __typename }";
const REVIEW_COMMENT_SELECTION: &str =
    "id body path line createdAt author { login __typename } replyTo { id }";
const REVIEW_THREAD_SELECTION: &str = "id comments(first: 50) { pageInfo { hasNextPage endCursor } \
     nodes { id body path line createdAt author { login __typename } replyTo { id } } }";
const CONVERSATION_COMMENT_SELECTION: &str = "id body createdAt author { login __typename }";
const FILE_SELECTION: &str = "path changeType additions deletions";
const CHECK_RUN_SELECTION: &str = "id name status conclusion startedAt completedAt";
const CHECK_SUITE_SELECTION: &str = "id checkRuns(first: 20, filterBy: {checkType: ALL}) { pageInfo { hasNextPage endCursor } \
    nodes { id name status conclusion startedAt completedAt } }";

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

/// Read a top-level connection's page status, failing closed.
///
/// A missing `pageInfo` or `hasNextPage` used to read as "last page", which
/// silently ended a walk on a malformed response and let it commit coverage it
/// never fetched. A page that claims a successor without a cursor is equally
/// unusable.
pub fn page_status(connection: &Value, label: &str) -> Result<(bool, Option<String>)> {
    let page_info = connection
        .get("pageInfo")
        .filter(|info| !info.is_null())
        .ok_or_else(|| Error::GitHubApi(format!("{label} has no pageInfo")))?;
    let has_next = page_info
        .get("hasNextPage")
        .and_then(Value::as_bool)
        .ok_or_else(|| Error::GitHubApi(format!("{label} has no hasNextPage")))?;
    let end_cursor = page_info
        .get("endCursor")
        .and_then(Value::as_str)
        .map(str::to_string);
    if has_next && end_cursor.is_none() {
        return Err(Error::GitHubApi(format!(
            "{label} reports another page but no endCursor"
        )));
    }
    Ok((has_next, end_cursor))
}

/// The `nodes` of `value[collection]`, failing closed on a missing or null
/// connection, a missing nodes array, or a null node.
pub fn strict_nodes<'a>(value: &'a Value, collection: &str, label: &str) -> Result<Vec<&'a Value>> {
    let nodes = value
        .get(collection)
        .filter(|connection| !connection.is_null())
        .and_then(|connection| connection.get("nodes"))
        .and_then(Value::as_array)
        .ok_or_else(|| Error::GitHubApi(format!("{label} has no nodes array")))?;
    if nodes.iter().any(Value::is_null) {
        return Err(Error::GitHubApi(format!("{label} contains a null node")));
    }
    Ok(nodes.iter().collect())
}

/// Parse the single-pull-request query response (`data` value).
///
/// A `null` pull request (deleted, or the number names an issue) is an error,
/// never an empty success.
pub fn parse_single_pull_request(data: &Value) -> Result<(RepositoryMetadata, PullRequestData)> {
    let repository = data
        .get("repository")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("repository not found in response".into()))?;
    let metadata = parse_repository_metadata(repository)?;
    let node = repository
        .get("pullRequest")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("pull request not found".into()))?;
    Ok((metadata, parse_pull_request_node(node)?))
}

/// Parse a `repository { … }` selection into its dimension metadata.
///
/// Shared with the user-search path, where the same selection arrives once per
/// node instead of once per page.
pub fn parse_repository_metadata(repository: &Value) -> Result<RepositoryMetadata> {
    Ok(RepositoryMetadata {
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
    })
}

/// Parse one page of the repository pull-requests query response (`data` value).
pub fn parse_pull_request_page(data: &Value) -> Result<PullRequestPage> {
    let repository = data
        .get("repository")
        .filter(|value| !value.is_null())
        .ok_or_else(|| Error::GitHubApi("repository not found in response".into()))?;

    let metadata = parse_repository_metadata(repository)?;

    let pull_requests_value = repository
        .get("pullRequests")
        .ok_or_else(|| Error::GitHubApi("repository.pullRequests missing".into()))?;

    let (has_next_page, end_cursor) = page_status(pull_requests_value, "repository.pullRequests")?;

    let mut pull_requests = Vec::new();
    for node in strict_nodes(repository, "pullRequests", "repository.pullRequests")? {
        pull_requests.push(parse_pull_request_node(node)?);
    }

    Ok(PullRequestPage {
        repository: metadata,
        pull_requests,
        has_next_page,
        end_cursor,
    })
}

/// Parse one pull-request node.
///
/// The repository query and the user-search query select the same node shape, so
/// a PR ingested through either path parses to the identical value.
pub fn parse_pull_request_node(node: &Value) -> Result<PullRequestData> {
    let number = node
        .get("number")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::GitHubApi("pull request number missing".into()))?;
    let created_at = string_field(node, "createdAt")
        .ok_or_else(|| Error::GitHubApi(format!("PR #{number} has no createdAt")))?;

    let node_id = string_field(node, "id");
    let label = format!("PR #{number}");
    let owner = node_id.as_deref();
    let mut pending = Vec::new();

    let (review_nodes, more) = required_connection(
        node,
        "reviews",
        &format!("{label} reviews"),
        owner,
        "PullRequest",
    )?;
    pending.extend(more);
    let reviews = review_nodes
        .into_iter()
        .map(parse_review)
        .collect::<Result<Vec<_>>>()?;

    let mut review_comments = Vec::new();
    let (threads, more) = required_connection(
        node,
        "reviewThreads",
        &format!("{label} reviewThreads"),
        owner,
        "PullRequest",
    )?;
    pending.extend(more);
    for thread in threads {
        collect_thread(thread, &mut review_comments, &mut pending)?;
    }

    let (comment_nodes, more) = required_connection(
        node,
        "comments",
        &format!("{label} comments"),
        owner,
        "PullRequest",
    )?;
    pending.extend(more);
    let conversation_comments = comment_nodes
        .into_iter()
        .map(parse_conversation_comment)
        .collect::<Result<Vec<_>>>()?;

    let (file_nodes, more) = required_connection(
        node,
        "files",
        &format!("{label} files"),
        owner,
        "PullRequest",
    )?;
    pending.extend(more);
    let files = file_nodes
        .into_iter()
        .map(parse_file)
        .collect::<Result<Vec<_>>>()?;

    let (head_sha, check_runs, check_runs_truncated) =
        parse_head_commit(node, &label, &mut pending)?;

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
        changed_files_reported: node.get("changedFiles").and_then(Value::as_i64).is_some(),
        author: node.get("author").and_then(ActorReference::from_value),
        merged_by: node.get("mergedBy").and_then(ActorReference::from_value),
        reviews,
        review_comments,
        conversation_comments,
        files,
        head_sha,
        check_runs,
        node_id,
        pending,
        check_runs_truncated,
    })
}

/// The head commit selected by `commits(last: 1)`.
///
/// That selection deliberately asks for the newest commit only and no
/// `pageInfo`: the full commit history is not walked, so its absence is not a
/// truncation. The connection and each commit object must still be present
/// and well formed, and the commit's check suites are a required, paginated
/// connection like any other.
fn parse_head_commit(
    node: &Value,
    label: &str,
    pending: &mut Vec<PendingConnection>,
) -> Result<(Option<String>, Vec<CheckRunData>, bool)> {
    let commits = node
        .get("commits")
        .filter(|connection| !connection.is_null())
        .and_then(|connection| connection.get("nodes"))
        .and_then(Value::as_array)
        .ok_or_else(|| Error::GitHubApi(format!("{label} commits has no nodes array")))?;
    let mut head_sha = None;
    let mut check_runs = Vec::new();
    let mut truncated = false;
    for commit_node in commits {
        let commit = commit_node
            .get("commit")
            .filter(|commit| !commit.is_null())
            .ok_or_else(|| Error::GitHubApi(format!("{label} head commit node has no commit")))?;
        let commit_label = format!("{label} head commit");
        let commit_id = required_str(commit, "id", &commit_label)?;
        head_sha = Some(required_str(commit, "oid", &commit_label)?);
        let (suites, more) = required_connection(
            commit,
            "checkSuites",
            &format!("{commit_label} checkSuites"),
            Some(&commit_id),
            "Commit",
        )?;
        truncated |= more.is_some();
        pending.extend(more);
        for suite in suites {
            truncated |= collect_check_suite(suite, &mut check_runs, pending)?;
        }
    }
    Ok((head_sha, check_runs, truncated))
}

fn parse_check_run(check: &Value) -> Result<CheckRunData> {
    Ok(CheckRunData {
        id: required_str(check, "id", "check run")?,
        name: string_field(check, "name").unwrap_or_default(),
        status: string_field(check, "status").unwrap_or_else(|| "COMPLETED".into()),
        conclusion: string_field(check, "conclusion"),
        started_at: string_field(check, "startedAt"),
        completed_at: string_field(check, "completedAt"),
    })
}

/// Collect one check suite's runs; returns true when its runs continue.
fn collect_check_suite(
    suite: &Value,
    check_runs: &mut Vec<CheckRunData>,
    pending: &mut Vec<PendingConnection>,
) -> Result<bool> {
    let suite_id = required_str(suite, "id", "check suite")?;
    let (runs, more) = required_connection(
        suite,
        "checkRuns",
        &format!("check suite {suite_id} checkRuns"),
        Some(&suite_id),
        "CheckSuite",
    )?;
    for check in runs {
        check_runs.push(parse_check_run(check)?);
    }
    let continues = more.is_some();
    pending.extend(more);
    Ok(continues)
}

fn parse_review(review: &Value) -> Result<ReviewData> {
    Ok(ReviewData {
        id: required_str(review, "id", "review")?,
        state: string_field(review, "state").unwrap_or_else(|| "COMMENTED".into()),
        body: string_field(review, "body").filter(|body| !body.is_empty()),
        // Null for a pending (unsubmitted) review: legitimately nullable.
        submitted_at: string_field(review, "submittedAt"),
        // Null for a deleted ("ghost") account: legitimately nullable.
        author: review.get("author").and_then(ActorReference::from_value),
    })
}

fn parse_review_comment(comment: &Value) -> Result<ReviewCommentData> {
    let id = required_str(comment, "id", "review comment")?;
    let label = format!("review comment {id}");
    Ok(ReviewCommentData {
        body: string_field(comment, "body"),
        path: Some(required_str(comment, "path", &label)?),
        // Null when the comment is outdated against the current diff.
        line: comment.get("line").and_then(Value::as_i64),
        created_at: required_str(comment, "createdAt", &label)?,
        author: comment.get("author").and_then(ActorReference::from_value),
        in_reply_to: comment
            .get("replyTo")
            .and_then(|reply| reply.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string),
        id,
    })
}

fn collect_thread(
    thread: &Value,
    review_comments: &mut Vec<ReviewCommentData>,
    pending: &mut Vec<PendingConnection>,
) -> Result<()> {
    let thread_id = required_str(thread, "id", "review thread")?;
    let (comments, more) = required_connection(
        thread,
        "comments",
        &format!("review thread {thread_id} comments"),
        Some(&thread_id),
        "PullRequestReviewThread",
    )?;
    for comment in comments {
        review_comments.push(parse_review_comment(comment)?);
    }
    pending.extend(more);
    Ok(())
}

fn parse_conversation_comment(comment: &Value) -> Result<ConversationCommentData> {
    let id = required_str(comment, "id", "conversation comment")?;
    Ok(ConversationCommentData {
        body: string_field(comment, "body"),
        created_at: required_str(comment, "createdAt", &format!("conversation comment {id}"))?,
        author: comment.get("author").and_then(ActorReference::from_value),
        id,
    })
}

fn parse_file(file: &Value) -> Result<FileDiffData> {
    Ok(FileDiffData {
        path: required_str(file, "path", "changed file")?,
        change_type: string_field(file, "changeType").unwrap_or_else(|| "MODIFIED".into()),
        additions: integer_field(file, "additions"),
        deletions: integer_field(file, "deletions"),
    })
}

/// Fetch every nested node the page query's first slices left behind.
///
/// Shared by the repository and user-search paths. On success `pending` is
/// empty. Returns a caveat when the provider served fewer files than the pull
/// request reports changing (GitHub stops enumerating files at 3,000), so the
/// caller can surface the truncation instead of treating the list as complete.
/// Any continuation failure is returned as an error, leaving the item to the
/// caller's failed-record handling.
pub fn hydrate_pull_request(
    client: &mut GhClient,
    pull_request: &mut PullRequestData,
) -> Result<Option<String>> {
    let mut queue: Vec<PendingConnection> = std::mem::take(&mut pull_request.pending);
    while let Some(next) = queue.pop() {
        match (next.owner_type, next.field) {
            ("PullRequest", "reviews") => {
                let more = fetch_remaining(client, &next, REVIEW_SELECTION)?;
                for review in &more {
                    pull_request.reviews.push(parse_review(review)?);
                }
            }
            ("PullRequest", "reviewThreads") => {
                let more = fetch_remaining(client, &next, REVIEW_THREAD_SELECTION)?;
                for thread in &more {
                    collect_thread(thread, &mut pull_request.review_comments, &mut queue)?;
                }
            }
            ("PullRequestReviewThread", "comments") => {
                let more = fetch_remaining(client, &next, REVIEW_COMMENT_SELECTION)?;
                for comment in &more {
                    pull_request
                        .review_comments
                        .push(parse_review_comment(comment)?);
                }
            }
            ("PullRequest", "comments") => {
                let more = fetch_remaining(client, &next, CONVERSATION_COMMENT_SELECTION)?;
                for comment in &more {
                    pull_request
                        .conversation_comments
                        .push(parse_conversation_comment(comment)?);
                }
            }
            ("PullRequest", "files") => {
                let more = fetch_remaining(client, &next, FILE_SELECTION)?;
                for file in &more {
                    pull_request.files.push(parse_file(file)?);
                }
            }
            ("Commit", "checkSuites") => {
                let more = fetch_remaining(client, &next, CHECK_SUITE_SELECTION)?;
                for suite in &more {
                    collect_check_suite(suite, &mut pull_request.check_runs, &mut queue)?;
                }
            }
            ("CheckSuite", "checkRuns") => {
                let more = fetch_remaining(client, &next, CHECK_RUN_SELECTION)?;
                for check in &more {
                    pull_request.check_runs.push(parse_check_run(check)?);
                }
            }
            (owner, field) => {
                return Err(Error::GitHubApi(format!(
                    "no continuation handler for {owner}.{field}"
                )));
            }
        }
    }
    pull_request.check_runs_truncated = false;
    let mut caveats = Vec::new();
    let served = pull_request.files.len() as i64;
    if served < pull_request.changed_files {
        caveats.push(format!(
            "PR #{}: provider listed {served} of {} changed files; file coverage is truncated",
            pull_request.number, pull_request.changed_files
        ));
    }
    if pull_request.check_runs_truncated {
        caveats.push(format!(
            "PR #{}: head commit has more check suites/runs than were fetched; check-run \
             coverage is truncated",
            pull_request.number
        ));
    }
    Ok((!caveats.is_empty()).then(|| caveats.join("; ")))
}

/// GitHub's REST ceiling on files listed for one pull request.
pub const REST_FILE_LIMIT: usize = 3000;

/// Why a hydrated file list cannot be treated as the complete file set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSetGap {
    pub reason: String,
    /// True for a provider inconsistency (duplicate or empty path, more paths
    /// than `changedFiles`) rather than an ordinary shortfall such as the
    /// provider's 3,000-file cap, which the hydrator already reports.
    pub anomalous: bool,
}

/// The hydrated file list as a provably complete set of paths, or why it is
/// not one.
///
/// A listing is authoritative only when every nested page was walked (nothing
/// pending), the provider reported `changedFiles`, every path is non-empty and
/// unique, and the number of distinct paths equals `changedFiles` exactly. A
/// provider-capped listing (fewer paths than `changedFiles`), a duplicate
/// path, or more paths than `changedFiles` all fail closed, so the caller can
/// keep stored rows instead of pruning against a set it cannot trust.
pub fn authoritative_file_paths(
    pull_request: &PullRequestData,
) -> std::result::Result<std::collections::HashSet<&str>, FileSetGap> {
    let gap = |reason: String, anomalous: bool| Err(FileSetGap { reason, anomalous });
    if pull_request
        .pending
        .iter()
        .any(|pending| pending.field == "files")
    {
        return gap("file pages were not all fetched".into(), false);
    }
    if !pull_request.changed_files_reported || pull_request.changed_files < 0 {
        return gap(
            "the provider did not report a changed-file total".into(),
            true,
        );
    }
    let mut paths = std::collections::HashSet::new();
    for file in &pull_request.files {
        if file.path.trim().is_empty() {
            return gap("the provider listed a file with an empty path".into(), true);
        }
        if !paths.insert(file.path.as_str()) {
            return gap(
                format!("the provider listed {} more than once", file.path),
                true,
            );
        }
    }
    let distinct = paths.len() as i64;
    if distinct != pull_request.changed_files {
        return gap(
            format!(
                "the provider listed {distinct} distinct paths for {} changed files",
                pull_request.changed_files
            ),
            distinct > pull_request.changed_files,
        );
    }
    Ok(paths)
}
/// Files per REST page (GitHub's maximum for this endpoint).
pub const REST_FILES_PER_PAGE: usize = 100;

/// Every patch the REST files endpoint served for one pull request.
#[derive(Debug, Default)]
pub struct PatchListing {
    /// (path, previous_path, patch) per served file, in provider order.
    pub files: Vec<(String, Option<String>, Option<String>)>,
    /// REST pages requested.
    pub pages: u32,
    /// True when the walk stopped at GitHub's 3,000-file REST ceiling, so
    /// further files (if any) are unreachable through this endpoint.
    pub provider_capped: bool,
}

/// Walk `/repos/{repo}/pulls/{number}/files` page by page.
///
/// The endpoint serves at most 100 files per page and 3,000 in total. A page
/// shorter than 100 is the last one; a full page always asks for the next, so
/// a pull request with exactly 100·N files ends on an empty page. Any failed or
/// non-array page is an error — the caller must not treat a partial walk as the
/// complete listing.
pub fn fetch_file_patches(
    client: &mut GhClient,
    repo_key: &str,
    number: i64,
) -> Result<PatchListing> {
    let mut listing = PatchListing::default();
    let max_pages = REST_FILE_LIMIT.div_ceil(REST_FILES_PER_PAGE);
    for page in 1..=max_pages {
        let path = format!(
            "repos/{repo_key}/pulls/{number}/files?per_page={REST_FILES_PER_PAGE}&page={page}"
        );
        let value = client.rest(&path)?;
        listing.pages += 1;
        let served = value.as_array().map(Vec::len).ok_or_else(|| {
            Error::GitHubApi(format!("PR #{number} files page {page} is not a list"))
        })?;
        listing.files.extend(parse_rest_file_patches(&value));
        if served < REST_FILES_PER_PAGE {
            return Ok(listing);
        }
    }
    listing.provider_capped = true;
    Ok(listing)
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
mod tests;
