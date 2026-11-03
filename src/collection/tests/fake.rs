//! An in-memory GitHub that answers the queries githubdw issues, so the light
//! and detailed collection paths can run against one pinned world.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate};
use serde_json::{Value, json};

use crate::error::{Error, Result};
use crate::fetch::GhTransport;

#[derive(Debug, Clone)]
pub struct FakeReview {
    pub id: String,
    pub reviewer: String,
    pub state: String,
    pub body: String,
    pub submitted_at: String,
}

#[derive(Debug, Clone)]
pub struct FakeComment {
    pub id: String,
    pub author: String,
    pub body: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct FakeFile {
    pub path: String,
    pub additions: i64,
    pub deletions: i64,
    pub patch: String,
}

#[derive(Debug, Clone)]
pub struct FakePr {
    pub repo: String,
    pub number: i64,
    pub author: String,
    pub title: String,
    pub body: String,
    pub created_at: String,
    pub updated_at: String,
    pub merged_at: Option<String>,
    pub reviews: Vec<FakeReview>,
    pub comments: Vec<FakeComment>,
    pub files: Vec<FakeFile>,
    pub base_oid: String,
    pub head_oid: String,
    /// Served as a FORBIDDEN (SAML) node.
    pub inaccessible: bool,
    /// What the search index still holds as `updatedAt` (index lag). The
    /// served node always shows the current values.
    pub search_updated_at: Option<String>,
}

impl FakePr {
    pub fn new(repo: &str, number: i64, author: &str, created: &str, updated: &str) -> Self {
        Self {
            repo: repo.into(),
            number,
            author: author.into(),
            title: format!("Title {number}"),
            body: format!("Body {number}"),
            created_at: created.into(),
            updated_at: updated.into(),
            merged_at: None,
            reviews: Vec::new(),
            comments: Vec::new(),
            files: vec![FakeFile {
                path: "src/lib.rs".into(),
                additions: 3,
                deletions: 1,
                patch: format!("@@ -1 +1 @@ PATCH-SENTINEL-{number}"),
            }],
            base_oid: format!("base{number:04}"),
            head_oid: format!("head{number:04}"),
            inaccessible: false,
            search_updated_at: None,
        }
    }

    pub fn node_id(&self) -> String {
        format!("PR_{}_{}", self.repo, self.number)
    }

    pub fn merged(mut self, at: &str) -> Self {
        self.merged_at = Some(at.into());
        self
    }

    pub fn reviewed(mut self, id: &str, reviewer: &str, at: &str) -> Self {
        self.reviews.push(FakeReview {
            id: id.into(),
            reviewer: reviewer.into(),
            state: "APPROVED".into(),
            body: format!("review body {id}"),
            submitted_at: at.into(),
        });
        self
    }

    pub fn commented(mut self, id: &str, author: &str, at: &str) -> Self {
        self.comments.push(FakeComment {
            id: id.into(),
            author: author.into(),
            body: format!("comment body {id}"),
            created_at: at.into(),
        });
        self
    }
}

/// What the fake was asked, for cost and shape assertions.
#[derive(Debug, Default, Clone)]
pub struct Requests {
    pub light_searches: usize,
    pub detailed_searches: usize,
    pub single_pr: usize,
    pub revision: usize,
    pub rest_files: usize,
    pub review_pages: usize,
    pub other: usize,
    pub queries: Vec<String>,
}

#[derive(Default)]
pub struct World {
    pub prs: Vec<FakePr>,
    pub requests: Requests,
    /// Errors returned (once each) before answering normally.
    pub fail_next: Vec<Error>,
    /// Answer every review-list continuation with a non-retryable error.
    pub fail_review_pages: bool,
}

#[derive(Clone)]
pub struct FakeGitHub(pub Arc<Mutex<World>>);

impl FakeGitHub {
    pub fn new(prs: Vec<FakePr>) -> Self {
        Self(Arc::new(Mutex::new(World {
            prs,
            ..Default::default()
        })))
    }

    pub fn client(&self) -> crate::fetch::GhClient {
        crate::fetch::GhClient::with_transport(Box::new(self.clone())).without_sleeping()
    }

    pub fn requests(&self) -> Requests {
        self.0.lock().unwrap().requests.clone()
    }

    pub fn reset_requests(&self) {
        self.0.lock().unwrap().requests = Requests::default();
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut World) -> R) -> R {
        f(&mut self.0.lock().unwrap())
    }
}

fn variable<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    let prefix = format!("{name}=");
    arguments
        .windows(2)
        .find(|pair| pair[0] == "-F" && pair[1].starts_with(&prefix))
        .map(|pair| &pair[1][prefix.len()..])
}

fn parse_instant(text: &str, end: bool) -> i64 {
    if let Ok(at) = DateTime::parse_from_rfc3339(text) {
        return at.timestamp();
    }
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("search date");
    let base = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
    if end { base + 86_399 } else { base }
}

fn seconds(text: &str) -> i64 {
    DateTime::parse_from_rfc3339(text).unwrap().timestamp()
}

struct Filter {
    author: Option<String>,
    reviewer: Option<String>,
    field: Option<(String, i64, i64)>,
}

fn parse_filter(q: &str) -> Filter {
    let mut filter = Filter {
        author: None,
        reviewer: None,
        field: None,
    };
    for token in q.split_whitespace() {
        if let Some(login) = token.strip_prefix("author:") {
            filter.author = Some(login.to_lowercase());
        } else if let Some(login) = token.strip_prefix("reviewed-by:") {
            filter.reviewer = Some(login.to_lowercase());
        } else {
            for field in ["created", "merged", "updated"] {
                if let Some(range) = token.strip_prefix(&format!("{field}:")) {
                    let (a, b) = if let Some(rest) = range.strip_prefix(">=") {
                        (rest, "2999-01-01T00:00:00Z")
                    } else {
                        range.split_once("..").expect("range")
                    };
                    filter.field = Some((
                        field.into(),
                        parse_instant(a, false),
                        parse_instant(b, true),
                    ));
                }
            }
        }
    }
    filter
}

fn matches(pr: &FakePr, filter: &Filter) -> bool {
    if let Some(author) = &filter.author
        && pr.author.to_lowercase() != *author
    {
        return false;
    }
    if let Some(reviewer) = &filter.reviewer
        && !pr
            .reviews
            .iter()
            .any(|r| r.reviewer.to_lowercase() == *reviewer)
    {
        return false;
    }
    if let Some((field, start, end)) = &filter.field {
        let value = match field.as_str() {
            "created" => Some(seconds(&pr.created_at)),
            "updated" => Some(seconds(
                pr.search_updated_at.as_deref().unwrap_or(&pr.updated_at),
            )),
            _ => pr.merged_at.as_deref().map(seconds),
        };
        match value {
            Some(v) if v >= *start && v <= *end => {}
            _ => return false,
        }
    }
    true
}

fn actor(login: &str) -> Value {
    json!({"login": login, "__typename": "User"})
}

fn repo_json(repo: &str) -> Value {
    json!({
        "nameWithOwner": repo, "primaryLanguage": {"name": "Rust"}, "isFork": false,
        "isPrivate": true, "defaultBranchRef": {"name": "main"},
        "createdAt": "2020-01-01T00:00:00Z"
    })
}

fn empty_page(nodes: Vec<Value>) -> Value {
    json!({"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": nodes})
}

fn state(pr: &FakePr) -> &'static str {
    if pr.merged_at.is_some() {
        "MERGED"
    } else {
        "OPEN"
    }
}

/// One 100-review page of `login`'s reviews after `after`.
fn login_reviews(pr: &FakePr, login: &str, after: usize) -> Value {
    let mine: Vec<Value> = pr
        .reviews
        .iter()
        .filter(|r| r.reviewer.eq_ignore_ascii_case(login))
        .map(|r| {
            json!({"id": r.id, "state": r.state, "submittedAt": r.submitted_at,
                   "author": actor(&r.reviewer)})
        })
        .collect();
    let total = mine.len();
    let end = (after + 100).min(total);
    let has_next = end < total;
    json!({"totalCount": total,
           "pageInfo": {"hasNextPage": has_next,
                        "endCursor": if has_next { Some(format!("r{end}")) } else { None }},
           "nodes": mine[after.min(total)..end]})
}

fn light_node(pr: &FakePr, login: &str) -> Value {
    json!({
        "id": pr.node_id(), "number": pr.number, "title": pr.title, "body": pr.body, "state": state(pr),
        "isDraft": false, "createdAt": pr.created_at, "updatedAt": pr.updated_at,
        "mergedAt": pr.merged_at, "closedAt": pr.merged_at,
        "baseRefName": "main", "headRefName": format!("f/{}", pr.number),
        "baseRefOid": pr.base_oid, "headRefOid": pr.head_oid,
        "additions": pr.files.iter().map(|f| f.additions).sum::<i64>(),
        "deletions": pr.files.iter().map(|f| f.deletions).sum::<i64>(),
        "changedFiles": pr.files.len(),
        "author": actor(&pr.author),
        "mergedBy": pr.merged_at.as_ref().map(|_| actor("merger")),
        "comments": {"totalCount": pr.comments.len()},
        "reviews": {"totalCount": pr.reviews.len()},
        "loginReviews": login_reviews(pr, login, 0),
        "repository": repo_json(&pr.repo),
    })
}

pub fn full_node(pr: &FakePr) -> Value {
    let reviews: Vec<Value> = pr
        .reviews
        .iter()
        .map(|r| {
            json!({"id": r.id, "state": r.state, "body": r.body,
                   "submittedAt": r.submitted_at, "author": actor(&r.reviewer)})
        })
        .collect();
    let comments: Vec<Value> = pr
        .comments
        .iter()
        .map(|c| {
            json!({"id": c.id, "body": c.body, "createdAt": c.created_at,
                        "author": actor(&c.author)})
        })
        .collect();
    let files: Vec<Value> = pr
        .files
        .iter()
        .map(|f| {
            json!({"path": f.path, "changeType": "MODIFIED",
                        "additions": f.additions, "deletions": f.deletions})
        })
        .collect();
    let mut node = light_node(pr, "");
    let object = node.as_object_mut().unwrap();
    object.remove("loginReviews");
    object.insert("reviews".into(), empty_page(reviews));
    object.insert("reviewThreads".into(), empty_page(vec![]));
    object.insert("comments".into(), empty_page(comments));
    object.insert("files".into(), empty_page(files));
    object.insert("commits".into(), json!({"nodes": []}));
    node
}

impl World {
    fn search(&self, q: &str, login: &str, light: bool, page_size: usize, after: usize) -> Value {
        let filter = parse_filter(q);
        let mut hits: Vec<&FakePr> = self.prs.iter().filter(|pr| matches(pr, &filter)).collect();
        hits.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then(b.number.cmp(&a.number))
        });
        let total = hits.len();
        // GitHub serves at most 1,000 results per query.
        let servable = total.min(1000);
        let end = (after + page_size).min(servable);
        let slice = if after < end {
            &hits[after..end]
        } else {
            &[][..]
        };
        let mut errors = Vec::new();
        let nodes: Vec<Value> = slice
            .iter()
            .enumerate()
            .map(|(i, pr)| {
                if pr.inaccessible {
                    errors.push(json!({"type": "FORBIDDEN",
                        "message": "Resource protected by organization SAML enforcement.",
                        "path": ["search", "nodes", i]}));
                    Value::Null
                } else if light {
                    light_node(pr, login)
                } else {
                    full_node(pr)
                }
            })
            .collect();
        let has_next = end < servable;
        let mut response = json!({"data": {
            "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999,
                          "resetAt": "2099-01-01T00:00:00Z"},
            "search": {
                "issueCount": total,
                "pageInfo": {"hasNextPage": has_next,
                             "endCursor": if has_next { Some(end.to_string()) } else { None }},
                "nodes": nodes
            }
        }});
        if !errors.is_empty() {
            response["errors"] = Value::Array(errors);
        }
        response
    }

    fn find(&self, owner: &str, name: &str, number: i64) -> Option<&FakePr> {
        let repo = format!("{owner}/{name}").to_lowercase();
        self.prs
            .iter()
            .find(|pr| pr.repo.to_lowercase() == repo && pr.number == number)
    }
}

impl GhTransport for FakeGitHub {
    fn run(&self, arguments: &[String]) -> Result<String> {
        let mut world = self.0.lock().unwrap();
        if !world.fail_next.is_empty() {
            return Err(world.fail_next.remove(0));
        }
        let query = arguments
            .iter()
            .find_map(|a| a.strip_prefix("query="))
            .unwrap_or_default()
            .to_string();
        if arguments.get(1).map(String::as_str) != Some("graphql") {
            // REST: repos/<o>/<n>/pulls/<num>/files?per_page=100&page=P
            let path = arguments.get(1).cloned().unwrap_or_default();
            world.requests.rest_files += 1;
            world.requests.queries.push(path.clone());
            let parts: Vec<&str> = path.split(['/', '?']).collect();
            let number: i64 = parts[4].parse().unwrap();
            let page: usize = path.rsplit("page=").next().unwrap().parse().unwrap();
            let files: Vec<Value> = world
                .find(parts[1], parts[2], number)
                .filter(|_| page == 1)
                .map(|pr| {
                    pr.files
                        .iter()
                        .map(|f| json!({"filename": f.path, "patch": f.patch}))
                        .collect()
                })
                .unwrap_or_default();
            return Ok(Value::Array(files).to_string());
        }
        if query.contains("search(query") {
            let q = variable(arguments, "q").unwrap_or_default().to_string();
            let login = variable(arguments, "login").unwrap_or_default().to_string();
            let light = query.contains("loginReviews");
            if light {
                world.requests.light_searches += 1;
            } else {
                world.requests.detailed_searches += 1;
            }
            world.requests.queries.push(q.clone());
            let page_size = if light {
                100
            } else {
                query
                    .split("first: ")
                    .nth(1)
                    .and_then(|r| r.split(',').next())
                    .and_then(|n| n.trim().parse().ok())
                    .unwrap_or(25)
            };
            let after = variable(arguments, "cursor")
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            return Ok(world
                .search(&q, &login, light, page_size, after)
                .to_string());
        }
        if query.contains("node(id: $id)") && query.contains("loginReviews") {
            world.requests.review_pages += 1;
            if world.fail_review_pages {
                return Err(Error::GhCommand {
                    exit_code: Some(1),
                    stderr: "HTTP 502: bad gateway, gave up".into(),
                });
            }
            let id = variable(arguments, "id").unwrap_or_default().to_string();
            let login = variable(arguments, "login").unwrap_or_default().to_string();
            let after: usize = variable(arguments, "cursor")
                .and_then(|c| c.strip_prefix('r'))
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            let Some(pr) = world.prs.iter().find(|pr| pr.node_id() == id) else {
                return Ok(json!({"data": {"node": null}}).to_string());
            };
            return Ok(json!({"data": {
                "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999,
                              "resetAt": "2099-01-01T00:00:00Z"},
                "node": {"loginReviews": login_reviews(pr, &login, after)}
            }})
            .to_string());
        }
        if query.contains("pullRequest(number: $number)") {
            let owner = variable(arguments, "owner").unwrap_or_default().to_string();
            let name = variable(arguments, "name").unwrap_or_default().to_string();
            let number: i64 = variable(arguments, "number").unwrap().parse().unwrap();
            let revision_only = !query.contains("reviewThreads");
            if revision_only {
                world.requests.revision += 1;
            } else {
                world.requests.single_pr += 1;
            }
            let pr = world.find(&owner, &name, number).cloned();
            let Some(pr) = pr else {
                return Ok(json!({"data": {"repository": null}}).to_string());
            };
            let node = if revision_only {
                json!({"baseRefOid": pr.base_oid, "headRefOid": pr.head_oid})
            } else {
                full_node(&pr)
            };
            let mut repository = repo_json(&pr.repo);
            repository["pullRequest"] = node;
            return Ok(json!({"data": {
                "rateLimit": {"limit": 5000, "cost": 1, "remaining": 4999,
                              "resetAt": "2099-01-01T00:00:00Z"},
                "repository": repository
            }})
            .to_string());
        }
        world.requests.other += 1;
        Err(Error::GitHubApi(format!(
            "fake: unhandled query {query:.80}"
        )))
    }
}
