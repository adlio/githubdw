//! `tick` against an in-memory GitHub that answers the light, aliased and
//! by-id queries the collector issues, and charges GitHub's points for them.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use serde_json::{Value, json};

use super::*;
use crate::GithubDW;
use crate::collection::tests::fake::{FakePr, full_node};
use crate::fetch::GhTransport;

#[derive(Default)]
struct World {
    prs: Vec<FakePr>,
    /// Head-commit check runs per node id: (id, status).
    checks: BTreeMap<String, Vec<(String, String)>>,
    /// Node ids served with a malformed check-run list.
    poison: HashSet<String>,
    missing_logins: HashSet<String>,
    saml_repos: HashSet<String>,
    /// Errors returned once each before answering, by query kind.
    fail_next: Vec<Error>,
    searches: Vec<String>,
    by_id: usize,
    aliased: usize,
    listings: usize,
    alias_queries: Vec<String>,
    /// Per node id: an error served instead of the node.
    node_errors: BTreeMap<String, Value>,
    /// Errors returned once each, only to `nodes(ids:)` / search / listing.
    fail_nodes: Vec<Error>,
    fail_search: Vec<Error>,
    fail_listing: Vec<Error>,
    /// Added to every search's `issueCount` (a short answer).
    lie_count: i64,
    /// Repositories whose head read answers with a non-access error.
    broken_repos: HashSet<String>,
    /// Newest issue update per repository, and the issue walk's count.
    issue_heads: BTreeMap<String, String>,
    issue_walks: usize,
    /// GraphQL budget used, as `rate_limit` reports it.
    used: i64,
    /// An error with no alias path served to every aliased query.
    alias_error: Option<String>,
    /// Pull requests per repository listing page (0 = all).
    listing_page: usize,
    /// `missing`: no repository; `bad`: a node without an id.
    listing_fault: Option<&'static str>,
    /// An extra error served with every search / `nodes(ids:)` answer.
    search_error: Option<Value>,
    nodes_error: Option<Value>,
    /// The issue walk fails outright.
    issue_fault: bool,
    /// The check rollup state served, whatever the runs say.
    rollup: Option<&'static str>,
    /// Per node id: the older `updatedAt` search filters on (index lag).
    /// Results still carry the real value.
    index_at: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct Fake(Arc<Mutex<World>>);

fn rate(cost: i64) -> Value {
    json!({"limit": 5000, "cost": cost, "remaining": 4000, "resetAt": "2099-01-01T00:00:00Z"})
}

fn secs(text: &str) -> i64 {
    DateTime::parse_from_rfc3339(text).unwrap().timestamp()
}

fn variable<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    let prefix = format!("{name}=");
    arguments.iter().find_map(|a| a.strip_prefix(&prefix))
}

impl World {
    fn node(&self, pr: &FakePr) -> Value {
        let mut node = full_node(pr);
        let id = pr.node_id();
        let runs: Vec<Value> = self
            .checks
            .get(&id)
            .map(|runs| {
                runs.iter()
                    .map(|(run, status)| {
                        json!({"id": run, "name": run, "status": status,
                               "conclusion": (status == "COMPLETED").then_some("SUCCESS"),
                               "startedAt": pr.updated_at, "completedAt": null})
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut run_nodes = Value::Array(runs);
        if self.poison.contains(&id) {
            run_nodes = json!([null]);
        }
        node["commits"] = json!({"nodes": [{"commit": {"id": format!("C_{id}"), "oid": pr.head_oid,
            "checkSuites": {"pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": [{"id": format!("S_{id}"), "checkRuns": {
                    "pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": run_nodes}}]}}}]});
        node
    }

    fn search(&mut self, q: &str, after: usize) -> Value {
        self.searches.push(q.to_string());
        let mut authors = HashSet::new();
        let mut reviewers = HashSet::new();
        let mut range: Option<(String, i64, i64)> = None;
        for token in q.split_whitespace() {
            if let Some(l) = token.strip_prefix("author:") {
                authors.insert(l.to_lowercase());
            } else if let Some(l) = token.strip_prefix("reviewed-by:") {
                reviewers.insert(l.to_lowercase());
            } else {
                for field in ["updated", "created"] {
                    if let Some(r) = token.strip_prefix(&format!("{field}:")) {
                        let (a, b) = r.split_once("..").unwrap();
                        range = Some((field.into(), secs(a), secs(b)));
                    }
                }
            }
        }
        let mut hits: Vec<&FakePr> = self
            .prs
            .iter()
            .filter(|pr| {
                (authors.is_empty() || authors.contains(&pr.author.to_lowercase()))
                    && (reviewers.is_empty()
                        || pr
                            .reviews
                            .iter()
                            .any(|r| reviewers.contains(&r.reviewer.to_lowercase())))
            })
            .filter(|pr| {
                let Some((field, a, b)) = &range else {
                    return true;
                };
                let at = secs(if field == "created" {
                    &pr.created_at
                } else {
                    self.index_at.get(&pr.node_id()).unwrap_or(&pr.updated_at)
                });
                *a <= at && at <= *b
            })
            .collect();
        hits.sort_by_key(|pr| (secs(&pr.updated_at), pr.number));
        let total = hits.len();
        let reported = total as i64 + self.lie_count;
        let end = (after + 100).min(total.min(1000));
        let mut errors = Vec::new();
        let nodes: Vec<Value> = hits[after.min(end)..end]
            .iter()
            .enumerate()
            .map(|(i, pr)| {
                if pr.inaccessible {
                    errors.push(json!({"type": "FORBIDDEN", "message": "SAML enforcement",
                                       "path": ["search", "nodes", i]}));
                    Value::Null
                } else {
                    json!({"id": pr.node_id(), "number": pr.number, "updatedAt": pr.updated_at,
                           "repository": {"nameWithOwner": pr.repo}})
                }
            })
            .collect();
        let has_next = end < total.min(1000);
        let mut response = json!({"data": {"rateLimit": rate(1), "search": {
            "issueCount": reported,
            "pageInfo": {"hasNextPage": has_next, "endCursor": has_next.then(|| end.to_string())},
            "nodes": nodes}}});
        errors.extend(self.search_error.clone());
        if !errors.is_empty() {
            response["errors"] = Value::Array(errors);
        }
        response
    }

    fn find(&self, repo: &str, number: i64) -> Option<&FakePr> {
        self.prs
            .iter()
            .find(|pr| pr.repo.eq_ignore_ascii_case(repo) && pr.number == number)
    }

    fn aliased(&mut self, query: &str) -> Value {
        self.aliased += 1;
        self.alias_queries.push(query.to_string());
        let mut data = serde_json::Map::new();
        data.insert("rateLimit".into(), rate(1));
        let mut errors = Vec::new();
        for line in query
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with('a') && l.contains(": "))
        {
            let (alias, body) = line.split_once(": ").unwrap();
            let quoted: Vec<&str> = body.split('"').skip(1).step_by(2).collect();
            if body.starts_with("user(") {
                let login = quoted[0];
                if self.missing_logins.contains(login) {
                    data.insert(alias.into(), Value::Null);
                    errors.push(json!({"type": "NOT_FOUND", "path": [alias],
                                       "message": format!("Could not resolve to a User with the login of '{login}'.")}));
                } else {
                    data.insert(alias.into(), json!({"login": login}));
                }
                continue;
            }
            let repo = format!("{}/{}", quoted[0], quoted[1]);
            if self.broken_repos.contains(&repo) {
                data.insert(alias.into(), Value::Null);
                errors.push(
                    json!({"type": "INTERNAL", "path": [alias], "message": "Something went wrong"}),
                );
                continue;
            }
            if self.broken_repos.contains(&repo) {
                data.insert(alias.into(), Value::Null);
                errors.push(
                    json!({"type": "INTERNAL", "path": [alias], "message": "Something went wrong"}),
                );
                continue;
            }
            if self.saml_repos.contains(&repo) {
                data.insert(alias.into(), Value::Null);
                errors.push(json!({"type": "FORBIDDEN", "path": [alias],
                                   "message": "Resource protected by organization SAML enforcement."}));
                continue;
            }
            if let Some(rest) = body.split("pullRequest(number: ").nth(1) {
                let number: i64 = rest.split(')').next().unwrap().parse().unwrap();
                let Some(pr) = self.find(&repo, number) else {
                    data.insert(alias.into(), json!({"pullRequest": null}));
                    continue;
                };
                let runs = self.checks.get(&pr.node_id()).cloned().unwrap_or_default();
                let state = if let Some(state) = self.rollup {
                    state
                } else if runs.iter().any(|(_, s)| s != "COMPLETED") {
                    "PENDING"
                } else {
                    "SUCCESS"
                };
                data.insert(
                    alias.into(),
                    json!({"pullRequest": {
                    "id": pr.node_id(), "updatedAt": pr.updated_at, "headRefOid": pr.head_oid,
                    "commits": {"nodes": [{"commit": {"oid": pr.head_oid,
                        "statusCheckRollup": {"state": state}}}]}}}),
                );
                continue;
            }
            let mut mine: Vec<&FakePr> = self
                .prs
                .iter()
                .filter(|p| p.repo.eq_ignore_ascii_case(&repo))
                .collect();
            mine.sort_by_key(|p| std::cmp::Reverse(secs(&p.updated_at)));
            let head = mine
                .first()
                .map(|p| json!([{"updatedAt": p.updated_at}]))
                .unwrap_or(json!([]));
            data.insert(
                alias.into(),
                json!({"pullRequests": {"nodes": head}, "issues": {"nodes": self
                    .issue_heads
                    .get(&repo)
                    .map(|at| json!([{"updatedAt": at}]))
                    .unwrap_or(json!([]))}}),
            );
        }
        if let Some(message) = &self.alias_error {
            errors.push(json!({"message": message}));
        }
        let mut response = json!({"data": Value::Object(data)});
        if !errors.is_empty() {
            response["errors"] = Value::Array(errors);
        }
        response
    }

    fn by_ids(&mut self, query: &str) -> Value {
        self.by_id += 1;
        let list = query
            .split("nodes(ids: [")
            .nth(1)
            .unwrap()
            .split(']')
            .next()
            .unwrap();
        let ids: Vec<String> = list
            .split(", ")
            .map(|s| s.trim_matches('"').to_string())
            .collect();
        let mut errors = Vec::new();
        let nodes: Vec<Value> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                if let Some(error) = self.node_errors.get(id) {
                    let mut error = error.clone();
                    error["path"] = json!(["nodes", i]);
                    errors.push(error);
                    return Value::Null;
                }
                self.prs
                    .iter()
                    .find(|pr| pr.node_id() == *id)
                    .map(|pr| self.node(pr))
                    .unwrap_or(Value::Null)
            })
            .collect();
        let mut response = json!({"data": {"rateLimit": rate(17), "nodes": nodes}});
        errors.extend(self.nodes_error.clone());
        if !errors.is_empty() {
            response["errors"] = Value::Array(errors);
        }
        response
    }

    fn listing(&mut self, owner: &str, name: &str, after: usize) -> Value {
        self.listings += 1;
        match self.listing_fault {
            Some("missing") => return json!({"data": {"rateLimit": rate(1), "repository": null}}),
            Some("bad") => {
                return json!({"data": {"rateLimit": rate(1), "repository": {"nameWithOwner": "a/b",
                    "pullRequests": {"pageInfo": {"hasNextPage": false, "endCursor": null},
                                     "nodes": [{"number": 1}]}}}});
            }
            Some("stuck") => {
                return json!({"data": {"rateLimit": rate(1), "repository": {"nameWithOwner": "a/b",
                    "pullRequests": {"pageInfo": {"hasNextPage": true, "endCursor": null},
                                     "nodes": [{"id": "PR_x", "number": 1, "updatedAt": Utc::now().to_rfc3339()}]}}}});
            }
            _ => {}
        }
        let repo = format!("{owner}/{name}");
        let mut mine: Vec<&FakePr> = self
            .prs
            .iter()
            .filter(|p| p.repo.eq_ignore_ascii_case(&repo))
            .collect();
        mine.sort_by_key(|p| std::cmp::Reverse(secs(&p.updated_at)));
        let size = if self.listing_page == 0 {
            mine.len().max(1)
        } else {
            self.listing_page
        };
        let end = (after + size).min(mine.len());
        let nodes: Vec<Value> = mine[after.min(end)..end]
            .iter()
            .map(|p| json!({"id": p.node_id(), "number": p.number, "updatedAt": p.updated_at}))
            .collect();
        let has_next = end < mine.len();
        json!({"data": {"rateLimit": rate(1), "repository": {"nameWithOwner": repo,
            "pullRequests": {"pageInfo": {"hasNextPage": has_next,
                                          "endCursor": has_next.then(|| end.to_string())},
                             "nodes": nodes}}}})
    }
}

impl GhTransport for Fake {
    fn run(&self, arguments: &[String]) -> crate::Result<String> {
        let arguments: Vec<String> = arguments
            .iter()
            .filter(|a| *a != "--include")
            .cloned()
            .collect();
        let arguments = arguments.as_slice();
        let mut world = self.0.lock().unwrap();
        if !world.fail_next.is_empty() {
            return Err(world.fail_next.remove(0));
        }
        let path = arguments.get(1).cloned().unwrap_or_default();
        if path == "rate_limit" {
            return Ok(
                json!({"resources": {"graphql": {"limit": 5000, "used": world.used, "remaining": 5000 - world.used,
                "reset": 4_000_000_000_i64}, "core": {"limit": 5000, "used": 10, "remaining": 4990,
                "reset": 4_000_000_000_i64}}})
                .to_string(),
            );
        }
        if path != "graphql" {
            // REST patch listing: repos/o/n/pulls/N/files?per_page=100&page=P
            let parts: Vec<&str> = path.split(['/', '?']).collect();
            let number: i64 = parts[4].parse().unwrap();
            let page: usize = path.rsplit("page=").next().unwrap().parse().unwrap();
            let files: Vec<Value> = world
                .find(&format!("{}/{}", parts[1], parts[2]), number)
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
        let query = variable(arguments, "query").unwrap_or_default().to_string();
        let response = if query.contains("search(query") {
            if !world.fail_search.is_empty() {
                return Err(world.fail_search.remove(0));
            }
            let q = variable(arguments, "q").unwrap().to_string();
            let after = variable(arguments, "cursor")
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            world.search(&q, after)
        } else if query.contains("nodes(ids:") {
            if !world.fail_nodes.is_empty() {
                return Err(world.fail_nodes.remove(0));
            }
            world.by_ids(&query)
        } else if query.contains("a0:") {
            world.aliased(&query)
        } else if query.contains("issues(first: 50") {
            world.issue_walks += 1;
            if world.issue_fault {
                return Err(Error::GitHubApi("issue walk broke".into()));
            }
            let owner = variable(arguments, "owner").unwrap().to_string();
            let name = variable(arguments, "name").unwrap().to_string();
            json!({"data": {"rateLimit": rate(2), "repository": {"nameWithOwner": format!("{owner}/{name}"),
                "issues": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []}}}})
        } else if query.contains("pullRequests(first: 100, after: $cursor") {
            if !world.fail_listing.is_empty() {
                return Err(world.fail_listing.remove(0));
            }
            let owner = variable(arguments, "owner").unwrap().to_string();
            let name = variable(arguments, "name").unwrap().to_string();
            let after = variable(arguments, "cursor")
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            world.listing(&owner, &name, after)
        } else {
            return Err(Error::GitHubApi(format!(
                "fake: unhandled query {query:.80}"
            )));
        };
        let has_errors = response.get("errors").is_some();
        crate::fetch::interpret_gh_output(
            arguments,
            Some(i32::from(has_errors)),
            &response.to_string(),
            "gh: partial errors",
        )
    }
}

impl Fake {
    fn new(prs: Vec<FakePr>) -> Self {
        Self(Arc::new(Mutex::new(World {
            prs,
            used: 100,
            ..Default::default()
        })))
    }
    fn client(&self) -> GhClient {
        GhClient::with_transport(Box::new(self.clone())).without_sleeping()
    }
    fn with<R>(&self, f: impl FnOnce(&mut World) -> R) -> R {
        f(&mut self.0.lock().unwrap())
    }
}

fn ago(minutes: i64) -> String {
    text(Utc::now() - Duration::minutes(minutes))
}

fn warehouse(users: &[&str]) -> GithubDW {
    let dw = GithubDW::open_in_memory().unwrap();
    for user in users {
        monitor_repository::add_user(dw.connection(), user).unwrap();
    }
    dw
}

fn policy() -> TickPolicy {
    TickPolicy {
        program: "test".into(),
        skip_diffs: true,
        repositories: false,
        backfill_points: 0,
        ..TickPolicy::default()
    }
}

/// A policy that detects on every tick.
fn eager() -> TickPolicy {
    TickPolicy {
        detect_every: Duration::zero(),
        ..policy()
    }
}

fn run_tick(dw: &GithubDW, fake: &Fake, policy: &TickPolicy) -> TickReport {
    let mut client = fake.client();
    tick(
        dw.connection(),
        &mut client,
        policy,
        &CollectionControl::default(),
    )
    .unwrap()
}

fn count(dw: &GithubDW, sql: &str) -> i64 {
    dw.connection().query_row(sql, [], |r| r.get(0)).unwrap()
}

fn two_people() -> Vec<FakePr> {
    vec![
        FakePr::new("acme/app", 1, "alice", &ago(300), &ago(120)),
        FakePr::new("acme/app", 2, "bob", &ago(300), &ago(90)).reviewed("R1", "alice", &ago(90)),
    ]
}

#[test]
fn a_quiet_tick_spends_only_the_light_searches() {
    let dw = warehouse(&["alice", "bob"]);
    let fake = Fake::new(two_people());
    let first = run_tick(&dw, &fake, &eager());
    assert_eq!(first.outcome, TickOutcome::Detected, "{first:?}");
    assert_eq!(first.fetch.landed, 2);
    assert_eq!(count(&dw, "SELECT COUNT(*) FROM fact_pull_requests"), 2);

    fake.with(|w| w.by_id = 0);
    let quiet = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert_eq!(quiet.outcome, TickOutcome::Detected);
    assert_eq!(quiet.queued, 0, "equal updatedAt and complete: skipped");
    assert_eq!(fake.with(|w| w.by_id), 0, "no full fetch");
    assert_eq!(quiet.points.fetch, 0);
    assert_eq!(quiet.points.detect, 2, "one light search per role");
    assert_eq!(quiet.points.total(), 2, "{}", quiet.cost_line());
}

#[test]
fn a_second_tick_inside_the_interval_asks_github_nothing() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    run_tick(&dw, &fake, &policy());
    let searches = fake.with(|w| w.searches.len());
    let second = run_tick(&dw, &fake, &policy());
    assert!(
        matches!(second.outcome, TickOutcome::UpToDate { ref by, .. } if by.as_deref() == Some("test"))
    );
    assert_eq!(fake.with(|w| w.searches.len()), searches);
    assert_eq!(second.points.total(), 0);
}

#[test]
fn a_live_holder_makes_the_tick_busy_and_a_dead_one_is_taken_over() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    crate::storage::schema::ensure_collector_tables(dw.connection()).unwrap();
    let insert = |pid: i64, start: Option<String>| {
        dw.connection()
            .execute(
                "INSERT OR REPLACE INTO collector_leases VALUES ('detect', 't', ?1, ?2, ?3, 'engdash', ?4, ?5)",
                rusqlite::params![pid, start, lease::host_name(), ago(1), text(Utc::now() + Duration::minutes(10))],
            )
            .unwrap();
    };
    let me = std::process::id();
    insert(i64::from(me), lease::process_start(me));
    let busy = run_tick(&dw, &fake, &policy());
    assert!(
        matches!(&busy.outcome, TickOutcome::Busy { holder } if holder.program == "engdash"),
        "{busy:?}"
    );
    assert!(fake.with(|w| w.searches.is_empty()));

    // Same pid, different start time: the pid was reused, the holder is gone.
    insert(i64::from(me), Some("1".into()));
    assert_eq!(
        run_tick(&dw, &fake, &policy()).outcome,
        TickOutcome::Detected
    );
}

#[test]
fn a_poison_pull_request_is_queued_parked_and_does_not_pin_the_cursor() {
    let dw = warehouse(&["alice", "bob"]);
    let prs = two_people();
    let poison = prs[0].node_id();
    let fake = Fake::new(prs);
    fake.with(|w| {
        w.poison.insert(poison.clone());
    });
    let first = run_tick(&dw, &fake, &eager());
    assert_eq!(first.outcome, TickOutcome::Detected, "{first:?}");
    assert_eq!((first.fetch.landed, first.fetch.failed), (1, 1));
    let cursor = state::detect_cursor(dw.connection()).unwrap().unwrap();
    assert!(
        Utc::now() - cursor < Duration::minutes(1),
        "the cursor moved past it"
    );
    for _ in 1..PARK_AFTER_ATTEMPTS {
        run_tick(&dw, &fake, &eager());
    }
    let held = parked(dw.connection()).unwrap();
    assert_eq!(held.len(), 1);
    assert!(
        held[0].last_error.as_deref().unwrap().contains("null"),
        "{held:?}"
    );
    // Parked: not tried again for a day.
    fake.with(|w| w.by_id = 0);
    let later = run_tick(&dw, &fake, &eager());
    assert_eq!(later.fetch.failed, 0);
    assert_eq!(later.parked.len(), 1);
    // A newer update starts it over, and it lands once the cause is gone.
    fake.with(|w| {
        w.poison.clear();
        w.prs[0].updated_at = ago(0);
    });
    let fixed = run_tick(&dw, &fake, &eager());
    assert_eq!(fixed.fetch.landed, 1, "{fixed:?}");
    assert!(parked(dw.connection()).unwrap().is_empty());
}

#[test]
fn finished_check_runs_are_fetched_although_updated_at_did_not_move() {
    let dw = warehouse(&["alice"]);
    let prs = vec![FakePr::new("acme/app", 1, "alice", &ago(60), &ago(20))];
    let id = prs[0].node_id();
    let fake = Fake::new(prs);
    fake.with(|w| {
        w.checks
            .insert(id.clone(), vec![("CR1".into(), "IN_PROGRESS".into())]);
    });
    run_tick(&dw, &fake, &eager());
    let status = || -> String {
        dw.connection()
            .query_row(
                "SELECT status FROM fact_check_runs WHERE check_run_key = 'CR1'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(status(), "IN_PROGRESS");
    fake.with(|w| {
        w.checks
            .insert(id.clone(), vec![("CR1".into(), "COMPLETED".into())]);
    });
    let again = run_tick(&dw, &fake, &eager());
    assert_eq!(again.fetch.landed, 1, "{again:?}");
    assert_eq!(status(), "COMPLETED");
    let quiet = run_tick(&dw, &fake, &eager());
    assert_eq!(quiet.fetch.landed, 0, "complete now: skipped");
}

#[test]
fn a_light_only_row_with_equal_updated_at_is_fetched_in_full() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(vec![FakePr::new(
        "acme/app",
        1,
        "alice",
        &ago(300),
        &ago(20),
    )]);
    run_tick(&dw, &fake, &eager());
    dw.connection()
        .execute(
            "UPDATE pr_detail_state SET detail_fetched_at = NULL WHERE pr_key = 'acme/app#1'",
            [],
        )
        .unwrap();
    let again = run_tick(&dw, &fake, &eager());
    assert_eq!(again.fetch.landed, 1);
}

#[test]
fn a_busy_window_is_bisected_until_every_leaf_fits_one_page() {
    let dw = warehouse(&["alice"]);
    let prs: Vec<FakePr> = (1..=250)
        .map(|n| FakePr::new("acme/app", n, "alice", &ago(400), &ago(n)))
        .collect();
    let fake = Fake::new(prs);
    let report = run_tick(&dw, &fake, &eager());
    assert_eq!(report.outcome, TickOutcome::Detected, "{report:?}");
    assert_eq!(report.fetch.landed, 250);
    assert!(fake.with(|w| w.searches.iter().all(|q| q.contains("sort:updated-asc"))));
    assert!(fake.with(|w| w.searches.len()) > 3, "bisected");
}

#[test]
fn every_role_and_login_share_one_search_per_role() {
    let dw = warehouse(&["alice", "bob", "carol"]);
    let fake = Fake::new(two_people());
    run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    let searches = fake.with(|w| w.searches.clone());
    let detect: Vec<&String> = searches
        .iter()
        .filter(|q| q.contains("author:alice"))
        .collect();
    assert!(
        detect
            .iter()
            .any(|q| q.contains("author:bob") && q.contains("author:carol")),
        "{searches:?}"
    );
    assert!(
        searches
            .iter()
            .any(|q| q.contains("reviewed-by:alice reviewed-by:bob"))
    );
}

#[test]
fn per_user_rows_the_sync_all_row_and_the_schema_version_are_kept() {
    let dw = warehouse(&["alice", "bob"]);
    let fake = Fake::new(two_people());
    run_tick(&dw, &fake, &eager());
    let conn = dw.connection();
    let fresh = user_freshness(conn, "alice").unwrap();
    assert!(fresh.last_sync_at.is_some(), "{fresh:?}");
    let whole = warehouse_freshness(conn).unwrap();
    assert!(whole.last_whole_set_sync_at.is_some());
    assert_eq!(whole.last_detect_by.as_deref(), Some("test"));
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 8, "0.5.x binaries still open the warehouse");
    let status = collector_status(conn).unwrap().unwrap();
    assert!(status.detected_through.is_some());
    assert!(status.last_tick_summary.unwrap().starts_with("tick: "));
}

#[test]
fn logins_that_no_longer_resolve_are_reported_and_not_stamped() {
    let dw = warehouse(&["alice", "gone-user"]);
    let fake = Fake::new(two_people());
    fake.with(|w| {
        w.missing_logins.insert("gone-user".into());
    });
    let report = run_tick(&dw, &fake, &eager());
    assert_eq!(report.unresolved_logins, vec!["gone-user".to_string()]);
    assert!(report.report_due);
    assert!(
        user_freshness(dw.connection(), "gone-user")
            .unwrap()
            .last_sync_at
            .is_none()
    );
    assert!(
        user_freshness(dw.connection(), "gone-user")
            .unwrap()
            .unresolved
    );
}

#[test]
fn a_refused_repository_is_backed_off_for_a_day() {
    let dw = warehouse(&["alice"]);
    monitor_repository::add_repo(dw.connection(), "corp/secret").unwrap();
    monitor_repository::add_repo(dw.connection(), "acme/app").unwrap();
    let fake = Fake::new(two_people());
    fake.with(|w| {
        w.saml_repos.insert("corp/secret".into());
    });
    let with_repos = TickPolicy {
        repositories: true,
        ..eager()
    };
    let first = run_tick(&dw, &fake, &with_repos);
    assert_eq!(first.outcome, TickOutcome::Detected, "{first:?}");
    assert_eq!(first.blocked.len(), 1);
    assert_eq!(first.blocked[0].source, "corp/secret");
    let aliased_before = fake.with(|w| w.aliased);
    let second = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..with_repos.clone()
        },
    );
    assert_eq!(second.outcome, TickOutcome::Detected);
    // The repo head read still runs for acme/app, but corp/secret is not in it.
    assert_eq!(
        fake.with(|w| w.aliased) - aliased_before,
        1,
        "one head read"
    );
    let last = fake.with(|w| w.alias_queries.last().cloned().unwrap());
    assert!(
        last.contains("\"app\"") && !last.contains("secret"),
        "{last}"
    );
    assert!(!second.report_due, "reported once a day");
}

#[test]
fn a_spent_budget_ends_the_tick_without_moving_the_cursor() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    fake.with(|w| {
        w.fail_next.push(Error::RateLimited {
            resource: "graphql".into(),
            wake_at: Utc::now() + Duration::minutes(40),
            reason: "at the cap".into(),
        })
    });
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert!(
        matches!(report.outcome, TickOutcome::CapPaused { .. }),
        "{report:?}"
    );
    assert!(
        state::raw_state(dw.connection())
            .unwrap()
            .last_detect_at
            .is_none()
    );
    assert!(
        lease_holder(dw.connection(), DETECT_LANE)
            .unwrap()
            .is_none(),
        "lease released"
    );
}

#[test]
fn the_backfill_lane_skips_covered_months_and_records_new_ones() {
    let dw = warehouse(&["alice"]);
    let conn = dw.connection();
    let today = time_dimension::today_as_of(conn, Utc::now()).unwrap();
    let yesterday = today - Duration::days(1);
    let windows = crate::sync::month_windows(today - Duration::days(70), yesterday);
    // The newest month is already covered, as a 0.5 backfill left it.
    let (newest_start, newest_end) = *windows.last().unwrap();
    sync_state::record_range(
        conn,
        "user:alice",
        &newest_start.to_string(),
        &newest_end.to_string(),
        1,
    )
    .unwrap();
    let old = FakePr::new(
        "acme/app",
        9,
        "alice",
        &format!("{}T12:00:00Z", windows[0].0),
        &ago(5000),
    );
    let fake = Fake::new(vec![old]);
    let policy = TickPolicy {
        history_from: Some(windows[0].0),
        backfill_points: 1_000,
        ..eager()
    };
    let report = run_tick(&dw, &fake, &policy);
    assert_eq!(report.backfill.skipped, None, "{report:?}");
    assert_eq!(report.backfill.windows_covered.len(), windows.len() - 1);
    let created: Vec<String> = fake.with(|w| {
        w.searches
            .iter()
            .filter(|q| q.contains("created:"))
            .cloned()
            .collect()
    });
    let newest = newest_start.format("%Y-%m-%d").to_string();
    assert!(
        !created
            .iter()
            .any(|q| q.contains(&format!("created:{newest}"))),
        "{created:?}"
    );
    assert_eq!(
        count(
            &dw,
            "SELECT COUNT(*) FROM fact_pull_requests WHERE number = 9"
        ),
        1
    );
    let covered = sync_state::coverage_extent(conn, "user:alice")
        .unwrap()
        .unwrap();
    assert_eq!(covered.0, windows[0].0.to_string());
    assert_eq!(report.backfill.remaining, 0);
}

#[test]
fn a_pending_update_checkpoint_becomes_a_catch_up_that_clears_it() {
    let dw = warehouse(&["alice"]);
    let conn = dw.connection();
    sync_state::record_range(conn, "user:alice", "2026-01-01", "2026-01-31", 1).unwrap();
    sync_state::save_checkpoint(
        conn,
        "user_update_checkpoint",
        "reviewed-by:alice",
        &json!({"version": 1, "base": ago(60 * 24 * 10), "upper": 0, "pending": []}).to_string(),
    )
    .unwrap();
    let fake = Fake::new(vec![
        FakePr::new("acme/app", 5, "bob", &ago(60 * 24 * 9), &ago(60 * 24 * 8)).reviewed(
            "R5",
            "alice",
            &ago(60 * 24 * 8),
        ),
    ]);
    let policy = TickPolicy {
        backfill_points: 1_000,
        ..eager()
    };
    let report = run_tick(&dw, &fake, &policy);
    // Coverage but no walk record owes the author side a catch-up from the
    // end of coverage; the checkpoint owes the reviewer side one from its base.
    assert_eq!(
        report.backfill.catch_ups_finished,
        vec!["author alice".to_string(), "reviewed-by alice".to_string()],
        "{report:?}"
    );
    assert!(
        sync_state::load_checkpoint(conn, "user_update_checkpoint", "reviewed-by:alice")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        count(
            &dw,
            "SELECT COUNT(*) FROM fact_pull_requests WHERE number = 5"
        ),
        1
    );
    assert!(
        user_freshness(conn, "alice")
            .unwrap()
            .last_sync_at
            .is_some()
    );
    assert!(!user_freshness(conn, "alice").unwrap().catch_up_owed);
}

#[test]
fn freshness_readers_work_on_a_warehouse_never_ticked() {
    let dw = warehouse(&["alice"]);
    let conn = dw.connection();
    conn.execute_batch("DROP TABLE collector_state").unwrap();
    assert!(collector_status(conn).unwrap().is_none());
    let whole = warehouse_freshness(conn).unwrap();
    assert_eq!(whole.monitored_sources, 1);
    assert!(!whole.collection_recorded);
    assert!(
        user_freshness(conn, "alice")
            .unwrap()
            .last_sync_at
            .is_none()
    );
    conn.execute_batch("DROP TABLE collector_backoff").unwrap();
    assert!(blocked_sources(conn).unwrap().is_empty());
    assert!(unresolved_logins(conn).unwrap().is_empty());
}

#[test]
fn lane_errors_map_to_outcomes_and_warehouse_errors_pass_up() {
    let wake = Utc::now();
    assert_eq!(
        lane_outcome(Error::Cancelled).unwrap(),
        TickOutcome::Stopped
    );
    assert!(matches!(
        lane_outcome(Error::RateLimited { resource: "graphql".into(), wake_at: wake, reason: "r".into() }).unwrap(),
        TickOutcome::CapPaused { wake_at, .. } if wake_at == wake
    ));
    assert!(matches!(
        lane_outcome(Error::QuotaReserve("q".into())).unwrap(),
        TickOutcome::CapPaused { .. }
    ));
    assert!(matches!(
        lane_outcome(Error::GhUnavailable("no gh".into())).unwrap(),
        TickOutcome::SignIn { .. }
    ));
    assert!(matches!(
        lane_outcome(Error::GhCommand {
            exit_code: Some(1),
            stderr: "run gh auth login".into()
        })
        .unwrap(),
        TickOutcome::SignIn { .. }
    ));
    assert!(matches!(
        lane_outcome(Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 401: Bad credentials".into()
        })
        .unwrap(),
        TickOutcome::SignIn { .. }
    ));
    assert!(matches!(
        lane_outcome(Error::GitHubApi("odd".into())).unwrap(),
        TickOutcome::Incomplete { gaps } if gaps == ["GitHub API error: odd"]
    ));
    assert!(lane_outcome(Error::SchemaIntegrity("x".into())).is_err());
    let shown = format!(
        "{:?}",
        TickPolicy {
            history_from: chrono::NaiveDate::from_ymd_opt(2026, 1, 1),
            ..policy()
        }
    );
    assert!(
        shown.contains("2026-01-01") && shown.contains("program"),
        "{shown}"
    );
}

#[test]
fn a_failing_repository_moves_the_people_cursor_but_not_last_detect_at() {
    let dw = warehouse(&["alice", "bob"]);
    monitor_repository::add_repo(dw.connection(), "acme/app").unwrap();
    monitor_repository::add_repo(dw.connection(), "acme/broken").unwrap();
    let fake = Fake::new(two_people());
    fake.with(|w| {
        w.broken_repos.insert("acme/broken".into());
        w.issue_heads.insert("acme/app".into(), ago(5));
    });
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            repositories: true,
            ..eager()
        },
    );
    assert!(
        matches!(&report.outcome, TickOutcome::Incomplete { gaps } if gaps[0].contains("acme/broken")),
        "{report:?}"
    );
    let raw = state::raw_state(dw.connection()).unwrap();
    assert!(raw.last_detect_at.is_none(), "not a complete detection");
    assert!(
        Utc::now() - raw.detect_cursor.unwrap() < Duration::minutes(1),
        "people were seen in full"
    );
    assert!(
        user_freshness(dw.connection(), "alice")
            .unwrap()
            .last_sync_at
            .is_some()
    );
    assert_eq!(
        fake.with(|w| w.issue_walks),
        1,
        "the moved issue head walked issues"
    );
    assert!(
        sync_state::last_updated_cursor(dw.connection(), "repo", "acme/app")
            .unwrap()
            .is_some()
    );

    // A listing GitHub fails is a gap too, and the repository cursor stays.
    fake.with(|w| {
        w.broken_repos.clear();
        w.prs[0].updated_at = ago(0);
        w.fail_listing
            .push(Error::GitHubApi("listing failed".into()));
    });
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            repositories: true,
            force_reconcile: false,
            ..eager()
        },
    );
    assert!(
        matches!(&report.outcome, TickOutcome::Incomplete { gaps } if gaps[0].contains("listing failed")),
        "{report:?}"
    );
}

#[test]
fn failures_older_walks_recorded_are_imported_and_retried() {
    let dw = warehouse(&["alice"]);
    let conn = dw.connection();
    sync_state::start_job(conn, "user:alice").unwrap();
    sync_state::complete_job(
        conn,
        "user:alice",
        0,
        0,
        &[
            ("acme/app#1".into(), "checkRuns contains a null node".into()),
            ("acme/app#404".into(), "gone since".into()),
            ("not a key".into(), "ignored".into()),
        ],
    )
    .unwrap();
    // PR #1 is outside the detection window: only the import brings it back.
    let fake = Fake::new(vec![FakePr::new(
        "acme/app",
        1,
        "bob",
        &ago(9000),
        &ago(8000),
    )]);
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert!(
        report
            .notes
            .iter()
            .any(|n| n.contains("queued 2 pull request(s)")),
        "{report:?}"
    );
    assert_eq!(
        (report.fetch.landed, report.fetch.gone),
        (1, 1),
        "{report:?}"
    );
    assert_eq!(count(&dw, "SELECT COUNT(*) FROM collector_pr_queue"), 0);
}

#[test]
fn per_node_errors_are_one_items_failure_and_batch_errors_retry_smaller() {
    let dw = warehouse(&["alice"]);
    let prs: Vec<FakePr> = (1..=4)
        .map(|n| FakePr::new("acme/app", n, "alice", &ago(100), &ago(10 + n)))
        .collect();
    let (gone, broken) = (prs[0].node_id(), prs[1].node_id());
    let fake = Fake::new(prs);
    fake.with(|w| {
        w.node_errors.insert(
            gone.clone(),
            json!({"type": "NOT_FOUND", "message": "gone"}),
        );
        w.node_errors.insert(
            broken.clone(),
            json!({"type": "INTERNAL", "message": "boom"}),
        );
        w.fail_nodes.push(Error::GhCommand {
            exit_code: Some(1),
            stderr: "HTTP 502: bad gateway".into(),
        });
    });
    let report = run_tick(&dw, &fake, &eager());
    assert_eq!(report.fetch.landed, 2, "{report:?}");
    assert_eq!(report.fetch.gone, 1);
    assert_eq!(report.fetch.failed, 1);
    let error: String = dw
        .connection()
        .query_row("SELECT last_error FROM collector_pr_queue", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(error, "boom");

    // A batch GitHub refuses outright fails every item in it, kept queued.
    fake.with(|w| {
        w.node_errors.clear();
        w.fail_nodes.push(Error::GitHubApi("no".into()));
    });
    // The gone one is still found by search, so it is queued again too.
    let again = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert_eq!(again.fetch.failed, 2, "{again:?}");
    let third = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert_eq!(third.fetch.landed, 2, "{third:?}");
}

#[test]
fn checks_finishing_after_the_overlap_are_found_by_the_probe() {
    let dw = warehouse(&["alice"]);
    let prs = vec![FakePr::new("acme/app", 1, "alice", &ago(400), &ago(180))];
    let id = prs[0].node_id();
    let fake = Fake::new(prs);
    fake.with(|w| {
        w.checks
            .insert(id.clone(), vec![("CR1".into(), "IN_PROGRESS".into())]);
    });
    // A one-hour overlap leaves the pull request (3 hours old) to the probe.
    run_tick(&dw, &fake, &eager());
    // Still running: the probe asks, and nothing is fetched.
    let still = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            overlap: Duration::hours(1),
            ..eager()
        },
    );
    assert_eq!(still.fetch.landed, 0, "{still:?}");
    fake.with(|w| {
        w.checks
            .insert(id.clone(), vec![("CR1".into(), "COMPLETED".into())]);
    });
    let done = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            overlap: Duration::hours(1),
            ..eager()
        },
    );
    assert_eq!((done.queued, done.fetch.landed), (1, 1), "{done:?}");
}

#[test]
fn a_short_or_failed_search_is_a_gap_and_the_cursor_stays() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    fake.with(|w| w.lie_count = 3);
    let short = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert!(
        matches!(&short.outcome, TickOutcome::Incomplete { gaps } if gaps[0].contains("but served")),
        "{short:?}"
    );
    assert!(
        state::raw_state(dw.connection())
            .unwrap()
            .last_detect_at
            .is_none()
    );
    fake.with(|w| {
        w.lie_count = 0;
        w.fail_search.push(Error::GitHubApi("search broke".into()));
    });
    let failed = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert!(
        matches!(&failed.outcome, TickOutcome::Incomplete { gaps } if gaps[0].contains("search broke")),
        "{failed:?}"
    );
}

#[test]
fn a_query_wide_error_ends_the_lane() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    fake.with(|w| w.alias_error = Some("API rate limit exceeded".into()));
    let report = run_tick(&dw, &fake, &eager());
    assert!(
        matches!(report.outcome, TickOutcome::CapPaused { .. }),
        "{report:?}"
    );
    fake.with(|w| w.alias_error = Some("Something else".into()));
    let report = run_tick(&dw, &fake, &eager());
    assert!(
        matches!(report.outcome, TickOutcome::Incomplete { .. }),
        "{report:?}"
    );
}

#[test]
fn backfill_waits_for_budget_its_lease_and_the_caller() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    let policy = TickPolicy {
        backfill_points: 1_000,
        history_from: Some(Utc::now().date_naive() - Duration::days(40)),
        ..eager()
    };
    // The fake reports 20% of the hour used.
    let busy = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            backfill_share: 0.1,
            ..policy.clone()
        },
    );
    assert_eq!(busy.outcome, TickOutcome::Detected);
    assert!(
        busy.backfill
            .skipped
            .as_deref()
            .unwrap()
            .contains("20% of the hourly"),
        "{busy:?}"
    );

    dw.connection()
        .execute(
            "INSERT INTO collector_leases VALUES ('backfill', 't', ?1, ?2, ?3, 'engdash', ?4, ?5)",
            rusqlite::params![
                i64::from(std::process::id()),
                lease::process_start(std::process::id()),
                lease::host_name(),
                ago(1),
                text(Utc::now() + Duration::minutes(10))
            ],
        )
        .unwrap();
    let held = run_tick(&dw, &fake, &policy);
    assert!(
        held.backfill
            .skipped
            .as_deref()
            .unwrap()
            .contains("engdash"),
        "{held:?}"
    );
    dw.connection()
        .execute("DELETE FROM collector_leases", [])
        .unwrap();

    let pause = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let control = CollectionControl {
        pause: Some(pause),
        ..Default::default()
    };
    let mut client = fake.client();
    let paused = tick(dw.connection(), &mut client, &policy, &control).unwrap();
    assert_eq!(paused.outcome, TickOutcome::Stopped, "{paused:?}");
    assert_eq!(paused.points.backfill, 0);
}

#[test]
fn a_lease_taken_over_cannot_be_renewed_and_holders_are_read() {
    let dw = warehouse(&[]);
    let conn = dw.connection();
    crate::storage::schema::ensure_collector_tables(conn).unwrap();
    let held = lease::Lease::acquire(conn, DETECT_LANE, "me", Duration::minutes(5))
        .unwrap()
        .unwrap();
    assert!(held.renew().unwrap());
    let holder = lease_holder(conn, DETECT_LANE).unwrap().unwrap();
    assert_eq!(
        (holder.program.as_str(), holder.lane.as_str()),
        ("me", "detect")
    );
    assert!(
        lease::Lease::acquire(conn, DETECT_LANE, "other", Duration::minutes(5))
            .unwrap()
            .is_err()
    );
    conn.execute("UPDATE collector_leases SET token = 'someone else'", [])
        .unwrap();
    assert!(!held.renew().unwrap());
    drop(held);
    assert!(
        lease_holder(conn, DETECT_LANE).unwrap().is_some(),
        "the new holder's row stays"
    );
    conn.execute(
        "UPDATE collector_leases SET expires_at = '2000-01-01T00:00:00Z'",
        [],
    )
    .unwrap();
    assert!(
        lease_holder(conn, DETECT_LANE).unwrap().is_none(),
        "expired"
    );
    conn.execute(
        "UPDATE collector_leases SET expires_at = '2999-01-01T00:00:00Z', host = 'elsewhere'",
        [],
    )
    .unwrap();
    assert!(
        lease_holder(conn, DETECT_LANE).unwrap().is_some(),
        "another host: trusted until expiry"
    );
    conn.execute(
        "UPDATE collector_leases SET host = ?1, holder_pid = 999999999",
        [lease::host_name()],
    )
    .unwrap();
    assert!(
        lease_holder(conn, DETECT_LANE).unwrap().is_none(),
        "dead pid on this host"
    );
}

#[test]
fn a_change_the_search_index_lags_is_found_inside_the_overlap() {
    assert_eq!(TickPolicy::default().overlap, Duration::hours(12));
    let dw = warehouse(&["alice", "bob"]);
    let fake = Fake::new(two_people());
    let first = run_tick(&dw, &fake, &eager());
    assert_eq!(first.fetch.landed, 2, "{first:?}");
    // Alice's pull request changes now, but search still filters it on a
    // value 9 h 20 min old, as measured on GitHub.
    let changed = ago(0);
    fake.with(|w| {
        w.by_id = 0;
        w.prs[0].updated_at = changed.clone();
        let id = w.prs[0].node_id();
        w.index_at.insert(id, ago(560));
    });
    let narrow = TickPolicy {
        overlap: Duration::hours(1),
        ..eager()
    };
    let missed = run_tick(&dw, &fake, &narrow);
    assert_eq!((missed.detected, missed.queued), (0, 0), "{missed:?}");

    let wide = run_tick(&dw, &fake, &eager());
    assert_eq!(wide.outcome, TickOutcome::Detected, "{wide:?}");
    assert_eq!(wide.detected, 2, "both pull requests are inside 12 hours");
    assert_eq!(wide.queued, 1, "Bob's unchanged pull request is skipped");
    assert_eq!(wide.fetch.landed, 1);
    assert_eq!(fake.with(|w| w.by_id), 1, "one full fetch, for Alice's");
    let stored: String = dw
        .connection()
        .query_row(
            "SELECT updated_at FROM fact_pull_requests WHERE number = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(secs(&stored), secs(&changed));
}

#[test]
fn coverage_extends_only_when_it_reaches_the_detected_window() {
    let dw = warehouse(&[]);
    let conn = dw.connection();
    let lower = Utc::now() - Duration::hours(DEFAULT_DETECT_OVERLAP_HOURS);
    let end_of = |key: &str| sync_state::coverage_extent(conn, key).unwrap().unwrap().1;
    // Ends long before the window: a gap the backfill owns, left alone.
    sync_state::record_range(conn, "user:old", "2026-01-01", "2026-01-31", 1).unwrap();
    extend_coverage(conn, "user:old", lower).unwrap();
    assert_eq!(end_of("user:old"), "2026-01-31");
    // Ends a day before the window: extended through yesterday.
    let end = lower.date_naive() - Duration::days(1);
    let start = end - Duration::days(5);
    sync_state::record_range(conn, "user:new", &start.to_string(), &end.to_string(), 1).unwrap();
    extend_coverage(conn, "user:new", lower).unwrap();
    let today = time_dimension::today_as_of(conn, Utc::now()).unwrap();
    assert_eq!(end_of("user:new"), (today - Duration::days(1)).to_string());
    // No coverage at all: nothing recorded.
    extend_coverage(conn, "user:none", lower).unwrap();
    assert!(
        sync_state::coverage_extent(conn, "user:none")
            .unwrap()
            .is_none()
    );
}

#[test]
fn repository_listings_page_down_to_the_cursor_and_report_bad_answers() {
    let dw = warehouse(&[]);
    monitor_repository::add_repo(dw.connection(), "acme/app").unwrap();
    let prs: Vec<FakePr> = (1..=5)
        .map(|n| FakePr::new("acme/app", n, "zed", &ago(9000), &ago(10 * n)))
        .collect();
    let fake = Fake::new(prs);
    fake.with(|w| w.listing_page = 2);
    let with_repos = TickPolicy {
        repositories: true,
        ..eager()
    };
    let first = run_tick(&dw, &fake, &with_repos);
    assert_eq!(
        (first.outcome.clone(), first.fetch.landed),
        (TickOutcome::Detected, 5),
        "{first:?}"
    );
    assert_eq!(fake.with(|w| w.listings), 3, "three pages of two");
    // The next listing stops at the stored cursor minus the overlap.
    fake.with(|w| {
        w.listings = 0;
        w.prs[0].updated_at = ago(0);
    });
    let quiet = TickPolicy {
        repositories: true,
        force_reconcile: false,
        overlap: Duration::minutes(5),
        ..eager()
    };
    let next = run_tick(&dw, &fake, &quiet);
    assert_eq!(
        (next.fetch.landed, fake.with(|w| w.listings)),
        (1, 1),
        "{next:?}"
    );
    for (fault, why) in [
        ("missing", "repository missing"),
        ("bad", "missing id, number or updatedAt"),
        ("stuck", "no new cursor"),
    ] {
        fake.with(|w| {
            w.listing_fault = Some(fault);
            w.prs[0].updated_at = text(Utc::now() + Duration::seconds(w.listings as i64 + 1));
        });
        let report = run_tick(&dw, &fake, &quiet);
        assert!(
            matches!(&report.outcome, TickOutcome::Incomplete { gaps } if gaps[0].contains(why)),
            "{fault}: {report:?}"
        );
    }
}

#[test]
fn a_repository_another_sync_holds_is_deferred_not_failed() {
    let dw = warehouse(&["alice"]);
    monitor_repository::add_repo(dw.connection(), "acme/app").unwrap();
    let fake = Fake::new(two_people());
    fake.with(|w| {
        w.issue_heads.insert("acme/app".into(), ago(5));
    });
    let held = sync_state::SyncLease::acquire(dw.connection(), "repo:acme/app").unwrap();
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            repositories: true,
            ..eager()
        },
    );
    assert_eq!(report.outcome, TickOutcome::Detected, "{report:?}");
    assert_eq!((report.fetch.deferred, report.fetch.failed), (2, 0));
    assert_eq!(
        fake.with(|w| w.issue_walks),
        0,
        "the holder is walking its issues"
    );
    let attempts: i64 = count(&dw, "SELECT SUM(attempts) FROM collector_pr_queue");
    assert_eq!(attempts, 0, "a deferral is not an attempt");
    held.release().unwrap();
    let after = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            repositories: true,
            force_reconcile: false,
            ..eager()
        },
    );
    assert_eq!(after.fetch.landed, 2, "{after:?}");
}

#[test]
fn members_are_seeded_from_old_walks_and_removed_with_their_monitor() {
    let dw = warehouse(&["alice", "bob", "bad_login"]);
    let conn = dw.connection();
    let recent = ago(30);
    let old = text(Utc::now() - Duration::days(9));
    for (source, login, at) in [
        ("user", "alice", &recent),
        ("user_reviews", "alice", &recent),
        ("user", "bob", &old),
        ("user_reviews", "bob", &recent),
    ] {
        conn.execute(
            "INSERT INTO sync_metadata (source_type, source_id, last_sync_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![source, login, at],
        )
        .unwrap();
    }
    let fake = Fake::new(two_people());
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            backfill_points: 0,
            ..eager()
        },
    );
    let cursor = state::raw_state(conn).unwrap().detect_cursor.unwrap();
    assert!(Utc::now() - cursor < Duration::minutes(1), "{report:?}");
    let bob = user_freshness(conn, "bob").unwrap();
    assert!(bob.catch_up_owed, "an old author walk is owed a catch-up");
    assert!(!user_freshness(conn, "alice").unwrap().catch_up_owed);
    assert_eq!(
        report.unresolved_logins,
        vec!["bad_login".to_string()],
        "not a GitHub login"
    );
    // The first detection read back to the oldest recent walk, minus margins.
    let searched = fake.with(|w| w.searches[0].clone());
    let since = searched
        .split("updated:")
        .nth(1)
        .unwrap()
        .split("..")
        .next()
        .unwrap();
    assert!(
        Utc::now()
            - DateTime::parse_from_rfc3339(since)
                .unwrap()
                .with_timezone(&Utc)
            > Duration::hours(3),
        "{searched}"
    );

    monitor_repository::remove(conn, "bob").unwrap();
    run_tick(
        &dw,
        &fake,
        &TickPolicy {
            backfill_points: 0,
            ..eager()
        },
    );
    assert_eq!(
        count(
            &dw,
            "SELECT COUNT(*) FROM collector_members WHERE login = 'bob'"
        ),
        0
    );
}

#[test]
fn backfill_pauses_at_the_cap_skips_held_people_and_retries_gaps() {
    let dw = warehouse(&["alice"]);
    let from = Utc::now().date_naive() - Duration::days(40);
    let fake = Fake::new(two_people());
    let policy = TickPolicy {
        backfill_points: 1_000,
        history_from: Some(from),
        ..eager()
    };
    // Detect first, with no backfill.
    run_tick(
        &dw,
        &fake,
        &TickPolicy {
            backfill_points: 0,
            ..policy.clone()
        },
    );
    let up_to_date = TickPolicy {
        detect_every: Duration::hours(1),
        ..policy.clone()
    };

    let held = sync_state::SyncLease::acquire(dw.connection(), "user:alice").unwrap();
    let skipped = run_tick(&dw, &fake, &up_to_date);
    assert!(matches!(skipped.outcome, TickOutcome::UpToDate { .. }));
    assert!(
        skipped.backfill.windows_covered.is_empty() && skipped.backfill.remaining == 1,
        "{skipped:?}"
    );
    held.release().unwrap();

    fake.with(|w| {
        w.fail_search.push(Error::RateLimited {
            resource: "graphql".into(),
            wake_at: Utc::now() + Duration::minutes(30),
            reason: "cap".into(),
        })
    });
    let capped = run_tick(&dw, &fake, &up_to_date);
    assert!(
        capped
            .backfill
            .skipped
            .as_deref()
            .unwrap()
            .contains("paused at the budget cap"),
        "{capped:?}"
    );

    // A short answer leaves the month uncovered and is reported.
    fake.with(|w| w.lie_count = 1);
    let short = run_tick(&dw, &fake, &up_to_date);
    assert!(short.backfill.windows_covered.is_empty(), "{short:?}");
    assert!(
        short.notes.iter().any(|n| n.contains("but served")),
        "{short:?}"
    );
    fake.with(|w| w.lie_count = 0);
    let done = run_tick(&dw, &fake, &up_to_date);
    assert!(!done.backfill.windows_covered.is_empty(), "{done:?}");
}

#[test]
fn rate_limit_and_page_size_answers_in_light_and_full_queries() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    fake.with(|w| w.search_error = Some(json!({"message": "API rate limit exceeded"})));
    let limited = run_tick(&dw, &fake, &eager());
    assert!(
        matches!(limited.outcome, TickOutcome::CapPaused { .. }),
        "{limited:?}"
    );
    fake.with(|w| w.search_error = Some(json!({"type": "INTERNAL", "message": "odd search"})));
    let odd = run_tick(&dw, &fake, &eager());
    assert!(
        matches!(&odd.outcome, TickOutcome::Incomplete { gaps } if gaps[0].contains("odd search")),
        "{odd:?}"
    );

    fake.with(|w| {
        w.search_error = None;
        w.fail_nodes
            .push(Error::GitHubApi("resource_limits exceeded".into()));
    });
    let halved = run_tick(&dw, &fake, &eager());
    assert_eq!(
        halved.fetch.landed, 2,
        "smaller batches after a page-size answer: {halved:?}"
    );
    fake.with(|w| {
        w.prs[0].updated_at = ago(0);
        w.nodes_error = Some(json!({"message": "API rate limit exceeded"}));
    });
    let wide = run_tick(&dw, &fake, &eager());
    assert!(
        matches!(wide.outcome, TickOutcome::CapPaused { .. }),
        "{wide:?}"
    );
}

#[test]
fn imported_failures_whose_lookup_fails_count_an_attempt() {
    let dw = warehouse(&["alice"]);
    let conn = dw.connection();
    sync_state::start_job(conn, "repo:acme/broken").unwrap();
    sync_state::complete_job(
        conn,
        "repo:acme/broken",
        0,
        0,
        &[
            ("acme/broken#3".into(), "x".into()),
            ("acme/broken#0".into(), "x".into()),
        ],
    )
    .unwrap();
    let fake = Fake::new(two_people());
    fake.with(|w| {
        w.broken_repos.insert("acme/broken".into());
    });
    let policy = TickPolicy {
        max_rate_use: Some(0.9),
        diff_retention: Some(DiffRetention::Never),
        policy_source: Some(PolicySource::Standalone),
        ..eager()
    };
    let report = run_tick(&dw, &fake, &policy);
    assert_eq!(
        (report.fetch.failed, report.fetch.gone),
        (1, 1),
        "{report:?}"
    );
    let attempts: i64 = count(
        &dw,
        "SELECT attempts FROM collector_pr_queue WHERE pr_key = 'acme/broken#3'",
    );
    assert_eq!(attempts, 1);
}

#[test]
fn more_than_a_page_in_one_second_is_paged() {
    let dw = warehouse(&["alice"]);
    let at = ago(10);
    let prs: Vec<FakePr> = (1..=150)
        .map(|n| FakePr::new("acme/app", n, "alice", &ago(100), &at))
        .collect();
    let fake = Fake::new(prs);
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert_eq!(
        (report.outcome.clone(), report.fetch.landed),
        (TickOutcome::Detected, 150),
        "{report:?}"
    );
}

#[test]
fn bad_repositories_and_failed_issue_walks_are_gaps() {
    let dw = warehouse(&["alice"]);
    monitor_repository::add_repo(dw.connection(), "acme/app").unwrap();
    monitor_repository::add_repo(dw.connection(), "acme/bad repo").unwrap();
    let fake = Fake::new(two_people());
    fake.with(|w| {
        w.issue_heads.insert("acme/app".into(), ago(5));
        w.issue_fault = true;
    });
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            repositories: true,
            ..eager()
        },
    );
    let TickOutcome::Incomplete { gaps } = &report.outcome else {
        panic!("{report:?}");
    };
    assert!(
        gaps.iter().any(|g| g.contains("not a valid owner/name")),
        "{gaps:?}"
    );
    assert!(
        gaps.iter().any(|g| g.contains("issue walk broke")),
        "{gaps:?}"
    );
}

#[test]
fn a_queued_pull_request_github_no_longer_serves_is_dropped() {
    let dw = warehouse(&["alice"]);
    let fake = Fake::new(two_people());
    run_tick(&dw, &fake, &eager());
    dw.connection()
        .execute(
            "INSERT INTO collector_pr_queue (pr_key, node_id, queued_at) VALUES ('acme/app#77', 'PR_gone', ?1)",
            [ago(1)],
        )
        .unwrap();
    let report = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert_eq!(report.fetch.gone, 1, "{report:?}");
    assert_eq!(count(&dw, "SELECT COUNT(*) FROM collector_pr_queue"), 0);
}

#[test]
fn a_short_catch_up_stays_owed_and_a_paused_backfill_does_nothing() {
    let dw = warehouse(&["alice"]);
    let conn = dw.connection();
    sync_state::save_checkpoint(
        conn,
        "user_update_checkpoint",
        "reviewed-by:alice",
        &json!({"version": 1, "base": ago(60 * 24 * 10), "upper": 0, "pending": []}).to_string(),
    )
    .unwrap();
    sync_state::record_range(conn, "user:alice", "2026-01-01", "2026-01-31", 1).unwrap();
    let fake = Fake::new(two_people());
    run_tick(
        &dw,
        &fake,
        &TickPolicy {
            backfill_points: 0,
            ..eager()
        },
    );
    let later = TickPolicy {
        backfill_points: 1_000,
        detect_every: Duration::hours(1),
        ..eager()
    };
    fake.with(|w| w.lie_count = 1);
    let short = run_tick(&dw, &fake, &later);
    assert!(short.backfill.catch_ups_finished.is_empty(), "{short:?}");
    assert!(user_freshness(conn, "alice").unwrap().catch_up_owed);

    let pause = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let control = CollectionControl {
        pause: Some(pause),
        ..Default::default()
    };
    let mut client = fake.client();
    let paused = tick(conn, &mut client, &later, &control).unwrap();
    assert_eq!(
        paused.backfill.skipped.as_deref(),
        Some("stopped"),
        "{paused:?}"
    );
}

#[test]
fn running_checks_cost_a_rollup_read_not_a_full_fetch() {
    let dw = warehouse(&["alice"]);
    let prs = vec![
        FakePr::new("acme/app", 1, "alice", &ago(60), &ago(20)).reviewed("R1", "alice", &ago(20)),
    ];
    let id = prs[0].node_id();
    let fake = Fake::new(prs);
    fake.with(|w| {
        w.checks
            .insert(id.clone(), vec![("CR1".into(), "IN_PROGRESS".into())]);
    });
    run_tick(&dw, &fake, &eager());
    fake.with(|w| w.by_id = 0);
    let running = run_tick(
        &dw,
        &fake,
        &TickPolicy {
            force_reconcile: false,
            ..eager()
        },
    );
    assert_eq!(
        running.detected, 1,
        "author and reviewer sightings are one pull request"
    );
    assert_eq!(
        (running.queued, fake.with(|w| w.by_id)),
        (0, 0),
        "{running:?}"
    );
    assert_eq!(running.points.fetch, 0);
}

#[test]
fn a_stale_unfinished_run_costs_one_refetch_per_head() {
    let dw = warehouse(&["alice"]);
    let prs = vec![FakePr::new("acme/app", 1, "alice", &ago(60), &ago(20))];
    let id = prs[0].node_id();
    let fake = Fake::new(prs);
    // GitHub still lists a run as queued on the head, but the rollup has
    // finished (a superseded re-run).
    fake.with(|w| {
        w.checks
            .insert(id.clone(), vec![("OLD".into(), "QUEUED".into())]);
        w.rollup = Some("SUCCESS");
    });
    run_tick(&dw, &fake, &eager());
    let quiet = TickPolicy {
        force_reconcile: false,
        ..eager()
    };
    let once = run_tick(&dw, &fake, &quiet);
    assert_eq!(once.fetch.landed, 1, "fetched again once: {once:?}");
    fake.with(|w| w.by_id = 0);
    for _ in 0..2 {
        let later = run_tick(&dw, &fake, &quiet);
        assert_eq!(later.queued, 0, "{later:?}");
    }
    assert_eq!(
        fake.with(|w| w.by_id),
        0,
        "not fetched again for the same head"
    );
}
