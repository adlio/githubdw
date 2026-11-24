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
                    &pr.updated_at
                });
                *a <= at && at <= *b
            })
            .collect();
        hits.sort_by_key(|pr| (secs(&pr.updated_at), pr.number));
        let total = hits.len();
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
            "issueCount": total,
            "pageInfo": {"hasNextPage": has_next, "endCursor": has_next.then(|| end.to_string())},
            "nodes": nodes}}});
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
                let state = if runs.iter().any(|(_, s)| s != "COMPLETED") {
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
                json!({"pullRequests": {"nodes": head}, "issues": {"nodes": []}}),
            );
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
        let nodes: Vec<Value> = ids
            .iter()
            .map(|id| {
                self.prs
                    .iter()
                    .find(|pr| pr.node_id() == *id)
                    .map(|pr| self.node(pr))
                    .unwrap_or(Value::Null)
            })
            .collect();
        json!({"data": {"rateLimit": rate(17), "nodes": nodes}})
    }

    fn listing(&mut self, owner: &str, name: &str) -> Value {
        self.listings += 1;
        let repo = format!("{owner}/{name}");
        let mut mine: Vec<&FakePr> = self
            .prs
            .iter()
            .filter(|p| p.repo.eq_ignore_ascii_case(&repo))
            .collect();
        mine.sort_by_key(|p| std::cmp::Reverse(secs(&p.updated_at)));
        let nodes: Vec<Value> = mine
            .iter()
            .map(|p| json!({"id": p.node_id(), "number": p.number, "updatedAt": p.updated_at}))
            .collect();
        json!({"data": {"rateLimit": rate(1), "repository": {"nameWithOwner": repo,
            "pullRequests": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": nodes}}}})
    }
}

impl GhTransport for Fake {
    fn run(&self, arguments: &[String]) -> crate::Result<String> {
        let mut world = self.0.lock().unwrap();
        if !world.fail_next.is_empty() {
            return Err(world.fail_next.remove(0));
        }
        let path = arguments.get(1).cloned().unwrap_or_default();
        if path == "rate_limit" {
            return Ok(
                json!({"resources": {"graphql": {"limit": 5000, "used": 100, "remaining": 4900,
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
            let q = variable(arguments, "q").unwrap().to_string();
            let after = variable(arguments, "cursor")
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            world.search(&q, after)
        } else if query.contains("nodes(ids:") {
            world.by_ids(&query)
        } else if query.contains("a0:") {
            world.aliased(&query)
        } else if query.contains("pullRequests(first: 100, after: $cursor") {
            let owner = variable(arguments, "owner").unwrap().to_string();
            let name = variable(arguments, "name").unwrap().to_string();
            world.listing(&owner, &name)
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
}
