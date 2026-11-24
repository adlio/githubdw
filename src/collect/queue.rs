//! Changed pull requests: decide what needs a full fetch, queue it, and land
//! it by node id through the walks' own hydrate and upsert path.
//!
//! The queue is what lets the shared detection cursor move past a pull
//! request that will not land (a "checkRuns contains a null node" answer, a
//! repository whose sync lock another process holds): the item waits here,
//! is retried every tick, and after [`PARK_AFTER_ATTEMPTS`] failures is parked
//! and retried once a day instead.

use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;

use super::detect::{self, Found};
use crate::error::{Error, Result};
use crate::fetch::is_inaccessible;
use crate::fetch::pull_requests;
use crate::storage::{repository, sync_state_repository as sync_state, time_dimension};
use crate::sync::{SyncOptions, Syncer};

/// Failed attempts after which a queued pull request is parked.
pub const PARK_AFTER_ATTEMPTS: i64 = 5;

/// Node ids per `nodes(ids:)` query: the same 25 the page walks use, halved
/// when GitHub reports a response too large to serve.
const FETCH_BATCH: usize = 25;

/// Aliased lookups per query. Each alias is about two GitHub requests, so 50
/// keeps a lookup query at about 1 point.
pub(crate) const ALIAS_BATCH: usize = 50;

/// A parked pull request, reported once a day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParkedPullRequest {
    pub pr_key: String,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub parked_at: String,
}

/// What one pass over the queue did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct FetchSummary {
    /// Pull requests fetched in full and stored.
    pub landed: i64,
    /// Fetches that failed; each stays queued.
    pub failed: i64,
    /// Left queued without counting an attempt (another sync held the
    /// repository's lock).
    pub deferred: i64,
    /// GitHub no longer shows them (deleted, or now behind SAML); dropped.
    pub gone: i64,
    /// Parked this pass after their last allowed attempt.
    pub newly_parked: i64,
}

fn now_text() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn instant(text: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// Whether the stored copy of `found` is complete and current, so fetching
/// it again would return nothing new. False (fetch it) when:
///
/// * nothing is stored, or the stored `updated_at` is older;
/// * only a light row is stored (`pr_detail_state` without a detail fetch);
/// * it is queued after a failure;
/// * its stored head commit still has a check run that is not completed:
///   check runs finishing do not move a pull request's `updatedAt`.
pub(crate) fn stored_complete(conn: &Connection, found: &Found) -> Result<bool> {
    let stored: Option<(Option<String>, i64, Option<String>, i64)> = conn
        .query_row(
            "SELECT p.updated_at,
                    s.pr_key IS NOT NULL,
                    s.detail_fetched_at,
                    EXISTS(SELECT 1 FROM collector_pr_queue q WHERE q.pr_key = p.pr_key)
             FROM fact_pull_requests p
             LEFT JOIN pr_detail_state s ON s.pr_key = p.pr_key
             WHERE p.pr_key = ?1",
            [&found.pr_key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((updated, has_state, detail_at, queued)) = stored else {
        return Ok(false);
    };
    let current = match (
        updated.as_deref().and_then(instant),
        instant(&found.updated_at),
    ) {
        (Some(stored), Some(seen)) => stored >= seen,
        _ => false,
    };
    // A row with no stage record was written by a full walk before stage
    // tracking existed: full walks land a pull request whole or not at all.
    let detailed = has_state == 0 || detail_at.is_some();
    if !current || !detailed || queued != 0 {
        return Ok(false);
    }
    Ok(!head_checks_incomplete(conn, &found.pr_key)?)
}

/// True when the stored head commit has a check run that is not completed.
pub(crate) fn head_checks_incomplete(conn: &Connection, pr_key: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM fact_check_runs c
             LEFT JOIN pr_detail_state s ON s.pr_key = c.pr_key
             WHERE c.pr_key = ?1 AND c.status <> 'COMPLETED'
               AND (s.head_oid IS NULL OR c.head_sha = s.head_oid))",
        [pr_key],
        |r| r.get::<_, bool>(0),
    )?)
}

/// Queue `found` for a full fetch. A pull request already queued keeps its
/// attempt count unless GitHub now reports a newer `updatedAt`, which may
/// mean the cause of its failures is gone: then it starts over and is
/// unparked.
pub(crate) fn enqueue(conn: &Connection, found: &Found) -> Result<()> {
    conn.execute(
        "INSERT INTO collector_pr_queue (pr_key, node_id, seen_updated_at, queued_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (pr_key) DO UPDATE SET
             node_id = COALESCE(excluded.node_id, collector_pr_queue.node_id),
             attempts = CASE WHEN excluded.seen_updated_at > COALESCE(collector_pr_queue.seen_updated_at, '')
                             THEN 0 ELSE collector_pr_queue.attempts END,
             parked_at = CASE WHEN excluded.seen_updated_at > COALESCE(collector_pr_queue.seen_updated_at, '')
                              THEN NULL ELSE collector_pr_queue.parked_at END,
             seen_updated_at = MAX(COALESCE(collector_pr_queue.seen_updated_at, ''), excluded.seen_updated_at)",
        params![found.pr_key, found.node_id, found.updated_at, now_text()],
    )?;
    Ok(())
}

/// Queue every item of `found` whose stored copy is not complete. Returns
/// how many were queued.
pub(crate) fn enqueue_changed(conn: &Connection, found: &[Found]) -> Result<i64> {
    let mut queued = 0;
    for item in found {
        if !stored_complete(conn, item)? {
            enqueue(conn, item)?;
            queued += 1;
        }
    }
    Ok(queued)
}

/// Seed the queue from failures older walks recorded in `sync_jobs`, so a
/// pull request a 0.5 walk left behind below its cursor is still retried.
/// Runs once per warehouse.
pub(crate) fn import_job_failures(conn: &Connection) -> Result<i64> {
    let mut statement =
        conn.prepare("SELECT failed_items FROM sync_jobs WHERE failed_items IS NOT NULL")?;
    let rows: Vec<String> = statement
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut imported = 0;
    for text in rows {
        let Ok(items) = serde_json::from_str::<Vec<(String, String)>>(&text) else {
            continue;
        };
        for (key, error) in items {
            let key = key.to_lowercase();
            let Some((repo, number)) = key.rsplit_once('#') else {
                continue;
            };
            if !detect::valid_repository(repo) || number.parse::<i64>().is_err() {
                continue;
            }
            imported += conn.execute(
                "INSERT INTO collector_pr_queue (pr_key, queued_at, last_error)
                 VALUES (?1, ?2, ?3) ON CONFLICT (pr_key) DO NOTHING",
                params![key, now_text(), error],
            )? as i64;
        }
    }
    Ok(imported)
}

struct Queued {
    pr_key: String,
    node_id: Option<String>,
}

/// Queued pull requests due for an attempt now: every unparked one, and
/// parked ones not tried for a day.
fn due(conn: &Connection) -> Result<Vec<Queued>> {
    let day_ago =
        (Utc::now() - Duration::hours(24)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut statement = conn.prepare(
        "SELECT pr_key, node_id FROM collector_pr_queue
         WHERE parked_at IS NULL OR COALESCE(last_attempt_at, '') < ?1
         ORDER BY attempts, queued_at, pr_key",
    )?;
    let rows = statement.query_map([day_ago], |r| {
        Ok(Queued {
            pr_key: r.get(0)?,
            node_id: r.get(1)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn record_failure(
    conn: &Connection,
    pr_key: &str,
    error: &str,
    summary: &mut FetchSummary,
) -> Result<()> {
    summary.failed += 1;
    let at = now_text();
    let parked = conn.execute(
        "UPDATE collector_pr_queue SET attempts = attempts + 1, last_attempt_at = ?2,
             last_error = ?3,
             parked_at = CASE WHEN parked_at IS NULL AND attempts + 1 >= ?4 THEN ?2 ELSE parked_at END
         WHERE pr_key = ?1",
        params![pr_key, at, error, PARK_AFTER_ATTEMPTS],
    )?;
    if parked == 1 {
        let newly: bool = conn.query_row(
            "SELECT COALESCE(parked_at = ?2, 0) FROM collector_pr_queue WHERE pr_key = ?1",
            params![pr_key, at],
            |r| r.get(0),
        )?;
        if newly {
            summary.newly_parked += 1;
        }
    }
    Ok(())
}

fn dequeue(conn: &Connection, pr_key: &str) -> Result<()> {
    conn.execute("DELETE FROM collector_pr_queue WHERE pr_key = ?1", [pr_key])?;
    Ok(())
}

/// Parked pull requests, oldest first.
pub fn parked(conn: &Connection) -> Result<Vec<ParkedPullRequest>> {
    let mut statement = conn.prepare(
        "SELECT pr_key, attempts, last_error, parked_at FROM collector_pr_queue
         WHERE parked_at IS NOT NULL ORDER BY parked_at, pr_key",
    )?;
    let rows = statement.query_map([], |r| {
        Ok(ParkedPullRequest {
            pr_key: r.get(0)?,
            attempts: r.get(1)?,
            last_error: r.get(2)?,
            parked_at: r.get(3)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// `repository(owner:"o", name:"n") { pullRequest(number: N) { <inner> } }`,
/// or `None` when the key cannot be inlined safely.
pub(crate) fn pull_request_selection(pr_key: &str, inner: &str) -> Option<String> {
    let (repo, number) = pr_key.rsplit_once('#')?;
    let number: i64 = number.parse().ok().filter(|n| *n > 0)?;
    if !detect::valid_repository(repo) {
        return None;
    }
    let (owner, name) = repo.split_once('/')?;
    Some(format!(
        "repository(owner: \"{owner}\", name: \"{name}\") {{ pullRequest(number: {number}) {{ {inner} }} }}"
    ))
}

/// Find node ids for queued pull requests that have none (imported from old
/// job records). Ones GitHub no longer shows are dropped.
fn resolve_node_ids(
    syncer: &mut Syncer<'_>,
    items: &mut [Queued],
    summary: &mut FetchSummary,
    after_query: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let conn = syncer.connection();
    let missing: Vec<usize> = (0..items.len())
        .filter(|i| items[*i].node_id.is_none())
        .collect();
    for chunk in missing.chunks(ALIAS_BATCH) {
        let mut selections = Vec::new();
        let mut owners = Vec::new();
        for index in chunk {
            match pull_request_selection(&items[*index].pr_key, "id") {
                Some(selection) => {
                    selections.push(selection);
                    owners.push(*index);
                }
                None => {
                    dequeue(conn, &items[*index].pr_key)?;
                    summary.gone += 1;
                }
            }
        }
        if selections.is_empty() {
            continue;
        }
        let answers = detect::aliased(syncer.client_mut(), &selections)?;
        after_query()?;
        for (index, (value, error)) in owners.into_iter().zip(answers) {
            let id = value
                .as_ref()
                .and_then(|v| v.get("pullRequest"))
                .and_then(|p| p.get("id"))
                .and_then(Value::as_str);
            match (id, error) {
                (Some(id), _) => {
                    conn.execute(
                        "UPDATE collector_pr_queue SET node_id = ?2 WHERE pr_key = ?1",
                        params![items[index].pr_key, id],
                    )?;
                    items[index].node_id = Some(id.to_string());
                }
                (None, Some(error)) if !is_inaccessible(&error) => {
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("lookup failed")
                        .to_string();
                    record_failure(conn, &items[index].pr_key, &message, summary)?;
                }
                (None, _) => {
                    dequeue(conn, &items[index].pr_key)?;
                    summary.gone += 1;
                }
            }
        }
    }
    Ok(())
}

fn nodes_query(ids: &[&str]) -> String {
    let list = ids
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "query {{\n  rateLimit {{ limit cost remaining resetAt }}\n  nodes(ids: [{list}]) {{\n    ... on PullRequest {{\n      {}\n      repository {{ {} }}\n    }}\n  }}\n}}",
        pull_requests::PULL_REQUEST_NODE_SELECTION,
        pull_requests::REPOSITORY_HEADER_SELECTION
    )
}

/// The `nodes` index a GraphQL error points at (`["nodes", i, ...]`).
fn nodes_index(error: &Value) -> Option<usize> {
    let path = error.get("path")?.as_array()?;
    if path.first()?.as_str()? != "nodes" {
        return None;
    }
    path.get(1)?.as_u64().map(|i| i as usize)
}

/// Fetch every due queued pull request (or only those in `only`) in full
/// and land it. Run-fatal
/// errors (a spent budget, cancellation, a failing database) stop the pass
/// with the rest still queued; anything else is one item's failure.
pub(crate) fn drain(
    syncer: &mut Syncer<'_>,
    options: &SyncOptions,
    after_query: &mut dyn FnMut() -> Result<()>,
    stop: &dyn Fn() -> bool,
    only: Option<&std::collections::HashSet<String>>,
) -> Result<FetchSummary> {
    let mut summary = FetchSummary::default();
    let conn = syncer.connection();
    let mut items = due(conn)?;
    if let Some(only) = only {
        items.retain(|q| only.contains(&q.pr_key));
    }
    if items.is_empty() {
        return Ok(summary);
    }
    resolve_node_ids(syncer, &mut items, &mut summary, after_query)?;
    let mut ready: Vec<(String, String)> = items
        .into_iter()
        .filter_map(|q| {
            let id = q.node_id?;
            detect::valid_node_id(&id).then_some((q.pr_key, id))
        })
        .collect();
    let mut batch = FETCH_BATCH;
    while !ready.is_empty() {
        if stop() {
            break;
        }
        let take = batch.min(ready.len());
        let ids: Vec<&str> = ready[..take].iter().map(|(_, id)| id.as_str()).collect();
        let answer = syncer
            .client_mut()
            .graphql_with_errors(&nodes_query(&ids), &[]);
        after_query()?;
        let (data, errors) = match answer {
            Ok(answer) => answer,
            Err(error) if error.is_run_fatal() => return Err(error),
            Err(error) if batch > 1 && error.is_page_size_symptom() => {
                batch = (batch / 2).max(1);
                continue;
            }
            Err(error) => {
                for (pr_key, _) in ready.drain(..take) {
                    record_failure(conn, &pr_key, &error.to_string(), &mut summary)?;
                }
                continue;
            }
        };
        let chunk: Vec<(String, String)> = ready.drain(..take).collect();
        let nodes = data
            .get("nodes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut node_errors: Vec<Option<Value>> = vec![None; chunk.len()];
        for error in errors {
            match nodes_index(&error) {
                Some(index) if index < chunk.len() => {
                    node_errors[index].get_or_insert(error);
                }
                _ => {
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown GraphQL error")
                        .to_string();
                    if message.to_lowercase().contains("rate limit") {
                        return Err(Error::RateLimited {
                            resource: "graphql".into(),
                            wake_at: Utc::now() + Duration::minutes(1),
                            reason: message,
                        });
                    }
                }
            }
        }
        for (index, (pr_key, _)) in chunk.iter().enumerate() {
            let node = nodes.get(index).filter(|n| !n.is_null());
            match (node, node_errors[index].take()) {
                (None, Some(error)) if is_inaccessible(&error) => {
                    dequeue(conn, pr_key)?;
                    summary.gone += 1;
                }
                (None, Some(error)) => {
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown GraphQL error")
                        .to_string();
                    record_failure(conn, pr_key, &message, &mut summary)?;
                }
                (None, None) => {
                    dequeue(conn, pr_key)?;
                    summary.gone += 1;
                }
                (Some(node), _) => {
                    land_one(syncer, pr_key, node, options, &mut summary)?;
                    after_query()?;
                }
            }
        }
    }
    Ok(summary)
}

/// Parse, hydrate and store one served node, holding its repository's sync
/// lock so it never races a walk of the same repository.
fn land_one(
    syncer: &mut Syncer<'_>,
    queued_key: &str,
    node: &Value,
    options: &SyncOptions,
    summary: &mut FetchSummary,
) -> Result<()> {
    let conn = syncer.connection();
    let parsed = node
        .get("repository")
        .filter(|r| !r.is_null())
        .ok_or_else(|| Error::GitHubApi("node has no repository".into()))
        .and_then(pull_requests::parse_repository_metadata)
        .and_then(|metadata| Ok((metadata, pull_requests::parse_pull_request_node(node)?)));
    let (metadata, pull_request) = match parsed {
        Ok(parsed) => parsed,
        Err(error) => return record_failure(conn, queued_key, &error.to_string(), summary),
    };
    let repo_key = metadata.name_with_owner.to_lowercase();
    let pr_key = format!("{repo_key}#{}", pull_request.number);
    let lease = match sync_state::SyncLease::acquire(conn, &format!("repo:{repo_key}")) {
        Ok(lease) => lease,
        Err(Error::SyncInProgress(_)) => {
            summary.deferred += 1;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let timezone = time_dimension::configured_timezone(conn)?;
    let core_hours = time_dimension::configured_core_hours(conn);
    let bot_suffix = repository::bot_login_suffix(conn);
    let landed = repository::upsert_repository(
        conn,
        &metadata.name_with_owner,
        metadata.primary_language.as_deref(),
        metadata.is_fork,
        metadata.is_private,
        metadata.default_branch.as_deref(),
        metadata.created_at.as_deref(),
    )
    .and_then(|_| {
        syncer.upsert_one_pull_request(
            &repo_key,
            &pr_key,
            &pull_request,
            timezone,
            core_hours,
            &bot_suffix,
            options,
        )
    });
    lease.release()?;
    match landed {
        Ok(_) => {
            dequeue(conn, queued_key)?;
            if pr_key != queued_key {
                // The repository was renamed or transferred since it was queued.
                dequeue(conn, &pr_key)?;
            }
            summary.landed += 1;
            Ok(())
        }
        Err(error) if error.is_run_fatal() => Err(error),
        Err(error) => record_failure(conn, queued_key, &error.to_string(), summary),
    }
}

/// Queue stored pull requests updated since `since` whose head commit still
/// has incomplete check runs, when GitHub says those checks have since
/// finished (or the head moved). Check runs finishing do not move a pull
/// request's `updatedAt`, so detection alone would never fetch them again.
/// At most `limit` candidates, newest first. Returns how many were queued.
pub(crate) fn probe_unfinished_checks(
    syncer: &mut Syncer<'_>,
    since: DateTime<Utc>,
    limit: usize,
    after_query: &mut dyn FnMut() -> Result<()>,
) -> Result<i64> {
    let conn = syncer.connection();
    let mut statement = conn.prepare(
        "SELECT p.pr_key, s.head_oid FROM fact_pull_requests p
         JOIN pr_detail_state s ON s.pr_key = p.pr_key
         WHERE p.updated_at >= ?1 AND s.head_oid IS NOT NULL
           AND EXISTS (SELECT 1 FROM fact_check_runs c
                       WHERE c.pr_key = p.pr_key AND c.head_sha = s.head_oid
                         AND c.status <> 'COMPLETED')
           AND NOT EXISTS (SELECT 1 FROM collector_pr_queue q WHERE q.pr_key = p.pr_key)
         ORDER BY p.updated_at DESC LIMIT ?2",
    )?;
    let candidates: Vec<(String, String)> = statement
        .query_map(
            params![
                since.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                limit as i64
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let mut queued = 0;
    const INNER: &str = "id updatedAt headRefOid commits(last: 1) { nodes { commit { oid statusCheckRollup { state } } } }";
    for chunk in candidates.chunks(ALIAS_BATCH) {
        let pairs: Vec<(&(String, String), String)> = chunk
            .iter()
            .filter_map(|c| pull_request_selection(&c.0, INNER).map(|s| (c, s)))
            .collect();
        if pairs.is_empty() {
            continue;
        }
        let selections: Vec<String> = pairs.iter().map(|(_, s)| s.clone()).collect();
        let answers = detect::aliased(syncer.client_mut(), &selections)?;
        after_query()?;
        for (((pr_key, stored_head), _), (value, _)) in pairs.into_iter().zip(answers) {
            let Some(pr) = value.as_ref().and_then(|v| v.get("pullRequest")) else {
                continue;
            };
            let head = pr.get("headRefOid").and_then(Value::as_str);
            let rollup = pr
                .pointer("/commits/nodes/0/commit/statusCheckRollup/state")
                .and_then(Value::as_str);
            let still_running = matches!(rollup, Some("PENDING" | "EXPECTED"));
            if head == Some(stored_head.as_str()) && still_running {
                continue;
            }
            let (Some(id), Some(updated)) = (
                pr.get("id").and_then(Value::as_str),
                pr.get("updatedAt").and_then(Value::as_str),
            ) else {
                continue;
            };
            enqueue(
                conn,
                &Found {
                    pr_key: pr_key.clone(),
                    node_id: id.to_string(),
                    updated_at: updated.to_string(),
                },
            )?;
            queued += 1;
        }
    }
    Ok(queued)
}
