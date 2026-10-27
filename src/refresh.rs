//! Targeted refresh of individual pull requests and issues.
//!
//! The repository and user walks are incremental: they stop at the stored
//! `updated_at` cursor, so a pull request that was ingested before nested
//! pagination existed — and has not been touched upstream since — is never
//! re-read, however short its stored comments or files are. Resetting the
//! cursor would replay the whole history. This module names a bounded set of
//! exact items instead, and re-fetches each one through the same parse,
//! hydrate, and upsert path the walks use.
//!
//! A refresh is deliberately side-effect-free outside the items themselves: it
//! takes the repository's ordinary sync lock (so it never races a walk of the
//! same repository), but it does not start or finish a sync job, enroll
//! anything for monitoring, move a cursor or checkpoint, or record coverage.

use std::collections::HashSet;

use rusqlite::Connection;
use serde::Serialize;

use crate::error::{Error, Result};
use crate::fetch::pull_requests::REST_FILE_LIMIT;

/// Most targets one refresh accepts. A refresh is for repairing named items,
/// not a substitute for a walk.
pub const MAX_REFRESH_TARGETS: usize = 64;

/// Size of the first slice the page queries request for each nested
/// connection. A stored count exactly at one of these is the signature of an
/// item ingested before nested pagination existed.
const FIRST_SLICE_COMMENTS: i64 = 50;
const FIRST_SLICE_REVIEWS: i64 = 50;
const FIRST_SLICE_LABELS: i64 = 20;
const FIRST_SLICE_ASSIGNEES: i64 = 10;

/// Which kind of item a target names. GitHub numbers pull requests and issues
/// from one sequence per repository, so the kind cannot be inferred from the
/// number and must be stated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    PullRequest,
    Issue,
}

impl TargetKind {
    fn noun(self) -> &'static str {
        match self {
            TargetKind::PullRequest => "pull request",
            TargetKind::Issue => "issue",
        }
    }
}

/// One exact item to refresh.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct RefreshTarget {
    pub kind: TargetKind,
    /// Canonical lowercase `owner/name`.
    pub repository: String,
    pub number: i64,
}

impl RefreshTarget {
    /// Parse `owner/name#123`, or a web URL `https://<host>/owner/name/pull/123`
    /// (`/issues/123` for an issue). A URL whose path contradicts `kind` is
    /// refused rather than reinterpreted.
    pub fn parse(kind: TargetKind, reference: &str) -> Result<Self> {
        let reference = reference.trim();
        let invalid = |why: &str| {
            Error::InvalidArgument(format!(
                "invalid {} reference '{reference}': {why} \
                 (expected owner/name#NUMBER or a pull/issues URL)",
                kind.noun()
            ))
        };
        let (repository, number) = if let Some(rest) = reference
            .strip_prefix("https://")
            .or_else(|| reference.strip_prefix("http://"))
        {
            let segments: Vec<&str> = rest.trim_end_matches('/').split('/').collect();
            let [_host, owner, name, section, number] = segments.as_slice() else {
                return Err(invalid("unrecognised URL shape"));
            };
            let url_kind = match *section {
                "pull" => TargetKind::PullRequest,
                "issues" => TargetKind::Issue,
                _ => return Err(invalid("URL is neither /pull/ nor /issues/")),
            };
            if url_kind != kind {
                return Err(invalid(&format!(
                    "the URL names a different kind ({})",
                    url_kind.noun()
                )));
            }
            (format!("{owner}/{name}"), *number)
        } else {
            let (repository, number) = reference
                .split_once('#')
                .ok_or_else(|| invalid("missing '#NUMBER'"))?;
            (repository.to_string(), number)
        };
        let (owner, name) = repository
            .split_once('/')
            .ok_or_else(|| invalid("repository must be owner/name"))?;
        let valid_segment = |segment: &str| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        };
        if !valid_segment(owner) || !valid_segment(name) {
            return Err(invalid("repository must be owner/name"));
        }
        let number: i64 = number
            .parse()
            .ok()
            .filter(|number| *number > 0)
            .ok_or_else(|| invalid("number must be a positive integer"))?;
        Ok(Self {
            kind,
            repository: repository.to_lowercase(),
            number,
        })
    }

    /// `owner/name#123`.
    pub fn reference(&self) -> String {
        format!("{}#{}", self.repository, self.number)
    }

    /// The key the warehouse stores this item under, for pull requests.
    fn pr_key(&self) -> String {
        self.reference()
    }
}

/// Deduplicate and bound a target list, preserving first-seen order.
///
/// Refuses an empty list, more than [`MAX_REFRESH_TARGETS`] distinct targets,
/// and one number requested as both a pull request and an issue (only one of
/// them can exist, so the request is contradictory).
pub fn validate_targets(targets: Vec<RefreshTarget>) -> Result<Vec<RefreshTarget>> {
    let mut seen = HashSet::new();
    let mut numbers = HashSet::new();
    let mut unique = Vec::new();
    for target in targets {
        if !seen.insert(target.clone()) {
            continue;
        }
        if !numbers.insert((target.repository.clone(), target.number)) {
            return Err(Error::InvalidArgument(format!(
                "{} was requested as both a pull request and an issue",
                target.reference()
            )));
        }
        unique.push(target);
    }
    if unique.is_empty() {
        return Err(Error::InvalidArgument(
            "name at least one pull request or issue to refresh".into(),
        ));
    }
    if unique.len() > MAX_REFRESH_TARGETS {
        return Err(Error::InvalidArgument(format!(
            "{} targets requested; a refresh accepts at most {MAX_REFRESH_TARGETS}",
            unique.len()
        )));
    }
    Ok(unique)
}

/// Options for a targeted refresh.
#[derive(Debug, Clone, Default)]
pub struct RefreshOptions {
    /// Skip the REST patch-text walk (fast mode).
    pub skip_diffs: bool,
}

/// Row counts the warehouse holds for one item.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct StoredCounts {
    /// False when the item has no row at all.
    pub present: bool,
    /// Conversation comments (PR) or comments (issue).
    pub comments: i64,
    pub reviews: i64,
    pub review_comments: i64,
    pub files: i64,
    pub files_with_patch: i64,
    /// Provider-reported changed-file count (PR metadata).
    pub changed_files: i64,
    pub check_runs: i64,
    pub labels: i64,
    pub assignees: i64,
}

/// What happened to one target.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetStatus {
    /// Fetched completely and stored.
    Refreshed,
    /// Nothing was written; the previously stored rows are untouched.
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct TargetOutcome {
    pub target: RefreshTarget,
    pub status: TargetStatus,
    /// Why the target failed.
    pub error: Option<String>,
    /// Coverage the provider would not serve (truncated file lists, capped
    /// patches, unretrieved patch text). A refreshed target with caveats is
    /// stored but explicitly NOT complete.
    pub caveats: Vec<String>,
    pub before: StoredCounts,
    pub after: StoredCounts,
}

/// Result of a targeted refresh.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RefreshReport {
    pub outcomes: Vec<TargetOutcome>,
}

impl RefreshReport {
    pub fn failed(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.status == TargetStatus::Failed)
            .count()
    }

    /// Refreshed targets that still carry a provider caveat.
    pub fn incomplete(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| {
                outcome.status == TargetStatus::Refreshed && !outcome.caveats.is_empty()
            })
            .count()
    }
}

fn count(conn: &Connection, sql: &str, key: &str) -> Result<i64> {
    Ok(conn.query_row(sql, [key], |row| row.get(0))?)
}

/// Read the stored row counts for one target. Read-only.
pub fn stored_counts(conn: &Connection, target: &RefreshTarget) -> Result<StoredCounts> {
    match target.kind {
        TargetKind::PullRequest => {
            let key = target.pr_key();
            let changed_files: Option<i64> = conn
                .query_row(
                    "SELECT changed_files FROM fact_pull_requests WHERE pr_key = ?1",
                    [&key],
                    |row| row.get(0),
                )
                .ok();
            Ok(StoredCounts {
                present: changed_files.is_some(),
                changed_files: changed_files.unwrap_or(0),
                comments: count(
                    conn,
                    "SELECT COUNT(*) FROM fact_issue_comments
                     WHERE parent_type = 'pull_request' AND parent_key = ?1",
                    &key,
                )?,
                reviews: count(
                    conn,
                    "SELECT COUNT(*) FROM fact_reviews WHERE pr_key = ?1",
                    &key,
                )?,
                review_comments: count(
                    conn,
                    "SELECT COUNT(*) FROM fact_review_comments WHERE pr_key = ?1",
                    &key,
                )?,
                files: count(
                    conn,
                    "SELECT COUNT(*) FROM fact_file_diffs WHERE pr_key = ?1",
                    &key,
                )?,
                files_with_patch: count(
                    conn,
                    "SELECT COUNT(*) FROM fact_file_diffs WHERE pr_key = ?1 AND patch IS NOT NULL",
                    &key,
                )?,
                check_runs: count(
                    conn,
                    "SELECT COUNT(*) FROM fact_check_runs WHERE pr_key = ?1",
                    &key,
                )?,
                ..Default::default()
            })
        }
        TargetKind::Issue => {
            let id: Option<String> = conn
                .query_row(
                    "SELECT id FROM issues WHERE repo_key = ?1 AND number = ?2",
                    rusqlite::params![target.repository, target.number],
                    |row| row.get(0),
                )
                .ok();
            let Some(id) = id else {
                return Ok(StoredCounts::default());
            };
            Ok(StoredCounts {
                present: true,
                comments: count(
                    conn,
                    "SELECT COUNT(*) FROM fact_issue_comments
                     WHERE parent_type = 'issue' AND parent_key = ?1",
                    &id,
                )?,
                labels: count(
                    conn,
                    "SELECT COUNT(*) FROM issue_labels WHERE issue_id = ?1",
                    &id,
                )?,
                assignees: count(
                    conn,
                    "SELECT COUNT(*) FROM issue_assignees WHERE issue_id = ?1",
                    &id,
                )?,
                ..Default::default()
            })
        }
    }
}

/// A stored item whose row counts suggest it was cut at a first slice.
#[derive(Debug, Clone, Serialize)]
pub struct RepairCandidate {
    pub target: RefreshTarget,
    /// Why it was selected, e.g. "50 comments (first-slice size)".
    pub reasons: Vec<String>,
    pub updated_at: Option<String>,
}

/// Select items worth a targeted refresh from stored data alone. Read-only;
/// makes no network calls.
///
/// Signals, strongest first:
/// * a pull request storing fewer file rows than the provider-reported
///   `changedFiles` (bounded by GitHub's 3,000-file ceiling, beyond which a
///   refresh cannot help);
/// * a pull request storing MORE file rows than `changedFiles`: files that left
///   the pull request (force-push, rebase) were never removed before file-set
///   pruning landed. A refresh with a provably complete listing prunes them;
///   a provider-capped listing keeps them and says so;
/// * a stored count exactly at a first-slice size (50 comments, 50 reviews;
///   issue labels 20, assignees 10). Items ingested before nested
///   pagination landed stopped exactly there. An item that genuinely has that
///   many is a harmless false positive: refreshing it costs one query.
///
/// Review-thread comments are not selected: they were cut per thread, and the
/// stored rows do not record which thread a comment belonged to.
pub fn repair_candidates(
    conn: &Connection,
    repository: Option<&str>,
    limit: usize,
) -> Result<Vec<RepairCandidate>> {
    let repository = repository.map(str::to_lowercase);
    let mut candidates = Vec::new();

    let mut statement = conn.prepare(
        "SELECT pr.pr_key, pr.repo_key, pr.number, pr.changed_files, pr.updated_at,
                (SELECT COUNT(*) FROM fact_file_diffs f WHERE f.pr_key = pr.pr_key),
                (SELECT COUNT(*) FROM fact_issue_comments c
                  WHERE c.parent_type = 'pull_request' AND c.parent_key = pr.pr_key),
                (SELECT COUNT(*) FROM fact_reviews r WHERE r.pr_key = pr.pr_key)
         FROM fact_pull_requests pr
         WHERE (?1 IS NULL OR pr.repo_key = ?1)
         ORDER BY pr.updated_at DESC",
    )?;
    let rows = statement.query_map([repository.as_deref()], |row| {
        Ok((
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    for row in rows {
        let (repo_key, number, changed_files, updated_at, files, comments, reviews) = row?;
        let mut reasons = Vec::new();
        let reachable_files = changed_files.min(REST_FILE_LIMIT as i64);
        if files < reachable_files {
            reasons.push(format!("{files} of {changed_files} changed files stored"));
        }
        if files > changed_files {
            reasons.push(format!(
                "{files} file rows stored for {changed_files} changed files (stale file rows)"
            ));
        }
        if comments == FIRST_SLICE_COMMENTS {
            reasons.push(format!("{comments} comments (first-slice size)"));
        }
        if reviews == FIRST_SLICE_REVIEWS {
            reasons.push(format!("{reviews} reviews (first-slice size)"));
        }
        if !reasons.is_empty() {
            candidates.push(RepairCandidate {
                target: RefreshTarget {
                    kind: TargetKind::PullRequest,
                    repository: repo_key,
                    number,
                },
                reasons,
                updated_at,
            });
        }
    }

    let mut statement = conn.prepare(
        "SELECT i.repo_key, i.number, i.updated_at,
                (SELECT COUNT(*) FROM fact_issue_comments c
                  WHERE c.parent_type = 'issue' AND c.parent_key = i.id),
                (SELECT COUNT(*) FROM issue_labels l WHERE l.issue_id = i.id),
                (SELECT COUNT(*) FROM issue_assignees a WHERE a.issue_id = i.id)
         FROM issues i
         WHERE (?1 IS NULL OR i.repo_key = ?1)
         ORDER BY i.updated_at DESC",
    )?;
    let rows = statement.query_map([repository.as_deref()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    for row in rows {
        let (repo_key, number, updated_at, comments, labels, assignees) = row?;
        let mut reasons = Vec::new();
        if comments == FIRST_SLICE_COMMENTS {
            reasons.push(format!("{comments} comments (first-slice size)"));
        }
        if labels == FIRST_SLICE_LABELS {
            reasons.push(format!("{labels} labels (first-slice size)"));
        }
        if assignees == FIRST_SLICE_ASSIGNEES {
            reasons.push(format!("{assignees} assignees (first-slice size)"));
        }
        if !reasons.is_empty() {
            candidates.push(RepairCandidate {
                target: RefreshTarget {
                    kind: TargetKind::Issue,
                    repository: repo_key,
                    number,
                },
                reasons,
                updated_at,
            });
        }
    }

    // Provider-metadata shortfalls first (they are certain), then newest.
    candidates.sort_by(|left, right| {
        let certain = |candidate: &RepairCandidate| {
            candidate.reasons.iter().any(|reason| {
                reason.contains("changed files stored") || reason.contains("stale file rows")
            })
        };
        certain(right)
            .cmp(&certain(left))
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });
    candidates.truncate(limit);
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pr(reference: &str) -> RefreshTarget {
        RefreshTarget::parse(TargetKind::PullRequest, reference).unwrap()
    }

    #[test]
    fn parses_canonical_references_and_urls() {
        let target = pr("AcmeCo/Widget#1467");
        assert_eq!(target.repository, "acmeco/widget");
        assert_eq!(target.number, 1467);
        let from_url = pr("https://github.com/acmeco/Widget/pull/1467/");
        assert_eq!(from_url, target);
        let issue =
            RefreshTarget::parse(TargetKind::Issue, "https://github.com/o/r/issues/5").unwrap();
        assert_eq!(issue.kind, TargetKind::Issue);
    }

    #[test]
    fn refuses_malformed_or_contradictory_references() {
        for bad in [
            "",
            "owner/name",
            "owner#1",
            "owner/name#0",
            "owner/name#-3",
            "owner/name#abc",
            "/name#1",
            "own er/name#1",
            "a/b/c#1",
            "https://github.com/o/r/commit/1",
            "https://github.com/o/r/pull",
        ] {
            assert!(
                RefreshTarget::parse(TargetKind::PullRequest, bad).is_err(),
                "{bad}"
            );
        }
        let error =
            RefreshTarget::parse(TargetKind::PullRequest, "https://github.com/o/r/issues/5")
                .unwrap_err();
        assert!(error.to_string().contains("different kind"), "{error}");
    }

    #[test]
    fn validation_dedupes_bounds_and_rejects_conflicts() {
        let deduped = validate_targets(vec![pr("o/r#1"), pr("O/R#1"), pr("o/r#2")]).unwrap();
        assert_eq!(deduped.len(), 2);
        assert!(validate_targets(vec![]).is_err());
        let many = (1..=65).map(|n| pr(&format!("o/r#{n}"))).collect();
        assert!(validate_targets(many).is_err());
        let exactly = (1..=64).map(|n| pr(&format!("o/r#{n}"))).collect();
        assert_eq!(validate_targets(exactly).unwrap().len(), 64);
        let both = vec![
            pr("o/r#3"),
            RefreshTarget::parse(TargetKind::Issue, "o/r#3").unwrap(),
        ];
        assert!(validate_targets(both).is_err());
    }
}
