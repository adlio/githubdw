//! Git-backed recovery of a pull request's complete file set.
//!
//! GitHub stops enumerating a pull request's files at 3,000 (REST and
//! GraphQL alike), refuses diffs over 300 files, and has been observed
//! reporting `changed_files = 0` for a 3,770-file pull request. For those pull
//! requests the API cannot prove what the file set is, so this module asks git:
//!
//! 1. REST `pulls/N` for the base and head commits (and fresh totals);
//! 2. REST `compare/{base}...{head}` for GitHub's own merge base;
//! 3. a shallow fetch of exactly those two commits into a bare cache;
//! 4. `git diff -M -l1 -z` (raw, numstat, patch) between them.
//!
//! For these large diffs GitHub's file list matches git with the rename limit
//! exceeded (`-M -l1`): exhaustive inexact rename detection is skipped, and
//! only exact (100%) and unique same-basename renames are paired (basename
//! matching needs git >= 2.33). That was proven record-for-record on seven
//! capped pull requests, but GitHub's own limit is unknown, so it is never
//! assumed: the result is returned only when it reconciles EXACTLY with the
//! provider. Every served record must equal git's record (status, rename
//! source, and line counts, the one exception being a record the API served as
//! 0/0 without a patch), git's record count must equal `changed_files`, and
//! git's summed line counts must equal the pull request's
//! `additions`/`deletions`. Any other outcome is an error; the caller keeps its
//! stored rows and reports the failure.
//!
//! The fetched repository is untrusted: hooks, external diff drivers,
//! textconv, attributes from the fetched tree, global/system config, and
//! every transport other than HTTPS are disabled, every subprocess is bounded
//! in time and output, and paths travel NUL-separated.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::GhClient;
use super::pull_requests::{PatchListing, PullRequestData};
use crate::error::{Error, Result};

/// Git's well-known empty tree; pointing `attr.tree` at it stops git reading
/// `.gitattributes` from the fetched (untrusted) tree.
const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
/// Stderr kept from a failed git command (the rest is drained and dropped).
const STDERR_CAPTURE_BYTES: u64 = 16 * 1024;

/// Where and how git-backed recovery runs.
#[derive(Debug, Clone)]
pub struct GitRecoveryConfig {
    /// Directory holding one bare cache repository per GitHub repository.
    pub cache_root: PathBuf,
    /// Remote URL prefix; `<remote_base><owner>/<name>.git` is fetched.
    pub remote_base: String,
    /// Wall-clock bound on one fetch.
    pub fetch_timeout: Duration,
    /// Wall-clock bound on one local git command (diff, cat-file, init).
    pub local_timeout: Duration,
    /// Maximum stdout accepted from one git command; larger output fails.
    pub max_output_bytes: usize,
    /// Patches longer than this are stored as NULL (GitHub omits large
    /// patches too) and counted in [`RecoveredFileSet::oversized_patches`].
    pub max_patch_bytes: usize,
}

impl GitRecoveryConfig {
    /// HTTPS against github.com (or `GH_HOST`), with production bounds.
    pub fn github(cache_root: PathBuf) -> Self {
        let host = std::env::var("GH_HOST")
            .ok()
            .filter(|host| is_safe_host(host))
            .unwrap_or_else(|| "github.com".to_string());
        Self {
            cache_root,
            remote_base: format!("https://{host}/"),
            fetch_timeout: Duration::from_secs(900),
            local_timeout: Duration::from_secs(600),
            max_output_bytes: 1024 * 1024 * 1024,
            max_patch_bytes: 16 * 1024 * 1024,
        }
    }

    fn allows_file_protocol(&self) -> bool {
        self.remote_base.starts_with("file://")
    }
}

fn is_safe_host(host: &str) -> bool {
    !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
}

/// One file record of the complete set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredFile {
    pub path: String,
    pub previous_path: Option<String>,
    /// GraphQL `PatchStatus` spelling (ADDED, DELETED, MODIFIED, RENAMED, CHANGED, ...).
    pub change_type: String,
    pub additions: i64,
    pub deletions: i64,
    pub patch: Option<String>,
    /// True when the record came from git rather than the API.
    pub from_git: bool,
}

/// A provably complete file set for one pull request.
#[derive(Debug, Clone)]
pub struct RecoveredFileSet {
    pub files: Vec<RecoveredFile>,
    /// Authoritative record paths. Rename sources are metadata, not additional
    /// current records; keeping them would preserve obsolete cached rows.
    pub keep: HashSet<String>,
    pub changed_files: i64,
    pub additions: i64,
    pub deletions: i64,
    pub base_sha: String,
    pub head_sha: String,
    pub merge_base_sha: String,
    /// Records filled from git beyond what the API served.
    pub git_records: usize,
    /// Patches dropped for exceeding [`GitRecoveryConfig::max_patch_bytes`].
    pub oversized_patches: usize,
}

/// One file record git reports between the merge base and the head, with
/// GitHub's large-diff rename detection (`-M -l1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitFile {
    pub path: String,
    /// Rename source when `status` is `R`.
    pub previous_path: Option<String>,
    /// Raw status letter: A, D, M, T, or R.
    pub status: char,
    /// `None` for a binary file (numstat `-`).
    pub lines: Option<(i64, i64)>,
    pub patch: Option<String>,
}

/// One record the API served (GraphQL numbers joined with REST rename/patch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedFile {
    pub path: String,
    pub previous_path: Option<String>,
    pub change_type: String,
    /// `None` when only the REST listing served the path.
    pub lines: Option<(i64, i64)>,
    pub patch: Option<String>,
}

/// Provider totals for the pull request, already reconciled across reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderTotals {
    pub changed_files: i64,
    pub additions: i64,
    pub deletions: i64,
}

/// True when the API's file listing cannot be trusted as the complete set, so
/// git-backed recovery should run.
pub fn needs_recovery(pull_request: &PullRequestData, listing: Option<&PatchListing>) -> bool {
    let served = pull_request.files.len();
    served >= super::pull_requests::REST_FILE_LIMIT
        || listing.is_some_and(|listing| {
            listing.provider_capped || listing.files.len() >= super::pull_requests::REST_FILE_LIMIT
        })
        || pull_request.changed_files > super::pull_requests::REST_FILE_LIMIT as i64
        || super::pull_requests::authoritative_file_paths(pull_request).is_err()
}

/// Recover the complete file set of one pull request through git.
///
/// `listing` must be the complete REST walk (it carries GitHub's rename
/// pairs). Nothing is written; the caller stores the result atomically.
pub fn recover_pull_request_files(
    client: &mut GhClient,
    config: &GitRecoveryConfig,
    repo_key: &str,
    pull_request: &PullRequestData,
    listing: &PatchListing,
    want_patches: bool,
) -> Result<RecoveredFileSet> {
    let (owner, name) = validate_repo_key(repo_key)?;
    let number = pull_request.number;
    let fail = |message: String| Error::GitRecovery(format!("PR #{number}: {message}"));

    let pull = client.rest(&format!("repos/{repo_key}/pulls/{number}"))?;
    let base_sha =
        sha_at(&pull, &["base", "sha"]).ok_or_else(|| fail("REST pull has no base sha".into()))?;
    let head_sha =
        sha_at(&pull, &["head", "sha"]).ok_or_else(|| fail("REST pull has no head sha".into()))?;
    if let Some(graph_head) = pull_request.head_sha.as_deref()
        && !graph_head.eq_ignore_ascii_case(&head_sha)
    {
        return Err(fail(format!(
            "head moved between reads (GraphQL {graph_head}, REST {head_sha}); stale snapshot"
        )));
    }
    let totals = reconcile_totals(pull_request, &pull).map_err(fail)?;

    let compare = client.rest(&format!(
        "repos/{repo_key}/compare/{base_sha}...{head_sha}?per_page=1"
    ));
    let merge_base_sha = match compare {
        Ok(compare) => sha_at(&compare, &["merge_base_commit", "sha"])
            .ok_or_else(|| fail("compare has no merge_base_commit sha".into()))?,
        // GitHub represents these unrelated-history PRs as the entire head
        // added from an empty tree. A direct base-to-head diff is NOT the PR
        // diff. Admit this representation only for the exact provider error
        // and an all-added response; full per-record/totals validation below
        // still must prove the recovered set.
        Err(Error::GhCommand { stderr, .. })
            if stderr.contains(&format!(
                "No common ancestor between {base_sha} and {head_sha}"
            )) && stderr.contains("HTTP 404")
                && totals.deletions == 0
                && !pull_request.files.is_empty()
                && pull_request
                    .files
                    .iter()
                    .all(|file| file.change_type == "ADDED") =>
        {
            EMPTY_TREE.to_string()
        }
        Err(error) => return Err(error),
    };

    ensure_git_version(config)?;
    let repository = ensure_cache_repository(config, owner, name)?;
    let url = format!("{}{owner}/{name}.git", config.remote_base);
    let commits = if merge_base_sha == EMPTY_TREE {
        vec![head_sha.as_str()]
    } else {
        vec![head_sha.as_str(), merge_base_sha.as_str()]
    };
    fetch_commits(config, &repository, &url, &commits)?;
    let git_files = diff_files(
        config,
        &repository,
        &merge_base_sha,
        &head_sha,
        want_patches,
    )?;
    let oversized = git_files.1;

    let served = served_files(pull_request, listing).map_err(fail)?;
    let files = reconcile(&served, &git_files.0, &totals).map_err(fail)?;
    let keep: HashSet<String> = files.iter().map(|file| file.path.clone()).collect();
    let git_records = files.iter().filter(|file| file.from_git).count();
    Ok(RecoveredFileSet {
        files,
        keep,
        changed_files: totals.changed_files,
        additions: totals.additions,
        deletions: totals.deletions,
        base_sha,
        head_sha,
        merge_base_sha,
        git_records,
        oversized_patches: oversized,
    })
}

fn validate_repo_key(repo_key: &str) -> Result<(&str, &str)> {
    let valid = |part: &str| {
        !part.is_empty()
            && !part.starts_with('.')
            && !part.starts_with('-')
            && part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    };
    match repo_key.split_once('/') {
        Some((owner, name)) if valid(owner) && valid(name) => Ok((owner, name)),
        _ => Err(Error::InvalidArgument(format!(
            "repository {repo_key:?} is not a plain owner/name"
        ))),
    }
}

fn is_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn sha_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut node = value;
    for key in path {
        node = node.get(key)?;
    }
    node.as_str()
        .filter(|sha| is_sha(sha))
        .map(str::to_ascii_lowercase)
}

/// Reconcile GraphQL and REST totals. A side reading all-zero next to a
/// nonzero side is the observed transient zero and is ignored; two nonzero
/// sides that disagree mean the pull request changed between reads.
pub fn reconcile_totals(
    pull_request: &PullRequestData,
    rest: &Value,
) -> std::result::Result<ProviderTotals, String> {
    let graph = pull_request.changed_files_reported.then_some((
        pull_request.changed_files,
        pull_request.additions,
        pull_request.deletions,
    ));
    let rest_files = rest.get("changed_files").and_then(Value::as_i64);
    let rest = rest_files.map(|files| {
        (
            files,
            rest.get("additions").and_then(Value::as_i64).unwrap_or(0),
            rest.get("deletions").and_then(Value::as_i64).unwrap_or(0),
        )
    });
    let nonzero = |side: Option<(i64, i64, i64)>| side.filter(|(files, _, _)| *files > 0);
    let chosen = match (nonzero(graph), nonzero(rest)) {
        (Some(a), Some(b)) if a != b => {
            return Err(format!(
                "provider totals disagree between reads (GraphQL {a:?}, REST {b:?}); stale snapshot"
            ));
        }
        (Some(a), _) => a,
        (None, Some(b)) => b,
        (None, None) => {
            return Err(
                "provider reported no nonzero changed-file total; cannot validate a recovered set"
                    .into(),
            );
        }
    };
    let (changed_files, additions, deletions) = chosen;
    if additions < 0 || deletions < 0 {
        return Err("provider reported negative line totals".into());
    }
    if (pull_request.files.len() as i64) > changed_files {
        return Err(format!(
            "provider served {} files for {changed_files} changed files",
            pull_request.files.len()
        ));
    }
    Ok(ProviderTotals {
        changed_files,
        additions,
        deletions,
    })
}

/// Join the GraphQL file records with the REST listing (rename sources and
/// patches). A duplicate or empty path on either side is a contradiction.
pub fn served_files(
    pull_request: &PullRequestData,
    listing: &PatchListing,
) -> std::result::Result<Vec<ServedFile>, String> {
    let mut rest: HashMap<&str, (Option<&String>, Option<&String>)> = HashMap::new();
    for (path, previous, patch) in &listing.files {
        if path.is_empty()
            || rest
                .insert(path.as_str(), (previous.as_ref(), patch.as_ref()))
                .is_some()
        {
            return Err(format!(
                "REST listing serves {path:?} more than once or empty"
            ));
        }
    }
    let mut seen = HashSet::new();
    let mut served = Vec::new();
    for file in &pull_request.files {
        if file.path.is_empty() || !seen.insert(file.path.as_str()) {
            return Err(format!(
                "GraphQL lists {:?} more than once or empty",
                file.path
            ));
        }
        let (previous, patch) = rest
            .get(file.path.as_str())
            .copied()
            .unwrap_or((None, None));
        served.push(ServedFile {
            path: file.path.clone(),
            previous_path: previous.filter(|p| **p != file.path).cloned(),
            change_type: file.change_type.clone(),
            lines: Some((file.additions, file.deletions)),
            patch: patch.cloned(),
        });
    }
    for (path, previous, patch) in &listing.files {
        if seen.insert(path.as_str()) {
            served.push(ServedFile {
                path: path.clone(),
                previous_path: previous.clone().filter(|p| p != path),
                change_type: if previous.as_ref().is_some_and(|p| p != path) {
                    "RENAMED".into()
                } else {
                    String::new()
                },
                lines: None,
                patch: patch.clone(),
            });
        }
    }
    Ok(served)
}

fn change_type_for(status: char) -> &'static str {
    match status {
        'A' => "ADDED",
        'D' => "DELETED",
        'T' => "CHANGED",
        'R' => "RENAMED",
        _ => "MODIFIED",
    }
}

fn status_matches(change_type: &str, status: char) -> bool {
    match change_type {
        "" => true,
        "ADDED" => status == 'A',
        "DELETED" => status == 'D',
        "RENAMED" => status == 'R',
        "MODIFIED" | "CHANGED" => matches!(status, 'M' | 'T'),
        _ => false,
    }
}

fn git_lines(file: &GitFile) -> (i64, i64) {
    file.lines.unwrap_or((0, 0))
}

/// Build the complete record set: git's records, with every served record
/// required to equal its git counterpart. Explains any contradiction.
pub fn reconcile(
    served: &[ServedFile],
    git: &[GitFile],
    totals: &ProviderTotals,
) -> std::result::Result<Vec<RecoveredFile>, String> {
    let mut by_path: HashMap<&str, &GitFile> = HashMap::new();
    for file in git {
        if file.path.is_empty() || by_path.insert(file.path.as_str(), file).is_some() {
            return Err(format!(
                "git reports {:?} more than once or empty",
                file.path
            ));
        }
    }
    if git.len() as i64 != totals.changed_files {
        return Err(format!(
            "git -M -l1 yields {} records for {} changed files; GitHub's rename pairing is \
             not reproduced for this pull request",
            git.len(),
            totals.changed_files
        ));
    }
    let mut served_by_path: HashMap<&str, &ServedFile> = HashMap::new();
    for record in served {
        let Some(found) = by_path.get(record.path.as_str()) else {
            return Err(format!("served file {:?} is not a git record", record.path));
        };
        if !status_matches(&record.change_type, found.status) {
            return Err(format!(
                "served {:?} as {} but git status is {}",
                record.path, record.change_type, found.status
            ));
        }
        if record.previous_path != found.previous_path {
            return Err(format!(
                "served {:?} with rename source {:?} but git pairs it with {:?}",
                record.path, record.previous_path, found.previous_path
            ));
        }
        let actual = git_lines(found);
        match record.lines {
            None => {}
            Some(lines) if lines == actual => {}
            // GitHub serves large or binary files as 0/0 with no patch; the
            // totals gate below is what proves git's counts for them.
            Some((0, 0)) if record.patch.is_none() => {}
            Some(lines) => {
                return Err(format!(
                    "served {:?} with +{}/-{} but git has +{}/-{}",
                    record.path, lines.0, lines.1, actual.0, actual.1
                ));
            }
        }
        if served_by_path
            .insert(record.path.as_str(), record)
            .is_some()
        {
            return Err(format!("served {:?} more than once", record.path));
        }
    }
    let additions: i64 = git.iter().map(|file| git_lines(file).0).sum();
    let deletions: i64 = git.iter().map(|file| git_lines(file).1).sum();
    if (additions, deletions) != (totals.additions, totals.deletions) {
        return Err(format!(
            "git records total +{additions}/-{deletions} but the provider reports +{}/-{}",
            totals.additions, totals.deletions
        ));
    }
    Ok(git
        .iter()
        .map(|file| {
            let (additions, deletions) = git_lines(file);
            match served_by_path.get(file.path.as_str()) {
                Some(record) => RecoveredFile {
                    path: file.path.clone(),
                    previous_path: record.previous_path.clone(),
                    change_type: if record.change_type.is_empty() {
                        change_type_for(file.status).into()
                    } else {
                        record.change_type.clone()
                    },
                    additions,
                    deletions,
                    patch: record.patch.clone().or_else(|| file.patch.clone()),
                    from_git: false,
                },
                None => RecoveredFile {
                    path: file.path.clone(),
                    previous_path: file.previous_path.clone(),
                    change_type: change_type_for(file.status).into(),
                    additions,
                    deletions,
                    patch: file.patch.clone(),
                    from_git: true,
                },
            }
        })
        .collect())
}

mod diff;
mod git;
#[cfg(test)]
pub mod test_repo;

pub use diff::{Lines, RecordKey, diff_files, parse_numstat, parse_raw, split_patches};
use git::*;
pub use git::{MINIMUM_GIT_VERSION, fetch_commits, parse_git_version};

#[cfg(test)]
mod tests;
