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

// ---------------------------------------------------------------------------
// git subprocesses
// ---------------------------------------------------------------------------

fn hardened_command(config: &GitRecoveryConfig, git_dir: Option<&Path>) -> Command {
    let mut command = Command::new("git");
    for variable in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_EXTERNAL_DIFF",
        "GIT_DIFF_OPTS",
        "GIT_CONFIG",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_PROXY_COMMAND",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "GIT_TRACE",
        "GIT_TRACE_CURL",
        "GIT_TRACE_PACKET",
        "GIT_CURL_VERBOSE",
    ] {
        command.env_remove(variable);
    }
    command
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_LITERAL_PATHSPECS", "1")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_PAGER", "cat")
        .env("LC_ALL", "C");
    for setting in [
        "core.hooksPath=/dev/null".to_string(),
        "core.fsmonitor=false".into(),
        "core.attributesFile=/dev/null".into(),
        format!("attr.tree={EMPTY_TREE}"),
        "core.quotePath=true".into(),
        "core.pager=cat".into(),
        "color.ui=false".into(),
        "diff.external=".into(),
        "diff.noprefix=false".into(),
        "diff.mnemonicPrefix=false".into(),
        "diff.relative=false".into(),
        "gc.auto=0".into(),
        "maintenance.auto=false".into(),
        "fetch.recurseSubmodules=false".into(),
        "fetch.writeCommitGraph=false".into(),
        "submodule.recurse=false".into(),
        "protocol.allow=never".into(),
        "protocol.https.allow=always".into(),
        format!(
            "protocol.file.allow={}",
            if config.allows_file_protocol() {
                "always"
            } else {
                "never"
            }
        ),
        "http.followRedirects=false".into(),
        "credential.helper=".into(),
        "credential.helper=!gh auth git-credential".into(),
    ] {
        command.arg("-c").arg(setting);
    }
    if let Some(dir) = git_dir {
        command.arg(format!("--git-dir={}", dir.display()));
    }
    command
}

/// Redact URL userinfo from git's stderr before it reaches an error message.
fn scrub(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let mut out = String::with_capacity(text.len());
    for word in text.split_inclusive(char::is_whitespace) {
        match word.find("://") {
            Some(scheme) if word[scheme + 3..].contains('@') => {
                let at = scheme + 3 + word[scheme + 3..].find('@').unwrap_or(0);
                out.push_str(&word[..scheme + 3]);
                out.push_str("***");
                out.push_str(&word[at..]);
            }
            _ => out.push_str(word),
        }
    }
    out.trim().chars().take(2000).collect()
}

/// Run one hardened git command with a time and output bound.
fn run_git(
    config: &GitRecoveryConfig,
    git_dir: Option<&Path>,
    arguments: &[&str],
    timeout: Duration,
) -> Result<Vec<u8>> {
    let label = arguments.first().copied().unwrap_or("git");
    let mut command = hardened_command(config, git_dir);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|error| Error::GitRecovery(format!("cannot run git {label}: {error}")))?;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let cap = config.max_output_bytes;
    let (out_tx, out_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buffer = Vec::new();
        let read = (&mut stdout)
            .take(cap as u64 + 1)
            .read_to_end(&mut buffer)
            .map(|_| ());
        let _ = out_tx.send((read, buffer));
    });
    let (err_tx, err_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = (&mut stderr)
            .take(STDERR_CAPTURE_BYTES)
            .read_to_end(&mut buffer);
        let _ = std::io::copy(&mut stderr, &mut std::io::sink());
        let _ = err_tx.send(buffer);
    });
    let deadline = Instant::now() + timeout;
    let mut captured: Option<(std::io::Result<()>, Vec<u8>)> = None;
    let kill = |child: &mut std::process::Child| {
        let _ = child.kill();
        let _ = child.wait();
    };
    let status = loop {
        if captured.is_none()
            && let Ok(message) = out_rx.try_recv()
        {
            if message.1.len() > cap {
                kill(&mut child);
                return Err(Error::GitRecovery(format!(
                    "git {label} produced more than {cap} bytes; refusing unbounded output"
                )));
            }
            captured = Some(message);
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            kill(&mut child);
            return Err(Error::GitRecovery(format!(
                "git {label} exceeded {}s",
                timeout.as_secs()
            )));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let (read, output) = match captured {
        Some(message) => message,
        None => out_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|_| Error::GitRecovery(format!("git {label} output was not collected")))?,
    };
    let stderr = err_rx
        .recv_timeout(Duration::from_secs(30))
        .unwrap_or_default();
    if !status.success() {
        return Err(Error::GitRecovery(format!(
            "git {label} failed ({status}): {}",
            scrub(&stderr)
        )));
    }
    read.map_err(|error| Error::GitRecovery(format!("git {label} output unreadable: {error}")))?;
    if output.len() > cap {
        return Err(Error::GitRecovery(format!(
            "git {label} produced more than {cap} bytes; refusing unbounded output"
        )));
    }
    Ok(output)
}

/// Basename-guided rename pairing (which GitHub's large-diff list matches)
/// arrived in git 2.33; an older git would pair differently.
pub const MINIMUM_GIT_VERSION: (u32, u32) = (2, 33);

/// Parse `git version X.Y.Z...` into (major, minor).
pub fn parse_git_version(output: &str) -> Option<(u32, u32)> {
    let version = output.trim().strip_prefix("git version ")?;
    let mut parts = version.split(|c: char| !c.is_ascii_digit());
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

fn ensure_git_version(config: &GitRecoveryConfig) -> Result<()> {
    let output = run_git(config, None, &["version"], config.local_timeout)?;
    let text = String::from_utf8_lossy(&output);
    match parse_git_version(&text) {
        Some(version) if version >= MINIMUM_GIT_VERSION => Ok(()),
        _ => Err(Error::GitRecovery(format!(
            "{} is older than git {}.{} (needed to match GitHub's rename pairing)",
            text.trim(),
            MINIMUM_GIT_VERSION.0,
            MINIMUM_GIT_VERSION.1
        ))),
    }
}

fn ensure_cache_repository(config: &GitRecoveryConfig, owner: &str, name: &str) -> Result<PathBuf> {
    let repository = config.cache_root.join(format!("{owner}__{name}.git"));
    if !repository.join("HEAD").is_file() {
        std::fs::create_dir_all(&config.cache_root)?;
        let path = repository.to_string_lossy().into_owned();
        run_git(
            config,
            None,
            &["init", "--bare", "--quiet", "--template=", "--", &path],
            config.local_timeout,
        )?;
    }
    Ok(repository)
}

fn has_commit(config: &GitRecoveryConfig, repository: &Path, sha: &str) -> bool {
    let spec = format!("{sha}^{{commit}}");
    run_git(
        config,
        Some(repository),
        &["cat-file", "-e", &spec],
        config.local_timeout,
    )
    .is_ok()
}

/// Shallow-fetch exactly the named commits (skipping ones already cached),
/// then verify each is present as a commit.
pub fn fetch_commits(
    config: &GitRecoveryConfig,
    repository: &Path,
    url: &str,
    shas: &[&str],
) -> Result<()> {
    if let Some(bad) = shas.iter().find(|sha| !is_sha(sha)) {
        return Err(Error::GitRecovery(format!(
            "{bad:?} is not a full commit sha"
        )));
    }
    let missing: Vec<&str> = shas
        .iter()
        .copied()
        .filter(|sha| !has_commit(config, repository, sha))
        .collect();
    if !missing.is_empty() {
        let mut arguments = vec![
            "fetch",
            "--depth=1",
            "--no-tags",
            "--no-write-fetch-head",
            "--no-recurse-submodules",
            "--quiet",
            "--",
            url,
        ];
        arguments.extend(missing.iter().copied());
        run_git(config, Some(repository), &arguments, config.fetch_timeout)?;
    }
    for sha in shas {
        if !has_commit(config, repository, sha) {
            return Err(Error::GitRecovery(format!(
                "commit {sha} is not present after fetch"
            )));
        }
    }
    Ok(())
}

fn diff_arguments<'a>(extra: &[&'a str], base: &'a str, head: &'a str) -> Vec<&'a str> {
    let mut arguments = vec![
        "diff",
        "--find-renames",
        "-l1",
        "--no-ext-diff",
        "--no-textconv",
        "--no-color",
        "--ignore-submodules=none",
        "--submodule=short",
    ];
    arguments.extend_from_slice(extra);
    arguments.extend([base, head, "--"]);
    arguments
}

fn utf8_path(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        Error::GitRecovery(format!(
            "git path {:?} is not UTF-8; it cannot be keyed like the API's paths",
            String::from_utf8_lossy(bytes)
        ))
    })
}

/// One raw/numstat record key: (path, rename source).
pub type RecordKey = (String, Option<String>);

/// Numstat added/deleted lines; `None` means binary.
pub type Lines = Option<(i64, i64)>;

/// Parse `git diff --raw -z -M` into ((path, rename source), status).
pub fn parse_raw(output: &[u8]) -> Result<Vec<(RecordKey, char)>> {
    let mut tokens = output.split(|byte| *byte == 0);
    let mut records = Vec::new();
    let path = |token: Option<&[u8]>| -> Result<String> {
        let token = token
            .filter(|path| !path.is_empty())
            .ok_or_else(|| Error::GitRecovery("raw diff record without a path".into()))?;
        utf8_path(token)
    };
    while let Some(meta) = tokens.next() {
        if meta.is_empty() {
            continue;
        }
        let meta = std::str::from_utf8(meta)
            .map_err(|_| Error::GitRecovery("malformed raw diff record".into()))?;
        let fields: Vec<&str> = meta.split(' ').collect();
        if !meta.starts_with(':') || fields.len() != 5 {
            return Err(Error::GitRecovery(format!(
                "malformed raw diff record {meta:?}"
            )));
        }
        let status_field = fields[4];
        let status = status_field
            .chars()
            .next()
            .ok_or_else(|| Error::GitRecovery(format!("raw record {meta:?} has no status")))?;
        match status {
            'A' | 'D' | 'M' | 'T' if status_field.len() == 1 => {
                records.push(((path(tokens.next())?, None), status));
            }
            'R' if status_field[1..].bytes().all(|b| b.is_ascii_digit()) => {
                let source = path(tokens.next())?;
                let destination = path(tokens.next())?;
                records.push(((destination, Some(source)), 'R'));
            }
            _ => {
                return Err(Error::GitRecovery(format!(
                    "unsupported raw diff status {status_field:?}"
                )));
            }
        }
    }
    Ok(records)
}

/// Parse `git diff --numstat -z -M` into ((path, rename source), lines),
/// where `None` lines mean binary.
pub fn parse_numstat(output: &[u8]) -> Result<Vec<(RecordKey, Lines)>> {
    let mut tokens = output.split(|byte| *byte == 0);
    let mut records = Vec::new();
    let malformed = |token: &[u8]| {
        Error::GitRecovery(format!(
            "malformed numstat record {:?}",
            String::from_utf8_lossy(token)
        ))
    };
    while let Some(token) = tokens.next() {
        if token.is_empty() {
            continue;
        }
        let mut fields = token.splitn(3, |byte| *byte == b'\t');
        let (Some(added), Some(deleted), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            return Err(malformed(token));
        };
        let number = |field: &[u8]| -> Option<i64> {
            std::str::from_utf8(field)
                .ok()?
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
        };
        let lines = match (added, deleted) {
            (b"-", b"-") => None,
            _ => match (number(added), number(deleted)) {
                (Some(a), Some(d)) => Some((a, d)),
                _ => return Err(malformed(token)),
            },
        };
        let key = if path.is_empty() {
            // A rename: source and destination follow as their own tokens.
            let source = tokens
                .next()
                .filter(|p| !p.is_empty())
                .ok_or_else(|| malformed(token))?;
            let destination = tokens
                .next()
                .filter(|p| !p.is_empty())
                .ok_or_else(|| malformed(token))?;
            (utf8_path(destination)?, Some(utf8_path(source)?))
        } else {
            (utf8_path(path)?, None)
        };
        records.push((key, lines));
    }
    Ok(records)
}

/// Split `git diff -p -M` into one chunk per file, in order.
pub fn split_patches(output: &[u8]) -> Result<Vec<&[u8]>> {
    let mut starts = Vec::new();
    let mut offset = 0;
    for line in output.split_inclusive(|byte| *byte == b'\n') {
        if line.starts_with(b"diff --git ") {
            starts.push(offset);
        } else if starts.is_empty() && !line.is_empty() {
            return Err(Error::GitRecovery(
                "patch output has text before the first file".into(),
            ));
        }
        offset += line.len();
    }
    let mut chunks = Vec::with_capacity(starts.len());
    for (index, start) in starts.iter().enumerate() {
        let end = starts.get(index + 1).copied().unwrap_or(output.len());
        chunks.push(&output[*start..end]);
    }
    Ok(chunks)
}

/// GitHub's `patch` field: hunks only, from the first `@@`, no trailing newline.
fn github_patch(chunk: &[u8]) -> Option<String> {
    let mut offset = 0;
    for line in chunk.split_inclusive(|byte| *byte == b'\n') {
        if line.starts_with(b"@@ ") {
            let body = &chunk[offset..];
            let body = body.strip_suffix(b"\n").unwrap_or(body);
            return Some(String::from_utf8_lossy(body).into_owned());
        }
        offset += line.len();
    }
    None
}

/// Header git prints for paths that need no quoting.
fn plain_header(path: &str, previous: Option<&str>) -> Option<String> {
    let plain = |p: &str| {
        p.bytes()
            .all(|b| (0x20..0x7f).contains(&b) && b != b'"' && b != b'\\')
    };
    let source = previous.unwrap_or(path);
    (plain(path) && plain(source)).then(|| format!("diff --git a/{source} b/{path}"))
}

/// Every file record between `base` and `head` (`-M -l1`), with numstat
/// lines and (optionally) GitHub-shaped patches. Returns the records and the
/// number of patches dropped for size.
pub fn diff_files(
    config: &GitRecoveryConfig,
    repository: &Path,
    base: &str,
    head: &str,
    want_patches: bool,
) -> Result<(Vec<GitFile>, usize)> {
    let raw = run_git(
        config,
        Some(repository),
        &diff_arguments(&["--raw", "-z", "--abbrev=40"], base, head),
        config.local_timeout,
    )?;
    let raw = parse_raw(&raw)?;
    let numstat = run_git(
        config,
        Some(repository),
        &diff_arguments(&["--numstat", "-z"], base, head),
        config.local_timeout,
    )?;
    let numstat = parse_numstat(&numstat)?;
    if numstat.len() != raw.len()
        || numstat
            .iter()
            .zip(&raw)
            .any(|((numstat_key, _), (raw_key, _))| numstat_key != raw_key)
    {
        return Err(Error::GitRecovery(format!(
            "numstat ({}) and raw ({}) record lists disagree",
            numstat.len(),
            raw.len()
        )));
    }
    let mut patches: Vec<Option<String>> = vec![None; raw.len()];
    let mut oversized = 0;
    if want_patches {
        let output = run_git(
            config,
            Some(repository),
            &diff_arguments(&["-p", "--full-index", "-U3"], base, head),
            config.local_timeout,
        )?;
        let chunks = split_patches(&output)?;
        if chunks.len() != raw.len() {
            return Err(Error::GitRecovery(format!(
                "patch output has {} files but the raw diff has {}",
                chunks.len(),
                raw.len()
            )));
        }
        for (index, chunk) in chunks.into_iter().enumerate() {
            let ((path, previous), _) = &raw[index];
            if let Some(header) = plain_header(path, previous.as_deref()) {
                let first = chunk
                    .split(|byte| *byte == b'\n')
                    .next()
                    .unwrap_or_default();
                if first != header.as_bytes() {
                    return Err(Error::GitRecovery(format!(
                        "patch {index} header does not name {path:?}"
                    )));
                }
            }
            if numstat[index].1.is_none() {
                continue; // binary: GitHub serves no patch
            }
            match github_patch(chunk) {
                Some(patch) if patch.len() > config.max_patch_bytes => oversized += 1,
                patch => patches[index] = patch,
            }
        }
    }
    let files = raw
        .into_iter()
        .zip(numstat)
        .zip(patches)
        .map(
            |((((path, previous_path), status), (_, lines)), patch)| GitFile {
                path,
                previous_path,
                status,
                lines,
                patch,
            },
        )
        .collect();
    Ok((files, oversized))
}

/// A real git repository shaped like a capped pull request, served over a
/// `file://` remote, for tests here and in the sync layer.
#[cfg(test)]
pub mod test_repo {
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use super::GitRecoveryConfig;

    pub struct FixtureRepo {
        pub dir: tempfile::TempDir,
        pub config: GitRecoveryConfig,
        pub repo_key: String,
        pub base: String,
        pub head: String,
    }

    pub const HOSTILE: [&str; 7] = [
        "-leading-dash.txt",
        "tab\tname.txt",
        "new\nline.txt",
        "quote\"name.txt",
        "\u{fc}n\u{ef}code.txt",
        "back\\slash.txt",
        "space name.txt",
    ];
    /// Modified files (beyond the hostile adds, deletes, renames, binary).
    pub const MODIFIED: usize = 3050;

    fn git(dir: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "-c",
                "user.name=fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "init.defaultBranch=main",
            ])
            .args(arguments)
            .current_dir(dir)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn write(root: &Path, path: &str, bytes: &[u8]) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    /// Base: 3,150 text files, a moved file, a basename-renamed file, a
    /// binary. Head: 3,050 modified, 100 deleted, 100 added, 7 hostile names
    /// added, one exact rename, one same-basename edited rename, binary edit.
    pub fn build() -> FixtureRepo {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-q"]);
        for i in 0..MODIFIED + 100 {
            write(
                &work,
                &format!("d/f{i:04}.txt"),
                format!("line {i}\n").as_bytes(),
            );
        }
        let moved: String = (0..40).map(|i| format!("moved content {i}\n")).collect();
        write(&work, "keep/moved.txt", moved.as_bytes());
        let renamed: String = (0..40).map(|i| format!("fn item_{i}() {{}}\n")).collect();
        write(&work, "old name/basename.rs", renamed.as_bytes());
        write(&work, "bin.dat", &[0, 1, 2, 3, 0, 255, 254]);
        // Untrusted attributes: honoured, they would turn every file binary.
        write(&work, ".gitattributes", b"* -diff\n");
        git(&work, &["add", "-A"]);
        git(&work, &["commit", "-q", "-m", "base"]);
        let base = git(&work, &["rev-parse", "HEAD"]);

        for i in 0..MODIFIED {
            write(
                &work,
                &format!("d/f{i:04}.txt"),
                format!("line {i}\nmore {i}\n").as_bytes(),
            );
        }
        for i in MODIFIED..MODIFIED + 100 {
            std::fs::remove_file(work.join(format!("d/f{i:04}.txt"))).unwrap();
        }
        for i in 0..100 {
            write(
                &work,
                &format!("new/n{i:04}.md"),
                format!("added {i}\n").as_bytes(),
            );
        }
        for (index, name) in HOSTILE.iter().enumerate() {
            write(&work, name, format!("hostile {index}\n").as_bytes());
        }
        std::fs::remove_file(work.join("keep/moved.txt")).unwrap();
        write(&work, "elsewhere/moved2.txt", moved.as_bytes());
        std::fs::remove_file(work.join("old name/basename.rs")).unwrap();
        write(
            &work,
            "new dir/basename.rs",
            format!("{renamed}fn extra() {{}}\n").as_bytes(),
        );
        write(&work, "bin.dat", &[0, 9, 8, 7, 0, 255]);
        git(&work, &["add", "-A"]);
        git(&work, &["commit", "-q", "-m", "head"]);
        let head = git(&work, &["rev-parse", "HEAD"]);

        let remote = dir.path().join("remote/owner");
        std::fs::create_dir_all(&remote).unwrap();
        git(
            &remote,
            &["clone", "-q", "--bare", work.to_str().unwrap(), "repo.git"],
        );
        git(
            &remote.join("repo.git"),
            &["config", "uploadpack.allowAnySHA1InWant", "true"],
        );
        let octocat = dir.path().join("remote/octocat");
        std::fs::create_dir_all(&octocat).unwrap();
        git(
            &octocat,
            &["clone", "-q", "--bare", work.to_str().unwrap(), "hello.git"],
        );
        git(
            &octocat.join("hello.git"),
            &["config", "uploadpack.allowAnySHA1InWant", "true"],
        );
        let mut config = GitRecoveryConfig::github(dir.path().join("cache"));
        config.remote_base = format!("file://{}/", dir.path().join("remote").display());
        FixtureRepo {
            dir,
            config,
            repo_key: "owner/repo".into(),
            base,
            head,
        }
    }

    impl FixtureRepo {
        /// Every git record, read through a cache of its own so the code
        /// under test still performs a real fetch.
        pub fn records(&self) -> Vec<super::GitFile> {
            let mut config = self.config.clone();
            config.cache_root = self.dir.path().join("probe-cache");
            let repository = super::ensure_cache_repository(&config, "owner", "repo").unwrap();
            let url = format!("{}owner/repo.git", config.remote_base);
            super::fetch_commits(&config, &repository, &url, &[&self.head, &self.base]).unwrap();
            super::diff_files(&config, &repository, &self.base, &self.head, true)
                .unwrap()
                .0
        }

        pub fn cache_root(&self) -> PathBuf {
            self.config.cache_root.clone()
        }

        /// Expected record count: modified + deleted + added + hostile +
        /// two renames + binary.
        pub fn record_count() -> usize {
            MODIFIED + 100 + 100 + HOSTILE.len() + 2 + 1
        }
    }
}

#[cfg(test)]
mod tests;
