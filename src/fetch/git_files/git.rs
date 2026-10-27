//! Hardened git subprocesses: version check, bare cache, commit fetch.

use super::*;

pub(super) fn hardened_command(config: &GitRecoveryConfig, git_dir: Option<&Path>) -> Command {
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
pub(super) fn scrub(stderr: &[u8]) -> String {
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
pub(super) fn run_git(
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
    let (Some(mut stdout), Some(mut stderr)) = (child.stdout.take(), child.stderr.take()) else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(Error::GitRecovery(format!(
            "git {label}: output pipes unavailable"
        )));
    };
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

pub(super) fn ensure_git_version(config: &GitRecoveryConfig) -> Result<()> {
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

pub(super) fn ensure_cache_repository(
    config: &GitRecoveryConfig,
    owner: &str,
    name: &str,
) -> Result<PathBuf> {
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

pub(super) fn has_commit(config: &GitRecoveryConfig, repository: &Path, sha: &str) -> bool {
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
