//! Parsing `git diff` output into GitHub-shaped file records.

use super::*;

pub(super) fn diff_arguments<'a>(extra: &[&'a str], base: &'a str, head: &'a str) -> Vec<&'a str> {
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

pub(super) fn utf8_path(bytes: &[u8]) -> Result<String> {
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
pub(super) fn github_patch(chunk: &[u8]) -> Option<String> {
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
pub(super) fn plain_header(path: &str, previous: Option<&str>) -> Option<String> {
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
