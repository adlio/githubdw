//! A real git repository shaped like a capped pull request, served over a
//! `file://` remote, for tests here and in the sync layer.

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
