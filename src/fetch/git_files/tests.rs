use super::test_repo::{self, FixtureRepo};
use super::*;
use crate::fetch::pull_requests::FileDiffData;
use crate::fetch::test_support::FixtureTransport;
use serde_json::json;

fn git_records(fixture: &FixtureRepo) -> Vec<GitFile> {
    fixture.records()
}

fn served_from(git: &[GitFile], count: usize) -> (Vec<FileDiffData>, PatchListing) {
    let files = git[..count]
        .iter()
        .map(|file| {
            let (additions, deletions) = git_lines(file);
            FileDiffData {
                path: file.path.clone(),
                change_type: change_type_for(file.status).into(),
                additions,
                deletions,
            }
        })
        .collect();
    let listing = PatchListing {
        files: git[..count]
            .iter()
            .map(|file| {
                (
                    file.path.clone(),
                    file.previous_path.clone(),
                    file.patch.clone(),
                )
            })
            .collect(),
        pages: 30,
        provider_capped: true,
    };
    (files, listing)
}

fn totals_of(git: &[GitFile]) -> ProviderTotals {
    ProviderTotals {
        changed_files: git.len() as i64,
        additions: git.iter().map(|f| git_lines(f).0).sum(),
        deletions: git.iter().map(|f| git_lines(f).1).sum(),
    }
}

pub(crate) fn pull_request_for(git: &[GitFile], served: usize, head: &str) -> PullRequestData {
    let totals = totals_of(git);
    let (files, _) = served_from(git, served);
    PullRequestData {
        number: 7,
        title: None,
        body: None,
        state: "CLOSED".into(),
        is_draft: false,
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: None,
        merged_at: None,
        closed_at: None,
        base_ref: None,
        head_ref: None,
        additions: totals.additions,
        deletions: totals.deletions,
        changed_files: totals.changed_files,
        changed_files_reported: true,
        author: None,
        merged_by: None,
        reviews: vec![],
        review_comments: vec![],
        conversation_comments: vec![],
        files,
        head_sha: Some(head.into()),
        check_runs: vec![],
        node_id: None,
        pending: vec![],
        check_runs_truncated: false,
    }
}

fn rest_fixtures(fixture: &FixtureRepo, totals: ProviderTotals) -> Vec<Result<String>> {
    vec![
        Ok(json!({
            "base": {"sha": fixture.base}, "head": {"sha": fixture.head},
            "changed_files": totals.changed_files,
            "additions": totals.additions, "deletions": totals.deletions
        })
        .to_string()),
        Ok(json!({"merge_base_commit": {"sha": fixture.base}}).to_string()),
    ]
}

fn client(responses: Vec<Result<String>>) -> GhClient {
    GhClient::with_transport(Box::new(FixtureTransport::new(responses))).without_sleeping()
}

#[test]
fn git_records_cover_every_path_with_renames_binary_and_hostile_names() {
    let fixture = test_repo::build();
    let git = git_records(&fixture);
    assert_eq!(git.len(), FixtureRepo::record_count());
    let by: HashMap<&str, &GitFile> = git.iter().map(|f| (f.path.as_str(), f)).collect();
    let exact = by["elsewhere/moved2.txt"];
    assert_eq!(exact.status, 'R');
    assert_eq!(exact.previous_path.as_deref(), Some("keep/moved.txt"));
    assert_eq!(exact.lines, Some((0, 0)));
    assert_eq!(exact.patch, None, "a 100% rename has no hunks");
    let basename = by["new dir/basename.rs"];
    assert_eq!(
        basename.previous_path.as_deref(),
        Some("old name/basename.rs")
    );
    assert_eq!(basename.lines, Some((1, 0)));
    assert!(basename.patch.as_deref().unwrap().starts_with("@@ "));
    assert!(!by.contains_key("keep/moved.txt"));
    let binary = by["bin.dat"];
    assert_eq!(
        (binary.status, binary.lines, binary.patch.clone()),
        ('M', None, None)
    );
    for (index, name) in test_repo::HOSTILE.iter().enumerate() {
        let file = by.get(name).unwrap_or_else(|| panic!("{name:?} missing"));
        assert_eq!((file.status, file.lines), ('A', Some((1, 0))), "{name:?}");
        let patch = file.patch.as_deref().unwrap();
        assert!(
            patch.starts_with("@@ ") && patch.ends_with(&format!("+hostile {index}")),
            "{name:?}: {patch:?}"
        );
    }
    assert_eq!(
        by["d/f0003.txt"].patch.as_deref(),
        Some("@@ -1 +1,2 @@\n line 3\n+more 3")
    );
    assert_eq!(by["d/f3060.txt"].status, 'D');
    assert!(
        by["new/n0042.md"].previous_path.is_none(),
        "different basenames stay unpaired"
    );
}

#[test]
fn unrelated_history_requires_exact_error_and_all_added_totals() {
    let fixture = test_repo::build();
    let (git, _) = diff_files(
        &fixture.config,
        &fixture.dir.path().join("remote/owner/repo.git"),
        EMPTY_TREE,
        &fixture.head,
        false,
    )
    .unwrap();
    let totals = totals_of(&git);
    let pr = pull_request_for(&git, 3000, &fixture.head);
    let (_, listing) = served_from(&git, 3000);
    let attempt = |message: String, input: &PullRequestData, total: ProviderTotals| {
        let mut replies = rest_fixtures(&fixture, total);
        replies[1] = Err(Error::GhCommand {
            exit_code: Some(1),
            stderr: message,
        });
        recover_pull_request_files(
            &mut client(replies),
            &fixture.config,
            &fixture.repo_key,
            input,
            &listing,
            false,
        )
    };
    let no_ancestor = format!(
        "gh: No common ancestor between {} and {}. (HTTP 404)",
        fixture.base, fixture.head,
    );
    let result = attempt(no_ancestor.clone(), &pr, totals).unwrap();
    assert_eq!(result.merge_base_sha, EMPTY_TREE);
    assert_eq!(result.files.len(), git.len());
    assert_eq!(result.additions, totals.additions);
    assert!(result.files.iter().all(|file| file.change_type == "ADDED"));
    assert!(attempt("gh: Not Found (HTTP 404)".into(), &pr, totals).is_err());
    assert!(attempt(no_ancestor.replace("404", "403"), &pr, totals).is_err());
    assert!(
        attempt(
            no_ancestor.replace(&fixture.head, &"f".repeat(40)),
            &pr,
            totals
        )
        .is_err()
    );
    let mut not_all_added = pr.clone();
    not_all_added.files[0].change_type = "MODIFIED".into();
    assert!(attempt(no_ancestor.clone(), &not_all_added, totals).is_err());
    let mut wrong_totals = totals;
    wrong_totals.additions += 1;
    let mut wrong_pr = pr.clone();
    wrong_pr.additions += 1;
    assert!(attempt(no_ancestor, &wrong_pr, wrong_totals).is_err());
}

#[test]
fn capped_listing_is_completed_exactly_and_failures_close() {
    let fixture = test_repo::build();
    let git = git_records(&fixture);
    let totals = totals_of(&git);
    let served = 3000;
    let pull_request = pull_request_for(&git, served, &fixture.head);
    let (_, listing) = served_from(&git, served);

    let mut ok_client = client(rest_fixtures(&fixture, totals));
    let set = recover_pull_request_files(
        &mut ok_client,
        &fixture.config,
        &fixture.repo_key,
        &pull_request,
        &listing,
        true,
    )
    .unwrap();
    assert_eq!(set.files.len(), git.len());
    assert_eq!(set.git_records, git.len() - served);
    assert_eq!(
        (set.additions, set.deletions),
        (totals.additions, totals.deletions)
    );
    assert_eq!(set.merge_base_sha, fixture.base);
    let paths: HashSet<&str> = set.files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths.len(), git.len());
    assert!(
        !set.keep.contains("keep/moved.txt"),
        "rename sources survive as metadata, not obsolete file records"
    );
    assert_eq!(set.keep.len(), set.files.len());

    // Transient zero on the REST read: GraphQL's nonzero totals are used.
    let zero = ProviderTotals {
        changed_files: 0,
        additions: 0,
        deletions: 0,
    };
    let mut zero_client = client(rest_fixtures(&fixture, zero));
    assert!(
        recover_pull_request_files(
            &mut zero_client,
            &fixture.config,
            &fixture.repo_key,
            &pull_request,
            &listing,
            false,
        )
        .is_ok()
    );

    let refuse = |pull_request: &PullRequestData,
                  rest: ProviderTotals,
                  listing: &PatchListing,
                  expected: &str| {
        let mut c = client(rest_fixtures(&fixture, rest));
        let error = recover_pull_request_files(
            &mut c,
            &fixture.config,
            &fixture.repo_key,
            pull_request,
            listing,
            false,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains(expected), "{expected}: {error}");
    };
    // Stale snapshot: GraphQL head differs from REST head.
    let mut stale = pull_request.clone();
    stale.head_sha = Some(fixture.base.clone());
    refuse(&stale, totals, &listing, "stale snapshot");
    // Totals disagree between reads.
    let mut moved = totals;
    moved.additions += 1;
    refuse(&pull_request, moved, &listing, "stale snapshot");
    // Count and line totals that git cannot reproduce.
    let mut more = pull_request.clone();
    more.changed_files += 1;
    let mut more_rest = totals;
    more_rest.changed_files += 1;
    refuse(&more, more_rest, &listing, "records for");
    let mut lines = pull_request.clone();
    lines.deletions += 5;
    let mut lines_rest = totals;
    lines_rest.deletions += 5;
    refuse(&lines, lines_rest, &listing, "provider reports");
    // Both reads report no total: nothing can be validated.
    let mut none = pull_request.clone();
    none.changed_files_reported = false;
    refuse(&none, zero, &listing, "no nonzero changed-file total");
    // A served record contradicting git.
    let mut wrong = pull_request.clone();
    wrong.files[0].additions += 3;
    refuse(&wrong, totals, &listing, "but git has");
    // An invented rename pair is refused.
    let mut invented = listing.clone_listing();
    invented.files[1].1 = Some("d/f3099.txt".into());
    refuse(&pull_request, totals, &invented, "rename source");

    // Network failure on the REST read.
    let mut down = client(vec![Err(Error::GhCommand {
        exit_code: Some(1),
        stderr: "HTTP 404".into(),
    })]);
    assert!(
        recover_pull_request_files(
            &mut down,
            &fixture.config,
            &fixture.repo_key,
            &pull_request,
            &listing,
            false,
        )
        .is_err()
    );
    // A commit the remote does not have.
    let repository = ensure_cache_repository(&fixture.config, "owner", "repo").unwrap();
    let url = format!("{}owner/repo.git", fixture.config.remote_base);
    let absent = "0123456789abcdef0123456789abcdef01234567";
    assert!(fetch_commits(&fixture.config, &repository, &url, &[absent]).is_err());
    assert!(fetch_commits(&fixture.config, &repository, &url, &["--upload-pack=x"]).is_err());
    assert!(fixture.cache_root().join("owner__repo.git").is_dir());
}

impl PatchListing {
    fn clone_listing(&self) -> PatchListing {
        PatchListing {
            files: self.files.clone(),
            pages: self.pages,
            provider_capped: self.provider_capped,
        }
    }
}

fn git_file(path: &str, status: char, lines: Option<(i64, i64)>) -> GitFile {
    GitFile {
        path: path.into(),
        previous_path: None,
        status,
        lines,
        patch: Some("@@ x".into()),
    }
}

fn served(path: &str, change_type: &str, lines: (i64, i64), patch: Option<&str>) -> ServedFile {
    ServedFile {
        path: path.into(),
        previous_path: None,
        change_type: change_type.into(),
        lines: Some(lines),
        patch: patch.map(str::to_string),
    }
}

#[test]
fn served_zero_without_patch_takes_git_counts_only_under_the_totals_proof() {
    let git = vec![
        git_file("big.json", 'M', Some((900, 10))),
        git_file("b", 'A', Some((1, 0))),
    ];
    let totals = ProviderTotals {
        changed_files: 2,
        additions: 901,
        deletions: 10,
    };
    let records = reconcile(
        &[served("big.json", "MODIFIED", (0, 0), None)],
        &git,
        &totals,
    )
    .unwrap();
    assert_eq!((records[0].additions, records[0].deletions), (900, 10));
    assert_eq!(records[0].patch.as_deref(), Some("@@ x"));
    // 0/0 WITH a patch is a real contradiction, not GitHub's omission.
    assert!(
        reconcile(
            &[served("big.json", "MODIFIED", (0, 0), Some("@@"))],
            &git,
            &totals
        )
        .is_err()
    );
    // Without the totals proof the git counts are not accepted.
    let off = ProviderTotals {
        additions: 1,
        ..totals
    };
    assert!(reconcile(&[served("big.json", "MODIFIED", (0, 0), None)], &git, &off).is_err());
    // Status contradiction and an unknown status.
    assert!(reconcile(&[served("b", "DELETED", (1, 0), None)], &git, &totals).is_err());
    assert!(reconcile(&[served("b", "COPIED", (1, 0), None)], &git, &totals).is_err());
    // Duplicate git records and served-only paths.
    let dup = vec![git[0].clone(), git[0].clone()];
    assert!(reconcile(&[], &dup, &totals).is_err());
    assert!(reconcile(&[served("ghost", "ADDED", (1, 0), None)], &git, &totals).is_err());
}

#[test]
fn totals_reconcile_ignores_a_transient_zero_and_rejects_disagreement() {
    let mut pr =
        super::tests::pull_request_for(&[git_file("a", 'A', Some((2, 1)))], 1, &"a".repeat(40));
    let rest = |files: i64, a: i64, d: i64| json!({"changed_files": files, "additions": a, "deletions": d});
    assert_eq!(
        reconcile_totals(&pr, &rest(0, 0, 0)).unwrap().changed_files,
        1
    );
    assert!(reconcile_totals(&pr, &rest(2, 2, 1)).is_err());
    pr.changed_files = 0;
    assert_eq!(reconcile_totals(&pr, &rest(1, 2, 1)).unwrap().additions, 2);
    assert!(reconcile_totals(&pr, &json!({})).is_err());
    pr.changed_files = 1;
    pr.files.push(pr.files[0].clone());
    assert!(
        reconcile_totals(&pr, &rest(1, 2, 1)).is_err(),
        "more served than total"
    );
}

#[test]
fn parsers_fail_closed_on_malformed_output() {
    assert!(parse_numstat(b"1\t2\0").is_err());
    assert!(parse_numstat(b"x\t2\tpath\0").is_err());
    assert!(parse_numstat(b"-1\t2\tpath\0").is_err());
    assert!(parse_numstat(b"1\t2\t\0only-source\0").is_err());
    assert_eq!(
        parse_numstat(b"-\t-\tbin\x003\t1\t\x00old\x00new\x00").unwrap(),
        vec![
            (("bin".into(), None), None),
            (("new".into(), Some("old".into())), Some((3, 1)))
        ]
    );
    assert!(
        parse_numstat(b"1\t1\t\xff\xfe\0").is_err(),
        "non-UTF-8 path"
    );
    assert!(
        parse_raw(b":100644 100644 a b C075\0x\0y\0").is_err(),
        "copies are not expected"
    );
    assert!(parse_raw(b":100644 100644 a b U\0x\0").is_err());
    assert!(parse_raw(b"garbage\0x\0").is_err());
    assert!(parse_raw(b":100644 100644 a b M\0").is_err());
    assert_eq!(
        parse_raw(b":100644 100644 a b R100\0old\0new\0:000000 100644 a b A\0z\0").unwrap(),
        vec![
            (("new".into(), Some("old".into())), 'R'),
            (("z".into(), None), 'A')
        ]
    );
    assert!(split_patches(b"junk\ndiff --git a/x b/x\n").is_err());
    assert_eq!(split_patches(b"").unwrap().len(), 0);
    assert_eq!(parse_git_version("git version 2.50.1\n"), Some((2, 50)));
    assert_eq!(
        parse_git_version("git version 2.32.0.windows.1"),
        Some((2, 32))
    );
    assert!(parse_git_version("nope").is_none());
    assert_eq!(
        scrub(b"fatal: https://user:tok@github.com/x failed"),
        "fatal: https://***@github.com/x failed"
    );
    assert!(validate_repo_key("owner/repo").is_ok());
    for bad in ["-x/repo", "a/b/c", "a/..", "a/b;rm", "a", "a/"] {
        assert!(validate_repo_key(bad).is_err(), "{bad}");
    }
}

#[test]
fn runaway_output_and_time_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = GitRecoveryConfig::github(dir.path().to_path_buf());
    config.max_output_bytes = 10;
    let error = run_git(&config, None, &["help", "-a"], Duration::from_secs(30)).unwrap_err();
    assert!(error.to_string().contains("more than 10 bytes"), "{error}");
    config.max_output_bytes = 1 << 20;
    // A command that outlives its bound. `git help -a` with a zero bound was
    // racy: it can exit before the first deadline check.
    let started = std::time::Instant::now();
    let error = run_git(
        &config,
        None,
        &["-c", "alias.pause=!sleep 5", "pause"],
        Duration::from_millis(50),
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeded"), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the child was not killed"
    );
}
