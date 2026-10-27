//! `githubdw config`, `monitor`, and `group`: the warehouse's own settings,
//! exercised through the binary.

mod support;

use support::Cli;

#[test]
fn config_values_round_trip() {
    let cli = Cli::new();

    let set = cli
        .run(&["config", "set", "timezone", "Europe/Berlin"])
        .ok();
    assert_eq!(set.stdout.trim(), "timezone = Europe/Berlin");

    let get = cli.run(&["config", "get", "timezone"]).ok();
    assert_eq!(get.stdout.trim(), "Europe/Berlin");

    let list = cli.run(&["config", "list"]).ok();
    assert!(
        list.stdout
            .lines()
            .any(|line| line == "timezone = Europe/Berlin"),
        "{}",
        list.stdout
    );
}

#[test]
fn missing_config_key_fails_with_a_message() {
    let cli = Cli::new();
    let run = cli.run(&["config", "get", "no-such-key"]).failed();
    assert!(run.stderr.starts_with("error: "), "{}", run.stderr);
    assert!(run.stderr.contains("no-such-key"), "{}", run.stderr);
}

#[test]
fn monitored_sources_are_added_listed_and_removed() {
    let cli = Cli::new();
    assert_eq!(
        cli.run(&["monitor", "list"]).ok().stdout.trim(),
        "nothing monitored yet"
    );

    assert_eq!(
        cli.run(&["monitor", "add-repo", "Octocat/Hello"])
            .ok()
            .stdout
            .trim(),
        "monitoring repo octocat/hello"
    );
    assert_eq!(
        cli.run(&["monitor", "add-user", "Hubot"])
            .ok()
            .stdout
            .trim(),
        "monitoring user hubot"
    );
    assert_eq!(
        cli.run(&["monitor", "add-org", "Acme"]).ok().stdout.trim(),
        "monitoring org acme"
    );

    let list = cli.run(&["monitor", "list"]).ok().stdout;
    for identifier in ["octocat/hello", "hubot", "acme"] {
        let line = list
            .lines()
            .find(|line| line.contains(identifier))
            .unwrap_or_else(|| panic!("{identifier} missing from:\n{list}"));
        assert!(line.contains("enabled"), "{line}");
        assert!(line.ends_with("last sync: never"), "{line}");
    }

    assert_eq!(
        cli.run(&["monitor", "remove", "hubot"]).ok().stdout.trim(),
        "removed hubot"
    );
    assert!(!cli.run(&["monitor", "list"]).ok().stdout.contains("hubot"));
}

#[test]
fn removing_an_unmonitored_source_fails() {
    let cli = Cli::new();
    let run = cli.run(&["monitor", "remove", "nobody"]).failed();
    assert!(run.stderr.contains("nobody"), "{}", run.stderr);
}

#[test]
fn user_group_lifecycle() {
    let cli = Cli::new();
    assert_eq!(
        cli.run(&[
            "group",
            "user",
            "create",
            "core",
            "--description",
            "core team"
        ])
        .ok()
        .stdout
        .trim(),
        "created group core"
    );
    assert_eq!(
        cli.run(&["group", "user", "add", "core", "alice", "bob", "carol"])
            .ok()
            .stdout
            .trim(),
        "added 3 member(s) to core"
    );
    assert_eq!(
        cli.run(&["group", "user", "remove", "core", "bob"])
            .ok()
            .stdout
            .trim(),
        "removed 1 member(s) from core"
    );
    let members = cli.run(&["group", "user", "show", "core"]).ok().stdout;
    assert!(
        members.contains("alice") && members.contains("carol"),
        "{members}"
    );
    assert!(!members.contains("bob"), "{members}");

    let list = cli.run(&["group", "user", "list"]).ok().stdout;
    assert_eq!(list.trim(), "core\t2 members\tcore team");

    assert_eq!(
        cli.run(&["group", "user", "set", "core", "dave"])
            .ok()
            .stdout
            .trim(),
        "set core to 1 member(s)"
    );
    assert!(
        cli.run(&["group", "user", "show", "core"])
            .ok()
            .stdout
            .contains("dave")
    );

    assert_eq!(
        cli.run(&["group", "user", "delete", "core"])
            .ok()
            .stdout
            .trim(),
        "deleted group core"
    );
    assert_eq!(cli.run(&["group", "user", "list"]).ok().stdout.trim(), "");
}

#[test]
fn repo_groups_are_separate_from_user_groups() {
    let cli = Cli::new();
    cli.run(&["group", "repo", "create", "services"]).ok();
    cli.run(&["group", "repo", "add", "services", "octocat/hello"])
        .ok();
    assert_eq!(
        cli.run(&["group", "repo", "list"]).ok().stdout.trim(),
        "services\t1 members"
    );
    assert_eq!(cli.run(&["group", "user", "list"]).ok().stdout.trim(), "");
}

#[test]
fn adding_to_a_missing_group_fails() {
    let cli = Cli::new();
    let run = cli
        .run(&["group", "user", "add", "ghost", "alice"])
        .failed();
    assert!(run.stderr.starts_with("error: "), "{}", run.stderr);
}
