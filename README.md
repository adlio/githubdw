# githubdw

A local, SQLite-based data warehouse for GitHub repositories, written in Rust.

`githubdw` syncs pull requests (with reviews, comments, file diffs, and check
runs) and issues (with labels, milestones, and comments) from GitHub into a
star-schema SQLite database, then provides:

- A **CLI** for sync, query, metrics, fulltext search, monitoring, and configuration
- **Fulltext search** (SQLite FTS5, trigram tokenizer) over PR/issue titles, bodies, and comments
- **Metrics** with period-over-period deltas (user, repo, group) and leaderboards
- An **MCP server** (stdio) so AI assistants can query the warehouse

Data is fetched via the [`gh` CLI](https://cli.github.com/), so authentication
is entirely delegated to `gh auth`. GitHub Enterprise works via `GH_HOST`.

## Install

Requires Rust (stable) and an authenticated `gh` CLI.

```bash
cargo install --path .
```

## Quick start

```bash
# Sync a repository's pull requests and issues
githubdw sync repo octocat/hello-world

# Query merged PRs
githubdw query --repo octocat/hello-world --state merged --count

# Fulltext search
githubdw search "rate limit"

# Metrics for a user
githubdw metrics user octocat
```

The database lives at `~/.githubdw/githubdw.db` by default (override with
`--db <path>`).

## License

MIT
