# githubdw

[![CI](https://github.com/adlio/githubdw/actions/workflows/ci.yml/badge.svg)](https://github.com/adlio/githubdw/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A local, SQLite-based data warehouse for GitHub repositories, written in Rust.

`githubdw` syncs pull requests (with reviews, comments, file diffs, and check
runs) and issues (with labels, milestones, and comments) from GitHub into a
star-schema SQLite database, then answers questions about them offline:

- **CLI** for sync, query, metrics, fulltext search, monitoring, and configuration
- **Fulltext search** (SQLite FTS5, trigram tokenizer) over PR/issue titles,
  bodies, and comments — substrings of 3+ characters match
- **Metrics** with period-over-period deltas (user, repo, custom groups) and
  ranked leaderboards with rank-movement tracking
- **MCP server** (stdio) so AI assistants can query the warehouse

Data is fetched via the [`gh` CLI](https://cli.github.com/) as a subprocess, so
authentication is entirely delegated to `gh auth` — githubdw never sees a token.

## Install

Requires Rust (stable) and an authenticated `gh` CLI (`gh auth login`).

```bash
cargo install --path .
```

## Quick start

```bash
# Sync a repository's pull requests and issues (first run backfills 90 days;
# use --days to widen the window)
githubdw sync repo adlio/mixtape --days 365

# Re-running is an incremental no-op — only changed PRs are fetched
githubdw sync repo adlio/mixtape

# Query merged PRs
githubdw query --repo adlio/mixtape --state merged --output count

# Break a query down
githubdw query --repo adlio/mixtape --output count-by-state

# Fulltext search (trigram: partial words match too)
githubdw search "rate limit"

# Metrics with period-over-period deltas
githubdw metrics repo adlio/mixtape --period this-quarter
githubdw metrics user adlio --period 2026-Q2 --json
```

The database lives at `~/.githubdw/githubdw.db` by default (override with
`--db <path>`).

## Sync

```bash
githubdw sync repo <owner/name> [--days N] [--skip-diffs] [--pull-requests-only | --issues-only]
githubdw sync all            # sync every enabled monitored source
githubdw sync watch          # live progress of a running sync
```

Sync is **incremental and resumable**: covered date ranges are recorded and
merged, an `updated_at` cursor stops pagination early when nothing changed, and
a stale-lock timeout recovers from crashes. `--skip-diffs` skips per-file patch
text for a much faster first pass.

Manage what `sync all` covers:

```bash
githubdw monitor add-repo owner/name
githubdw monitor add-user login
githubdw monitor list
githubdw monitor remove owner/name
```

## Query

```bash
githubdw query [--author L] [--reviewer L] [--repo R] [--org O] [--group G]
               [--label X] [--state open|merged|closed] [--merged]
               [--period 2026-Q1] [--since D] [--until D]
               [--limit N] [--offset N]
               [--output table|json|csv|count|count-by-author|count-by-repo|count-by-state|count-by-period]
```

Periods accept `2026`, `2026-H1`, `2026-Q1`, `2026-01`, `2026-W02`, `last-30`,
and relative forms (`this-quarter`, `previous-month`, ...).

## Metrics

```bash
githubdw metrics user <login>       [--period P] [--top N] [--json]
githubdw metrics repo <owner/name>  [--period P] [--top N] [--json]
githubdw metrics group <name>       [--period P] [--json]
```

Every headline number is compared against the previous period —
`42 (+8, +24%)` — and leaderboards report rank movement
(`#1 alice — 42 (+8) [was #3]`). When the current period is still in progress,
the previous period is truncated to the same number of elapsed days so
comparisons stay apples-to-apples.

Custom groups treat several users or repos as one unit:

```bash
githubdw group user create core --description "core team"
githubdw group user add core alice bob
githubdw metrics group core --period this-quarter
```

## Fulltext search

```bash
githubdw search "connection pool" [--pull-requests|--issues|--comments]
                [--repo owner/name] [--limit N] [--json]
```

Trigram indexing means partial words match (`edrock` finds "Bedrock"), and
results include a highlighted snippet. Diff/patch text is deliberately not
indexed.

## MCP server (AI assistants)

`githubdw mcp` starts a Model Context Protocol server on stdio with five tools:
`query_pull_requests`, `get_metrics`, `search`, `manage_monitors`, and
`trigger_sync`.

Example client configuration (any MCP-capable assistant):

```json
{
  "mcpServers": {
    "githubdw": {
      "command": "githubdw",
      "args": ["mcp"]
    }
  }
}
```

Row-returning tools cap output at 50 rows by default (page with `offset`) and
include a `_meta.result_count` so the model can tell a capped result from a
complete one.

## GitHub Enterprise

Everything goes through the `gh` CLI, so GitHub Enterprise works by pointing
`gh` at your host:

```bash
GH_HOST=github.example.com githubdw sync repo owner/name
```

## Configuration

```bash
githubdw config list
githubdw config set timezone America/New_York   # IANA timezone for date bucketing
githubdw config set core_hours_start 8
```

Timestamps are stored as UTC; date/time dimension keys are derived in the
configured timezone (DST-correct), so "activity by day / by hour" reflects your
wall clock.

## Development

```bash
make ci     # fmt-check + clippy -D warnings + test + release build
make test
make lint
```

## License

MIT
