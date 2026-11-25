# githubdw

[![CI](https://github.com/adlio/githubdw/actions/workflows/ci.yml/badge.svg)](https://github.com/adlio/githubdw/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/adlio/githubdw/branch/main/graph/badge.svg)](https://codecov.io/gh/adlio/githubdw)
[![crates.io](https://img.shields.io/crates/v/githubdw.svg)](https://crates.io/crates/githubdw)
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

Requires an authenticated `gh` CLI (`gh auth login`).

From [crates.io](https://crates.io/crates/githubdw) (requires Rust/Cargo):

```bash
cargo install githubdw --locked
```

Or download a prebuilt binary from the
[GitHub releases](https://github.com/adlio/githubdw/releases) page
(`x86_64`/`aarch64`, Linux musl or macOS) and verify the checksum:

```bash
curl -LO https://github.com/adlio/githubdw/releases/latest/download/githubdw-0.5.0-x86_64-unknown-linux-musl.tar.gz
curl -LO https://github.com/adlio/githubdw/releases/latest/download/githubdw-0.5.0-x86_64-unknown-linux-musl.tar.gz.sha256
sha256sum -c githubdw-0.5.0-x86_64-unknown-linux-musl.tar.gz.sha256
tar xzf githubdw-0.5.0-x86_64-unknown-linux-musl.tar.gz
```

## Quick start

```bash
# Sync a repository's pull requests and issues (first run backfills 90 days
# of coverage by default; see "Sync" below for what --days actually does)
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

The database lives at `~/.githubdw/githubdw.db` by default. Every subcommand
accepts a global `--db <path>` to use a different file.

## Sync

```bash
githubdw sync repo <owner/name> [--days N] [--skip-diffs] [--pull-requests-only | --issues-only]
githubdw sync user <login>      [--days N] [--skip-diffs]
githubdw sync all [--history-from YYYY-MM-DD] [--reconcile] [--skip-diffs] [--json]
                             # one shared collection tick over every monitored source
githubdw sync watch          # print one snapshot of any sync in progress, then exit
```

Sync is **incremental and resumable**: covered date ranges are recorded and
merged, an `updated_at` cursor stops pagination early when nothing changed, and
a stale-lock timeout recovers from crashes. `--skip-diffs` skips per-file patch
text for a much faster first pass.

`--days N` limits the *recorded coverage window*, not the fetch itself: the
PR/issue walk still pages until the source is exhausted or the incremental
cursor stops it. A repository or user synced for the first time backfills 90
days by default (`--days` overrides that starting point); the MCP
`trigger_sync` tool defaults to 30 days instead. `sync watch` is a one-shot
status check, not a live stream — run it again to refresh.

`sync repo` walks one repository's pull requests and issues. `sync user`
answers the other question — everything one person did, wherever they did it —
by searching for the pull requests they authored (`author:`) and the ones they
reviewed (`reviewed-by:`), each with its own watermark. Repositories found this
way get stored so the facts have somewhere to hang, but they are **not** added
to the monitored set: syncing a person is not an instruction to start tracking
every repository they contributed to.

Search is bounded in two ways githubdw works around rather than ignores. It
serves at most 1,000 results per query, so a backfill is partitioned into
calendar-month `created:` windows and any window still reporting more is
bisected until each part fits; if a single day is over the cap, the run says so
instead of claiming coverage it does not have. Search also has its own budget of
30 requests per minute, well under the 5,000 points/hour ordinary GraphQL gets,
so search calls are paced on their own clock.

Sync shares your `gh` sign-in, so it draws on the same hourly budgets (5,000
GraphQL points and 5,000 REST requests) as your own `gh` commands. Before each
request it reads the budget GitHub reports (response headers, or the free
`rate_limit` endpoint) and, once 95% of a budget is used, waits for that
budget's reset instead of spending the rest. Change the share per run with
`--max-rate-use 0.8` (or `80%`), or for every run with
`githubdw config set max_rate_use 0.8`; library callers use
`GhClient::set_max_rate_use`.

### Diff retention

Whether a sync stores the per-file diff (patch) text it fetches is one
setting with three values:

| Value | What a sync does |
|---|---|
| `retain` (default) | Fetches and stores patch text. `sync all` also fills in patch text missing for pull requests already stored, up to 25 per run, while less than half of the hourly REST budget is used. |
| `transient` | Fetches no patch text and stores none. Meant for a caller that reads diffs live when it needs them. |
| `never` | The same as `transient` for a sync. |

Set it per run with `--diff-retention transient`, or for every run with
`githubdw config set diff_retention transient`; the flag wins over the
config value. `--skip-diffs` is `never` for that run. Library callers pass
it to `Syncer::new` and `TickPolicy::diff_retention`.

No setting removes anything. Every write keeps what is stored: a title,
body, review, comment or patch is never replaced by an empty value, and no
stored file row is deleted, even one a force push removed from the pull
request. So two callers with different settings sharing one warehouse
accumulate: a `transient` run leaves stored diffs alone, and later `retain`
runs fill in the diffs it skipped. A pull request whose diff was stored and
then cleared with `githubdw diffs purge` is not fetched again.

Releases 0.4 to 0.6 also read a shared policy file,
`$XDG_CONFIG_HOME/adlio-collection/policy.json`, which capped every caller's
retention. That file is obsolete: 0.7 neither reads nor writes it, and it
can be deleted.

Manage what `sync all` covers:

```bash
githubdw monitor add-repo owner/name
githubdw monitor add-user login
githubdw monitor add-org org-login
githubdw monitor list
githubdw monitor remove owner/name
```

`sync all` runs one **collection tick** (`githubdw::collect::tick` in the
library), the same pass any application linking the crate runs, so two
collectors on one warehouse never repeat each other's work:

- **Detect** (when the last complete detection is over 30 minutes old). One
  light search per role names every monitored user at once (`is:pr
  author:a author:b … updated:<since>..<now>`) and returns only ids and
  timestamps, for about 1 point per 100 pull requests. `<since>` is 12 hours
  before the last detection, because GitHub's search index can filter on an
  `updatedAt` several hours older than the real one (`TickPolicy::overlap`
  changes it). One aliased query reads
  every monitored repository's newest pull-request and issue update. Only pull
  requests that are new, newer than the stored copy, stored incomplete, or
  waiting on check runs (which do not move `updatedAt`) are fetched in full, by
  node id. A pass that finds nothing new costs about 3 points. Once a day the
  walk also covers the previous 48 hours (search-index lag), checks that every
  monitored login still resolves, and retries repositories GitHub refused
  (SAML, IP allow list), which are otherwise asked only once a day.
- **Backfill** (while under half the hourly budget is used, at most 1,000
  points per tick). People whose older per-user state stops short get a
  catch-up first; then history back to `--history-from` (remembered per user)
  or 90 days for a new user, one created month at a time. Months already
  covered are skipped, so an interrupted backfill resumes.

A pull request that keeps failing to land is queued and retried each tick,
then parked after 5 attempts and retried daily, so it never holds back the
shared cursor. Detect and backfill each take their own lease in
`githubdw.db`; a run that finds the detect lease held reports who holds it
and exits 0. Every run ends with one cost line, for example
`tick: 3 points (detect 3, fetch 0, reconcile 0, logins 0, backfill 0); 0 REST calls`.
Blocked repositories, parked pull requests and unresolved logins are printed
once a day.

`sync all` prints a line for every enabled source it will not serve: an
unimplemented source type or a disabled row is named, never dropped in
silence. Monitored **orgs** are one such source: `sync all` currently names
and skips them rather than syncing their repositories.

### Re-fetching specific items

```bash
# Re-fetch exact PRs/issues through the normal hydrate/store path (1-64 at a
# time). Does not move cursors or coverage, and does not enroll monitoring.
githubdw sync targets --pr octocat/hello#1467 --issue octocat/hello#12

# Find stored items whose row counts suggest first-slice truncation
# (read-only, prints --pr/--issue arguments for `sync targets`)
githubdw sync repair-plan --repo octocat/hello

# Re-read check runs across every head commit a PR has had, storing each run
# under its own commit (idempotent — re-run the same targets to resume)
githubdw sync check-history --pr octocat/hello#1467 --json

# Find stored PRs with several check-run heads — the signature of partly
# observed history (read-only, prints --pr arguments for `sync check-history`)
githubdw sync check-history-plan --repo octocat/hello
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

Custom groups treat several users or repos as one unit. `group user` and
`group repo` take the same subcommands:

```bash
githubdw group user create core --description "core team"
githubdw group user add core alice bob       # add members
githubdw group user show core                # list a group's members
githubdw group user list                     # list all groups
githubdw group user set core alice bob carol # replace the member set
githubdw group user remove core bob          # remove members
githubdw group user delete core              # delete the group
githubdw metrics group core --period this-quarter
```

`group repo` works the same way, with repository names (`owner/name`) as
members instead of logins.

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

- `query_pull_requests` — filter by author, reviewer, repo, org, group,
  state, label, and period; `output` controls totals vs. breakdowns.
- `get_metrics` — period-over-period metrics for a user, repo, user_group, or
  repo_group.
- `search` — full-text search over synced PR/issue titles, bodies, and
  comments.
- `manage_monitors` — list, add, or remove monitored users, repos, and orgs
  (`action='list'` is the default).
- `trigger_sync` — fetch fresh PR/issue data for one repository from the
  GitHub API (`days` defaults to 30, independently of the CLI's 90-day
  default). Requires an authenticated `gh` CLI.

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
githubdw config get timezone
githubdw config set timezone America/New_York   # IANA timezone for date bucketing
githubdw config set core_hours_start 8
githubdw config set max_rate_use 0.8              # share of each GitHub budget sync may use
githubdw config set diff_retention transient      # never | transient | retain (default)
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
