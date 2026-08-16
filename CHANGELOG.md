# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-08-16

### Fixed

- Relative periods (`this-week`, `this-month`, `this-quarter`, `this-year`,
  `previous-*`, `last-N`) resolved against the UTC calendar date while every
  `*_date_key` column in the warehouse is built in the configured IANA
  timezone. During the hours when the two calendars disagree — up to 14 a day,
  7 for a US Pacific warehouse — this shifted every window by one day: a
  rolling window silently included a day still in progress and dropped a
  complete one, and at a quarter, month, or year boundary the wrong period was
  reported entirely (asking for `this-quarter` on the last evening of a quarter
  answered about the next one). "Today" is now resolved in the warehouse's own
  zone, DST-aware, everywhere it is used: period parsing in the CLI and MCP
  server, and `MetricsEngine`'s reference date, which drives partial-period
  truncation for period-over-period deltas. Explicit `--since` / `--until`
  dates are unchanged: a bare `YYYY-MM-DD` a person types already means their
  local day, which is the calendar the keys use.
- `synced_ranges` recorded coverage through the day that was still in
  progress, and computed that day in UTC. A range that claims a running day is
  a claim the data does not support: an item created later that same local day
  falls inside a window recorded as fully covered, so any reader that trusts
  the record would never fetch it. Coverage now ends at the last *completed*
  day in the configured zone, and is clamped again on read — so a database
  written by an earlier version is interpreted honestly, with the trailing day
  resurfacing as a gap rather than being silently skipped. No migration is
  needed; the repair happens in place on the next write.
- The incremental sync cursor stopped at `updated_at <= cursor`, which
  permanently dropped an item updated in the *same second* as the cursor but
  served after the previous run's last page. Only a further update — which
  moves its timestamp — could ever surface it again. The stop is now strict, so
  the boundary second is re-read; re-ingesting it costs nothing because the
  upserts are idempotent.
- The cursor advanced past items whose upsert *failed*, moving the watermark
  over data the warehouse does not hold. The next run's stop condition then
  fired before reaching the item, turning a single reported failure into a
  permanent hole with no way to self-heal. The cursor now advances only past
  items that actually persisted, so a failure is retried on the following run.

### Changed

- `Period::parse` is deprecated: it resolves relative periods against the UTC
  date, which is only correct for a warehouse configured to UTC. Use
  `Period::parse_in_zone` / `Period::parse_as_of`, or
  `Period::parse_with_reference` with `storage::time_dimension::today`.
- `--days` on `sync` is documented for what it actually does: it sets the
  recorded coverage window, not a bound on the fetch. The fetch is governed by
  the incremental cursor and pages until the source is exhausted.

### Added

- `storage::time_dimension::today` / `today_as_of` / `last_complete_day` /
  `last_complete_day_as_of` / `local_date_for`: the one rule relating an
  instant to a `date_key`, matching how stored facts are keyed.
- Instant-anchored twins of the coverage functions
  (`record_range_as_of`, `gaps_as_of`, `coverage_extent_as_of`) and
  `Syncer::as_of`, so completeness arithmetic is deterministic under test
  instead of reading the wall clock.
- `MetricsEngine::reference_date` accessor.

## [0.2.1] - 2026-08-15

### Fixed

- Restored the git hash in `--version` output. The 0.2.0 release notes
  document this feature, but the shipped 0.2.0 artifacts do not contain
  it: the summaries PR was squashed against a stale base and silently
  reverted it. `githubdw --version` once again prints
  `githubdw <version> (<short-hash>)`.

### Changed

- Linux release binaries are now statically linked (musl targets). The
  0.2.0 gnu binaries required glibc >= 2.39 and failed to load on older
  hosts; the musl binaries have no runtime library requirements.

## [0.2.0] - 2026-08-15

### Added

- Opt-in AI summaries for pull requests (`--features summaries`): agentic
  single-PR summarization storing a headline, what-changed / why-it-matters
  prose, a 0-10 notability score, and change-type / impact-area / complexity
  classification in a new `pr_summaries` table (migration 003). Ships two
  agent tools: `query_database` (read-only SQL, enforced at the prepared-
  statement level via `sqlite3_stmt_readonly`, with row caps applied during
  collection) and `write_pr_summary` (upsert that preserves `created_at`).
  Default builds compile none of the LLM dependencies.
- `githubdw --version` now embeds the build's short git hash, e.g.
  `githubdw 0.2.0 (03574c8)`, so installed binaries are auditable across
  machines. Builds outside a git checkout (crates.io tarballs) report
  `(unknown)`.
- Tagged releases now build and attach binary tarballs with sha256 checksums
  for Linux x86_64 / aarch64 and macOS arm64 / x86_64.

### Changed

- MCP server pinned to rmcp 0.12 with a hand-written tool router, keeping the
  crate resolvable from vendored and mirrored crates.io registries.

## [0.1.0] - 2026-07-24

### Added

- Star-schema SQLite warehouse: conformed date/time/entity/repository
  dimensions, PR-grain facts (reviews, review comments, conversation comments,
  check runs, file diffs), and issue tables (labels, milestones, assignees)
- Incremental, resumable sync via the `gh` CLI: synced-range merging with gap
  detection, `updated_at` cursor early-stop, stale-lock crash recovery, live
  `sync watch` progress, adaptive GraphQL page-size degradation for very
  large repositories
- Fluent `QueryBuilder` with author/reviewer/repo/org/group/state/label/period
  filters and table/CSV/JSON/count/count-by-* outputs
- Metrics engine: period-over-period deltas (user, repo, custom groups) with
  apples-to-apples partial-period truncation and ranked leaderboards with
  rank-movement tracking
- Fulltext search (SQLite FTS5, trigram tokenizer) over PR/issue titles,
  bodies, and comments with highlighted snippets
- MCP stdio server with five tools: `query_pull_requests`, `get_metrics`,
  `search`, `manage_monitors`, `trigger_sync`
- Custom user and repository groups
- Configurable timezone-aware date bucketing (DST-correct)

[Unreleased]: https://github.com/adlio/githubdw/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/adlio/githubdw/releases/tag/v0.2.2
[0.1.0]: https://github.com/adlio/githubdw/releases/tag/v0.1.0
