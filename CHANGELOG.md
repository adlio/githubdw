# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/adlio/githubdw/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/adlio/githubdw/releases/tag/v0.1.0
