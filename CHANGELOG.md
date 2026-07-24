# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
