-- Covering indexes for the repository metrics and leaderboards. Additive
-- only: no table, row or existing index changes.
--
-- Without them, each repository metric reached its rows through
-- single-column indexes and then read every wide row they pointed at (pull
-- request titles and bodies, comment bodies). On a warehouse that has not
-- been read recently, every query of `repo_metrics` / `repo_aggregations` paid
-- for that disk I/O: on a large warehouse, ten to ninety seconds per query
-- before the operating system had the pages cached. With these, the same
-- queries read only index pages.

-- A repository's pull requests by creation date, with what the metrics read
-- from them (state for merges, author for leaderboards, pr_key to reach
-- reviews, comments and check runs).
CREATE INDEX IF NOT EXISTS idx_pr_repo_created_cover
    ON fact_pull_requests(repo_key, created_date_key, state, author_key, pr_key);

-- Check-run outcomes per pull request, for the check failure rate.
CREATE INDEX IF NOT EXISTS idx_check_runs_pr_conclusion
    ON fact_check_runs(pr_key, conclusion);

-- Comments and reviews per pull request, with their date and author.
CREATE INDEX IF NOT EXISTS idx_issue_comments_parent_cover
    ON fact_issue_comments(parent_type, parent_key, created_date_key, author_key);
CREATE INDEX IF NOT EXISTS idx_review_comments_pr_cover
    ON fact_review_comments(pr_key, created_date_key, author_key);
CREATE INDEX IF NOT EXISTS idx_reviews_pr_cover
    ON fact_reviews(pr_key, submitted_date_key, reviewer_key);
