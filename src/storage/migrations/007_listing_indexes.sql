-- Pull-request listings and per-author work lists. Additive only.

-- Newest-first listings (`ORDER BY created_at DESC LIMIT n`) sorted every
-- pull request in a temp B-tree after reading its full row; on a warehouse
-- whose pages are not cached that is a read of the whole table. DESC on
-- purpose: read forward, it returns rows that share a created_at in rowid
-- order, the order the scan-and-sort returned, so listings do not reorder.
CREATE INDEX IF NOT EXISTS idx_pr_created_at
    ON fact_pull_requests(created_at DESC);

-- A cohort's pull requests in a date window with their repository and
-- title (what a work list shows), read from the index alone.
CREATE INDEX IF NOT EXISTS idx_pr_author_created_title
    ON fact_pull_requests(author_key, created_date_key, repo_key, title);
