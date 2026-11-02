-- Covering indexes for per-author activity counts over a date window: pull
-- requests opened (with their size), reviews submitted and review comments
-- written. The single-column author/reviewer indexes made every match read
-- its full row. With these SQLite reads index pages only, and picks them
-- without statistics. Additive only.
CREATE INDEX IF NOT EXISTS idx_pr_author_created_cover
    ON fact_pull_requests(author_key, created_date_key, additions, deletions, changed_files, pr_key);
CREATE INDEX IF NOT EXISTS idx_reviews_reviewer_cover
    ON fact_reviews(reviewer_key, submitted_date_key, pr_key);
CREATE INDEX IF NOT EXISTS idx_review_comments_author_cover
    ON fact_review_comments(author_key, created_date_key, pr_key);
