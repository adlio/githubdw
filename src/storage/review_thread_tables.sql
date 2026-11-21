-- Additive review-thread storage. Kept outside numbered migrations so older
-- binaries can continue opening the warehouse and ignore these objects.
CREATE TABLE IF NOT EXISTS fact_review_threads (
    thread_key        TEXT PRIMARY KEY,
    pr_key            TEXT NOT NULL REFERENCES fact_pull_requests(pr_key),
    is_resolved       INTEGER NOT NULL CHECK (is_resolved IN (0, 1)),
    is_outdated       INTEGER NOT NULL CHECK (is_outdated IN (0, 1)),
    resolution_reason TEXT,
    resolved_by_key   TEXT REFERENCES dim_entities(entity_key)
);

CREATE INDEX IF NOT EXISTS idx_review_threads_pr ON fact_review_threads(pr_key);
