-- PR AI summaries. Mirrors the CRUX warehouse CR-summary table, so each data
-- warehouse owns the summaries of the data it holds, stored alongside it.
--
-- Deliberately NOT foreign-keyed to fact_pull_requests: a resync can replace PR
-- rows, and a summary outliving a transient delete beats losing the summary.
-- Staleness compares source_updated_at against the PR updated_at, because pull
-- requests have no revision number (unlike CRUX CRs, which key staleness on it).
CREATE TABLE pr_summaries (
    pr_key                   TEXT PRIMARY KEY,  -- "{owner}/{repo}#{number}"
    repo_key                 TEXT NOT NULL,     -- "{owner}/{repo}"
    number                   INTEGER NOT NULL,
    author_key               TEXT,              -- "user:<login-lowercase>"
    headline                 TEXT NOT NULL,
    what_changed             TEXT NOT NULL,
    why_it_matters           TEXT NOT NULL,
    notability_score         INTEGER NOT NULL,
    notability_justification TEXT,
    change_types             TEXT NOT NULL,     -- JSON array
    impact_areas             TEXT NOT NULL,     -- JSON array
    complexity_signal        TEXT NOT NULL,
    notable_aspects          TEXT,              -- JSON array
    technical_keywords       TEXT,              -- JSON array
    domain_keywords          TEXT,              -- JSON array
    context_tags             TEXT,              -- JSON array
    prompt_version           INTEGER NOT NULL,
    source_updated_at        TEXT NOT NULL,
    is_stale                 INTEGER NOT NULL DEFAULT 0,
    runtime_ms               INTEGER,
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);

CREATE INDEX idx_pr_summaries_author     ON pr_summaries(author_key);
CREATE INDEX idx_pr_summaries_repo       ON pr_summaries(repo_key);
CREATE INDEX idx_pr_summaries_notability ON pr_summaries(notability_score DESC);
