-- githubdw migration 001: dimensions, facts, issues, FTS5, sync/monitoring/config

-- ===== Dimension tables =====

CREATE TABLE dim_entities (
    entity_key    TEXT PRIMARY KEY,
    entity_type   TEXT NOT NULL,
    login         TEXT,
    is_human      INTEGER NOT NULL,
    is_bot        INTEGER NOT NULL,
    name          TEXT NOT NULL
);

CREATE INDEX idx_entities_type  ON dim_entities(entity_type);
CREATE INDEX idx_entities_login ON dim_entities(login);

CREATE TABLE dim_repositories (
    repo_key         TEXT PRIMARY KEY,
    owner            TEXT NOT NULL,
    name             TEXT NOT NULL,
    primary_language TEXT,
    is_fork          INTEGER NOT NULL DEFAULT 0,
    is_private       INTEGER NOT NULL DEFAULT 0,
    default_branch   TEXT,
    created_at       TEXT,
    UNIQUE (owner, name)
);

CREATE INDEX idx_repositories_language ON dim_repositories(primary_language);

CREATE TABLE dim_date (
    date_key         TEXT PRIMARY KEY,
    year             INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,
    month            INTEGER NOT NULL,
    day_of_month     INTEGER NOT NULL,
    day_of_week      INTEGER NOT NULL,
    is_weekend       INTEGER NOT NULL,
    week_of_year     INTEGER NOT NULL,
    week_key         TEXT NOT NULL,
    month_key        TEXT NOT NULL,
    quarter_key      TEXT NOT NULL,
    year_key         TEXT NOT NULL,
    half_key         TEXT NOT NULL,
    day_of_quarter   INTEGER NOT NULL DEFAULT 0,
    day_of_year      INTEGER NOT NULL DEFAULT 0,
    day_of_half      INTEGER NOT NULL DEFAULT 0,
    week_of_quarter  INTEGER NOT NULL DEFAULT 0,
    month_of_quarter INTEGER NOT NULL DEFAULT 0,
    month_of_half    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE dim_period (
    period_key     TEXT PRIMARY KEY,
    period_type    TEXT NOT NULL,
    start_date_key TEXT NOT NULL,
    end_date_key   TEXT NOT NULL,
    year           INTEGER NOT NULL,
    period_number  INTEGER NOT NULL
);

CREATE TABLE dim_time (
    time_key      TEXT PRIMARY KEY,
    hour          INTEGER NOT NULL,
    hour_12       INTEGER NOT NULL,
    am_pm         TEXT NOT NULL,
    time_bucket   TEXT NOT NULL,
    is_core_hours INTEGER NOT NULL
);

-- ===== Fact tables (pull-request grain) =====

CREATE TABLE fact_pull_requests (
    pr_key           TEXT PRIMARY KEY,
    number           INTEGER NOT NULL,
    repo_key         TEXT NOT NULL REFERENCES dim_repositories(repo_key),
    author_key       TEXT NOT NULL REFERENCES dim_entities(entity_key),
    state            TEXT NOT NULL,
    is_draft         INTEGER NOT NULL DEFAULT 0,
    title            TEXT,
    body             TEXT,
    base_ref         TEXT,
    head_ref         TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT,
    merged_at        TEXT,
    closed_at        TEXT,
    merged_by_key    TEXT REFERENCES dim_entities(entity_key),
    created_date_key TEXT NOT NULL REFERENCES dim_date(date_key),
    created_time_key TEXT NOT NULL REFERENCES dim_time(time_key),
    updated_date_key TEXT REFERENCES dim_date(date_key),
    updated_time_key TEXT REFERENCES dim_time(time_key),
    merged_date_key  TEXT REFERENCES dim_date(date_key),
    comment_count    INTEGER NOT NULL DEFAULT 0,
    review_count     INTEGER NOT NULL DEFAULT 0,
    changed_files    INTEGER NOT NULL DEFAULT 0,
    additions        INTEGER NOT NULL DEFAULT 0,
    deletions        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_pr_repo         ON fact_pull_requests(repo_key);
CREATE INDEX idx_pr_author       ON fact_pull_requests(author_key);
CREATE INDEX idx_pr_state        ON fact_pull_requests(state);
CREATE INDEX idx_pr_created_date ON fact_pull_requests(created_date_key);
CREATE INDEX idx_pr_merged_date  ON fact_pull_requests(merged_date_key);
CREATE INDEX idx_pr_updated      ON fact_pull_requests(updated_at);

CREATE TABLE fact_reviews (
    review_key         TEXT PRIMARY KEY,
    pr_key             TEXT NOT NULL REFERENCES fact_pull_requests(pr_key),
    reviewer_key       TEXT NOT NULL REFERENCES dim_entities(entity_key),
    state              TEXT NOT NULL,
    body               TEXT,
    submitted_at       TEXT NOT NULL,
    submitted_date_key TEXT NOT NULL REFERENCES dim_date(date_key),
    submitted_time_key TEXT NOT NULL REFERENCES dim_time(time_key)
);

CREATE INDEX idx_reviews_pr       ON fact_reviews(pr_key);
CREATE INDEX idx_reviews_reviewer ON fact_reviews(reviewer_key);
CREATE INDEX idx_reviews_state    ON fact_reviews(state);

CREATE TABLE fact_review_comments (
    comment_key      TEXT PRIMARY KEY,
    pr_key           TEXT NOT NULL REFERENCES fact_pull_requests(pr_key),
    review_key       TEXT REFERENCES fact_reviews(review_key),
    author_key       TEXT NOT NULL REFERENCES dim_entities(entity_key),
    in_reply_to      TEXT,
    path             TEXT,
    line             INTEGER,
    body             TEXT,
    created_at       TEXT NOT NULL,
    created_date_key TEXT NOT NULL REFERENCES dim_date(date_key),
    created_time_key TEXT NOT NULL REFERENCES dim_time(time_key)
);

CREATE INDEX idx_review_comments_pr       ON fact_review_comments(pr_key);
CREATE INDEX idx_review_comments_author   ON fact_review_comments(author_key);
CREATE INDEX idx_review_comments_reply_to ON fact_review_comments(in_reply_to);

CREATE TABLE fact_issue_comments (
    comment_key      TEXT PRIMARY KEY,
    parent_type      TEXT NOT NULL,
    parent_key       TEXT NOT NULL,
    author_key       TEXT NOT NULL REFERENCES dim_entities(entity_key),
    in_reply_to      TEXT,
    body             TEXT,
    created_at       TEXT NOT NULL,
    created_date_key TEXT NOT NULL REFERENCES dim_date(date_key),
    created_time_key TEXT NOT NULL REFERENCES dim_time(time_key)
);

CREATE INDEX idx_issue_comments_parent ON fact_issue_comments(parent_type, parent_key);
CREATE INDEX idx_issue_comments_author ON fact_issue_comments(author_key);

CREATE TABLE fact_check_runs (
    check_run_key    TEXT PRIMARY KEY,
    pr_key           TEXT NOT NULL REFERENCES fact_pull_requests(pr_key),
    head_sha         TEXT NOT NULL,
    name             TEXT NOT NULL,
    status           TEXT NOT NULL,
    conclusion       TEXT,
    started_at       TEXT,
    completed_at     TEXT
);

CREATE INDEX idx_check_runs_pr         ON fact_check_runs(pr_key);
CREATE INDEX idx_check_runs_conclusion ON fact_check_runs(conclusion);

CREATE TABLE fact_file_diffs (
    file_diff_key  TEXT PRIMARY KEY,
    pr_key         TEXT NOT NULL REFERENCES fact_pull_requests(pr_key),
    repo_key       TEXT NOT NULL REFERENCES dim_repositories(repo_key),
    file_path      TEXT NOT NULL,
    previous_path  TEXT,
    change_type    TEXT NOT NULL,
    patch          TEXT,
    additions      INTEGER NOT NULL DEFAULT 0,
    deletions      INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_file_diffs_pr   ON fact_file_diffs(pr_key);
CREATE INDEX idx_file_diffs_repo ON fact_file_diffs(repo_key);
CREATE INDEX idx_file_diffs_path ON fact_file_diffs(file_path);

-- ===== GitHub Issues tables =====

CREATE TABLE issues (
    id               TEXT PRIMARY KEY,
    repo_key         TEXT NOT NULL REFERENCES dim_repositories(repo_key),
    number           INTEGER NOT NULL,
    title            TEXT NOT NULL DEFAULT '',
    body             TEXT,
    state            TEXT NOT NULL DEFAULT 'open',
    state_reason     TEXT,
    author_key       TEXT REFERENCES dim_entities(entity_key),
    milestone_id     TEXT REFERENCES milestones(id),
    comment_count    INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    closed_at        TEXT,
    created_date_key TEXT REFERENCES dim_date(date_key),
    UNIQUE (repo_key, number)
);

CREATE INDEX idx_issues_repo    ON issues(repo_key);
CREATE INDEX idx_issues_state   ON issues(state);
CREATE INDEX idx_issues_author  ON issues(author_key);
CREATE INDEX idx_issues_updated ON issues(repo_key, updated_at);

CREATE TABLE labels (
    id          TEXT PRIMARY KEY,
    repo_key    TEXT NOT NULL REFERENCES dim_repositories(repo_key),
    name        TEXT NOT NULL DEFAULT '',
    color       TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_labels_repo ON labels(repo_key);

CREATE TABLE issue_labels (
    issue_id TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    label_id TEXT NOT NULL REFERENCES labels(id),
    PRIMARY KEY (issue_id, label_id)
);

CREATE INDEX idx_issue_labels_label ON issue_labels(label_id);

CREATE TABLE milestones (
    id          TEXT PRIMARY KEY,
    repo_key    TEXT NOT NULL REFERENCES dim_repositories(repo_key),
    number      INTEGER NOT NULL,
    title       TEXT NOT NULL DEFAULT '',
    description TEXT,
    state       TEXT NOT NULL DEFAULT 'open',
    due_on      TEXT,
    created_at  TEXT,
    UNIQUE (repo_key, number)
);

CREATE INDEX idx_milestones_repo ON milestones(repo_key);

CREATE TABLE issue_assignees (
    issue_id   TEXT NOT NULL REFERENCES issues(id) ON DELETE CASCADE,
    entity_key TEXT NOT NULL REFERENCES dim_entities(entity_key),
    PRIMARY KEY (issue_id, entity_key)
);

CREATE INDEX idx_issue_assignees_entity ON issue_assignees(entity_key);

-- ===== Full-text search (FTS5 trigram) =====

CREATE VIRTUAL TABLE pull_requests_fts USING fts5(
    pr_key UNINDEXED,
    title,
    body,
    tokenize = 'trigram'
);

CREATE TRIGGER pull_requests_fts_insert AFTER INSERT ON fact_pull_requests
BEGIN
    DELETE FROM pull_requests_fts WHERE pr_key = new.pr_key;
    INSERT INTO pull_requests_fts (pr_key, title, body)
    VALUES (new.pr_key, COALESCE(new.title, ''), COALESCE(new.body, ''));
END;

CREATE TRIGGER pull_requests_fts_update AFTER UPDATE ON fact_pull_requests
BEGIN
    DELETE FROM pull_requests_fts WHERE pr_key = new.pr_key;
    INSERT INTO pull_requests_fts (pr_key, title, body)
    VALUES (new.pr_key, COALESCE(new.title, ''), COALESCE(new.body, ''));
END;

CREATE TRIGGER pull_requests_fts_delete AFTER DELETE ON fact_pull_requests
BEGIN
    DELETE FROM pull_requests_fts WHERE pr_key = old.pr_key;
END;

CREATE VIRTUAL TABLE issues_fts USING fts5(
    id UNINDEXED,
    title,
    body,
    tokenize = 'trigram'
);

CREATE TRIGGER issues_fts_insert AFTER INSERT ON issues
BEGIN
    DELETE FROM issues_fts WHERE id = new.id;
    INSERT INTO issues_fts (id, title, body)
    VALUES (new.id, new.title, COALESCE(new.body, ''));
END;

CREATE TRIGGER issues_fts_update AFTER UPDATE ON issues
BEGIN
    DELETE FROM issues_fts WHERE id = new.id;
    INSERT INTO issues_fts (id, title, body)
    VALUES (new.id, new.title, COALESCE(new.body, ''));
END;

CREATE TRIGGER issues_fts_delete AFTER DELETE ON issues
BEGIN
    DELETE FROM issues_fts WHERE id = old.id;
END;

CREATE VIRTUAL TABLE review_comments_fts USING fts5(
    comment_key UNINDEXED,
    body,
    tokenize = 'trigram'
);

CREATE TRIGGER review_comments_fts_insert AFTER INSERT ON fact_review_comments
BEGIN
    DELETE FROM review_comments_fts WHERE comment_key = new.comment_key;
    INSERT INTO review_comments_fts (comment_key, body)
    VALUES (new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER review_comments_fts_update AFTER UPDATE ON fact_review_comments
BEGIN
    DELETE FROM review_comments_fts WHERE comment_key = new.comment_key;
    INSERT INTO review_comments_fts (comment_key, body)
    VALUES (new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER review_comments_fts_delete AFTER DELETE ON fact_review_comments
BEGIN
    DELETE FROM review_comments_fts WHERE comment_key = old.comment_key;
END;

CREATE VIRTUAL TABLE issue_comments_fts USING fts5(
    comment_key UNINDEXED,
    body,
    tokenize = 'trigram'
);

CREATE TRIGGER issue_comments_fts_insert AFTER INSERT ON fact_issue_comments
BEGIN
    DELETE FROM issue_comments_fts WHERE comment_key = new.comment_key;
    INSERT INTO issue_comments_fts (comment_key, body)
    VALUES (new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER issue_comments_fts_update AFTER UPDATE ON fact_issue_comments
BEGIN
    DELETE FROM issue_comments_fts WHERE comment_key = new.comment_key;
    INSERT INTO issue_comments_fts (comment_key, body)
    VALUES (new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER issue_comments_fts_delete AFTER DELETE ON fact_issue_comments
BEGIN
    DELETE FROM issue_comments_fts WHERE comment_key = old.comment_key;
END;

-- ===== Sync-tracking tables =====

CREATE TABLE sync_metadata (
    source_type          TEXT NOT NULL,
    source_id            TEXT NOT NULL,
    last_sync_at         TEXT,
    last_updated_cursor  TEXT,
    PRIMARY KEY (source_type, source_id)
);

CREATE TABLE sync_locks (
    entity_key         TEXT PRIMARY KEY,
    started_at         TEXT NOT NULL,
    total_items        INTEGER,
    current_item       INTEGER DEFAULT 0,
    current_item_id    TEXT,
    synced             INTEGER DEFAULT 0,
    skipped            INTEGER DEFAULT 0,
    failed             INTEGER DEFAULT 0,
    chunk_start_date   TEXT,
    chunk_end_date     TEXT,
    current_chunk      INTEGER DEFAULT 1,
    total_chunks       INTEGER DEFAULT 1,
    cumulative_synced  INTEGER DEFAULT 0,
    cumulative_skipped INTEGER DEFAULT 0,
    cumulative_failed  INTEGER DEFAULT 0
);

CREATE TABLE sync_jobs (
    entity_key   TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'running',
    started_at   TEXT NOT NULL,
    completed_at TEXT,
    synced       INTEGER DEFAULT 0,
    skipped      INTEGER DEFAULT 0,
    failed_items TEXT,
    error        TEXT
);

CREATE INDEX idx_sync_jobs_status ON sync_jobs(status);

CREATE TABLE synced_ranges (
    entity_key TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date   TEXT NOT NULL,
    synced_at  TEXT NOT NULL,
    item_count INTEGER DEFAULT 0,
    PRIMARY KEY (entity_key, start_date, end_date)
);

CREATE INDEX idx_synced_ranges_entity       ON synced_ranges(entity_key);
CREATE INDEX idx_synced_ranges_entity_dates ON synced_ranges(entity_key, start_date, end_date);

-- ===== Monitoring tables =====

CREATE TABLE monitored_users (
    login        TEXT PRIMARY KEY,
    added_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_sync_at TEXT,
    sync_enabled INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_monitored_users_enabled ON monitored_users(sync_enabled) WHERE sync_enabled = 1;

CREATE TABLE monitored_repos (
    repo_key     TEXT PRIMARY KEY,
    added_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_sync_at TEXT,
    sync_enabled INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_monitored_repos_enabled ON monitored_repos(sync_enabled) WHERE sync_enabled = 1;

CREATE TABLE monitored_orgs (
    org_login    TEXT PRIMARY KEY,
    added_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_sync_at TEXT,
    sync_enabled INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_monitored_orgs_enabled ON monitored_orgs(sync_enabled) WHERE sync_enabled = 1;

-- ===== Config =====

CREATE TABLE config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO config (key, value) VALUES
    ('timezone', 'America/Los_Angeles'),
    ('core_hours_start', '9'),
    ('core_hours_end', '17'),
    ('bot_login_suffix', '[bot]');
