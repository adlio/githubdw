-- githubdw migration 008: make full-text index maintenance O(log n) per write.
--
-- Migration 001's triggers found a row's FTS entry with
-- `DELETE FROM <x>_fts WHERE <key> = ...`. The key is an UNINDEXED FTS5
-- column, and FTS5 has no index on such a column, so every upsert of a PR,
-- issue or comment scanned the whole FTS table. With 259k issue comments that
-- scan made one `sync all` run exceed two hours, and it grew with the
-- warehouse.
--
-- Each FTS row now carries the rowid of its source row, so the triggers reach
-- it by rowid lookup. Two further rules:
--
--   * The update triggers fire only when an indexed column changed. A resync
--     upserts every row it fetches, and most of those writes change nothing
--     the index holds.
--   * The delete also matches the key column. Source tables use TEXT primary
--     keys, and VACUUM may renumber their rowids. If that ever happens the
--     guarded delete removes nothing, and the insert that follows collides with
--     an existing FTS rowid and fails the write, rather than silently removing
--     another row's index entry.
--
-- The FTS tables are rebuilt from their source tables, which also repairs any
-- drift in the old ones.

DROP TRIGGER pull_requests_fts_insert;
DROP TRIGGER pull_requests_fts_update;
DROP TRIGGER pull_requests_fts_delete;
DROP TRIGGER issues_fts_insert;
DROP TRIGGER issues_fts_update;
DROP TRIGGER issues_fts_delete;
DROP TRIGGER review_comments_fts_insert;
DROP TRIGGER review_comments_fts_update;
DROP TRIGGER review_comments_fts_delete;
DROP TRIGGER issue_comments_fts_insert;
DROP TRIGGER issue_comments_fts_update;
DROP TRIGGER issue_comments_fts_delete;

DROP TABLE pull_requests_fts;
DROP TABLE issues_fts;
DROP TABLE review_comments_fts;
DROP TABLE issue_comments_fts;

-- ===== Pull requests =====

CREATE VIRTUAL TABLE pull_requests_fts USING fts5(
    pr_key UNINDEXED,
    title,
    body,
    tokenize = 'trigram'
);

INSERT INTO pull_requests_fts (rowid, pr_key, title, body)
SELECT rowid, pr_key, COALESCE(title, ''), COALESCE(body, '') FROM fact_pull_requests;

CREATE TRIGGER pull_requests_fts_insert AFTER INSERT ON fact_pull_requests
BEGIN
    INSERT INTO pull_requests_fts (rowid, pr_key, title, body)
    VALUES (new.rowid, new.pr_key, COALESCE(new.title, ''), COALESCE(new.body, ''));
END;

CREATE TRIGGER pull_requests_fts_update AFTER UPDATE OF pr_key, title, body ON fact_pull_requests
WHEN old.pr_key IS NOT new.pr_key OR old.title IS NOT new.title OR old.body IS NOT new.body
BEGIN
    DELETE FROM pull_requests_fts WHERE rowid = old.rowid AND pr_key = old.pr_key;
    INSERT INTO pull_requests_fts (rowid, pr_key, title, body)
    VALUES (new.rowid, new.pr_key, COALESCE(new.title, ''), COALESCE(new.body, ''));
END;

CREATE TRIGGER pull_requests_fts_delete AFTER DELETE ON fact_pull_requests
BEGIN
    DELETE FROM pull_requests_fts WHERE rowid = old.rowid AND pr_key = old.pr_key;
END;

-- ===== Issues =====

CREATE VIRTUAL TABLE issues_fts USING fts5(
    id UNINDEXED,
    title,
    body,
    tokenize = 'trigram'
);

INSERT INTO issues_fts (rowid, id, title, body)
SELECT rowid, id, title, COALESCE(body, '') FROM issues;

CREATE TRIGGER issues_fts_insert AFTER INSERT ON issues
BEGIN
    INSERT INTO issues_fts (rowid, id, title, body)
    VALUES (new.rowid, new.id, new.title, COALESCE(new.body, ''));
END;

CREATE TRIGGER issues_fts_update AFTER UPDATE OF id, title, body ON issues
WHEN old.id IS NOT new.id OR old.title IS NOT new.title OR old.body IS NOT new.body
BEGIN
    DELETE FROM issues_fts WHERE rowid = old.rowid AND id = old.id;
    INSERT INTO issues_fts (rowid, id, title, body)
    VALUES (new.rowid, new.id, new.title, COALESCE(new.body, ''));
END;

CREATE TRIGGER issues_fts_delete AFTER DELETE ON issues
BEGIN
    DELETE FROM issues_fts WHERE rowid = old.rowid AND id = old.id;
END;

-- ===== Review comments =====

CREATE VIRTUAL TABLE review_comments_fts USING fts5(
    comment_key UNINDEXED,
    body,
    tokenize = 'trigram'
);

INSERT INTO review_comments_fts (rowid, comment_key, body)
SELECT rowid, comment_key, COALESCE(body, '') FROM fact_review_comments;

CREATE TRIGGER review_comments_fts_insert AFTER INSERT ON fact_review_comments
BEGIN
    INSERT INTO review_comments_fts (rowid, comment_key, body)
    VALUES (new.rowid, new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER review_comments_fts_update AFTER UPDATE OF comment_key, body ON fact_review_comments
WHEN old.comment_key IS NOT new.comment_key OR old.body IS NOT new.body
BEGIN
    DELETE FROM review_comments_fts WHERE rowid = old.rowid AND comment_key = old.comment_key;
    INSERT INTO review_comments_fts (rowid, comment_key, body)
    VALUES (new.rowid, new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER review_comments_fts_delete AFTER DELETE ON fact_review_comments
BEGIN
    DELETE FROM review_comments_fts WHERE rowid = old.rowid AND comment_key = old.comment_key;
END;

-- ===== Issue / PR conversation comments =====

CREATE VIRTUAL TABLE issue_comments_fts USING fts5(
    comment_key UNINDEXED,
    body,
    tokenize = 'trigram'
);

INSERT INTO issue_comments_fts (rowid, comment_key, body)
SELECT rowid, comment_key, COALESCE(body, '') FROM fact_issue_comments;

CREATE TRIGGER issue_comments_fts_insert AFTER INSERT ON fact_issue_comments
BEGIN
    INSERT INTO issue_comments_fts (rowid, comment_key, body)
    VALUES (new.rowid, new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER issue_comments_fts_update AFTER UPDATE OF comment_key, body ON fact_issue_comments
WHEN old.comment_key IS NOT new.comment_key OR old.body IS NOT new.body
BEGIN
    DELETE FROM issue_comments_fts WHERE rowid = old.rowid AND comment_key = old.comment_key;
    INSERT INTO issue_comments_fts (rowid, comment_key, body)
    VALUES (new.rowid, new.comment_key, COALESCE(new.body, ''));
END;

CREATE TRIGGER issue_comments_fts_delete AFTER DELETE ON fact_issue_comments
BEGIN
    DELETE FROM issue_comments_fts WHERE rowid = old.rowid AND comment_key = old.comment_key;
END;
