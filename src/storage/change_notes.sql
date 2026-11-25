-- One plain-English line per pull request saying what it does. Additive only:
-- no existing table, row, or column is altered, and user_version is unchanged.
--
-- A note is replaced only by a newer non-empty note; nothing deletes a row or
-- blanks one. `input_hash` names the title, body, file list and prompt the
-- note was written from, so a caller can tell a current note from one written
-- before the pull request changed. No foreign key: a note outlives a purge.
CREATE TABLE IF NOT EXISTS change_notes (
    pr_key         TEXT PRIMARY KEY,
    note           TEXT NOT NULL CHECK (length(trim(note)) > 0),
    unclear        INTEGER NOT NULL DEFAULT 0 CHECK (unclear IN (0, 1)),
    input_hash     TEXT NOT NULL,
    prompt_version INTEGER NOT NULL,
    model          TEXT,
    written_by     TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
