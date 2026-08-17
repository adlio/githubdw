-- githubdw migration 004: make the entity-identity invariants unregressable.
--
-- Three rules have always held in `dim_entities`, upheld by the ingest code
-- alone. This migration moves them into the schema, so a future writer that
-- bypasses the normalizer is stopped by the database rather than producing rows
-- that read back wrong:
--
--   1. `login` is bare — non-empty and never namespaced. A prefixed spelling
--      stored here becomes a row no bare-login lookup can ever match, and the
--      write still reports success.
--   2. `entity_key` is exactly `entity_type || ':' || login`. Every fact table
--      joins on that key, so a key that disagrees with its own parts silently
--      splits one identity into two.
--   3. `entity_type` is one of the namespaces ingestion mints. An unknown
--      namespace is unreachable by `resolve_entity_keys`, which enumerates
--      them.
--
-- SQLite cannot add a CHECK constraint to an existing table, so the table is
-- rebuilt. The order matters: `dim_entities` is the parent of every fact
-- table's author/reviewer foreign key, and renaming it would rewrite those
-- child clauses to point at the renamed copy. So the new table is built under a
-- temporary name and renamed *into* place — nothing references the temporary
-- name, so no child clause is touched and they keep referring to
-- `dim_entities`, which exists again by the end of the migration.
--
-- Foreign key enforcement is off while migrations run (see `schema::init`,
-- which cannot toggle it from inside the migration transaction) and the whole
-- schema is re-checked afterwards, so a rebuild that dropped a row is caught
-- before the connection is handed out.
--
-- The copy is a plain INSERT ... SELECT against the new constraints, so clean
-- data migrates as a no-op and violating data aborts the migration with the
-- failed constraint named. That is the point: the alternative is discovering
-- the bad row later, through output that looks plausible.
CREATE TABLE dim_entities_004 (
    entity_key    TEXT PRIMARY KEY,
    entity_type   TEXT NOT NULL CHECK (entity_type IN ('user', 'bot', 'org')),
    login         TEXT NOT NULL CHECK (login <> '' AND login NOT LIKE '%:%'),
    is_human      INTEGER NOT NULL,
    is_bot        INTEGER NOT NULL,
    name          TEXT NOT NULL,
    CHECK (entity_key = entity_type || ':' || login)
);

INSERT INTO dim_entities_004 (entity_key, entity_type, login, is_human, is_bot, name)
SELECT entity_key, entity_type, login, is_human, is_bot, name
FROM dim_entities;

DROP TABLE dim_entities;

ALTER TABLE dim_entities_004 RENAME TO dim_entities;

CREATE INDEX idx_entities_type  ON dim_entities(entity_type);
CREATE INDEX idx_entities_login ON dim_entities(login);
