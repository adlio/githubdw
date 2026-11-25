-- Shared collector state for `collect::tick`. Additive only, and outside the
-- numbered migrations: `PRAGMA user_version` stays at 8, so a 0.5.x binary
-- (Aaron's CLI timer, an engdash that has not been upgraded) still opens the
-- warehouse and simply ignores these tables.

-- One row (id = 1): the shared detection cursors and when each job last
-- completed. Cursors are frozen search bounds (the instant a complete
-- detection began), never the newest `updatedAt` a search returned.
CREATE TABLE IF NOT EXISTS collector_state (
    id                   INTEGER PRIMARY KEY CHECK (id = 1),
    detect_cursor        TEXT,
    last_detect_at       TEXT,
    last_detect_by       TEXT,
    last_reconcile_at    TEXT,
    last_login_check_at  TEXT,
    last_report_at       TEXT,
    last_tick_at         TEXT,
    last_tick_by         TEXT,
    last_tick_points     INTEGER,
    last_tick_summary    TEXT
);

-- Two independent leases, `detect` and `backfill`, so a long backfill never
-- stops another process from detecting. A lease is free when it expired, or
-- when its holder on this host is gone (pid absent, or the pid now belongs
-- to a process with a different start time).
CREATE TABLE IF NOT EXISTS collector_leases (
    lane          TEXT PRIMARY KEY,
    token         TEXT NOT NULL,
    holder_pid    INTEGER NOT NULL,
    holder_start  TEXT,
    host          TEXT NOT NULL,
    program       TEXT NOT NULL,
    acquired_at   TEXT NOT NULL,
    expires_at    TEXT NOT NULL
);

-- Pull requests detection found changed whose full fetch has not landed.
-- One poison pull request waits here; the shared cursor moves past it.
CREATE TABLE IF NOT EXISTS collector_pr_queue (
    pr_key          TEXT PRIMARY KEY,
    node_id         TEXT,
    seen_updated_at TEXT,
    queued_at       TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    last_error      TEXT,
    parked_at       TEXT
);

-- Monitored logins as the collector knows them: when each joined the
-- batched detector, how far back its history is wanted, whether it still
-- resolves on GitHub, and any one-off catch-up owed from older state.
CREATE TABLE IF NOT EXISTS collector_members (
    login            TEXT PRIMARY KEY,
    joined_at        TEXT NOT NULL,
    history_from     TEXT,
    unresolved_since TEXT,
    catch_up_author  TEXT,
    catch_up_review  TEXT
);

-- Heads whose check rollup GitHub reported finished while stored check
-- runs were not, and which were fetched again for it. A head is fetched for
-- this once: a stored run GitHub no longer lists (a superseded re-run) would
-- otherwise look unfinished forever.
CREATE TABLE IF NOT EXISTS collector_check_refetch (
    pr_key       TEXT PRIMARY KEY,
    head_oid     TEXT NOT NULL,
    refetched_at TEXT NOT NULL
);

-- Sources GitHub refuses (SAML, IP allow list, forbidden): retried once a
-- day instead of every pass.
CREATE TABLE IF NOT EXISTS collector_backoff (
    source          TEXT PRIMARY KEY,
    reason          TEXT NOT NULL,
    first_seen_at   TEXT NOT NULL,
    last_attempt_at TEXT NOT NULL,
    retry_after     TEXT NOT NULL
);

-- Diff fill (`collect::diff_fill`): the last attempt to store patch text
-- for a pull request synced without it. A failed attempt waits a day.
CREATE TABLE IF NOT EXISTS collector_diff_fill (
    pr_key       TEXT PRIMARY KEY,
    attempted_at TEXT NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT
);
