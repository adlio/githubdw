-- Counts-first collection and detail state. Additive only: no existing
-- table, row, or column is altered or removed.

-- One row per (login, role, warehouse-calendar day) for the counts stage.
-- `complete_as_of` is set only when a fetch served every match; a later
-- failed or deferred attempt records itself in the last_* columns without
-- withdrawing an earlier complete claim.
CREATE TABLE IF NOT EXISTS collection_coverage (
    login              TEXT NOT NULL,
    role               TEXT NOT NULL CHECK (role IN ('authored', 'merged', 'reviewed')),
    day                TEXT NOT NULL,
    complete_as_of     TEXT,
    complete_day_closed INTEGER,
    provider_total     INTEGER,
    served             INTEGER,
    inaccessible       INTEGER,
    last_attempt_at    TEXT NOT NULL,
    last_status        TEXT NOT NULL CHECK (last_status IN
                           ('complete', 'partial', 'failed', 'rate_limited', 'cancelled')),
    last_reason        TEXT,
    wake_at            TEXT,
    PRIMARY KEY (login, role, day)
);

-- Reviews are found through `reviewed-by:` + `updated:` windows. A review on
-- day D can sit on a pull request last updated any time after D, so a day is
-- complete only as part of a contiguous range [oldest_day, sealed day] whose
-- every update since `oldest_day` began was walked, sealed at `sealed_at`.
CREATE TABLE IF NOT EXISTS collection_review_frontier (
    login      TEXT PRIMARY KEY,
    oldest_day TEXT NOT NULL,
    sealed_at  TEXT NOT NULL
);

-- Per pull request: which stages have been fetched, and the exact revision
-- identity a summary can be keyed on. Never holds diff text.
CREATE TABLE IF NOT EXISTS pr_detail_state (
    pr_key               TEXT PRIMARY KEY,
    light_seen_at        TEXT,
    detail_fetched_at    TEXT,
    diff_stored_at       TEXT,
    base_oid             TEXT,
    head_oid             TEXT,
    revision_fingerprint TEXT
);
