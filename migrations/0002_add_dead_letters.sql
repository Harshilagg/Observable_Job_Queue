-- Dead-letter table: a self-contained, independently queryable record
-- of every job that reached a terminal 'failed' state, regardless of
-- which code path produced it (Worker.Run giving up, or the reaper
-- reclaiming an exhausted lease). Self-contained (type/payload copied
-- in, not just referenced) so it stays inspectable even if the source
-- jobs row is ever purged by a future retention job.
CREATE TABLE dead_letters (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id     BIGINT NOT NULL REFERENCES jobs (id),
    type       TEXT NOT NULL,
    payload    JSONB NOT NULL,
    attempts   INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A job can only ever reach terminal 'failed' once (the claim query
    -- only matches status='queued', so a failed row can never be
    -- reclaimed) — this makes that invariant explicit and enforced by
    -- the database, not just an emergent property of application logic.
    UNIQUE (job_id)
);

-- Serves "show me recent dead letters" — the only access pattern this
-- table has right now.
CREATE INDEX idx_dead_letters_failed_at ON dead_letters (failed_at DESC);
