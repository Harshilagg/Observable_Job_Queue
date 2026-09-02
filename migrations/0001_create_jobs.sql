CREATE TABLE jobs (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type             TEXT NOT NULL,
    payload          JSONB NOT NULL,
    status           TEXT NOT NULL DEFAULT 'queued'
                         CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    priority         INTEGER NOT NULL DEFAULT 0,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    run_after        TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at       TIMESTAMPTZ,
    claimed_by       TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    last_error       TEXT
);

-- Serves the claim query: eligible queued jobs, highest priority first,
-- oldest first within a priority tier. Partial on status='queued' so the
-- index stays small as completed/failed history accumulates.
CREATE INDEX idx_jobs_claim ON jobs (priority DESC, run_after)
    WHERE status = 'queued';

-- Serves the reaper: running jobs whose lease has expired.
CREATE INDEX idx_jobs_lease_expiry ON jobs (lease_expires_at)
    WHERE status = 'running';
