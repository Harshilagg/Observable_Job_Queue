-- job_events is an outbox: every state-changing Store method writes a
-- row here in the same atomic statement as its own UPDATE/INSERT (same
-- CTE pattern as dead_letters). A separate background shipper reads
-- unshipped rows, sends them to ClickHouse in batches, and marks them
-- shipped. Postgres stays authoritative for job state regardless of
-- whether ClickHouse is reachable — this table can grow unshipped for
-- a while without affecting claim/execute/complete at all.
CREATE TABLE job_events (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id        BIGINT NOT NULL,
    job_type      TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    attempts      INTEGER NOT NULL,
    worker_id     TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    shipped_at    TIMESTAMPTZ
);

-- Serves the shipper's only query: "give me unshipped rows, oldest
-- first". Partial for the same reason idx_jobs_claim is partial —
-- shipped rows (the overwhelming majority, over time) never need to be
-- found by this index again.
CREATE INDEX idx_job_events_unshipped ON job_events (id) WHERE shipped_at IS NULL;
