-- job_events: an analytical copy of the job lifecycle, shipped
-- asynchronously from the Postgres job_events outbox table
-- (migrations/0003_add_job_events_outbox.sql). Postgres remains the
-- source of truth for job state; this table exists to answer
-- questions Postgres's transactional jobs table is the wrong shape
-- for — throughput over time, failure rate by job type, latency
-- percentiles — without competing with the live claim query for
-- Postgres's attention.
--
-- event_id is the Postgres outbox row's own id, copied in — it's what
-- ReplacingMergeTree dedupes on. Shipping is at-least-once (see the
-- shipper's design note in internal/analytics), so the same event can
-- legitimately be inserted more than once; ReplacingMergeTree merges
-- duplicates away in the background rather than the shipper needing
-- its own distributed-transaction machinery to guarantee exactly-once
-- delivery into a second database. Queries that need a guaranteed-
-- deduped read before a background merge has happened should add
-- FINAL; aggregate queries over meaningful time ranges usually don't
-- need to bother, since occasional not-yet-merged duplicates are a
-- rounding error at analytical scale.
CREATE TABLE IF NOT EXISTS job_events
(
    event_id      Int64,
    job_id        Int64,
    job_type      LowCardinality(String),
    event_type    LowCardinality(String),
    attempts      UInt32,
    worker_id     LowCardinality(String),
    error_message String,
    occurred_at   DateTime64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (event_id)
PARTITION BY toYYYYMM(occurred_at);
