-- trace_context holds the W3C traceparent string captured at Submit
-- time, so a trace can span submit -> claim -> execute -> complete
-- even though those happen in different processes (serve vs work),
-- potentially seconds or minutes apart. Claim reads it back out and
-- the worker continues the same trace rather than starting a new one.
ALTER TABLE jobs ADD COLUMN trace_context TEXT NOT NULL DEFAULT '';
