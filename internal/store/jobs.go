package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

// Enqueue inserts a new job, eligible for claim immediately, and
// returns its id. Status, priority, and max_attempts take the schema's
// defaults ('queued', 0, 5) — this is deliberately minimal; callers
// needing to override them can be added when something actually needs it.
func (s *Store) Enqueue(ctx context.Context, jobType string, payload json.RawMessage, traceContext string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		WITH inserted AS (
		    INSERT INTO jobs (type, payload, trace_context)
		    VALUES ($1, $2, $3)
		    RETURNING id, type, attempts
		),
		event AS (
		    INSERT INTO job_events (job_id, job_type, event_type, attempts)
		    SELECT id, type, 'enqueued', attempts FROM inserted
		)
		SELECT id FROM inserted
	`, jobType, payload, traceContext).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: enqueue: %w", err)
	}
	return id, nil
}

// GetStatus returns a point-in-time snapshot of one job's state. If no
// job with that id exists, the returned error satisfies
// errors.Is(err, store.ErrNotFound) — callers (e.g. the gRPC layer) use
// that to distinguish "not found" from other failures.
func (s *Store) GetStatus(ctx context.Context, id int64) (job.Snapshot, error) {
	var snap job.Snapshot
	var status string
	var lastErr *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, status, attempts, max_attempts, last_error
		FROM jobs
		WHERE id = $1
	`, id).Scan(&snap.ID, &status, &snap.Attempts, &snap.MaxAttempts, &lastErr)
	if err != nil {
		return job.Snapshot{}, fmt.Errorf("store: get status: %w", err)
	}
	snap.Status = job.Status(status)
	if lastErr != nil {
		snap.LastError = *lastErr
	}
	return snap, nil
}

// Claim atomically finds up to limit eligible jobs, marks them running
// under workerID, and returns them. lease determines how long the claim
// is valid before the reaper is allowed to reclaim it.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration, limit int) ([]job.Job, error) {
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
		    UPDATE jobs
		    SET status = 'running',
		        claimed_by = $1,
		        claimed_at = now(),
		        lease_expires_at = now() + $2::interval,
		        attempts = attempts + 1
		    WHERE id IN (
		        SELECT id FROM jobs
		        WHERE status = 'queued' AND run_after <= now()
		        ORDER BY priority DESC, run_after
		        LIMIT $3
		        FOR UPDATE SKIP LOCKED
		    )
		    RETURNING id, type, payload, attempts, max_attempts, trace_context
		),
		events AS (
		    INSERT INTO job_events (job_id, job_type, event_type, attempts, worker_id)
		    SELECT id, type, 'claimed', attempts, $1 FROM claimed
		)
		SELECT id, type, payload, attempts, max_attempts, trace_context FROM claimed
	`, workerID, lease.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}

	// RowToStructByPos matches columns to job.Job's fields by position, in
	// declaration order (ID, Type, Payload, Attempts, MaxAttempts,
	// TraceContext) — which is why the RETURNING list above is written
	// in that exact order.
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[job.Job])
	if err != nil {
		return nil, fmt.Errorf("store: claim: scanning rows: %w", err)
	}
	return jobs, nil
}

// Complete marks a job as successfully finished.
func (s *Store) Complete(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `
		WITH completed AS (
		    UPDATE jobs
		    SET status = 'completed', completed_at = now()
		    WHERE id = $1
		    RETURNING id, type, attempts
		)
		INSERT INTO job_events (job_id, job_type, event_type, attempts)
		SELECT id, type, 'completed', attempts FROM completed
	`, id)
	if err != nil {
		return fmt.Errorf("store: complete: %w", err)
	}
	return nil
}

// Retry returns a failed job to the queue, eligible for claim again at
// runAfter. Clearing the claim fields is what lets any worker — not
// necessarily the one that just failed it — pick it up next time.
func (s *Store) Retry(ctx context.Context, id int64, runAfter time.Time, lastErr string) error {
	_, err := s.pool.Exec(ctx, `
		WITH retried AS (
		    UPDATE jobs
		    SET status = 'queued',
		        run_after = $2,
		        claimed_by = NULL,
		        claimed_at = NULL,
		        lease_expires_at = NULL,
		        last_error = $3
		    WHERE id = $1
		    RETURNING id, type, attempts
		)
		INSERT INTO job_events (job_id, job_type, event_type, attempts, error_message)
		SELECT id, type, 'retried', attempts, $3 FROM retried
	`, id, runAfter, lastErr)
	if err != nil {
		return fmt.Errorf("store: retry: %w", err)
	}
	return nil
}

// Fail moves a job to a terminal failed state — no further retries —
// and, in the same atomic statement, writes a dead_letters record for
// it. One statement rather than an UPDATE plus a separate INSERT for
// the same reason the claim query is one statement: no window where
// one side committed and the other didn't.
func (s *Store) Fail(ctx context.Context, id int64, lastErr string) error {
	_, err := s.pool.Exec(ctx, `
		WITH failed AS (
		    UPDATE jobs
		    SET status = 'failed',
		        completed_at = now(),
		        claimed_by = NULL,
		        claimed_at = NULL,
		        lease_expires_at = NULL,
		        last_error = $2
		    WHERE id = $1
		    RETURNING id, type, payload, attempts
		),
		dead_letter AS (
		    INSERT INTO dead_letters (job_id, type, payload, attempts, last_error)
		    SELECT id, type, payload, attempts, $2 FROM failed
		)
		INSERT INTO job_events (job_id, job_type, event_type, attempts, error_message)
		SELECT id, type, 'failed', attempts, $2 FROM failed
	`, id, lastErr)
	if err != nil {
		return fmt.Errorf("store: fail: %w", err)
	}
	return nil
}

// ReapExpiredLeases reclaims jobs left in 'running' past their
// lease_expires_at — the recovery path for a worker that claimed a job
// and then crashed or was killed before finishing it. A job under its
// max_attempts goes back to 'queued'; one that has exhausted its
// attempts (already incremented at claim time) goes straight to
// 'failed' and gets a dead_letters record, in the same statement, same
// as Fail. Returns (total reclaimed, of which dead-lettered).
func (s *Store) ReapExpiredLeases(ctx context.Context) (reclaimed int64, deadLettered int64, err error) {
	const lastErr = "lease expired: worker did not report back in time"
	err = s.pool.QueryRow(ctx, `
		WITH reaped AS (
		    UPDATE jobs
		    SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
		        completed_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
		        claimed_by = NULL,
		        claimed_at = NULL,
		        lease_expires_at = NULL,
		        last_error = $1
		    WHERE status = 'running' AND lease_expires_at < now()
		    RETURNING id, type, payload, attempts, status
		),
		dead_lettered AS (
		    INSERT INTO dead_letters (job_id, type, payload, attempts, last_error)
		    SELECT id, type, payload, attempts, $1 FROM reaped WHERE status = 'failed'
		    RETURNING job_id
		),
		events AS (
		    INSERT INTO job_events (job_id, job_type, event_type, attempts, error_message)
		    SELECT id, type,
		           CASE WHEN status = 'failed' THEN 'reaped_failed' ELSE 'reaped_retry' END,
		           attempts, $1
		    FROM reaped
		)
		SELECT
		    (SELECT count(*) FROM reaped)::bigint,
		    (SELECT count(*) FROM dead_lettered)::bigint
	`, lastErr).Scan(&reclaimed, &deadLettered)
	if err != nil {
		return 0, 0, fmt.Errorf("store: reap expired leases: %w", err)
	}
	return reclaimed, deadLettered, nil
}

// ListDeadLetters returns the most recent dead-lettered jobs, newest
// first, up to limit.
func (s *Store) ListDeadLetters(ctx context.Context, limit int) ([]job.DeadLetter, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id, type, payload, attempts, last_error, failed_at
		FROM dead_letters
		ORDER BY failed_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters: %w", err)
	}
	letters, err := pgx.CollectRows(rows, pgx.RowToStructByPos[job.DeadLetter])
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters: scanning rows: %w", err)
	}
	return letters, nil
}

// FetchUnshippedEvents returns up to limit rows from the job_events
// outbox that haven't been shipped to ClickHouse yet, oldest first.
func (s *Store) FetchUnshippedEvents(ctx context.Context, limit int) ([]job.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id, job_type, event_type, attempts, worker_id, error_message, occurred_at
		FROM job_events
		WHERE shipped_at IS NULL
		ORDER BY id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: fetch unshipped events: %w", err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[job.Event])
	if err != nil {
		return nil, fmt.Errorf("store: fetch unshipped events: scanning rows: %w", err)
	}
	return events, nil
}

// MarkEventsShipped records that the given outbox rows were
// successfully sent to ClickHouse, so FetchUnshippedEvents doesn't
// return them again.
func (s *Store) MarkEventsShipped(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE job_events SET shipped_at = now() WHERE id = ANY($1)
	`, ids)
	if err != nil {
		return fmt.Errorf("store: mark events shipped: %w", err)
	}
	return nil
}

// CountByStatusAndType returns the number of jobs in the given status,
// grouped by type. Backs the queue-depth and in-flight metrics.
func (s *Store) CountByStatusAndType(ctx context.Context, status string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT type, count(*) FROM jobs WHERE status = $1 GROUP BY type
	`, status)
	if err != nil {
		return nil, fmt.Errorf("store: count by status and type: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var jobType string
		var n int64
		if err := rows.Scan(&jobType, &n); err != nil {
			return nil, fmt.Errorf("store: count by status and type: scanning row: %w", err)
		}
		counts[jobType] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: count by status and type: %w", err)
	}
	return counts, nil
}

// ShipperLag returns how far behind the shipper is: the age of the
// oldest unshipped job_events row, or 0 if nothing is unshipped.
func (s *Store) ShipperLag(ctx context.Context) (time.Duration, error) {
	var occurredAt time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT occurred_at FROM job_events WHERE shipped_at IS NULL ORDER BY id LIMIT 1
	`).Scan(&occurredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: shipper lag: %w", err)
	}
	return time.Since(occurredAt), nil
}
