package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

// Enqueue inserts a new job, eligible for claim immediately, and
// returns its id. Status, priority, and max_attempts take the schema's
// defaults ('queued', 0, 5) — this is deliberately minimal; callers
// needing to override them can be added when something actually needs it.
func (s *Store) Enqueue(ctx context.Context, jobType string, payload json.RawMessage) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (type, payload)
		VALUES ($1, $2)
		RETURNING id
	`, jobType, payload).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("store: enqueue: %w", err)
	}
	return id, nil
}

// Claim atomically finds up to limit eligible jobs, marks them running
// under workerID, and returns them. lease determines how long the claim
// is valid before the reaper is allowed to reclaim it.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration, limit int) ([]job.Job, error) {
	rows, err := s.pool.Query(ctx, `
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
		RETURNING id, type, payload, attempts, max_attempts
	`, workerID, lease.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}

	// RowToStructByPos matches columns to job.Job's fields by position, in
	// declaration order (ID, Type, Payload, Attempts, MaxAttempts) — which
	// is why the RETURNING list above is written in that exact order.
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[job.Job])
	if err != nil {
		return nil, fmt.Errorf("store: claim: scanning rows: %w", err)
	}
	return jobs, nil
}

// Complete marks a job as successfully finished.
func (s *Store) Complete(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'completed', completed_at = now()
		WHERE id = $1
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
		UPDATE jobs
		SET status = 'queued',
		    run_after = $2,
		    claimed_by = NULL,
		    claimed_at = NULL,
		    lease_expires_at = NULL,
		    last_error = $3
		WHERE id = $1
	`, id, runAfter, lastErr)
	if err != nil {
		return fmt.Errorf("store: retry: %w", err)
	}
	return nil
}

// Fail moves a job to a terminal failed state — no further retries.
func (s *Store) Fail(ctx context.Context, id int64, lastErr string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'failed',
		    completed_at = now(),
		    claimed_by = NULL,
		    claimed_at = NULL,
		    lease_expires_at = NULL,
		    last_error = $2
		WHERE id = $1
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
// 'failed' instead of being handed out again. It returns how many rows
// were reclaimed, for logging/metrics.
func (s *Store) ReapExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
		    completed_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
		    claimed_by = NULL,
		    claimed_at = NULL,
		    lease_expires_at = NULL,
		    last_error = 'lease expired: worker did not report back in time'
		WHERE status = 'running' AND lease_expires_at < now()
	`)
	if err != nil {
		return 0, fmt.Errorf("store: reap expired leases: %w", err)
	}
	return tag.RowsAffected(), nil
}
