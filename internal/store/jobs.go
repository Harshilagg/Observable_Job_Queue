package store

import (
	"context"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

// Claim atomically finds up to limit eligible jobs, marks them running
// under workerID, and returns them. lease determines how long the claim
// is valid before the reaper is allowed to reclaim it.
//
// This is yours to write — same query you already designed and I
// verified against real concurrency. Wire it up here.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration, limit int) ([]job.Job, error) {
	panic("todo: implement Claim")
}

// Complete marks a job as successfully finished.
func (s *Store) Complete(ctx context.Context, id int64) error {
	panic("todo: implement Complete")
}

// Retry returns a failed job to the queue, eligible for claim again at
// runAfter.
func (s *Store) Retry(ctx context.Context, id int64, runAfter time.Time, lastErr string) error {
	panic("todo: implement Retry")
}

// Fail moves a job to a terminal failed state — no further retries.
func (s *Store) Fail(ctx context.Context, id int64, lastErr string) error {
	panic("todo: implement Fail")
}
