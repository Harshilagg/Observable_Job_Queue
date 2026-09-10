package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/config"
)

// newTestStore connects to the Postgres started by docker-compose and
// wipes the jobs table so each test starts from a known state. These
// tests need that container running (docker compose up -d).
func newTestStore(t *testing.T) *Store {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx := context.Background()
	s, err := New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("store.Ping (is docker compose up?): %v", err)
	}
	t.Cleanup(s.Close)

	// dead_letters has a foreign key to jobs, so it must be cleared first.
	if _, err := s.pool.Exec(ctx, "DELETE FROM dead_letters"); err != nil {
		t.Fatalf("cleaning dead_letters table: %v", err)
	}
	if _, err := s.pool.Exec(ctx, "DELETE FROM jobs"); err != nil {
		t.Fatalf("cleaning jobs table: %v", err)
	}
	return s
}

func TestEnqueueClaimComplete(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	id, err := s.Enqueue(ctx, "demo_job", []byte(`{"n":1}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobs, err := s.Claim(ctx, "worker-1", 30*time.Second, 1)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(jobs))
	}
	got := jobs[0]
	if got.ID != id {
		t.Errorf("claimed id = %d, want %d", got.ID, id)
	}
	if got.Type != "demo_job" {
		t.Errorf("claimed type = %q, want demo_job", got.Type)
	}
	if got.Attempts != 1 {
		t.Errorf("claimed attempts = %d, want 1 (claim increments it)", got.Attempts)
	}
	if got.MaxAttempts != 5 {
		t.Errorf("claimed max_attempts = %d, want schema default 5", got.MaxAttempts)
	}

	// A second claim attempt must find nothing — the job is already running.
	again, err := s.Claim(ctx, "worker-2", 30*time.Second, 1)
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("expected no jobs on second claim, got %d", len(again))
	}

	if err := s.Complete(ctx, id); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	var status string
	var completedAt *time.Time
	err = s.pool.QueryRow(ctx, "SELECT status, completed_at FROM jobs WHERE id = $1", id).Scan(&status, &completedAt)
	if err != nil {
		t.Fatalf("reading back row: %v", err)
	}
	if status != "completed" {
		t.Errorf("status = %q, want completed", status)
	}
	if completedAt == nil {
		t.Error("completed_at is nil, want set")
	}
}

func TestRetryReturnsJobToQueueWithBackoff(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	id, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Claim(ctx, "worker-1", 30*time.Second, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	runAfter := time.Now().Add(1 * time.Hour)
	if err := s.Retry(ctx, id, runAfter, "boom"); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	var status, lastErr string
	var claimedBy *string
	var gotRunAfter time.Time
	err = s.pool.QueryRow(ctx,
		"SELECT status, run_after, claimed_by, last_error FROM jobs WHERE id = $1", id,
	).Scan(&status, &gotRunAfter, &claimedBy, &lastErr)
	if err != nil {
		t.Fatalf("reading back row: %v", err)
	}
	if status != "queued" {
		t.Errorf("status = %q, want queued", status)
	}
	if claimedBy != nil {
		t.Errorf("claimed_by = %v, want nil (cleared)", *claimedBy)
	}
	if lastErr != "boom" {
		t.Errorf("last_error = %q, want boom", lastErr)
	}
	if gotRunAfter.Before(time.Now().Add(50 * time.Minute)) {
		t.Errorf("run_after = %v, want roughly 1 hour from now", gotRunAfter)
	}

	// Not eligible yet — run_after is an hour out.
	jobs, err := s.Claim(ctx, "worker-2", 30*time.Second, 1)
	if err != nil {
		t.Fatalf("Claim after retry: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected job to be ineligible until run_after, but it was claimed")
	}
}

func TestFailIsTerminal(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	id, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Claim(ctx, "worker-1", 30*time.Second, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := s.Fail(ctx, id, "unrecoverable"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	var status, lastErr string
	var completedAt *time.Time
	err = s.pool.QueryRow(ctx,
		"SELECT status, completed_at, last_error FROM jobs WHERE id = $1", id,
	).Scan(&status, &completedAt, &lastErr)
	if err != nil {
		t.Fatalf("reading back row: %v", err)
	}
	if status != "failed" {
		t.Errorf("status = %q, want failed", status)
	}
	if completedAt == nil {
		t.Error("completed_at is nil, want set")
	}
	if lastErr != "unrecoverable" {
		t.Errorf("last_error = %q, want unrecoverable", lastErr)
	}

	var dlAttempts int
	var dlLastErr string
	err = s.pool.QueryRow(ctx,
		"SELECT attempts, last_error FROM dead_letters WHERE job_id = $1", id,
	).Scan(&dlAttempts, &dlLastErr)
	if err != nil {
		t.Fatalf("expected a dead_letters row for job %d, got: %v", id, err)
	}
	if dlAttempts != 1 {
		t.Errorf("dead_letters attempts = %d, want 1", dlAttempts)
	}
	if dlLastErr != "unrecoverable" {
		t.Errorf("dead_letters last_error = %q, want unrecoverable", dlLastErr)
	}
}

func TestReapExpiredLeasesRequeuesUnderAttemptCeiling(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	id, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// A negative lease makes lease_expires_at land in the past
	// immediately — simulating a worker that claimed this and then
	// vanished before its lease would naturally have expired.
	if _, err := s.Claim(ctx, "worker-1", -1*time.Second, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	reclaimed, deadLettered, err := s.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if reclaimed != 1 {
		t.Fatalf("reclaimed %d rows, want 1", reclaimed)
	}
	if deadLettered != 0 {
		t.Errorf("dead_lettered = %d, want 0 (job is still under max_attempts)", deadLettered)
	}

	var status string
	var claimedBy, leaseExpiresAt *string
	err = s.pool.QueryRow(ctx,
		"SELECT status, claimed_by, lease_expires_at::text FROM jobs WHERE id = $1", id,
	).Scan(&status, &claimedBy, &leaseExpiresAt)
	if err != nil {
		t.Fatalf("reading back row: %v", err)
	}
	if status != "queued" {
		t.Errorf("status = %q, want queued (attempts=1 < default max_attempts=5)", status)
	}
	if claimedBy != nil {
		t.Errorf("claimed_by = %v, want nil (cleared)", *claimedBy)
	}
	if leaseExpiresAt != nil {
		t.Errorf("lease_expires_at = %v, want nil (cleared)", *leaseExpiresAt)
	}

	var dlCount int
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM dead_letters WHERE job_id = $1", id).Scan(&dlCount); err != nil {
		t.Fatalf("counting dead_letters: %v", err)
	}
	if dlCount != 0 {
		t.Errorf("dead_letters rows for job %d = %d, want 0 (only requeued, not failed)", id, dlCount)
	}
}

func TestReapExpiredLeasesFailsWhenAttemptsExhausted(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Insert directly with max_attempts=1 so a single claim already
	// exhausts it — avoids looping claim+reap five times just to reach
	// the schema's default ceiling.
	var id int64
	err := s.pool.QueryRow(ctx,
		"INSERT INTO jobs (type, payload, max_attempts) VALUES ('demo_job', '{}', 1) RETURNING id",
	).Scan(&id)
	if err != nil {
		t.Fatalf("inserting fixture row: %v", err)
	}

	if _, err := s.Claim(ctx, "worker-1", -1*time.Second, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	reclaimed, deadLettered, err := s.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if reclaimed != 1 {
		t.Errorf("reclaimed = %d, want 1", reclaimed)
	}
	if deadLettered != 1 {
		t.Errorf("dead_lettered = %d, want 1 (attempts=1 >= max_attempts=1)", deadLettered)
	}

	var status string
	if err := s.pool.QueryRow(ctx, "SELECT status FROM jobs WHERE id = $1", id).Scan(&status); err != nil {
		t.Fatalf("reading back row: %v", err)
	}
	if status != "failed" {
		t.Errorf("status = %q, want failed (attempts=1 >= max_attempts=1)", status)
	}

	var dlAttempts int
	if err := s.pool.QueryRow(ctx, "SELECT attempts FROM dead_letters WHERE job_id = $1", id).Scan(&dlAttempts); err != nil {
		t.Fatalf("expected a dead_letters row for job %d, got: %v", id, err)
	}
	if dlAttempts != 1 {
		t.Errorf("dead_letters attempts = %d, want 1", dlAttempts)
	}
}

func TestListDeadLettersReturnsMostRecentFirst(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	var ids []int64
	for i := 0; i < 3; i++ {
		id, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if _, err := s.Claim(ctx, "worker-1", 30*time.Second, 1); err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if err := s.Fail(ctx, id, fmt.Sprintf("boom-%d", i)); err != nil {
			t.Fatalf("Fail: %v", err)
		}
		ids = append(ids, id)
	}

	letters, err := s.ListDeadLetters(ctx, 10)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(letters) != 3 {
		t.Fatalf("got %d dead letters, want 3", len(letters))
	}
	// Most recent first — the last one Fail'd (ids[2]) should be first.
	if letters[0].JobID != ids[2] {
		t.Errorf("letters[0].JobID = %d, want %d (most recently failed)", letters[0].JobID, ids[2])
	}

	limited, err := s.ListDeadLetters(ctx, 1)
	if err != nil {
		t.Fatalf("ListDeadLetters with limit=1: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("got %d dead letters with limit=1, want 1", len(limited))
	}
}

// TestClaimUnderConcurrencyNeverDoublesClaim is the invariant this whole
// design exists to guarantee: with many workers racing against the same
// queue, every job is claimed exactly once — not zero times, not twice.
func TestClaimUnderConcurrencyNeverDoublesClaim(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	const totalJobs = 2000
	const workerCount = 20

	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (type, payload)
		SELECT 'demo_job', '{}'::jsonb FROM generate_series(1, $1)
	`, totalJobs)
	if err != nil {
		t.Fatalf("seeding jobs: %v", err)
	}

	var mu sync.Mutex
	var claimed []int64

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				jobs, err := s.Claim(ctx, workerID, 30*time.Second, 1)
				if err != nil {
					t.Errorf("worker %s: Claim: %v", workerID, err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				claimed = append(claimed, jobs[0].ID)
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", i))
	}
	wg.Wait()

	if len(claimed) != totalJobs {
		t.Errorf("claimed %d jobs total, want %d", len(claimed), totalJobs)
	}

	seen := make(map[int64]int, len(claimed))
	for _, id := range claimed {
		seen[id]++
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("job %d was claimed %d times, want exactly 1", id, count)
		}
	}
}
