package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/config"
	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

// newTestStore connects to the Postgres started by docker-compose and
// wipes the jobs table so each test starts from a known state. These
// tests need that container running (docker compose up -d).
func newTestStore(t *testing.T) *Store {
	t.Helper()
	return newTestStoreWithPoolSize(t, 0)
}

// newTestStoreWithPoolSize is newTestStore, but with the underlying
// pgxpool sized to guarantee at least minConns connections (0 keeps
// pgxpool's default, max(4, runtime.NumCPU())).
//
// Why this exists: pgxpool's default is tuned for typical production
// concurrency, not for a test that deliberately hammers the store with
// far more concurrent callers than the machine has CPUs. On a
// CPU-constrained machine (this project was debugged on one reporting
// runtime.NumCPU()==4), a pool that small combined with goroutines
// that retry immediately on a successful claim (no delay) can starve
// slower goroutines indefinitely: by the time a starved goroutine
// finally wins a connection, whatever row it was about to claim has
// already been taken by whichever goroutine currently dominates the
// pool. Enough consecutive "someone beat me to it" results trips the
// give-up threshold, and it exits even though most of the queue is
// still unclaimed -- reproduced and confirmed by this exact fix:
// widening the pool alone took a test that failed most runs (claiming
// as few as 4 of 2000 jobs) to passing consistently, no other change.
// This is the same "size the pool to your actual concurrent demand"
// lesson as sizing a real worker fleet's pool, just triggered here by
// the test's own artificial concurrency instead of production traffic.
func newTestStoreWithPoolSize(t *testing.T, minConns int) *Store {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx := context.Background()
	s, err := New(ctx, cfg.DatabaseURL, int32(minConns))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatalf("store.Ping (is docker compose up?): %v", err)
	}
	t.Cleanup(s.Close)

	// dead_letters has a foreign key to jobs, so it must be cleared first.
	// job_events has no FK (it's an independent outbox, by design), so
	// its cleanup order relative to the others doesn't matter.
	if _, err := s.pool.Exec(ctx, "DELETE FROM dead_letters"); err != nil {
		t.Fatalf("cleaning dead_letters table: %v", err)
	}
	if _, err := s.pool.Exec(ctx, "DELETE FROM job_events"); err != nil {
		t.Fatalf("cleaning job_events table: %v", err)
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

	const totalJobs = 2000
	const workerCount = 20

	// See newTestStoreWithPoolSize's doc comment: this test's 20-way
	// concurrency needs a pool that can actually serve 20 simultaneous
	// callers, or the default (sized for CPU count, not for this test's
	// artificial concurrency) can starve some of them.
	s := newTestStoreWithPoolSize(t, workerCount)

	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (type, payload)
		SELECT 'demo_job', '{}'::jsonb FROM generate_series(1, $1)
	`, totalJobs)
	if err != nil {
		t.Fatalf("seeding jobs: %v", err)
	}

	var mu sync.Mutex
	var claimed []int64

	// A single empty Claim result isn't reliable proof the queue is
	// exhausted — under real contention (many concurrent claimers, each
	// holding a row lock for the duration of its own statement), a
	// worker can transiently see nothing available even while rows
	// remain, the same reason production Worker.Run backs off and
	// retries rather than giving up on the first empty result. Mirror
	// that here: only conclude "done" after several consecutive empty
	// results, not the first one.
	const maxConsecutiveEmpty = 10

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			consecutiveEmpty := 0
			for {
				jobs, err := s.Claim(ctx, workerID, 30*time.Second, 1)
				if err != nil {
					t.Errorf("worker %s: Claim: %v", workerID, err)
					return
				}
				if len(jobs) == 0 {
					consecutiveEmpty++
					if consecutiveEmpty >= maxConsecutiveEmpty {
						return
					}
					time.Sleep(20 * time.Millisecond)
					continue
				}
				consecutiveEmpty = 0
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

// eventTypesFor returns the event_type values recorded for a job, in
// the order they occurred — the outbox's own view of a job's history.
func eventTypesFor(t *testing.T, ctx context.Context, s *Store, jobID int64) []string {
	t.Helper()
	rows, err := s.pool.Query(ctx, "SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY id", jobID)
	if err != nil {
		t.Fatalf("querying job_events: %v", err)
	}
	defer rows.Close()

	var types []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("scanning event_type: %v", err)
		}
		types = append(types, et)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating job_events: %v", err)
	}
	return types
}

func TestEventsRecordedForFullSuccessLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	id, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Claim(ctx, "worker-1", 30*time.Second, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := s.Complete(ctx, id); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got := eventTypesFor(t, ctx, s, id)
	want := []string{"enqueued", "claimed", "completed"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("event sequence = %v, want %v", got, want)
	}
}

func TestEventsRecordedForRetryAndFail(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	id, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Claim(ctx, "worker-1", 30*time.Second, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	// A clearly-past run_after, not exactly time.Now() — the Go process
	// and the Postgres container don't share a clock, so "now" from one
	// can be a hair after "now" from the other's perspective, and this
	// job must be unambiguously eligible for the next Claim.
	if err := s.Retry(ctx, id, time.Now().Add(-1*time.Second), "transient"); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if _, err := s.Claim(ctx, "worker-2", 30*time.Second, 1); err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if err := s.Fail(ctx, id, "permanent"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	got := eventTypesFor(t, ctx, s, id)
	want := []string{"enqueued", "claimed", "retried", "claimed", "failed"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("event sequence = %v, want %v", got, want)
	}
}

func TestEventsDistinguishReapedRetryFromReapedFailed(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// max_attempts=1 so a single claim already exhausts it, matching
	// TestReapExpiredLeasesFailsWhenAttemptsExhausted's fixture pattern.
	var exhaustedID int64
	err := s.pool.QueryRow(ctx,
		"INSERT INTO jobs (type, payload, max_attempts) VALUES ('demo_job', '{}', 1) RETURNING id",
	).Scan(&exhaustedID)
	if err != nil {
		t.Fatalf("inserting exhausted-attempts fixture: %v", err)
	}
	requeuedID, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, err := s.Claim(ctx, "worker-1", -1*time.Second, 2); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if _, _, err := s.ReapExpiredLeases(ctx); err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}

	if got := eventTypesFor(t, ctx, s, exhaustedID); len(got) == 0 || got[len(got)-1] != "reaped_failed" {
		t.Errorf("exhausted job's last event = %v, want last element 'reaped_failed'", got)
	}
	if got := eventTypesFor(t, ctx, s, requeuedID); len(got) == 0 || got[len(got)-1] != "reaped_retry" {
		t.Errorf("requeued job's last event = %v, want last element 'reaped_retry'", got)
	}
}

func TestFetchAndMarkUnshippedEvents(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	id, err := s.Enqueue(ctx, "demo_job", []byte(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	events, err := s.FetchUnshippedEvents(ctx, 10)
	if err != nil {
		t.Fatalf("FetchUnshippedEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d unshipped events, want 1", len(events))
	}
	if events[0].JobID != id {
		t.Errorf("event JobID = %d, want %d", events[0].JobID, id)
	}
	if events[0].EventType != job.EventEnqueued {
		t.Errorf("event EventType = %q, want %q", events[0].EventType, job.EventEnqueued)
	}

	if err := s.MarkEventsShipped(ctx, []int64{events[0].ID}); err != nil {
		t.Fatalf("MarkEventsShipped: %v", err)
	}

	again, err := s.FetchUnshippedEvents(ctx, 10)
	if err != nil {
		t.Fatalf("FetchUnshippedEvents after marking shipped: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("got %d unshipped events after marking shipped, want 0", len(again))
	}
}
