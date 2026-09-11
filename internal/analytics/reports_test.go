package analytics

import (
	"context"
	"testing"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/config"
	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

// newTestClient connects to the real ClickHouse container and
// truncates job_events so each test starts clean. Needs docker compose
// up -d and make ch-migrate, same as shipper_test.go's testEnv — this
// is a lighter version that skips Postgres/the Store entirely, since
// these report queries only ever touch ClickHouse.
func newTestClient(t *testing.T) *Client {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx := context.Background()
	ch, err := NewClient(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUser, cfg.ClickHousePassword)
	if err != nil {
		t.Fatalf("analytics.NewClient (is docker compose up and has make ch-migrate run?): %v", err)
	}
	t.Cleanup(func() { ch.Close() })
	if err := ch.conn.Exec(ctx, "TRUNCATE TABLE job_events"); err != nil {
		t.Fatalf("truncating clickhouse job_events: %v", err)
	}
	return ch
}

func TestArrivalsVsCompletions(t *testing.T) {
	ctx := context.Background()
	ch := newTestClient(t)

	now := time.Now().UTC()
	events := []job.Event{
		{ID: 1, JobID: 101, JobType: "sum_numbers", EventType: job.EventEnqueued, Attempts: 0, OccurredAt: now.Add(-5 * time.Minute)},
		{ID: 2, JobID: 102, JobType: "sum_numbers", EventType: job.EventEnqueued, Attempts: 0, OccurredAt: now.Add(-5 * time.Minute)},
		{ID: 3, JobID: 101, JobType: "sum_numbers", EventType: job.EventCompleted, Attempts: 1, OccurredAt: now.Add(-4 * time.Minute)},
		{ID: 4, JobID: 103, JobType: "http_check", EventType: job.EventEnqueued, Attempts: 0, OccurredAt: now.Add(-1 * time.Minute)},
		{ID: 5, JobID: 103, JobType: "http_check", EventType: job.EventFailed, Attempts: 1, OccurredAt: now.Add(-1 * time.Minute)},
	}
	if err := ch.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents: %v", err)
	}

	rows, err := ch.ArrivalsVsCompletions(ctx, time.Hour, time.Minute)
	if err != nil {
		t.Fatalf("ArrivalsVsCompletions: %v", err)
	}

	var totalArrivals, totalCompletions uint64
	for _, r := range rows {
		totalArrivals += r.Arrivals
		totalCompletions += r.Completions
	}
	if totalArrivals != 3 {
		t.Errorf("total arrivals = %d, want 3", totalArrivals)
	}
	if totalCompletions != 2 {
		t.Errorf("total completions = %d, want 2 (one completed, one failed; retried doesn't count as a departure)", totalCompletions)
	}
}

func TestDurationPercentiles(t *testing.T) {
	ctx := context.Background()
	ch := newTestClient(t)

	now := time.Now().UTC()
	events := []job.Event{
		// job 201, attempt 1: claimed then completed 2s later.
		{ID: 1, JobID: 201, JobType: "sum_numbers", EventType: job.EventClaimed, Attempts: 1, OccurredAt: now.Add(-10 * time.Second)},
		{ID: 2, JobID: 201, JobType: "sum_numbers", EventType: job.EventCompleted, Attempts: 1, OccurredAt: now.Add(-8 * time.Second)},
		// job 202, attempt 1: claimed then failed 4s later.
		{ID: 3, JobID: 202, JobType: "sum_numbers", EventType: job.EventClaimed, Attempts: 1, OccurredAt: now.Add(-10 * time.Second)},
		{ID: 4, JobID: 202, JobType: "sum_numbers", EventType: job.EventFailed, Attempts: 1, OccurredAt: now.Add(-6 * time.Second)},
		// a retried job: claimed but only retried, never a terminal
		// event for this attempt — must not be counted as a sample.
		{ID: 5, JobID: 203, JobType: "sum_numbers", EventType: job.EventClaimed, Attempts: 1, OccurredAt: now.Add(-10 * time.Second)},
		{ID: 6, JobID: 203, JobType: "sum_numbers", EventType: job.EventRetried, Attempts: 1, OccurredAt: now.Add(-9 * time.Second)},
	}
	if err := ch.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents: %v", err)
	}

	rows, err := ch.DurationPercentiles(ctx, time.Hour)
	if err != nil {
		t.Fatalf("DurationPercentiles: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d job types, want 1", len(rows))
	}
	if rows[0].JobType != "sum_numbers" {
		t.Errorf("job type = %q, want sum_numbers", rows[0].JobType)
	}
	if rows[0].Samples != 2 {
		t.Errorf("samples = %d, want 2 (the retried attempt has no terminal event yet)", rows[0].Samples)
	}
	// p99 of {2s, 4s} is close to 4s.
	if rows[0].P99Seconds < 3.9 || rows[0].P99Seconds > 4.1 {
		t.Errorf("p99Seconds = %v, want ~4.0", rows[0].P99Seconds)
	}
}

func TestBacklogVsWorkers(t *testing.T) {
	ctx := context.Background()
	ch := newTestClient(t)

	now := time.Now().UTC()
	events := []job.Event{
		{ID: 1, JobID: 301, JobType: "sum_numbers", EventType: job.EventEnqueued, OccurredAt: now.Add(-3 * time.Minute)},
		{ID: 2, JobID: 302, JobType: "sum_numbers", EventType: job.EventEnqueued, OccurredAt: now.Add(-3 * time.Minute)},
		{ID: 3, JobID: 301, JobType: "sum_numbers", EventType: job.EventClaimed, Attempts: 1, WorkerID: "worker-1", OccurredAt: now.Add(-2 * time.Minute)},
		{ID: 4, JobID: 302, JobType: "sum_numbers", EventType: job.EventClaimed, Attempts: 1, WorkerID: "worker-2", OccurredAt: now.Add(-2 * time.Minute)},
		{ID: 5, JobID: 301, JobType: "sum_numbers", EventType: job.EventCompleted, Attempts: 1, OccurredAt: now.Add(-1 * time.Minute)},
		{ID: 6, JobID: 302, JobType: "sum_numbers", EventType: job.EventCompleted, Attempts: 1, OccurredAt: now.Add(-1 * time.Minute)},
	}
	if err := ch.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents: %v", err)
	}

	rows, err := ch.BacklogVsWorkers(ctx, time.Hour, time.Minute)
	if err != nil {
		t.Fatalf("BacklogVsWorkers: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("got 0 buckets, want at least 1")
	}
	// Two arrived and two departed across the whole window, so the
	// final bucket's cumulative backlog must be back to 0.
	last := rows[len(rows)-1]
	if last.CumulativeBacklog != 0 {
		t.Errorf("final cumulative backlog = %d, want 0 (2 arrivals, 2 departures)", last.CumulativeBacklog)
	}
	// worker-1 and worker-2 both claimed in the same minute-wide bucket.
	var maxWorkers uint64
	for _, r := range rows {
		if r.ActiveWorkers > maxWorkers {
			maxWorkers = r.ActiveWorkers
		}
	}
	if maxWorkers != 2 {
		t.Errorf("max active workers seen = %d, want 2", maxWorkers)
	}
}

func TestRecentCompletionRates(t *testing.T) {
	ctx := context.Background()
	ch := newTestClient(t)

	now := time.Now().UTC()
	events := []job.Event{
		{ID: 1, JobID: 401, JobType: "sum_numbers", EventType: job.EventCompleted, Attempts: 1, OccurredAt: now.Add(-30 * time.Second)},
		{ID: 2, JobID: 402, JobType: "sum_numbers", EventType: job.EventCompleted, Attempts: 1, OccurredAt: now.Add(-20 * time.Second)},
		{ID: 3, JobID: 403, JobType: "http_check", EventType: job.EventFailed, Attempts: 5, OccurredAt: now.Add(-10 * time.Second)},
		// Outside the window — must not be counted.
		{ID: 4, JobID: 404, JobType: "sum_numbers", EventType: job.EventCompleted, Attempts: 1, OccurredAt: now.Add(-10 * time.Minute)},
	}
	if err := ch.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents: %v", err)
	}

	rates, err := ch.RecentCompletionRates(ctx, time.Minute)
	if err != nil {
		t.Fatalf("RecentCompletionRates: %v", err)
	}

	got := make(map[string]uint64, len(rates))
	for _, r := range rates {
		got[r.JobType] = r.Completions
	}
	if got["sum_numbers"] != 2 {
		t.Errorf("sum_numbers completions = %d, want 2", got["sum_numbers"])
	}
	if got["http_check"] != 1 {
		t.Errorf("http_check completions = %d, want 1", got["http_check"])
	}
}

func TestEstimateDrainTime(t *testing.T) {
	estimates := EstimateDrainTime(
		map[string]int64{"sum_numbers": 100, "write_file": 5},
		map[string]float64{"sum_numbers": 10, "http_check": 2},
	)

	byType := make(map[string]DrainEstimate, len(estimates))
	for _, e := range estimates {
		byType[e.JobType] = e
	}

	// In both maps, positive rate: a real estimate.
	sn := byType["sum_numbers"]
	if !sn.CanEstimate || sn.EstimatedSeconds != 10 {
		t.Errorf("sum_numbers = %+v, want CanEstimate=true, EstimatedSeconds=10", sn)
	}

	// Backlog with no matching rate: zero rate, can't estimate.
	wf := byType["write_file"]
	if wf.CanEstimate {
		t.Errorf("write_file = %+v, want CanEstimate=false (no recent completions)", wf)
	}
	if wf.Backlog != 5 {
		t.Errorf("write_file backlog = %d, want 5", wf.Backlog)
	}

	// Rate with no matching backlog: zero backlog, drains instantly.
	hc := byType["http_check"]
	if !hc.CanEstimate || hc.EstimatedSeconds != 0 {
		t.Errorf("http_check = %+v, want CanEstimate=true, EstimatedSeconds=0", hc)
	}

	if len(estimates) != 3 {
		t.Fatalf("got %d estimates, want 3 (union of both maps' job types)", len(estimates))
	}
	for i := 1; i < len(estimates); i++ {
		if estimates[i-1].JobType >= estimates[i].JobType {
			t.Errorf("estimates not sorted by job type: %q before %q", estimates[i-1].JobType, estimates[i].JobType)
		}
	}
}
