package analytics

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Harshilagg/Observable_Job_Queue/internal/config"
	"github.com/Harshilagg/Observable_Job_Queue/internal/logging"
	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
)

// testEnv connects to the real Postgres and ClickHouse containers and
// wipes both job_events tables so each test starts clean. Needs
// docker compose up -d and both schemas applied (make migrate,
// make ch-migrate). Cleanup uses a raw pgxpool connection rather than
// Store — Store's pool field is unexported by design (nothing outside
// internal/store should touch pgx directly), and this test-only
// cleanup isn't a reason to weaken that.
func testEnv(t *testing.T) (*store.Store, *Client, config.Config) {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	ctx := context.Background()

	rawPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("pgxpool.New (cleanup connection): %v", err)
	}
	defer rawPool.Close()
	// dead_letters has a foreign key to jobs, so it must be cleared
	// first — same ordering constraint as store's own newTestStore.
	if _, err := rawPool.Exec(ctx, "DELETE FROM dead_letters"); err != nil {
		t.Fatalf("cleaning dead_letters: %v", err)
	}
	if _, err := rawPool.Exec(ctx, "DELETE FROM job_events"); err != nil {
		t.Fatalf("cleaning job_events: %v", err)
	}
	if _, err := rawPool.Exec(ctx, "DELETE FROM jobs"); err != nil {
		t.Fatalf("cleaning jobs: %v", err)
	}

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := st.Ping(ctx); err != nil {
		t.Fatalf("store.Ping (is docker compose up?): %v", err)
	}
	t.Cleanup(st.Close)

	ch, err := NewClient(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUser, cfg.ClickHousePassword)
	if err != nil {
		t.Fatalf("analytics.NewClient (is docker compose up and has make ch-migrate run?): %v", err)
	}
	t.Cleanup(func() { ch.Close() })
	if err := ch.conn.Exec(ctx, "TRUNCATE TABLE job_events"); err != nil {
		t.Fatalf("truncating clickhouse job_events: %v", err)
	}

	return st, ch, cfg
}

func TestShipOnceMovesEventsFromPostgresToClickHouse(t *testing.T) {
	ctx := context.Background()
	st, ch, cfg := testEnv(t)
	logger := logging.New("error")

	id, err := st.Enqueue(ctx, "demo_job", []byte(`{"n":1}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := st.Claim(ctx, "worker-1", 30*time.Second, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := st.Complete(ctx, id); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Three events now sit unshipped in Postgres: enqueued, claimed, completed.
	before, err := st.FetchUnshippedEvents(ctx, 100)
	if err != nil {
		t.Fatalf("FetchUnshippedEvents: %v", err)
	}
	if len(before) != 3 {
		t.Fatalf("got %d unshipped events before shipping, want 3", len(before))
	}

	shipOnce(ctx, st, ch, cfg.ShipBatchSize, logger)

	after, err := st.FetchUnshippedEvents(ctx, 100)
	if err != nil {
		t.Fatalf("FetchUnshippedEvents after shipping: %v", err)
	}
	if len(after) != 0 {
		t.Errorf("got %d unshipped events after shipping, want 0", len(after))
	}

	rows, err := ch.conn.Query(ctx, "SELECT event_type FROM job_events WHERE job_id = ? ORDER BY event_id", id)
	if err != nil {
		t.Fatalf("querying clickhouse: %v", err)
	}
	defer rows.Close()

	var gotTypes []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("scanning clickhouse row: %v", err)
		}
		gotTypes = append(gotTypes, et)
	}
	want := []string{"enqueued", "claimed", "completed"}
	if len(gotTypes) != len(want) {
		t.Fatalf("clickhouse has %v, want %v", gotTypes, want)
	}
	for i := range want {
		if gotTypes[i] != want[i] {
			t.Errorf("clickhouse event[%d] = %q, want %q", i, gotTypes[i], want[i])
		}
	}
}

func TestShipOnceIsANoOpWithNothingToShip(t *testing.T) {
	ctx := context.Background()
	st, ch, cfg := testEnv(t)
	logger := logging.New("error")

	// Must not error or panic when there's simply nothing unshipped.
	shipOnce(ctx, st, ch, cfg.ShipBatchSize, logger)
}

func TestRunShipperStopsOnContextCancellation(t *testing.T) {
	st, ch, cfg := testEnv(t)
	logger := logging.New("error")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunShipper(ctx, st, ch, 10*time.Millisecond, cfg.ShipBatchSize, logger)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunShipper did not return within 2s of context cancellation")
	}
}
