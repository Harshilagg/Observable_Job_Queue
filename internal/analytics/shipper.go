package analytics

import (
	"context"
	"log/slog"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
)

// RunShipper polls st for unshipped job_events, sends them to ch in
// batches, and marks them shipped, until ctx is cancelled.
//
// A single failed cycle never stops the loop or returns an error —
// it's logged and retried next tick. This has to be true regardless of
// how RunShipper is wired into a process: a persistent ClickHouse
// outage is an analytics problem, not a reason to stop claiming or
// executing jobs, and if this ever runs alongside workers in the same
// errgroup, an error returned here would cancel that group's shared
// context and stop them too.
func RunShipper(ctx context.Context, st *store.Store, ch *Client, interval time.Duration, batchSize int, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			shipOnce(ctx, st, ch, batchSize, logger)
		}
	}
}

func shipOnce(ctx context.Context, st *store.Store, ch *Client, batchSize int, logger *slog.Logger) {
	events, err := st.FetchUnshippedEvents(ctx, batchSize)
	if err != nil {
		logger.Error("fetching unshipped events failed", "error", err)
		return
	}
	if len(events) == 0 {
		return
	}

	if err := ch.InsertEvents(ctx, events); err != nil {
		logger.Error("shipping events to clickhouse failed", "error", err, "count", len(events))
		return
	}

	ids := make([]int64, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	if err := st.MarkEventsShipped(ctx, ids); err != nil {
		logger.Error("marking events shipped failed", "error", err, "count", len(ids))
		return
	}

	logger.Info("shipped events to clickhouse", "count", len(events))
}
