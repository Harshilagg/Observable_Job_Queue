// Package analytics ships job lifecycle events to ClickHouse. Nothing
// here is on the critical path for claiming or executing jobs — it
// exists purely so the Shipper can fail, retry, or lag arbitrarily far
// behind without affecting job processing at all.
package analytics

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
)

// Client wraps a ClickHouse connection for inserting job events.
type Client struct {
	conn clickhouse.Conn
}

// NewClient connects to ClickHouse and verifies it's reachable.
func NewClient(ctx context.Context, addr, database, username, password string) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("analytics: opening connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("analytics: ping: %w", err)
	}
	return &Client{conn: conn}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// InsertEvents batches and sends events to ClickHouse's job_events
// table in one round trip. event_id is the Postgres outbox row's own
// id — see clickhouse/schema.sql for why that's what ReplacingMergeTree
// dedupes on.
func (c *Client) InsertEvents(ctx context.Context, events []job.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO job_events
			(event_id, job_id, job_type, event_type, attempts, worker_id, error_message, occurred_at)
	`)
	if err != nil {
		return fmt.Errorf("analytics: preparing batch: %w", err)
	}

	for _, e := range events {
		err := batch.Append(
			e.ID,
			e.JobID,
			e.JobType,
			string(e.EventType),
			uint32(e.Attempts),
			e.WorkerID,
			e.ErrorMessage,
			e.OccurredAt,
		)
		if err != nil {
			return fmt.Errorf("analytics: appending event %d: %w", e.ID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("analytics: sending batch: %w", err)
	}
	return nil
}
