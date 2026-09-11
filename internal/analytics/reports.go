package analytics

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// The four reports in this file exist to answer questions Postgres's
// jobs table structurally cannot: it only holds each job's CURRENT
// row, overwritten in place on every claim/retry/complete. There is no
// way to ask it "how many jobs arrived per minute over the last hour"
// or "what was p99 duration during last Tuesday's incident" — that
// history simply isn't there once a job's status has moved on. It
// exists only in job_events, the immutable, append-only copy shipped
// to ClickHouse by the outbox (see internal/analytics/shipper.go and
// clickhouse/schema.sql).
//
// Even where Postgres's own job_events outbox table technically holds
// the same rows (before shipping), running these aggregate scans
// against it would mean competing with the live claim/complete
// queries for the same buffer cache and I/O the hot path depends on —
// the exact problem the outbox+shipper split exists to avoid. ClickHouse
// is a second, physically separate store built for exactly this shape
// of query (columnar storage, vectorized aggregation, quantile()) with
// zero risk of slowing down job processing, no matter how large the
// event history grows or how expensive the report.

// ArrivalCompletion is one time bucket's job arrivals versus
// departures — jobs enqueued versus jobs that reached a terminal state
// (completed, failed, or reaped_failed) in that bucket.
type ArrivalCompletion struct {
	Bucket      time.Time `ch:"bucket"`
	Arrivals    uint64    `ch:"arrivals"`
	Completions uint64    `ch:"completions"`
}

// ArrivalsVsCompletions buckets the last window of job history into
// bucket-sized intervals and counts arrivals against completions in
// each — the first thing to look at when backlog is growing: is it
// arrivals spiking, or completions falling behind?
//
// Why ClickHouse: this is exactly the query shape a columnar store is
// built for — a full scan over (potentially millions of rows of)
// event history, grouped into time buckets, aggregated with simple
// counts. Postgres's jobs table can't answer this at all (see this
// file's package-level doc comment); running it against Postgres's own
// job_events outbox would mean scanning a growing, row-store table not
// indexed for this access pattern, competing with the live claim path
// for the same I/O.
func (c *Client) ArrivalsVsCompletions(ctx context.Context, window, bucket time.Duration) ([]ArrivalCompletion, error) {
	var rows []ArrivalCompletion
	err := c.conn.Select(ctx, &rows, `
		SELECT
			toStartOfInterval(occurred_at, INTERVAL ? SECOND) AS bucket,
			countIf(event_type = 'enqueued') AS arrivals,
			countIf(event_type IN ('completed', 'failed', 'reaped_failed')) AS completions
		FROM job_events
		WHERE occurred_at >= now() - INTERVAL ? SECOND
		GROUP BY bucket
		ORDER BY bucket
	`, int64(bucket.Seconds()), int64(window.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("analytics: arrivals vs completions: %w", err)
	}
	return rows, nil
}

// DurationPercentile is one job type's p99 execution duration —
// claimed to terminal (completed or failed) — across a window, and how
// many samples that's based on.
type DurationPercentile struct {
	JobType    string  `ch:"job_type"`
	P99Seconds float64 `ch:"p99_seconds"`
	Samples    uint64  `ch:"samples"`
}

// DurationPercentiles computes p99 job duration by job type across
// window, by pairing each job's 'claimed' event with its terminal
// event for the same attempt (job_id, attempts uniquely identifies one
// attempt, since Claim increments attempts and every event from that
// attempt — claimed, then completed/retried/failed — carries the same
// value).
//
// Why ClickHouse: Prometheus's jobqueue_job_duration_seconds histogram
// (internal/metrics) answers "what does duration look like right now,
// live" but is an in-process instrument — it resets on every work
// restart and can't be recomputed for a window that's already passed.
// This query can answer "what was p99 during last Tuesday's incident"
// months later, because job_events is durable, queryable history, not
// a live counter. Computing this from Postgres would mean self-joining
// job_events by job_id — a real join, not an indexed point lookup —
// against the same table the outbox pattern exists to keep off the hot
// path.
func (c *Client) DurationPercentiles(ctx context.Context, window time.Duration) ([]DurationPercentile, error) {
	var rows []DurationPercentile
	err := c.conn.Select(ctx, &rows, `
		SELECT
			job_type,
			quantile(0.99)(duration_seconds) AS p99_seconds,
			count() AS samples
		FROM (
			SELECT
				job_id,
				attempts,
				any(job_type) AS job_type,
				minIf(occurred_at, event_type = 'claimed') AS claimed_at,
				minIf(occurred_at, event_type IN ('completed', 'failed')) AS terminal_at,
				dateDiff('millisecond', claimed_at, terminal_at) / 1000.0 AS duration_seconds
			FROM job_events
			WHERE occurred_at >= now() - INTERVAL ? SECOND
			  AND event_type IN ('claimed', 'completed', 'failed')
			GROUP BY job_id, attempts
			HAVING claimed_at > toDateTime64(0, 3) AND terminal_at > claimed_at
		)
		GROUP BY job_type
		ORDER BY p99_seconds DESC
	`, int64(window.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("analytics: duration percentiles: %w", err)
	}
	return rows, nil
}

// BacklogPoint is one time bucket's active worker count and the
// running (cumulative) backlog change up to and including that bucket.
type BacklogPoint struct {
	Bucket            time.Time `ch:"bucket"`
	ActiveWorkers     uint64    `ch:"active_workers"`
	CumulativeBacklog int64     `ch:"cumulative_backlog"`
}

// BacklogVsWorkers buckets job history into bucket-sized intervals and
// tracks, per bucket, how many distinct workers claimed a job (a proxy
// for how many workers were actually active) alongside the running net
// change in backlog (arrivals minus departures, summed from the start
// of window) — so scaling workers up or down can be visually
// correlated with backlog actually draining or growing. The cumulative
// figure is relative to the start of window, not an absolute backlog
// count — it answers "did backlog grow or shrink," not "how many jobs
// are queued right now" (that's Store.CountByStatusAndType's job, an
// exact instantaneous count Postgres — not this history — is the
// authority on).
//
// Why ClickHouse: worker_id is only recorded on 'claimed' events in
// job_events, and Postgres's jobs table doesn't track history of past
// worker counts at all — a claimed job's claimed_by is overwritten on
// every subsequent claim, so the past is gone the moment a job is
// reclaimed or retried. Only the immutable event log has enough
// history to reconstruct this trend.
func (c *Client) BacklogVsWorkers(ctx context.Context, window, bucket time.Duration) ([]BacklogPoint, error) {
	var rows []BacklogPoint
	err := c.conn.Select(ctx, &rows, `
		SELECT
			bucket,
			active_workers,
			sum(net_change) OVER (ORDER BY bucket) AS cumulative_backlog
		FROM (
			SELECT
				toStartOfInterval(occurred_at, INTERVAL ? SECOND) AS bucket,
				uniqIf(worker_id, event_type = 'claimed') AS active_workers,
				countIf(event_type = 'enqueued')
					- countIf(event_type IN ('completed', 'failed', 'reaped_failed')) AS net_change
			FROM job_events
			WHERE occurred_at >= now() - INTERVAL ? SECOND
			GROUP BY bucket
		)
		ORDER BY bucket
	`, int64(bucket.Seconds()), int64(window.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("analytics: backlog vs workers: %w", err)
	}
	return rows, nil
}

// CompletionRate is one job type's recent completion (departure) count
// over some window — the raw input CompletionsPerSecond turns into a
// rate.
type CompletionRate struct {
	JobType     string `ch:"job_type"`
	Completions uint64 `ch:"completions"`
}

// RecentCompletionRates counts departures (completed, failed, or
// reaped_failed) by job type over the trailing window — the throughput
// figure EstimateDrainTime divides a live backlog by.
//
// Why ClickHouse: this is a rate computed from historical event
// volume, which only job_events has (see this file's package doc
// comment); Postgres's jobs table has no record of jobs that already
// left the queue to count in the first place.
//
// This rate is only as fresh as the shipper: an event isn't visible
// here until it's been shipped from the Postgres outbox to ClickHouse,
// so a very short window (seconds) right after a burst of completions
// can under-count until the next shipper tick catches up — the same
// lag jobqueue_shipper_lag_seconds (internal/metrics) surfaces on the
// dashboard. Windows of a minute or more make this negligible.
func (c *Client) RecentCompletionRates(ctx context.Context, window time.Duration) ([]CompletionRate, error) {
	var rows []CompletionRate
	err := c.conn.Select(ctx, &rows, `
		SELECT
			job_type,
			countIf(event_type IN ('completed', 'failed', 'reaped_failed')) AS completions
		FROM job_events
		WHERE occurred_at >= now() - INTERVAL ? SECOND
		GROUP BY job_type
	`, int64(window.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("analytics: recent completion rates: %w", err)
	}
	return rows, nil
}

// DrainEstimate projects how long a job type's current backlog will
// take to drain at its recent completion rate.
type DrainEstimate struct {
	JobType           string
	Backlog           int64
	CompletionsPerSec float64
	EstimatedSeconds  float64 // meaningless unless CanEstimate
	CanEstimate       bool    // false when CompletionsPerSec is 0 (queue idle or nothing has finished recently)
}

// EstimateDrainTime combines backlog (a live, exact count from
// Postgres — Store.CountByStatusAndType(ctx, "queued") — the only
// store that knows the current state) with completionsPerSec (a
// recent throughput figure derived from ClickHouse's event history —
// the only store with enough history to compute a rate) into a
// drain-time projection. Neither store can answer this alone: Postgres
// has no historical throughput to compute a rate from, and ClickHouse
// has no live "right now" queue depth (job_events only records that a
// job WAS enqueued/claimed/completed, not how many are queued at this
// instant — that's derived state Postgres already tracks directly).
//
// A pure function over already-fetched maps, not a query itself, so
// it's testable without either database — see reports_test.go.
func EstimateDrainTime(backlog map[string]int64, completionsPerSec map[string]float64) []DrainEstimate {
	jobTypes := make(map[string]struct{}, len(backlog)+len(completionsPerSec))
	for t := range backlog {
		jobTypes[t] = struct{}{}
	}
	for t := range completionsPerSec {
		jobTypes[t] = struct{}{}
	}

	estimates := make([]DrainEstimate, 0, len(jobTypes))
	for t := range jobTypes {
		rate := completionsPerSec[t]
		est := DrainEstimate{
			JobType:           t,
			Backlog:           backlog[t],
			CompletionsPerSec: rate,
		}
		if rate > 0 {
			est.EstimatedSeconds = float64(backlog[t]) / rate
			est.CanEstimate = true
		}
		estimates = append(estimates, est)
	}
	sort.Slice(estimates, func(i, j int) bool { return estimates[i].JobType < estimates[j].JobType })
	return estimates
}
