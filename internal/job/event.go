package job

import "time"

// EventType identifies a point in a job's lifecycle, for the analytics
// outbox (job_events). Kept distinct from Status: a job has one
// current Status, but accumulates many Events over its life — and some
// events distinguish causes a Status alone can't (e.g. a worker's
// handler returning an error vs. the reaper reclaiming a crashed
// worker's lease both lead to a retry, but they're different events).
type EventType string

const (
	EventEnqueued     EventType = "enqueued"
	EventClaimed      EventType = "claimed"
	EventCompleted    EventType = "completed"
	EventRetried      EventType = "retried"
	EventFailed       EventType = "failed"
	EventReapedRetry  EventType = "reaped_retry"
	EventReapedFailed EventType = "reaped_failed"
)

// Event is one row of the job_events outbox — an immutable record of
// something that happened to a job, destined for ClickHouse.
type Event struct {
	ID           int64
	JobID        int64
	JobType      string
	EventType    EventType
	Attempts     int
	WorkerID     string
	ErrorMessage string
	OccurredAt   time.Time
}
