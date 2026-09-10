package job

import (
	"encoding/json"
	"time"
)

// Job is the shape of a claimed row: exactly what a handler needs to
// execute it, and what the worker needs to decide retry vs terminal
// failure.
type Job struct {
	ID          int64
	Type        string
	Payload     json.RawMessage
	Attempts    int
	MaxAttempts int
	// TraceContext is the W3C traceparent string captured when the job
	// was submitted (empty for jobs submitted before this existed). The
	// worker extracts it to continue the same trace through claim,
	// execute, and complete instead of starting a disconnected one.
	TraceContext string
}

// Snapshot is a point-in-time view of a job's state, for status
// queries. Deliberately a separate type from Job: a Snapshot is a read
// of any job in any state, not a row a worker has claimed and is about
// to execute.
type Snapshot struct {
	ID          int64
	Status      Status
	Attempts    int
	MaxAttempts int
	LastError   string
}

// DeadLetter is a self-contained record of a job that reached terminal
// failure — a copy of what it was, not just a pointer back to a row
// that might later be purged.
type DeadLetter struct {
	ID        int64
	JobID     int64
	Type      string
	Payload   json.RawMessage
	Attempts  int
	LastError string
	FailedAt  time.Time
}
