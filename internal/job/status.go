// Package job defines the domain types shared across the storage layer,
// worker loop, and CLI. It intentionally has no database dependency.
package job

// Status is the lifecycle state of a job row. Defined here as a type
// rather than a bare string so the compiler catches typos and invalid
// values at call sites instead of at runtime.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)
