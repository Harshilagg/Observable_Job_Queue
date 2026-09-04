package job

import "encoding/json"

// Job is the shape of a claimed row: exactly what a handler needs to
// execute it, and what the worker needs to decide retry vs terminal
// failure.
type Job struct {
	ID          int64
	Type        string
	Payload     json.RawMessage
	Attempts    int
	MaxAttempts int
}
