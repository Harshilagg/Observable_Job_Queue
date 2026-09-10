// Package handlers provides real job-type implementations and the
// registry that dispatches a claimed job to the right one by its Type.
package handlers

import (
	"context"
	"fmt"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
	"github.com/Harshilagg/Observable_Job_Queue/internal/worker"
)

// Registry maps a job type to the handler that executes it.
type Registry map[string]worker.Handler

// NewRegistry returns an empty registry. Register handlers onto it,
// then pass its Dispatch method to worker.New as the single Handler
// every Worker calls.
func NewRegistry() Registry {
	return make(Registry)
}

// Register adds a handler for jobType, overwriting any existing one
// for that type.
func (r Registry) Register(jobType string, h worker.Handler) {
	r[jobType] = h
}

// Dispatch is itself a worker.Handler: it looks up the job's type and
// runs the matching handler.
//
// Design decision: an unregistered type is treated as an ordinary,
// retryable failure — routed through the exact same Attempts/
// MaxAttempts path as any other handler error — not failed
// immediately.
//
// Why: this worker binary's registry is fixed at process startup, but
// in a rolling deploy a job can be claimed by an *older* worker
// instance than the one that enqueued it, before newer instances (with
// the type registered) are up. Failing such a job immediately and
// permanently would lose work that a worker mere seconds away from
// existing could have handled correctly. The alternative failure mode
// — a genuine typo in job.Type that will never resolve — is bounded
// instead of unlimited: it still burns at most MaxAttempts retries
// (with backoff) before landing in the terminal 'failed' state and,
// once the dead-letter path exists, is visible there like any other
// permanent failure. Paying a small bounded retry cost in the rare
// "real bug" case is a better trade than silently losing work in the
// common "deploy in progress" case.
func (r Registry) Dispatch(ctx context.Context, j job.Job) error {
	h, ok := r[j.Type]
	if !ok {
		return fmt.Errorf("no handler registered for job type %q", j.Type)
	}
	return h(ctx, j)
}
