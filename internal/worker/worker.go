// Package worker implements the claim/execute/complete loop that turns
// rows in the jobs table into actual work.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
)

// Handler executes one job. Returning an error signals failure; the
// worker decides whether that means a retry or a terminal failure.
type Handler func(ctx context.Context, j job.Job) error

// Worker repeatedly claims and executes jobs until its context is
// cancelled.
type Worker struct {
	store           *store.Store
	handler         Handler
	id              string
	leaseDuration   time.Duration
	pollInterval    time.Duration
	maxPollInterval time.Duration
	retryBaseDelay  time.Duration
	logger          *slog.Logger
}

func New(st *store.Store, handler Handler, id string, leaseDuration, pollInterval, maxPollInterval, retryBaseDelay time.Duration, logger *slog.Logger) *Worker {
	return &Worker{
		store:           st,
		handler:         handler,
		id:              id,
		leaseDuration:   leaseDuration,
		pollInterval:    pollInterval,
		maxPollInterval: maxPollInterval,
		retryBaseDelay:  retryBaseDelay,
		logger:          logger,
	}
}

// Run loops until ctx is cancelled. On cancellation it stops claiming
// new work but does not abandon a job already in progress.
func (w *Worker) Run(ctx context.Context) error {
	interval := w.pollInterval
	for {
		if ctx.Err() != nil {
			return nil
		}
		jobs, err := w.store.Claim(ctx, w.id, w.leaseDuration, 1)
		if err != nil {
			w.logger.Error("claim failed", "error", err)
		}
		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(interval):
			}

			interval *= 2
			if interval > w.maxPollInterval {
				interval = w.maxPollInterval
			}
			continue
		}
		interval = w.pollInterval
		// Once a job is claimed, finishing it — running the handler AND
		// recording the outcome — must not be cut short by shutdown
		// cancellation. Only the next loop iteration's claim/wait should
		// see ctx cancelled.
		jobCtx := context.WithoutCancel(ctx)
		err = w.execute(jobCtx, jobs[0])
		if err == nil {
			err = w.store.Complete(jobCtx, jobs[0].ID)
			if err != nil {
				w.logger.Error("complete failed", "error", err)
			}
			continue
		}

		if jobs[0].Attempts >= jobs[0].MaxAttempts {
			failErr := w.store.Fail(jobCtx, jobs[0].ID, err.Error())
			if failErr != nil {
				w.logger.Error("fail failed", "error", failErr)
			}
		} else {
			delay := w.calculateRetryDelay(jobs[0])
			runAt := time.Now().Add(delay)
			err = w.store.Retry(jobCtx, jobs[0].ID, runAt, err.Error())
			if err != nil {
				w.logger.Error("retry failed", "error", err)
			}
		}
	}
}

// execute runs the handler for a single job, converting a panic into an
// error instead of letting it crash the worker.
func (w *Worker) execute(ctx context.Context, j job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return w.handler(ctx, j)
}

func (w *Worker) calculateRetryDelay(j job.Job) time.Duration {
	return w.retryBaseDelay * time.Duration(j.Attempts)
}
