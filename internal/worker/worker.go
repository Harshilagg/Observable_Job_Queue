// Package worker implements the claim/execute/complete loop that turns
// rows in the jobs table into actual work.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
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
	maxRetryDelay   time.Duration
	logger          *slog.Logger
}

func New(st *store.Store, handler Handler, id string, leaseDuration, pollInterval, maxPollInterval, retryBaseDelay, maxRetryDelay time.Duration, logger *slog.Logger) *Worker {
	return &Worker{
		store:           st,
		handler:         handler,
		id:              id,
		leaseDuration:   leaseDuration,
		pollInterval:    pollInterval,
		maxPollInterval: maxPollInterval,
		retryBaseDelay:  retryBaseDelay,
		maxRetryDelay:   maxRetryDelay,
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

// calculateRetryDelay computes a retry delay using exponential backoff
// with full jitter: the delay doubles with each attempt (capped at
// maxRetryDelay), and the actual value used is a uniformly random pick
// between 0 and that computed cap — not the cap itself.
//
// The jitter matters for a different reason than the backoff does.
// Backoff spaces out *one job's own* retries over time. Jitter exists
// to break *correlation across many jobs*: if a downstream dependency
// (an API a handler calls, or Postgres itself under load) has a brief
// outage, many jobs can fail within the same few seconds. Without
// jitter, every one of them computes the exact same deterministic
// delay from the same attempt count, so they all become eligible to
// retry at (almost) the same instant — a thundering herd that hits the
// just-recovering dependency with a synchronized burst instead of a
// steady trickle, potentially knocking it back over and repeating the
// same synchronized failure on the next round, indefinitely. Full
// jitter spreads that same batch of retries across the whole backoff
// window at random, turning one spike into something the recovering
// system can actually absorb — at the cost of no longer having a
// precise, predictable retry time for any single job, which is a trade
// worth making here.
func (w *Worker) calculateRetryDelay(j job.Job) time.Duration {
	delay := w.retryBaseDelay
	for i := 1; i < j.Attempts; i++ {
		delay *= 2
		if delay > w.maxRetryDelay || delay <= 0 {
			delay = w.maxRetryDelay
			break
		}
	}
	if delay <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(delay)))
}
