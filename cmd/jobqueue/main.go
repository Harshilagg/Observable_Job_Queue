// Command jobqueue is the entrypoint for both enqueueing jobs and
// running workers. Usage:
//
//	jobqueue enqueue -type=<type> -payload=<json>
//	jobqueue work -workers=<n>
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Harshilagg/Observable_Job_Queue/internal/config"
	"github.com/Harshilagg/Observable_Job_Queue/internal/job"
	"github.com/Harshilagg/Observable_Job_Queue/internal/logging"
	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
	"github.com/Harshilagg/Observable_Job_Queue/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: jobqueue <enqueue|work> [flags]")
		os.Exit(1)
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	logger := logging.New(os.Getenv("LOG_LEVEL"))

	ctx := context.Background()
	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Ping(ctx); err != nil {
		logger.Error("database ping failed", "error", err)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "enqueue":
		runEnqueue(ctx, st, os.Args[2:])
	case "work":
		runWork(ctx, cfg, st, logger, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(1)
	}
}

// runEnqueue inserts a single job from CLI flags and prints its id.
func runEnqueue(ctx context.Context, st *store.Store, args []string) {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	jobType := fs.String("type", "", "job type (required)")
	payload := fs.String("payload", "{}", "JSON payload")
	fs.Parse(args)

	if *jobType == "" {
		fmt.Fprintln(os.Stderr, "enqueue: -type is required")
		os.Exit(1)
	}
	if !json.Valid([]byte(*payload)) {
		fmt.Fprintln(os.Stderr, "enqueue: -payload is not valid JSON")
		os.Exit(1)
	}

	id, err := st.Enqueue(ctx, *jobType, json.RawMessage(*payload))
	if err != nil {
		fmt.Fprintln(os.Stderr, "enqueue failed:", err)
		os.Exit(1)
	}
	fmt.Printf("enqueued job %d (type=%s)\n", id, *jobType)
}

// runWork starts -workers concurrent workers plus a reaper, and blocks
// until SIGINT/SIGTERM triggers a graceful shutdown.
func runWork(parentCtx context.Context, cfg config.Config, st *store.Store, logger *slog.Logger, args []string) {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	workerCount := fs.Int("workers", cfg.WorkerCount, "number of concurrent workers")
	fs.Parse(args)

	// Cancelled the moment SIGINT/SIGTERM arrives — this is the signal
	// each worker's Run loop watches to stop claiming new work.
	ctx, stop := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	handler := demoHandler(logger)

	g, gctx := errgroup.WithContext(ctx)

	for i := 0; i < *workerCount; i++ {
		id := fmt.Sprintf("worker-%d", i)
		w := worker.New(st, handler, id, cfg.LeaseDuration, cfg.PollInterval, cfg.MaxPollInterval, cfg.RetryBaseDelay, logger)
		g.Go(func() error {
			return w.Run(gctx)
		})
	}

	g.Go(func() error {
		runReaper(gctx, st, cfg.ReapInterval, logger)
		return nil
	})

	logger.Info("workers started", "count", *workerCount)
	if err := g.Wait(); err != nil {
		logger.Error("worker group exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}

// runReaper periodically reclaims jobs whose lease expired without the
// worker holding them reporting back — the recovery path for a crashed
// or killed worker. It exits when ctx is cancelled.
func runReaper(ctx context.Context, st *store.Store, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.ReapExpiredLeases(ctx)
			if err != nil {
				logger.Error("reap failed", "error", err)
				continue
			}
			if n > 0 {
				logger.Info("reaped expired leases", "count", n)
			}
		}
	}
}

// demoHandler is a placeholder job handler for this stage: it logs the
// job and simulates a small amount of work. Real handlers (dispatched
// by job.Type) are outside this session's scope.
func demoHandler(logger *slog.Logger) worker.Handler {
	return func(ctx context.Context, j job.Job) error {
		logger.Info("processing job", "id", j.ID, "type", j.Type, "payload", string(j.Payload))
		time.Sleep(200 * time.Millisecond)
		return nil
	}
}
