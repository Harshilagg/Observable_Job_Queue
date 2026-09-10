// Command jobqueue is the entrypoint for submitting jobs, querying
// their status, and running the workers/API that back the queue.
// Usage:
//
//	jobqueue serve                          # start the gRPC API
//	jobqueue work -workers=<n>               # start N workers
//	jobqueue enqueue -type=<t> -payload=<j>  # submit a job (via gRPC)
//	jobqueue status -id=<id>                 # one status snapshot (via gRPC)
//	jobqueue watch -id=<id>                  # stream status until terminal (via gRPC)
//	jobqueue dead-letters -limit=<n>          # inspect terminally-failed jobs (direct DB read)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"golang.org/x/sync/errgroup"

	"github.com/Harshilagg/Observable_Job_Queue/internal/analytics"
	"github.com/Harshilagg/Observable_Job_Queue/internal/config"
	"github.com/Harshilagg/Observable_Job_Queue/internal/grpcserver"
	"github.com/Harshilagg/Observable_Job_Queue/internal/handlers"
	"github.com/Harshilagg/Observable_Job_Queue/internal/logging"
	"github.com/Harshilagg/Observable_Job_Queue/internal/pb/jobqueuepb"
	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
	"github.com/Harshilagg/Observable_Job_Queue/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: jobqueue <serve|work|enqueue|status|watch|dead-letters> [flags]")
		os.Exit(1)
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	logger := logging.New(os.Getenv("LOG_LEVEL"))

	switch os.Args[1] {
	// enqueue, status, and watch are pure gRPC clients — no direct
	// database access. They talk to a running `jobqueue serve` process
	// exactly the way any other client of this API would.
	case "enqueue":
		runEnqueue(cfg, os.Args[2:])
	case "status":
		runStatus(cfg, os.Args[2:])
	case "watch":
		runWatch(cfg, os.Args[2:])
	// serve and work are the two processes with direct database access.
	case "serve":
		runServe(cfg, logger)
	case "work":
		runWork(cfg, logger, os.Args[2:])
	// dead-letters is a direct, read-only DB query — an operational/
	// debugging surface, not (yet) part of the client-facing gRPC API.
	case "dead-letters":
		runDeadLetters(cfg, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(1)
	}
}

// dialClient connects to the gRPC API and returns a client plus a
// closer the caller must run when done.
func dialClient(addr string) (jobqueuepb.JobQueueClient, func(), error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dialing %s: %w", addr, err)
	}
	return jobqueuepb.NewJobQueueClient(conn), func() { conn.Close() }, nil
}

func runEnqueue(cfg config.Config, args []string) {
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

	client, closeConn, err := dialClient(cfg.GRPCAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "enqueue:", err)
		os.Exit(1)
	}
	defer closeConn()

	resp, err := client.Submit(context.Background(), &jobqueuepb.SubmitRequest{
		Type:    *jobType,
		Payload: []byte(*payload),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "enqueue failed:", err)
		os.Exit(1)
	}
	fmt.Printf("enqueued job %d (type=%s)\n", resp.GetId(), *jobType)
}

func runStatus(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	id := fs.Int64("id", 0, "job id (required)")
	fs.Parse(args)

	if *id == 0 {
		fmt.Fprintln(os.Stderr, "status: -id is required")
		os.Exit(1)
	}

	client, closeConn, err := dialClient(cfg.GRPCAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "status:", err)
		os.Exit(1)
	}
	defer closeConn()

	resp, err := client.GetStatus(context.Background(), &jobqueuepb.GetStatusRequest{Id: *id})
	if err != nil {
		fmt.Fprintln(os.Stderr, "status failed:", err)
		os.Exit(1)
	}
	printStatusUpdate(resp.GetStatus())
}

func runWatch(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	id := fs.Int64("id", 0, "job id (required)")
	fs.Parse(args)

	if *id == 0 {
		fmt.Fprintln(os.Stderr, "watch: -id is required")
		os.Exit(1)
	}

	client, closeConn, err := dialClient(cfg.GRPCAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "watch:", err)
		os.Exit(1)
	}
	defer closeConn()

	stream, err := client.WatchStatus(context.Background(), &jobqueuepb.WatchStatusRequest{Id: *id})
	if err != nil {
		fmt.Fprintln(os.Stderr, "watch failed:", err)
		os.Exit(1)
	}
	for {
		update, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "watch stream error:", err)
			os.Exit(1)
		}
		printStatusUpdate(update)
	}
}

func printStatusUpdate(u *jobqueuepb.StatusUpdate) {
	fmt.Printf("job %d: status=%s attempts=%d/%d last_error=%q\n",
		u.GetId(), u.GetStatus(), u.GetAttempts(), u.GetMaxAttempts(), u.GetLastError())
}

// runDeadLetters prints the most recent terminally-failed jobs.
func runDeadLetters(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("dead-letters", flag.ExitOnError)
	limit := fs.Int("limit", 20, "max number of dead letters to show, most recent first")
	fs.Parse(args)

	ctx := context.Background()
	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dead-letters:", err)
		os.Exit(1)
	}
	defer st.Close()

	letters, err := st.ListDeadLetters(ctx, *limit)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dead-letters failed:", err)
		os.Exit(1)
	}
	if len(letters) == 0 {
		fmt.Println("no dead letters")
		return
	}
	for _, dl := range letters {
		fmt.Printf("job %d (type=%s, attempts=%d, failed_at=%s): %s\n",
			dl.JobID, dl.Type, dl.Attempts, dl.FailedAt.Format(time.RFC3339), dl.LastError)
	}
}

// runServe starts the gRPC API on cfg.GRPCAddr and blocks until
// SIGINT/SIGTERM, at which point it stops accepting new RPCs and lets
// in-flight ones finish before exiting.
func runServe(cfg config.Config, logger *slog.Logger) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to listen", "addr", cfg.GRPCAddr, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	jobqueuepb.RegisterJobQueueServer(grpcServer, grpcserver.New(st))

	go func() {
		<-ctx.Done()
		logger.Info("shutting down gRPC server")
		grpcServer.GracefulStop()
	}()

	logger.Info("gRPC server listening", "addr", cfg.GRPCAddr)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("gRPC server exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}

// runWork starts -workers concurrent workers plus a reaper, and blocks
// until SIGINT/SIGTERM triggers a graceful shutdown.
func runWork(cfg config.Config, logger *slog.Logger, args []string) {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	workerCount := fs.Int("workers", cfg.WorkerCount, "number of concurrent workers")
	fs.Parse(args)

	if *workerCount <= 0 {
		fmt.Fprintf(os.Stderr, "work: -workers must be positive, got %d\n", *workerCount)
		os.Exit(1)
	}

	// Cancelled the moment SIGINT/SIGTERM arrives — this is the signal
	// each worker's Run loop watches to stop claiming new work.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	registry := newHandlerRegistry(cfg, logger)

	g, gctx := errgroup.WithContext(ctx)

	for i := 0; i < *workerCount; i++ {
		id := fmt.Sprintf("worker-%d", i)
		w := worker.New(st, registry.Dispatch, id, cfg.LeaseDuration, cfg.PollInterval, cfg.MaxPollInterval, cfg.RetryBaseDelay, cfg.MaxRetryDelay, logger)
		g.Go(func() error {
			return w.Run(gctx)
		})
	}

	g.Go(func() error {
		runReaper(gctx, st, cfg.ReapInterval, logger)
		return nil
	})

	// ClickHouse being unreachable at startup must not stop job
	// processing — that would defeat the entire point of shipping
	// events through an outbox instead of writing them synchronously.
	// If this fails, events simply accumulate unshipped in Postgres
	// until a future `work` process finds ClickHouse reachable.
	chClient, err := analytics.NewClient(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUser, cfg.ClickHousePassword)
	if err != nil {
		logger.Error("clickhouse unreachable, running without event shipping", "error", err)
	} else {
		defer chClient.Close()
		g.Go(func() error {
			analytics.RunShipper(gctx, st, chClient, cfg.ShipInterval, cfg.ShipBatchSize, logger)
			return nil
		})
	}

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
			reclaimed, deadLettered, err := st.ReapExpiredLeases(ctx)
			if err != nil {
				logger.Error("reap failed", "error", err)
				continue
			}
			if reclaimed > 0 {
				logger.Info("reaped expired leases", "reclaimed", reclaimed, "dead_lettered", deadLettered)
			}
		}
	}
}

// newHandlerRegistry builds and populates the job-type -> Handler
// registry. Add a new job type by registering it here.
func newHandlerRegistry(cfg config.Config, logger *slog.Logger) handlers.Registry {
	r := handlers.NewRegistry()
	r.Register("http_check", handlers.HTTPCheck)
	r.Register("write_file", handlers.NewWriteFileHandler(cfg.WriteFileDir))
	r.Register("sum_numbers", handlers.NewSumNumbersHandler(logger))
	return r
}
